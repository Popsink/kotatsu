//! Building a per-`(topic, partition)` view over prefix-coalesced segments (#82).
//!
//! Records for a segment-backed topic live in shared, immutable per-prefix
//! segment objects (`prefixes/{prefix}/segments/{seq:020}.seg`), each
//! multiplexing many `(topic, partition)` sub-streams. To read one topition we
//! list the prefix's segments, read each footer (a single over-reading ranged
//! GET, cached — footers are immutable), and collect the entries for our
//! topition.
//!
//! Overlaps are resolved by the wire contract's tie-break — on the rare overlap
//! left by a compaction or a writer failover, the **higher `writer_epoch`
//! wins** — turning the collected entries into a set of non-overlapping
//! [`OwnerPiece`]s that partition the topition's segment-backed offset space.

use bytes::Bytes;
use futures::StreamExt;
use object_store::{GetOptions, GetRange};

use super::{
    keys::Keys,
    segment::{decode_segment_footer, FooterOutcome, SegmentFooter, SEGMENT_FOOTER_OVER_READ},
    StorageError, StorageSource,
};

/// A contiguous run of absolute offsets `[lo, hi)` for a topition, owned by one
/// segment region (after overlap resolution). Reading it means a ranged GET of
/// `[byte_start, byte_start + byte_len)` from the segment, decoding the region
/// as a batch concatenation running from `base_offset`, and keeping the records
/// whose offset falls in `[lo, hi)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OwnerPiece {
    pub lo: i64,
    pub hi: i64,
    pub prefix: String,
    pub seq: u64,
    /// The owning segment region's base offset (records decode from here).
    pub base_offset: i64,
    pub byte_start: u64,
    pub byte_len: u64,
    pub max_timestamp: i64,
}

/// The resolved, non-overlapping owner pieces of a topition's segment-backed
/// offset space, sorted ascending by `lo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct SegView {
    pub pieces: Vec<OwnerPiece>,
}

impl SegView {
    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }

    /// Earliest segment-backed offset (the seam `C` for a hybrid topic).
    pub fn low(&self) -> Option<i64> {
        self.pieces.first().map(|p| p.lo)
    }

    /// Exclusive high watermark of the segment-backed region.
    pub fn high(&self) -> Option<i64> {
        self.pieces.iter().map(|p| p.hi).max()
    }
}

/// One `(topic, partition)` entry located in a specific segment, with the
/// segment's priority (`writer_epoch`, then sequence) for overlap resolution.
struct Placed {
    seq: u64,
    epoch: i64,
    prefix: String,
    base_offset: i64,
    end_offset: i64,
    byte_start: u64,
    byte_len: u64,
    max_timestamp: i64,
}

impl StorageSource {
    /// Reads and caches a segment's footer. One over-reading ranged GET of the
    /// tail (`SEGMENT_FOOTER_OVER_READ`) covers trailer+footer for almost every
    /// segment; a larger footer falls back to a second exact suffix GET.
    ///
    /// - `Ok(Some(footer))` — a multi-topic segment.
    /// - `Ok(None)` — a legacy v0 object (no trailer) **or** a segment that
    ///   vanished (compaction deleted it mid-read, `NotFound`): the caller treats
    ///   both as "nothing to read here".
    async fn segment_footer(
        &self,
        prefix: &str,
        seq: u64,
    ) -> Result<Option<SegmentFooter>, StorageError> {
        let path = self.keys().segment(prefix, seq);
        let key = path.as_ref().to_string();

        if let Ok(cache) = self.segment_footers.lock() {
            if let Some(footer) = cache.get(&key) {
                return Ok(Some(footer.clone()));
            }
        }

        let tail = match self.get_suffix(&path, SEGMENT_FOOTER_OVER_READ).await? {
            Some(bytes) => bytes,
            None => return Ok(None), // compaction deleted the segment
        };

        let footer = match decode_segment_footer(&tail)? {
            FooterOutcome::Footer(footer) => footer,
            FooterOutcome::Legacy => return Ok(None),
            FooterOutcome::NeedBytes(n) => {
                // Footer larger than the over-read: fetch the exact suffix.
                match self.get_suffix(&path, n as u64).await? {
                    Some(bytes) => match decode_segment_footer(&bytes)? {
                        FooterOutcome::Footer(footer) => footer,
                        _ => return Ok(None),
                    },
                    None => return Ok(None),
                }
            }
        };

        if let Ok(mut cache) = self.segment_footers.lock() {
            cache.insert(key, footer.clone());
        }
        Ok(Some(footer))
    }

    /// GETs the last `n` bytes of an object; `Ok(None)` if it does not exist.
    async fn get_suffix(
        &self,
        path: &object_store::path::Path,
        n: u64,
    ) -> Result<Option<Bytes>, StorageError> {
        let opts = GetOptions {
            range: Some(GetRange::Suffix(n)),
            ..Default::default()
        };
        match self.store().get_opts(path, opts).await {
            Ok(result) => result
                .bytes()
                .await
                .map(Some)
                .map_err(|e| StorageError::from_object(e, path)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StorageError::from_object(e, path)),
        }
    }

    /// Builds the segment view for a topition by listing its routed prefix's
    /// segments and resolving overlaps. Empty when the topition has no live
    /// segment — an empty log.
    ///
    /// The prefix comes from the **pin** (#92), not from a derivation over the
    /// topic name: a compacted topic is routed under its own name, and looking for
    /// its segments under `org.env.conn` finds none, which renders as an empty
    /// topic rather than as an error.
    pub(super) async fn build_segment_view(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<SegView, StorageError> {
        let prefix = self.routed_prefix_of(topic).await?;
        let list_prefix = self.keys().segment_prefix(&prefix);

        let mut placed = Vec::new();
        let mut stream = self.store().list(Some(&list_prefix));
        while let Some(meta) = stream.next().await {
            let location = meta?.location;
            let Some(seq) = Keys::seq_from_segment(&location) else {
                continue;
            };
            let Some(footer) = self.segment_footer(&prefix, seq).await? else {
                continue;
            };
            if let Some(entry) = footer.get(topic, partition) {
                placed.push(Placed {
                    seq,
                    epoch: footer.writer_epoch,
                    prefix: prefix.clone(),
                    base_offset: entry.base_offset,
                    end_offset: entry.end_offset(),
                    byte_start: entry.byte_start,
                    byte_len: entry.byte_len,
                    max_timestamp: entry.max_timestamp,
                });
            }
        }

        Ok(SegView {
            pieces: resolve_owners(placed),
        })
    }

    /// Reads a segment region's raw bytes (`[byte_start, byte_start + byte_len)`).
    /// `Ok(None)` if the segment vanished (compaction) between listing and read.
    pub(super) async fn read_segment_region(
        &self,
        piece: &OwnerPiece,
    ) -> Result<Option<Bytes>, StorageError> {
        let path = self.keys().segment(&piece.prefix, piece.seq);
        let range = piece.byte_start..piece.byte_start + piece.byte_len;
        match self.store().get_range(&path, range).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StorageError::from_object(e, &path)),
        }
    }
}

/// Resolves possibly-overlapping placed entries into non-overlapping owner
/// pieces. Priority is `writer_epoch` descending, then `seq` descending: a
/// higher-epoch (or, at equal epoch, later/compacted) segment claims its offset
/// range first, and lower-priority entries own only the offsets left unclaimed.
/// Pieces are returned sorted ascending by `lo`.
fn resolve_owners(mut placed: Vec<Placed>) -> Vec<OwnerPiece> {
    // Highest priority first.
    placed.sort_by(|a, b| b.epoch.cmp(&a.epoch).then(b.seq.cmp(&a.seq)));

    // Union of already-claimed offset ranges, kept sorted and merged.
    let mut claimed: Vec<(i64, i64)> = Vec::new();
    let mut pieces = Vec::new();

    for p in &placed {
        if p.end_offset <= p.base_offset {
            continue; // empty region
        }
        for (lo, hi) in subtract(&claimed, p.base_offset, p.end_offset) {
            pieces.push(OwnerPiece {
                lo,
                hi,
                prefix: p.prefix.clone(),
                seq: p.seq,
                base_offset: p.base_offset,
                byte_start: p.byte_start,
                byte_len: p.byte_len,
                max_timestamp: p.max_timestamp,
            });
        }
        insert_claim(&mut claimed, p.base_offset, p.end_offset);
    }

    pieces.sort_by_key(|p| p.lo);
    pieces
}

/// Parts of `[start, end)` not covered by the sorted-merged `claimed` ranges.
fn subtract(claimed: &[(i64, i64)], start: i64, end: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let mut cursor = start;
    for &(lo, hi) in claimed {
        if hi <= cursor {
            continue;
        }
        if lo >= end {
            break;
        }
        if lo > cursor {
            out.push((cursor, lo.min(end)));
        }
        cursor = cursor.max(hi);
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        out.push((cursor, end));
    }
    out
}

/// Adds `[start, end)` to `claimed`, keeping it sorted and merged.
fn insert_claim(claimed: &mut Vec<(i64, i64)>, start: i64, end: i64) {
    claimed.push((start, end));
    claimed.sort_by_key(|&(lo, _)| lo);
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(claimed.len());
    for &(lo, hi) in claimed.iter() {
        match merged.last_mut() {
            Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
            _ => merged.push((lo, hi)),
        }
    }
    *claimed = merged;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placed(seq: u64, epoch: i64, base: i64, count: i64) -> Placed {
        Placed {
            seq,
            epoch,
            prefix: "p".into(),
            base_offset: base,
            end_offset: base + count,
            byte_start: 0,
            byte_len: 10,
            max_timestamp: 0,
        }
    }

    fn ranges(pieces: &[OwnerPiece]) -> Vec<(i64, i64, u64)> {
        pieces.iter().map(|p| (p.lo, p.hi, p.seq)).collect()
    }

    #[test]
    fn contiguous_non_overlapping_segments_pass_through() {
        let pieces = resolve_owners(vec![
            placed(0, 0, 0, 10),
            placed(1, 0, 10, 5),
            placed(2, 0, 15, 20),
        ]);
        assert_eq!(ranges(&pieces), [(0, 10, 0), (10, 15, 1), (15, 35, 2)]);
    }

    #[test]
    fn compaction_merged_segment_supersedes_originals() {
        // A merged segment [0,100) at a fresh higher seq (same epoch) covers the
        // originals entirely → the originals own nothing.
        let pieces = resolve_owners(vec![
            placed(0, 0, 0, 25),
            placed(1, 0, 25, 25),
            placed(5, 0, 0, 100), // merged, higher seq
        ]);
        assert_eq!(ranges(&pieces), [(0, 100, 5)]);
    }

    #[test]
    fn failover_overlap_higher_epoch_wins() {
        // Seg A [0,50) epoch 1, seg B [40,90) epoch 2 re-wrote 40..50 under a
        // higher epoch → B owns the overlap, A keeps only [0,40).
        let pieces = resolve_owners(vec![placed(0, 1, 0, 50), placed(1, 2, 40, 50)]);
        assert_eq!(ranges(&pieces), [(0, 40, 0), (40, 90, 1)]);
    }

    #[test]
    fn empty_when_no_segments() {
        assert!(resolve_owners(vec![]).is_empty());
    }

    #[test]
    fn subtract_splits_around_a_claimed_hole() {
        // [0,100) minus a claimed [40,60) → [0,40) and [60,100).
        assert_eq!(subtract(&[(40, 60)], 0, 100), [(0, 40), (60, 100)]);
        // Fully covered → nothing.
        assert_eq!(subtract(&[(0, 100)], 10, 90), Vec::<(i64, i64)>::new());
        // No overlap → whole range.
        assert_eq!(subtract(&[(200, 300)], 0, 100), [(0, 100)]);
    }
}
