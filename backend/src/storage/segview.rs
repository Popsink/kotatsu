//! Building a per-`(topic, partition)` view over prefix-coalesced segments (#82).
//!
//! Records for a segment-backed topic live in shared, immutable per-prefix
//! segment objects (`prefixes/{prefix}/segments/{seq:020}.seg`), each
//! multiplexing many sub-streams. To read one topition we list the prefix's
//! segments, read each footer (a single over-reading ranged GET, cached —
//! footers are immutable), and collect the entries for our topition. Which
//! entries those are is decided by the topic's routed identity, not its name
//! alone: a v4 segment keys an id-keyed sub-stream by uuid (#118).
//!
//! Overlaps are resolved by the wire contract's tie-break — on the rare overlap
//! left by a compaction or a writer failover, the **higher `writer_epoch`
//! wins** — turning the collected entries into a set of non-overlapping
//! [`OwnerPiece`]s that partition the topition's segment-backed offset space.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange};

use super::{
    keys::Keys,
    segment::{
        decode_segment_footer, FooterOutcome, SegmentFooter, SubstreamId, SEGMENT_FOOTER_OVER_READ,
    },
    StorageError, StorageSource,
};

/// A prefix's live multi-topic segments, `(seq, footer)`, in listing order.
/// Footers are shared with the cache (`Arc`), not copied out of it: a large
/// prefix holds thousands of them, each listing every sub-stream it carries,
/// and copying them all on every read dominated the pod's memory.
pub(super) type PrefixFooters = Vec<(u64, Arc<SegmentFooter>)>;

/// How long a prefix's segment listing is served as is (Popsink/data-plane#6096).
///
/// It bounds how stale a served `high` can be: a segment flushed after the
/// listing is invisible until it expires. Past it, the listing is brought up
/// to date incrementally — one LIST page from the highest sequence known —
/// so a short TTL costs little.
pub(super) const LISTING_TTL: Duration = Duration::from_secs(10);

/// How often a prefix is listed again **in full** (Popsink/data-plane#6096).
///
/// An incremental refresh only sees segments created above the highest known
/// sequence — the only place Kodansu creates them, compaction included: a
/// segment is created at `max(tail + 1, seq floor)` and a sequence is never
/// reused. What it cannot see is a deletion (retention, or the segments a
/// compaction replaced), so a deleted segment stays listed — `low` and the
/// message count lag — until the next full listing. That listing runs in the
/// background: the request that finds it due is served incrementally.
pub(super) const FULL_LISTING_INTERVAL: Duration = Duration::from_secs(300);

/// A prefix not read for this long is forgotten, and listed in full on its
/// next read. Kept well above [`LISTING_TTL`]: the listing is what makes the
/// next refresh incremental.
const LISTING_IDLE: Duration = Duration::from_secs(900);

/// Segment footers read side by side on a cold pass over a prefix.
///
/// A cold prefix means one footer GET per segment, and a busy connector's
/// prefix holds thousands: at [`super::FANOUT`] (8) at a time the first read of the
/// largest prefixes took ~27 s, round trip after round trip. A footer read is
/// one bounded ranged GET (`SEGMENT_FOOTER_OVER_READ`, 64 KiB), so 64 in
/// flight is at most 4 MiB per prefix being read.
const FOOTER_FANOUT: usize = 64;

/// Keys one LIST request returns (S3 and GCS both page by 1000).
const LIST_PAGE: usize = 1000;

/// Ranges a large prefix is listed in, side by side, on a full listing.
pub(super) const LIST_RANGES: usize = 16;

/// A prefix's segment sequences, as last listed.
pub(super) struct Listing {
    /// Last brought up to date, fully or incrementally.
    at: Instant,
    /// When the last full listing started — what it saw is at least that fresh.
    full_at: Instant,
    /// Ascending.
    seqs: Arc<Vec<u64>>,
    /// A background full listing is running.
    full_in_flight: bool,
}

/// One slot per routed prefix. The outer lock only finds the slot; the slot's
/// async lock is held across a refresh, so callers arriving meanwhile wait for
/// it instead of starting their own (single-flight). A refresh runs detached
/// from the caller, so a caller that gives up does not cancel it.
pub(super) type Slot = Arc<futures::lock::Mutex<Option<Listing>>>;
pub(super) type SegmentListings = Arc<Mutex<HashMap<String, Slot>>>;

/// One sub-stream's slice of a prefix, per partition: its resolved segment view
/// and the bytes it occupies. Partitions with no slice are absent from both.
#[derive(Default)]
pub(super) struct TopicSegments {
    pub views: BTreeMap<i32, SegView>,
    pub bytes: BTreeMap<i32, i64>,
}

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
    /// The log start: the earliest offset any live segment holds. `None` when the
    /// topition has no segment at all — an empty log.
    pub fn low(&self) -> Option<i64> {
        self.pieces.first().map(|p| p.lo)
    }

    /// Exclusive end of the segment region — the log end, unless a segment expiry
    /// raised the persisted `watermark.high` above it.
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
    pub(super) async fn segment_footer(
        &self,
        prefix: &str,
        seq: u64,
    ) -> Result<Option<Arc<SegmentFooter>>, StorageError> {
        let path = self.keys().segment(prefix, seq);
        let key = path.as_ref().to_string();

        if let Some(footer) = self.segment_footers.get(&key) {
            return Ok(Some(footer));
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

        let footer = Arc::new(footer);
        self.segment_footers.insert(key, footer.clone());
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
    /// Both the prefix and the sub-stream key come from the **pin** (#92, #118),
    /// not from a derivation over the topic name: a compacted topic is routed
    /// under its own name, and an id-keyed topic's slices are filed under a uuid.
    /// Getting either wrong finds no entry, which renders as an empty topic
    /// rather than as an error.
    pub(super) async fn build_segment_view(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<SegView, StorageError> {
        Ok(self
            .topic_segments(topic)
            .await?
            .views
            .remove(&partition)
            .unwrap_or_default())
    }

    /// Every partition's segment view and on-disk size for a topic, from one
    /// listing of its routed prefix — rather than one listing per partition plus
    /// one more for the size.
    pub(super) async fn topic_segments(&self, topic: &str) -> Result<TopicSegments, StorageError> {
        let route = self.route_of(topic).await?;
        let footers = self.prefix_footers(&route.prefix).await?;
        Ok(substream_segments(
            &footers,
            &route.prefix,
            route.substream(topic),
        ))
    }

    /// Lists a prefix's segments once and reads their footers (cached,
    /// immutable), [`FOOTER_FANOUT`] at a time on a cold pass. A topic's count and size
    /// are folded from this one listing, for every partition — and, in a topic
    /// listing, for every row under the same prefix.
    ///
    /// The listing itself is reused for [`LISTING_TTL`], then brought up to
    /// date incrementally (see [`FULL_LISTING_INTERVAL`]). A segment that a
    /// reused listing names but that has since been deleted, and whose footer
    /// was never read, reads as no footer: the listing is then dropped and
    /// taken again in full once.
    pub(super) async fn prefix_footers(&self, prefix: &str) -> Result<PrefixFooters, StorageError> {
        let (seqs, reused) = self.segment_seqs(prefix).await?;
        let footers = self.footers_of(prefix, &seqs).await?;
        if reused && footers.len() < seqs.len() {
            self.invalidate_listing(prefix);
            let (seqs, _) = self.segment_seqs(prefix).await?;
            return self.footers_of(prefix, &seqs).await;
        }
        Ok(footers)
    }

    async fn footers_of(&self, prefix: &str, seqs: &[u64]) -> Result<PrefixFooters, StorageError> {
        futures::stream::iter(seqs.iter().copied())
            .map(|seq| async move { Ok(self.segment_footer(prefix, seq).await?.map(|f| (seq, f))) })
            .buffered(FOOTER_FANOUT)
            .try_collect::<Vec<_>>()
            .await
            .map(|footers| footers.into_iter().flatten().collect())
    }

    /// The prefix's segment sequences, ascending, and whether part of them
    /// comes from a listing taken earlier rather than by this call.
    async fn segment_seqs(&self, prefix: &str) -> Result<(Arc<Vec<u64>>, bool), StorageError> {
        let slot = {
            let mut slots = self
                .segment_listings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Listings idle past LISTING_IDLE are dropped on the way. A slot
            // someone holds is kept.
            let idle = LISTING_IDLE.max(self.listing_ttl);
            slots.retain(|_, slot| {
                Arc::strong_count(slot) > 1
                    || slot
                        .try_lock()
                        .is_some_and(|l| l.as_ref().is_some_and(|l| l.at.elapsed() < idle))
            });
            slots.entry(prefix.to_string()).or_default().clone()
        };

        // The refresh runs in a detached task that owns the slot, not in the
        // request's future (Popsink/data-plane#6123). When the API timeout
        // drops the request, the LIST carries on and fills the cache for the
        // next caller; otherwise a prefix whose LIST outlasts the timeout
        // would be listed from zero on every request and never warm. Callers
        // arriving meanwhile still queue on the slot's lock and join it.
        let this = self.clone();
        let prefix = prefix.to_string();
        let listing = tokio::spawn(async move {
            let mut guard = slot.lock().await;
            let Some(listing) = guard.as_mut() else {
                let started = Instant::now();
                let seqs = Arc::new(this.list_seqs(&prefix, None).await?);
                *guard = Some(Listing {
                    at: Instant::now(),
                    full_at: started,
                    seqs: seqs.clone(),
                    full_in_flight: false,
                });
                return Ok((seqs, false));
            };
            if listing.at.elapsed() < this.listing_ttl {
                return Ok((listing.seqs.clone(), true));
            }

            // Only what was created since: one LIST page, however large the
            // prefix, where a full listing of a busy connector runs to many.
            let added = this
                .list_seqs(&prefix, listing.seqs.last().copied())
                .await?;
            if !added.is_empty() {
                let mut seqs = Vec::with_capacity(listing.seqs.len() + added.len());
                seqs.extend_from_slice(&listing.seqs);
                seqs.extend(added);
                listing.seqs = Arc::new(seqs);
            }
            listing.at = Instant::now();
            if listing.full_at.elapsed() >= this.full_listing_interval && !listing.full_in_flight {
                listing.full_in_flight = true;
                let known = listing.seqs.last().copied();
                this.spawn_full_listing(prefix.clone(), slot.clone(), known);
            }
            Ok((listing.seqs.clone(), true))
        });
        match listing.await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            // Only on runtime shutdown.
            Err(e) => Err(StorageError::Unreachable(format!(
                "segment listing aborted: {e}"
            ))),
        }
    }

    /// Lists a prefix again in full, without holding its slot — a busy
    /// prefix's full LIST can outlast a request, and readers keep being served
    /// from the incremental listing meanwhile — then swaps it in.
    ///
    /// `known` is the highest sequence listed when it started. Of the cached
    /// sequences, only those above it are kept on top of the full listing:
    /// they were created after it started (sequences only grow), so it may
    /// have missed them. Anything at or below `known` that the full listing
    /// did not see is gone.
    fn spawn_full_listing(&self, prefix: String, slot: Slot, known: Option<u64>) {
        let this = self.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let full = this.list_seqs(&prefix, None).await;
            let mut guard = slot.lock().await;
            let Some(listing) = guard.as_mut() else {
                return;
            };
            listing.full_in_flight = false;
            if let Ok(mut seqs) = full {
                let floor = seqs.last().copied().max(known);
                seqs.extend(
                    listing
                        .seqs
                        .iter()
                        .copied()
                        .filter(|s| floor.is_none_or(|floor| *s > floor)),
                );
                listing.seqs = Arc::new(seqs);
                listing.full_at = started;
            }
        });
    }

    /// The segment sequences under a prefix, ascending — all of them, or only
    /// those above `after` (an incremental refresh: one LIST page).
    pub(super) async fn list_seqs(
        &self,
        prefix: &str,
        after: Option<u64>,
    ) -> Result<Vec<u64>, StorageError> {
        let mut seqs = match after {
            Some(after) => self.list_range(prefix, Some(after), None).await?,
            None => self.list_all(prefix).await?,
        };
        seqs.retain(|seq| after.is_none_or(|after| *seq > after));
        seqs.sort_unstable();
        seqs.dedup();
        Ok(seqs)
    }

    /// Every segment sequence under a prefix.
    ///
    /// A plain LIST pages through the prefix one page after the other, so a
    /// busy connector's prefix — hundreds of thousands of segments — took
    /// ~30 s to list. Segment names are zero-padded sequences, so the store
    /// lists them in sequence order and a listing can start at any sequence:
    /// past the first page, the prefix is cut into [`LIST_RANGES`] ranges
    /// listed side by side. A prefix that fits in one page is listed in one
    /// LIST, as before.
    async fn list_all(&self, prefix: &str) -> Result<Vec<u64>, StorageError> {
        let mut head = Vec::new();
        let mut listed = self.store().list(Some(&self.keys().segment_prefix(prefix)));
        while let Some(meta) = listed.next().await {
            if let Some(seq) = Keys::seq_from_segment(&meta?.location) {
                head.push(seq);
            }
            if head.len() >= LIST_PAGE {
                break;
            }
        }
        // Stop before the stream asks for a second page.
        drop(listed);
        let Some(&last) = head.iter().max().filter(|_| head.len() >= LIST_PAGE) else {
            return Ok(head);
        };

        // An upper bound on the tail: probe `last + span`, doubling the span,
        // until nothing lies above. One request per probe, ~log2(segments).
        let mut span = LIST_PAGE as u64;
        while last.checked_add(span).is_some()
            && self.first_above(prefix, last + span).await?.is_some()
        {
            span = span.saturating_mul(2);
        }

        // (last, last + span] in equal ranges, the last one open-ended — so a
        // segment created meanwhile, or above a sequence-floor jump, is still
        // listed.
        let width = (span / LIST_RANGES as u64).max(1);
        let ranges = (0..LIST_RANGES as u64).map(|i| {
            let from = last.saturating_add(i * width);
            let until = (i + 1 < LIST_RANGES as u64).then(|| from.saturating_add(width + 1));
            (from, until)
        });
        let rest: Vec<Vec<u64>> = futures::stream::iter(ranges)
            .map(|(from, until)| self.list_range(prefix, Some(from), until))
            .buffer_unordered(LIST_RANGES)
            .try_collect()
            .await?;
        head.extend(rest.into_iter().flatten());
        Ok(head)
    }

    /// The sequences in `(after, until)` — from the start without `after`, to
    /// the end without `until`. Stops at `until` without reading past it,
    /// relying on the store listing in key order (S3, GCS).
    async fn list_range(
        &self,
        prefix: &str,
        after: Option<u64>,
        until: Option<u64>,
    ) -> Result<Vec<u64>, StorageError> {
        let dir = self.keys().segment_prefix(prefix);
        let mut listed = match after {
            Some(seq) => self
                .store()
                .list_with_offset(Some(&dir), &self.keys().segment(prefix, seq)),
            None => self.store().list(Some(&dir)),
        };
        let mut seqs = Vec::new();
        while let Some(meta) = listed.next().await {
            let Some(seq) = Keys::seq_from_segment(&meta?.location) else {
                continue;
            };
            if until.is_some_and(|until| seq >= until) {
                break;
            }
            seqs.push(seq);
        }
        Ok(seqs)
    }

    /// The first sequence above `after`, if any — one LIST request.
    async fn first_above(&self, prefix: &str, after: u64) -> Result<Option<u64>, StorageError> {
        let mut listed = self.store().list_with_offset(
            Some(&self.keys().segment_prefix(prefix)),
            &self.keys().segment(prefix, after),
        );
        while let Some(meta) = listed.next().await {
            if let Some(seq) = Keys::seq_from_segment(&meta?.location) {
                return Ok(Some(seq));
            }
        }
        Ok(None)
    }

    /// Forgets a prefix's listing, so the next read lists it again — after a
    /// segment it named turned out to be gone.
    pub(super) fn invalidate_listing(&self, prefix: &str) {
        if let Ok(mut slots) = self.segment_listings.lock() {
            slots.remove(prefix);
        }
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

/// Folds a prefix's footers into one sub-stream's per-partition views and byte
/// sizes, in a single pass over the entries.
///
/// Byte spans are per sub-stream, so a topic is charged its own share of a
/// shared segment and never a sibling's — nor, since it is charged by the same
/// identity it is read by (#118), a retired incarnation of its own name's.
pub(super) fn substream_segments(
    footers: &[(u64, Arc<SegmentFooter>)],
    prefix: &str,
    substream: SubstreamId<'_>,
) -> TopicSegments {
    let mut placed: BTreeMap<i32, Vec<Placed>> = BTreeMap::new();
    let mut bytes = BTreeMap::new();
    for (seq, footer) in footers {
        for entry in footer.entries.iter().filter(|e| substream.owns(e)) {
            *bytes.entry(entry.partition).or_insert(0) += entry.byte_len as i64;
            placed.entry(entry.partition).or_default().push(Placed {
                seq: *seq,
                epoch: footer.writer_epoch,
                prefix: prefix.to_string(),
                base_offset: entry.base_offset,
                end_offset: entry.end_offset(),
                byte_start: entry.byte_start,
                byte_len: entry.byte_len,
                max_timestamp: entry.max_timestamp,
            });
        }
    }
    TopicSegments {
        views: placed
            .into_iter()
            .map(|(partition, placed)| {
                (
                    partition,
                    SegView {
                        pieces: resolve_owners(placed),
                    },
                )
            })
            .collect(),
        bytes,
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
