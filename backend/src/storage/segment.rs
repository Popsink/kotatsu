//! Decoding of Tansu's prefix-coalesced virtual-topic segments (#82).
//!
//! Wire contract: Popsink/tansu `docs/virtual-topics-format.md`, verified
//! against `encode_footer` at `v0.7.0-beta.39` — the version the deployed broker
//! runs and this crate's decoder is pinned to (#96). Many topics' records are
//! multiplexed into shared, immutable per-prefix **segment** objects:
//!
//! ```text
//! clusters/{cluster}/prefixes/{prefix}/segments/{seq:020}.seg
//! ```
//!
//! A segment is `[sub-stream region 0][region 1]…[footer][trailer]`. Each region
//! is that sub-stream's Kafka `RecordBatch` bytes concatenated — byte-for-byte
//! what a legacy coalesced object (#50) holds. The **footer** carries, per
//! sub-stream, its absolute offset range, byte range and newest timestamp; the
//! fixed 18-byte **trailer** locates the footer. All integers are big-endian.
//!
//! **Which sub-stream a region belongs to** is `(topic, partition)` by name up to
//! v3, and `(topic_id, partition)` from v4 for any entry carrying a non-nil id
//! (#118). See [`SubstreamId`]: the two are separate key spaces, deliberately, so
//! that a topic recreated under a dead topic's name cannot find its predecessor's
//! slices — the segment they sit in is immutable and reclaimed whole, so those
//! slices outlive the topic and would otherwise fold into the new one's offsets.
//!
//! This module is pure (no I/O): it decodes a tail buffer the reader fetches.
//! Offsets come from the footer, never the object name — the segment sequence is
//! monotonic but, because compaction (#66) rewrites merged low-offset segments
//! under a fresh higher sequence, sequence order is **not** offset order.

#[cfg(test)]
use bytes::Bytes;
use uuid::Uuid;

use super::StorageError;

/// ASCII `TSEG`, the trailer magic. A legacy single-topic coalesced object (#50)
/// has no trailer, so its trailing bytes are record data and will not equal
/// this — that is the v0 discriminator.
pub const SEGMENT_MAGIC: u32 = 0x5453_4547;

/// Fixed trailer at the very end of every multi-topic segment:
/// `footer_len (u64) + entry_count (u32) + version (u16) + magic (u32)`.
pub const SEGMENT_TRAILER_LEN: usize = 8 + 4 + 2 + 4;

/// Speculative suffix size for reading a footer in one ranged GET: the trailer +
/// footer of almost every segment fits within this, so one over-reading GET
/// replaces a read-trailer-then-read-footer two-GET dance. A larger footer
/// (a prefix with very many sub-streams) falls back to a second exact GET.
pub const SEGMENT_FOOTER_OVER_READ: u64 = 64 * 1024;

/// First self-describing multi-topic footer (#64). Read-only history: no writer
/// emits it any more.
const SEGMENT_FORMAT_VERSION_V1: u16 = 1;
/// Adds a per-flush nonce and per-batch producer coordinates (#87). Emitted by
/// the leaseless writer from `beta.13` until v3 superseded it — read-only
/// history too, and what the older segments still in a bucket carry.
const SEGMENT_FORMAT_VERSION_V2: u16 = 2;
/// Appends one `flags: u8` per producer coordinate and widens coordinate emission
/// to transactional and control batches (Popsink/tansu#174). **This is what
/// production writes** — unconditionally, on every write path including both
/// compactions, since Popsink/tansu#188 (`0.7.0-beta.25`). So it is the only
/// branch that runs on current data; v1/v2 are what history decodes under.
///
/// The coordinates themselves are discarded; what matters is the **stride**,
/// which grows from 22 to 23 bytes (see `decode_footer`).
const SEGMENT_FORMAT_VERSION_V3: u16 = 3;
/// Inserts a 16-byte `topic_id` in every footer entry, immediately after `topic`
/// (Popsink/kodansu#467). Written only once a broker is configured with
/// `segment_format=4`; the default is still 3, so nothing in the field emits it
/// yet — but the version follows the **writer**, not the entry, so the first
/// flush after that flip makes every new segment in every prefix v4, including
/// the ones holding only name-keyed topics.
///
/// The id is what a sub-stream is keyed by when it is non-nil; a nil uuid means
/// the entry stayed name-keyed. Both kinds share one segment for as long as any
/// topic created before the flip is alive, so "the segment is v4" and "this
/// sub-stream is id-keyed" are independent facts.
const SEGMENT_FORMAT_VERSION_V4: u16 = 4;

/// Width of the `topic_id` field on the wire (v4+).
const TOPIC_ID_LEN: usize = 16;

/// One sub-stream's self-describing footer entry: where its batches live in the
/// shared object and what offset span they cover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubstreamEntry {
    pub topic: String,
    /// The sub-stream's identity when it is **id-keyed** (v4+ and a non-nil uuid
    /// on the wire). `None` for a name-keyed sub-stream — every entry below v4,
    /// and a v4 entry written for a topic created before the flip, which encodes
    /// the nil uuid. Never mix the two: see [`SubstreamId`].
    pub topic_id: Option<Uuid>,
    pub partition: i32,
    /// Absolute offset of this sub-stream's first record in the segment.
    pub base_offset: i64,
    /// `last_offset == base_offset + record_count - 1`.
    pub record_count: i64,
    /// Byte offset of this sub-stream's contiguous region within the segment.
    pub byte_start: u64,
    /// Byte length of that region.
    pub byte_len: u64,
    /// Greatest record timestamp in the sub-stream (for time-seek / retention).
    pub max_timestamp: i64,
}

impl SubstreamEntry {
    /// Exclusive end offset: one past this sub-stream's last record.
    pub fn end_offset(&self) -> i64 {
        self.base_offset + self.record_count
    }
}

/// A decoded segment footer: the writer epoch (era/lease, `0` if unleased) and
/// its sub-stream entries. The v2 per-flush nonce and per-batch producer
/// coordinates are parsed-and-skipped — a read-only browser does not use them,
/// but they sit *between* entries on the wire, so they must be consumed to keep
/// the decode cursor aligned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentFooter {
    pub writer_epoch: i64,
    pub entries: Vec<SubstreamEntry>,
}

impl SegmentFooter {
    /// The entry for a sub-stream, if the segment holds one.
    pub fn get(&self, id: SubstreamId<'_>, partition: i32) -> Option<&SubstreamEntry> {
        self.entries
            .iter()
            .find(|e| e.partition == partition && id.owns(e))
    }
}

/// What identifies a sub-stream inside a segment (#118). A topic is one or the
/// other for its whole life, decided at creation and pinned:
/// `topic-routing/{topic}.json` carries a `substream_id` for an id-keyed topic
/// and omits it for a name-keyed one.
///
/// The two are **separate key spaces**, and matching must never fall back from
/// one to the other. "Match the id, else match the name" would let an id-keyed
/// entry answer to its own name, which is exactly the confusion v4 exists to
/// end: a recreated topic reading its dead predecessor's slices, at offsets the
/// live topic also uses, with no error to show for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubstreamId<'a> {
    /// Id-keyed: only an entry carrying this exact uuid belongs to it.
    Id(Uuid),
    /// Name-keyed: only an entry with **no** id and this name belongs to it.
    Name(&'a str),
}

impl SubstreamId<'_> {
    /// Whether `entry` belongs to this sub-stream (partition aside).
    pub fn owns(&self, entry: &SubstreamEntry) -> bool {
        match self {
            Self::Id(id) => entry.topic_id == Some(*id),
            Self::Name(name) => entry.topic_id.is_none() && entry.topic == *name,
        }
    }
}

/// Outcome of decoding a segment tail buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FooterOutcome {
    /// No trailer magic: a legacy single-topic coalesced object (#50, v0). Decode
    /// the whole object as a bare `RecordBatch` concatenation.
    Legacy,
    /// A fully decoded footer.
    Footer(SegmentFooter),
    /// The trailer was present but the buffer did not include the whole footer
    /// (the over-read was too small). Re-fetch a suffix of at least this many
    /// bytes and decode again.
    NeedBytes(usize),
}

/// Cursor helper mirroring Tansu's `take`: advance over `n` bytes or fail.
fn take<'a>(cursor: &mut &'a [u8], n: usize) -> Result<&'a [u8], StorageError> {
    if cursor.len() < n {
        return Err(StorageError::Decode("truncated segment footer".into()));
    }
    let (head, tail) = cursor.split_at(n);
    *cursor = tail;
    Ok(head)
}

fn be_u16(b: &[u8]) -> u16 {
    u16::from_be_bytes(b.try_into().unwrap())
}
fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}
fn be_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b.try_into().unwrap())
}
fn be_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes(b.try_into().unwrap())
}
fn be_i64(b: &[u8]) -> i64 {
    i64::from_be_bytes(b.try_into().unwrap())
}

/// Decodes the footer from a segment's **tail** buffer (the last bytes fetched
/// by a ranged GET, or the whole object).
///
/// - Magic absent ⇒ [`FooterOutcome::Legacy`] (v0 bare-frame object).
/// - Footer fits in the buffer ⇒ [`FooterOutcome::Footer`].
/// - Trailer present but footer truncated ⇒ [`FooterOutcome::NeedBytes`] with the
///   suffix length the caller must re-fetch.
/// - Unknown format version ⇒ error (never guess).
pub fn decode_segment_footer(tail: &[u8]) -> Result<FooterOutcome, StorageError> {
    if tail.len() < SEGMENT_TRAILER_LEN {
        return Ok(FooterOutcome::Legacy);
    }

    let trailer = &tail[tail.len() - SEGMENT_TRAILER_LEN..];
    let magic = be_u32(&trailer[14..18]);
    if magic != SEGMENT_MAGIC {
        return Ok(FooterOutcome::Legacy);
    }

    let footer_len = be_u64(&trailer[0..8]) as usize;
    let entry_count = be_u32(&trailer[8..12]) as usize;
    let version = be_u16(&trailer[12..14]);
    if !(SEGMENT_FORMAT_VERSION_V1..=SEGMENT_FORMAT_VERSION_V4).contains(&version) {
        return Err(StorageError::Decode(format!(
            "unsupported segment format version {version}"
        )));
    }

    let footer_end = tail.len() - SEGMENT_TRAILER_LEN;
    let Some(footer_start) = footer_end.checked_sub(footer_len) else {
        // The over-read did not reach the footer start: ask for a bigger suffix.
        return Ok(FooterOutcome::NeedBytes(SEGMENT_TRAILER_LEN + footer_len));
    };

    let footer = decode_footer(&tail[footer_start..footer_end], entry_count, version)?;
    Ok(FooterOutcome::Footer(footer))
}

/// Parses the `footer_len` footer bytes preceding the trailer. Inverse of
/// Tansu's `encode_footer`. A truncated or malformed footer is a corrupt
/// segment, not a legacy object.
fn decode_footer(
    footer_bytes: &[u8],
    entry_count: usize,
    version: u16,
) -> Result<SegmentFooter, StorageError> {
    let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
    let v4 = version >= SEGMENT_FORMAT_VERSION_V4;
    let mut cursor = footer_bytes;

    let writer_epoch = be_i64(take(&mut cursor, 8)?);
    if v2 {
        // Per-flush nonce (v2) — skipped.
        let _ = take(&mut cursor, 8)?;
    }

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let topic_len = be_u16(take(&mut cursor, 2)?) as usize;
        let topic = String::from_utf8(take(&mut cursor, topic_len)?.to_vec())
            .map_err(|e| StorageError::Decode(e.to_string()))?;
        // Sub-stream identity (v4+): 16 raw uuid bytes. The nil uuid is the
        // writer's way of saying "this one is still keyed by name", so it must
        // decode back to `None` rather than to a uuid that happens to be zero.
        let topic_id = if v4 {
            Some(Uuid::from_slice(take(&mut cursor, TOPIC_ID_LEN)?).expect("16 bytes"))
                .filter(|id| !id.is_nil())
        } else {
            None
        };
        let partition = be_i32(take(&mut cursor, 4)?);
        let base_offset = be_i64(take(&mut cursor, 8)?);
        let record_count = be_i64(take(&mut cursor, 8)?);
        let byte_start = be_u64(take(&mut cursor, 8)?);
        let byte_len = be_u64(take(&mut cursor, 8)?);
        let max_timestamp = be_i64(take(&mut cursor, 8)?);

        if v2 {
            // Per-batch producer coordinates (v2+): parse-and-skip. Each is
            // producer_id(i64) + producer_epoch(i16) + base_sequence(i32) +
            // last_sequence(i32) + offset_delta(u32) = 22 bytes, **plus a
            // flags(u8) at v3** = 23. Skipping by the wrong stride does not
            // fail loudly: the cursor lands mid-coordinate and every following
            // coordinate and entry decodes as garbage, so a v3 footer read with
            // the v2 stride silently mis-resolves sub-streams.
            let stride = if version >= SEGMENT_FORMAT_VERSION_V3 {
                23
            } else {
                22
            };
            let pcoord_count = be_u16(take(&mut cursor, 2)?) as usize;
            let _ = take(&mut cursor, pcoord_count.saturating_mul(stride))?;
        }

        entries.push(SubstreamEntry {
            topic,
            topic_id,
            partition,
            base_offset,
            record_count,
            byte_start,
            byte_len,
            max_timestamp,
        });
    }

    Ok(SegmentFooter {
        writer_epoch,
        entries,
    })
}

/// Convenience: decode a footer from a `Bytes` tail, erroring if it turns out to
/// need more bytes (used where the whole object is already in hand).
#[cfg(test)]
pub fn decode_whole_segment(bytes: &Bytes) -> Result<FooterOutcome, StorageError> {
    decode_segment_footer(bytes)
}

/// One sub-stream to place in a test segment: `(topic, partition, base_offset,
/// record_count, region_bytes, max_timestamp, topic_id)`. `region_bytes` is the
/// sub-stream's concatenated `RecordBatch` wire bytes (e.g. real `.batch`
/// fixtures joined). `topic_id` is written only at v4 — `None` there encodes the
/// nil uuid, i.e. a name-keyed sub-stream sharing a v4 segment.
#[cfg(test)]
pub(crate) type TestRegion<'a> = (&'a str, i32, i64, i64, &'a [u8], i64, Option<Uuid>);

/// Builds a segment object (regions, then footer, then the 18-byte trailer) the
/// way Tansu's writer does — for reader integration tests. Emits no producer
/// coordinates (their skip-decode is covered by a dedicated unit test).
#[cfg(test)]
pub(crate) fn build_test_segment(version: u16, writer_epoch: i64, regions: &[TestRegion]) -> Bytes {
    let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
    let v4 = version >= SEGMENT_FORMAT_VERSION_V4;
    let mut body = Vec::new();
    let mut footer = Vec::new();
    footer.extend_from_slice(&writer_epoch.to_be_bytes());
    if v2 {
        footer.extend_from_slice(&0u64.to_be_bytes()); // nonce
    }

    for (topic, partition, base_offset, record_count, region, max_ts, topic_id) in regions {
        let byte_start = body.len() as u64;
        body.extend_from_slice(region);
        let byte_len = body.len() as u64 - byte_start;

        let t = topic.as_bytes();
        footer.extend_from_slice(&(t.len() as u16).to_be_bytes());
        footer.extend_from_slice(t);
        if v4 {
            footer.extend_from_slice(topic_id.unwrap_or(Uuid::nil()).as_bytes());
        }
        footer.extend_from_slice(&partition.to_be_bytes());
        footer.extend_from_slice(&base_offset.to_be_bytes());
        footer.extend_from_slice(&record_count.to_be_bytes());
        footer.extend_from_slice(&byte_start.to_be_bytes());
        footer.extend_from_slice(&byte_len.to_be_bytes());
        footer.extend_from_slice(&max_ts.to_be_bytes());
        if v2 {
            footer.extend_from_slice(&0u16.to_be_bytes()); // pcoord_count
        }
    }

    let mut out = body;
    out.extend_from_slice(&footer);
    out.extend_from_slice(&(footer.len() as u64).to_be_bytes());
    out.extend_from_slice(&(regions.len() as u32).to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a segment body the way Tansu's `encode_segment_v2` /
    /// `encode_segment` do: region bytes, then footer, then the 18-byte trailer.
    struct SegBuilder {
        version: u16,
        writer_epoch: i64,
        nonce: u64,
        body: Vec<u8>,
        entries: Vec<SubstreamEntry>,
        // parallel to entries: how many dummy producer coords to emit (v2 only)
        pcoords: Vec<u16>,
    }

    impl SegBuilder {
        fn new(version: u16, writer_epoch: i64) -> Self {
            Self {
                version,
                writer_epoch,
                nonce: 0xDEAD_BEEF,
                body: Vec::new(),
                entries: Vec::new(),
                pcoords: Vec::new(),
            }
        }

        /// Append a name-keyed sub-stream region of `region` raw bytes.
        #[allow(clippy::too_many_arguments)]
        fn region(
            self,
            topic: &str,
            partition: i32,
            base_offset: i64,
            record_count: i64,
            max_timestamp: i64,
            region: &[u8],
            pcoords: u16,
        ) -> Self {
            self.keyed_region(
                topic,
                None,
                partition,
                base_offset,
                record_count,
                max_timestamp,
                region,
                pcoords,
            )
        }

        /// Append a region whose sub-stream is keyed by `topic_id` (v4). `None`
        /// encodes the nil uuid — a name-keyed entry in a v4 segment.
        #[allow(clippy::too_many_arguments)]
        fn keyed_region(
            mut self,
            topic: &str,
            topic_id: Option<Uuid>,
            partition: i32,
            base_offset: i64,
            record_count: i64,
            max_timestamp: i64,
            region: &[u8],
            pcoords: u16,
        ) -> Self {
            let byte_start = self.body.len() as u64;
            self.body.extend_from_slice(region);
            self.entries.push(SubstreamEntry {
                topic: topic.to_string(),
                topic_id,
                partition,
                base_offset,
                record_count,
                byte_start,
                byte_len: region.len() as u64,
                max_timestamp,
            });
            self.pcoords.push(pcoords);
            self
        }

        fn encode_footer(&self) -> Vec<u8> {
            let v2 = self.version >= SEGMENT_FORMAT_VERSION_V2;
            let mut buf = Vec::new();
            buf.extend_from_slice(&self.writer_epoch.to_be_bytes());
            if v2 {
                buf.extend_from_slice(&self.nonce.to_be_bytes());
            }
            for (e, &pc) in self.entries.iter().zip(&self.pcoords) {
                let t = e.topic.as_bytes();
                buf.extend_from_slice(&(t.len() as u16).to_be_bytes());
                buf.extend_from_slice(t);
                if self.version >= SEGMENT_FORMAT_VERSION_V4 {
                    buf.extend_from_slice(e.topic_id.unwrap_or(Uuid::nil()).as_bytes());
                }
                buf.extend_from_slice(&e.partition.to_be_bytes());
                buf.extend_from_slice(&e.base_offset.to_be_bytes());
                buf.extend_from_slice(&e.record_count.to_be_bytes());
                buf.extend_from_slice(&e.byte_start.to_be_bytes());
                buf.extend_from_slice(&e.byte_len.to_be_bytes());
                buf.extend_from_slice(&e.max_timestamp.to_be_bytes());
                if v2 {
                    buf.extend_from_slice(&pc.to_be_bytes());
                    for i in 0..pc {
                        // dummy producer coord: 22 bytes at v2, 23 at v3
                        buf.extend_from_slice(&(i as i64).to_be_bytes()); // producer_id
                        buf.extend_from_slice(&0i16.to_be_bytes()); // producer_epoch
                        buf.extend_from_slice(&0i32.to_be_bytes()); // base_sequence
                        buf.extend_from_slice(&0i32.to_be_bytes()); // last_sequence
                        buf.extend_from_slice(&0u32.to_be_bytes()); // offset_delta
                        if self.version >= SEGMENT_FORMAT_VERSION_V3 {
                            buf.push(0b11); // flags: transactional | control
                        }
                    }
                }
            }
            buf
        }

        fn build(&self) -> Bytes {
            let footer = self.encode_footer();
            let mut out = self.body.clone();
            out.extend_from_slice(&footer);
            out.extend_from_slice(&(footer.len() as u64).to_be_bytes());
            out.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
            out.extend_from_slice(&self.version.to_be_bytes());
            out.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());
            Bytes::from(out)
        }
    }

    #[test]
    fn decodes_v1_footer() {
        let seg = SegBuilder::new(1, 0)
            .region("orders", 0, 100, 3, 555, b"AAAA", 0)
            .region("orders", 1, 0, 2, 777, b"BB", 0)
            .build();
        let out = decode_whole_segment(&seg).unwrap();
        let footer = match out {
            FooterOutcome::Footer(f) => f,
            other => panic!("expected footer, got {other:?}"),
        };
        assert_eq!(footer.writer_epoch, 0);
        assert_eq!(footer.entries.len(), 2);
        let e = footer.get(SubstreamId::Name("orders"), 0).unwrap();
        assert_eq!(
            (e.base_offset, e.record_count, e.end_offset()),
            (100, 3, 103)
        );
        assert_eq!((e.byte_start, e.byte_len), (0, 4));
        assert_eq!(e.max_timestamp, 555);
        let e1 = footer.get(SubstreamId::Name("orders"), 1).unwrap();
        assert_eq!((e1.byte_start, e1.byte_len), (4, 2));
    }

    #[test]
    fn decodes_v2_footer_skipping_producer_coords() {
        // v2 with producer coords interleaved between entries — a reader that
        // failed to skip them would desync on the second entry.
        let seg = SegBuilder::new(2, 7)
            .region("t", 0, 0, 5, 10, b"XXXX", 3)
            .region("t", 1, 0, 4, 20, b"YYYYYY", 1)
            .build();
        let footer = match decode_whole_segment(&seg).unwrap() {
            FooterOutcome::Footer(f) => f,
            other => panic!("expected footer, got {other:?}"),
        };
        assert_eq!(footer.writer_epoch, 7);
        assert_eq!(footer.entries.len(), 2);
        assert_eq!(
            footer.get(SubstreamId::Name("t"), 0).unwrap().max_timestamp,
            10
        );
        let e1 = footer.get(SubstreamId::Name("t"), 1).unwrap();
        assert_eq!(
            e1.byte_start, 4,
            "second region starts after the first (4B)"
        );
        assert_eq!(e1.record_count, 4);
    }

    #[test]
    fn decodes_v3_footer_skipping_flagged_producer_coords() {
        // A v3 coordinate is 23 bytes, not 22: it carries a trailing flags byte
        // (Popsink/tansu#174). Skipping the block with the v2 stride does not
        // fail loudly — the cursor lands mid-coordinate and every following
        // coordinate and entry decodes as garbage — so this pins the stride by
        // reading a second entry back correctly from behind three coordinates.
        let seg = SegBuilder::new(3, 9)
            .region("t", 0, 0, 5, 10, b"XXXX", 3)
            .region("t", 1, 0, 4, 20, b"YYYYYY", 1)
            .build();
        let footer = match decode_whole_segment(&seg).unwrap() {
            FooterOutcome::Footer(f) => f,
            other => panic!("expected footer, got {other:?}"),
        };
        assert_eq!(footer.writer_epoch, 9);
        assert_eq!(footer.entries.len(), 2);
        assert_eq!(
            footer.get(SubstreamId::Name("t"), 0).unwrap().max_timestamp,
            10
        );
        let e1 = footer.get(SubstreamId::Name("t"), 1).unwrap();
        assert_eq!(
            e1.byte_start, 4,
            "second region starts after the first (4B)"
        );
        assert_eq!(e1.record_count, 4);
        assert_eq!(e1.max_timestamp, 20);
    }

    #[test]
    fn legacy_object_without_trailer_is_legacy() {
        // Bytes whose trailing 4 do not spell TSEG.
        let bytes = Bytes::from_static(b"not-a-segment-just-record-bytes");
        assert_eq!(decode_whole_segment(&bytes).unwrap(), FooterOutcome::Legacy);
        // Too short to even hold a trailer → legacy.
        assert_eq!(
            decode_whole_segment(&Bytes::from_static(b"tiny")).unwrap(),
            FooterOutcome::Legacy
        );
    }

    #[test]
    fn unknown_version_is_rejected() {
        // 5 as much as 99 and 0: widening the accepted set to admit v4 must not
        // widen it to "anything at least as new as v4". A version we do not know
        // the entry layout of decodes as garbage, silently.
        for version in [0u16, 5, 99] {
            let mut seg = SegBuilder::new(1, 0)
                .region("t", 0, 0, 1, 1, b"Z", 0)
                .build()
                .to_vec();
            // Overwrite version field (bytes [len-6..len-4]).
            let n = seg.len();
            seg[n - 6..n - 4].copy_from_slice(&version.to_be_bytes());
            let err = decode_segment_footer(&seg).unwrap_err();
            assert!(
                matches!(err, StorageError::Decode(_)),
                "version {version} got {err:?}"
            );
        }
    }

    /// v4 inserts 16 bytes per entry; below v4 they are not on the wire and must
    /// not be read. Every pre-v4 entry is therefore name-keyed — and the second
    /// entry decoding correctly is what proves the cursor did not advance.
    #[test]
    fn pre_v4_entries_are_name_keyed() {
        for version in [1u16, 2, 3] {
            let seg = SegBuilder::new(version, 4)
                .region("orders", 0, 100, 3, 555, b"AAAA", 0)
                .region("orders", 1, 0, 2, 777, b"BB", 0)
                .build();
            let footer = match decode_whole_segment(&seg).unwrap() {
                FooterOutcome::Footer(f) => f,
                other => panic!("v{version}: expected footer, got {other:?}"),
            };
            assert!(
                footer.entries.iter().all(|e| e.topic_id.is_none()),
                "v{version}: no id on the wire below v4"
            );
            let e1 = footer.get(SubstreamId::Name("orders"), 1).unwrap();
            assert_eq!(
                (e1.byte_start, e1.byte_len, e1.max_timestamp),
                (4, 2, 777),
                "v{version}: second entry decoded from an aligned cursor"
            );
        }
    }

    /// A v4 segment multiplexes both kinds of sub-stream — it must, for as long
    /// as a topic created before the flip is alive — and each answers only to its
    /// own key.
    #[test]
    fn decodes_v4_footer_with_mixed_substream_identity() {
        let id = Uuid::from_u128(0x0192_ABCD);
        let seg = SegBuilder::new(4, 11)
            // Same name, same partition, different incarnations: the retired
            // name-keyed one and the live id-keyed one, in one segment.
            .keyed_region("orders", None, 0, 10, 3, 555, b"AAAA", 2)
            .keyed_region("orders", Some(id), 0, 0, 2, 777, b"BB", 1)
            .build();
        let footer = match decode_whole_segment(&seg).unwrap() {
            FooterOutcome::Footer(f) => f,
            other => panic!("expected footer, got {other:?}"),
        };
        assert_eq!(footer.writer_epoch, 11);
        assert_eq!(footer.entries.len(), 2);
        // The nil uuid is "no id", not a uuid that happens to be zero.
        assert_eq!(footer.entries[0].topic_id, None);
        assert_eq!(footer.entries[1].topic_id, Some(id));

        let by_name = footer.get(SubstreamId::Name("orders"), 0).unwrap();
        assert_eq!((by_name.base_offset, by_name.record_count), (10, 3));
        let by_id = footer.get(SubstreamId::Id(id), 0).unwrap();
        assert_eq!(
            (by_id.base_offset, by_id.record_count),
            (0, 2),
            "the live topic starts at 0, not at its predecessor's 10"
        );
        assert_eq!(
            (by_id.byte_start, by_id.byte_len),
            (4, 2),
            "and reads its own region, decoded from behind a producer coordinate"
        );
    }

    /// The two key spaces never fall back to one another.
    #[test]
    fn identities_do_not_cross_over() {
        let id = Uuid::from_u128(7);
        let seg = SegBuilder::new(4, 0)
            .keyed_region("id-keyed", Some(id), 0, 0, 1, 1, b"A", 0)
            .keyed_region("name-keyed", None, 0, 0, 1, 1, b"B", 0)
            .build();
        let footer = match decode_whole_segment(&seg).unwrap() {
            FooterOutcome::Footer(f) => f,
            other => panic!("expected footer, got {other:?}"),
        };
        assert!(
            footer.get(SubstreamId::Name("id-keyed"), 0).is_none(),
            "an id-keyed entry must not answer to its own name"
        );
        assert!(
            footer.get(SubstreamId::Id(id), 0).is_some(),
            "it answers to its id"
        );
        assert!(
            footer.get(SubstreamId::Id(Uuid::nil()), 0).is_none(),
            "and the nil uuid is nobody's id, least of all a name-keyed entry's"
        );
        assert!(
            footer.get(SubstreamId::Name("name-keyed"), 0).is_some(),
            "a name-keyed entry answers to its name"
        );
    }

    #[test]
    fn truncated_over_read_asks_for_more_bytes() {
        let seg = SegBuilder::new(2, 1)
            .region("longtopicname", 0, 0, 1, 1, b"Z", 0)
            .build();
        // Feed only the last (trailer + a couple) bytes: trailer present, footer
        // start not reached → NeedBytes.
        let tail = &seg[seg.len() - (SEGMENT_TRAILER_LEN + 2)..];
        match decode_segment_footer(tail).unwrap() {
            FooterOutcome::NeedBytes(n) => {
                assert!(n > SEGMENT_TRAILER_LEN);
                // Re-decode with the full suffix succeeds.
                let full = &seg[seg.len() - n..];
                assert!(matches!(
                    decode_segment_footer(full).unwrap(),
                    FooterOutcome::Footer(_)
                ));
            }
            other => panic!("expected NeedBytes, got {other:?}"),
        }
    }
}
