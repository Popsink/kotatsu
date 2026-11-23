//! A prefix of tens of thousands of segments behind a store with a GCS-like
//! round trip (Popsink/data-plane#6603, DEV-664). `#[ignore]`d: it runs for
//! about a minute and prints timings rather than asserting them.
//!
//! ```text
//! cargo test --release scale_bench -- --ignored --nocapture
//! BENCH_SEGMENTS=40000 BENCH_LATENCY_MS=45 cargo test --release scale_bench -- --ignored --nocapture
//! ```
//!
//! `BENCH_CACHE_MIB` bounds the footer cache (default: the production bound).
//!
//! Every GET and every LIST page (1000 keys) waits `BENCH_LATENCY_MS` first;
//! bandwidth is not modelled (a footer read is one 64 KiB suffix, well under
//! a millisecond at GCS throughput). Requests are cut at 30 s, as
//! `http::API_TIMEOUT` cuts them.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};

use super::{segment::build_test_segment, StorageSource};
use crate::pagination::Page;

const API_TIMEOUT: Duration = Duration::from_secs(30);
const PREFIX: &str = "acme.prod.db2";
const TOPICS: usize = 50;
/// Sub-streams per segment: a flush carries a handful of the prefix's topics.
const REGIONS: usize = 5;

#[derive(Debug)]
struct SlowStore {
    inner: InMemory,
    latency: Duration,
    gets: AtomicUsize,
    list_pages: Arc<AtomicUsize>,
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore")
    }
}

impl SlowStore {
    /// One round trip before the first key, then one per further page.
    fn paged(
        &self,
        listing: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::ObjectMeta>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let latency = self.latency;
        let pages = self.list_pages.clone();
        let pages2 = pages.clone();
        futures::stream::once(async move {
            pages.fetch_add(1, SeqCst);
            tokio::time::sleep(latency).await;
            listing
        })
        .flatten()
        .enumerate()
        .then(move |(i, meta)| {
            let pages = pages2.clone();
            async move {
                if i > 0 && i % 1000 == 0 {
                    pages.fetch_add(1, SeqCst);
                    tokio::time::sleep(latency).await;
                }
                meta
            }
        })
        .boxed()
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.gets.fetch_add(1, SeqCst);
        tokio::time::sleep(self.latency).await;
        self.inner.get_opts(location, options).await
    }
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.paged(self.inner.list(prefix))
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.paged(self.inner.list_with_offset(prefix, offset))
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        tokio::time::sleep(self.latency).await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn env_or(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn topic(i: usize) -> String {
    format!("{PREFIX}.table_{i:03}")
}

/// `segments` segments under one prefix shared by [`TOPICS`] one-partition
/// topics, each segment carrying [`REGIONS`] of them, with real footers.
async fn seed(inner: &InMemory, src: &StorageSource, segments: u64) {
    for i in 0..TOPICS {
        let t = topic(i);
        let meta = serde_json::json!({ "topic": { "name": t, "num_partitions": 1 } });
        let pin = serde_json::json!({ "prefix": PREFIX });
        for (path, body) in [
            (src.keys().topic_metadata(&t), meta),
            (src.keys().topic_routing(&t), pin),
        ] {
            inner
                .put(&path, PutPayload::from(serde_json::to_vec(&body).unwrap()))
                .await
                .unwrap();
        }
    }
    let names: Vec<String> = (0..TOPICS).map(topic).collect();
    let body = [0u8; 64];
    for seq in 0..segments {
        let regions: Vec<super::segment::TestRegion> = (0..REGIONS)
            .map(|r| {
                let t = (seq as usize * REGIONS + r) % TOPICS;
                // Each topic's offsets advance by one record per segment it is in.
                let base = (seq as i64 * REGIONS as i64 + r as i64) / TOPICS as i64;
                (names[t].as_str(), 0, base, 1, &body[..], seq as i64, None)
            })
            .collect();
        let bytes = build_test_segment(3, 1, &regions);
        inner
            .put(
                &src.keys().segment(PREFIX, seq),
                PutPayload::from(bytes.to_vec()),
            )
            .await
            .unwrap();
    }
}

async fn source(segments: u64, latency: Duration) -> (Arc<SlowStore>, StorageSource) {
    let store = Arc::new(SlowStore {
        inner: InMemory::new(),
        latency,
        gets: AtomicUsize::new(0),
        list_pages: Arc::new(AtomicUsize::new(0)),
    });
    let mut src = StorageSource::with_store(store.clone(), "c");
    if let Ok(mib) = std::env::var("BENCH_CACHE_MIB") {
        src = src.with_footer_cache_bytes(mib.parse::<usize>().unwrap() * 1024 * 1024);
    }
    seed(&store.inner, &src, segments).await;
    (store, src)
}

fn footers_cached(src: &StorageSource) -> usize {
    src.footer_cache_stats().entries
}

/// Runs `fut` under the API timeout; prints and returns whether it finished.
async fn timed<T, E: std::fmt::Debug>(
    label: &str,
    store: &SlowStore,
    src: &StorageSource,
    fut: impl std::future::Future<Output = Result<T, E>>,
) -> bool {
    let (gets, pages) = (store.gets.load(SeqCst), store.list_pages.load(SeqCst));
    let started = Instant::now();
    let out = tokio::time::timeout(API_TIMEOUT, fut).await;
    let elapsed = started.elapsed();
    let done = match &out {
        Ok(Ok(_)) => "200",
        Ok(Err(e)) => {
            eprintln!("  error: {e:?}");
            "error"
        }
        Err(_) => "504 (cut at 30 s)",
    };
    eprintln!(
        "{label:<44} {done:<18} {:>6.2} s  GETs {:>6}  LIST pages {:>3}  footers cached {:>6}",
        elapsed.as_secs_f64(),
        store.gets.load(SeqCst) - gets,
        store.list_pages.load(SeqCst) - pages,
        footers_cached(src),
    );
    matches!(out, Ok(Ok(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale benchmark: ~1 min, prints timings"]
async fn scale_bench() {
    let segments = env_or("BENCH_SEGMENTS", 40_000);
    let latency = Duration::from_millis(env_or("BENCH_LATENCY_MS", 45));
    eprintln!(
        "\n{segments} segments under one prefix, {TOPICS} topics, {} ms per GET / LIST page",
        latency.as_millis()
    );

    // `/topics?stats=true`, the Topics page, against a cold pod — retried the
    // way a user (or the DEV-485 probe loop) retries after a 504.
    let (store, src) = source(segments, latency).await;
    let page = Page::new(None, 50, 0);
    let mut attempt = 1;
    while !timed(
        &format!("cold /topics, attempt {attempt}"),
        &store,
        &src,
        src.list_topics(&page, true),
    )
    .await
    {
        attempt += 1;
        assert!(attempt <= 5, "never warms");
    }
    // Warm: the footers cached, the row summaries not (the catalog serves them
    // for 45 s; past that every row is folded again from the footers).
    *src.topic_catalog.lock().unwrap() = None;
    timed(
        "warm /topics (catalog expired)",
        &store,
        &src,
        src.list_topics(&page, true),
    )
    .await;
    timed(
        "warm /topics/{t}",
        &store,
        &src,
        src.topic_detail(&topic(7)),
    )
    .await;
    eprintln!("footer cache after warm-up: {:?}", src.footer_cache_stats());

    // `/topics/{t}` against a cold pod.
    let (store, src) = source(segments, latency).await;
    let mut attempt = 1;
    while !timed(
        &format!("cold /topics/{{t}}, attempt {attempt}"),
        &store,
        &src,
        src.topic_detail(&topic(7)),
    )
    .await
    {
        attempt += 1;
        assert!(attempt <= 5, "never warms");
    }
}

/// The DEV-485 probe: four loops hitting the topic endpoints of one cold
/// prefix side by side, each retried after a 504.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale benchmark: ~1 min, prints timings"]
async fn scale_bench_concurrent() {
    let segments = env_or("BENCH_SEGMENTS", 40_000);
    let latency = Duration::from_millis(env_or("BENCH_LATENCY_MS", 45));
    eprintln!(
        "\n{segments} segments, 4 concurrent cold readers, {} ms per GET / LIST page",
        latency.as_millis()
    );
    let (store, src) = source(segments, latency).await;
    let started = Instant::now();
    let loops = (0..4).map(|i| {
        let (store, src) = (store.clone(), src.clone());
        async move {
            for attempt in 1..=5 {
                let ok = if i == 0 {
                    timed(
                        &format!("reader {i} /topics, attempt {attempt}"),
                        &store,
                        &src,
                        src.list_topics(&Page::new(None, 50, 0), true),
                    )
                    .await
                } else {
                    timed(
                        &format!("reader {i} /topics/{{t}}, attempt {attempt}"),
                        &store,
                        &src,
                        src.topic_detail(&topic(i)),
                    )
                    .await
                };
                if ok {
                    return attempt;
                }
            }
            panic!("never warms");
        }
    });
    futures::future::join_all(loops).await;
    eprintln!(
        "all served after {:.1} s, {} GETs in all for {segments} footers",
        started.elapsed().as_secs_f64(),
        store.gets.load(SeqCst)
    );
}
