//! In-process **catalog cache** for the list/search views (#84).
//!
//! Listing and searching topics and consumer groups both go through the same
//! path — a full-prefix `list_with_delimiter` plus per-page enrichment — so at
//! scale (~15k topics) every request, including every debounced search
//! keystroke, re-scans the object store. This caches the *catalog*: the name
//! index (the big win — search becomes an in-memory substring scan) and the
//! lightweight per-item summaries filled lazily as pages are served.
//!
//! Freshness is a short TTL, warmed lazily on a miss, per-process — no
//! background poller, so an idle instance still does no work. Staleness is
//! bounded and acceptable: list summaries are already approximate
//! (watermark-derived), and detail reads stay exact and uncached. Each kind
//! (topics / groups) has its own cache; a `StorageSource` is bound to one
//! cluster, so the cache is per-cluster by construction.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a warmed catalog (name index + summaries) is served before the next
/// request re-lists. Within the 30–60 s window the issue calls for: long enough
/// to absorb a burst of list/search requests, short enough that a new or removed
/// topic/group shows up quickly.
const CATALOG_TTL: Duration = Duration::from_secs(45);

/// A cached catalog for one kind: the full name index plus the per-item
/// summaries filled lazily as pages are served, all stamped with a single fill
/// time so the whole catalog expires together.
pub(super) struct Catalog<S> {
    names: Vec<String>,
    summaries: HashMap<String, S>,
    filled_at: Instant,
}

/// The cached name index if the catalog is present and fresh, else `None`
/// (the caller re-lists and calls [`set_names`]).
pub(super) fn fresh_names<S>(cell: &Mutex<Option<Catalog<S>>>) -> Option<Vec<String>> {
    let guard = cell.lock().ok()?;
    let cat = guard.as_ref()?;
    (cat.filled_at.elapsed() < CATALOG_TTL).then(|| cat.names.clone())
}

/// Replaces the catalog with a freshly-listed name index (summaries cleared,
/// clock reset).
pub(super) fn set_names<S>(cell: &Mutex<Option<Catalog<S>>>, names: Vec<String>) {
    if let Ok(mut guard) = cell.lock() {
        *guard = Some(Catalog {
            names,
            summaries: HashMap::new(),
            filled_at: Instant::now(),
        });
    }
}

/// A cached summary for `name`, if the catalog is present and still fresh.
pub(super) fn cached_summary<S: Clone>(cell: &Mutex<Option<Catalog<S>>>, name: &str) -> Option<S> {
    let guard = cell.lock().ok()?;
    let cat = guard.as_ref()?;
    if cat.filled_at.elapsed() >= CATALOG_TTL {
        return None;
    }
    cat.summaries.get(name).cloned()
}

/// Stores a computed summary into the current catalog (no-op if it was evicted
/// or expired since the name index was read).
pub(super) fn store_summary<S>(cell: &Mutex<Option<Catalog<S>>>, name: String, summary: S) {
    if let Ok(mut guard) = cell.lock() {
        if let Some(cat) = guard.as_mut() {
            if cat.filled_at.elapsed() < CATALOG_TTL {
                cat.summaries.insert(name, summary);
            }
        }
    }
}
