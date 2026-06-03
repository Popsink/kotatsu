//! Server-side search + pagination over name-keyed lists.
//!
//! Lists come from a single source (meta.json / a prefix listing / Kora), so we
//! filter the names by a case-insensitive substring and slice the page in
//! memory. Callers then do any expensive per-item work only for the page.

use serde::Serialize;

#[derive(Clone, Debug)]
pub struct Page {
    search: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

impl Page {
    /// Builds a page spec, dropping an empty search and clamping the limit.
    pub fn new(search: Option<String>, limit: usize, offset: usize) -> Self {
        Self {
            search: search.filter(|s| !s.is_empty()),
            limit: limit.clamp(1, 200),
            offset,
        }
    }

    fn matches(&self, name: &str) -> bool {
        match &self.search {
            Some(q) => name.to_lowercase().contains(&q.to_lowercase()),
            None => true,
        }
    }

    /// Filters `names` by the search term, sorts them, and returns the requested
    /// page along with the total number of matches (before paging).
    pub fn select(&self, names: Vec<String>) -> (Vec<String>, usize) {
        let mut filtered: Vec<String> = names.into_iter().filter(|n| self.matches(n)).collect();
        filtered.sort();
        let total = filtered.len();
        let page = filtered
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .collect();
        (page, total)
    }
}

/// A page of results plus the total match count.
#[derive(Serialize)]
pub struct Paged<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

impl<T> Paged<T> {
    pub fn new(items: Vec<T>, total: usize, page: &Page) -> Self {
        Self {
            items,
            total,
            limit: page.limit,
            offset: page.offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names() -> Vec<String> {
        ["orders", "events", "order-events", "audit"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn filters_case_insensitive_and_sorts() {
        let (page, total) = Page::new(Some("ORDER".into()), 50, 0).select(names());
        assert_eq!(total, 2);
        assert_eq!(page, vec!["order-events", "orders"]); // sorted
    }

    #[test]
    fn paginates() {
        let (page, total) = Page::new(None, 2, 0).select(names());
        assert_eq!(total, 4);
        assert_eq!(page, vec!["audit", "events"]);
        let (page2, _) = Page::new(None, 2, 2).select(names());
        assert_eq!(page2, vec!["order-events", "orders"]);
    }

    #[test]
    fn empty_search_is_ignored_and_limit_clamped() {
        let p = Page::new(Some(String::new()), 0, 0);
        assert_eq!(p.limit, 1); // clamped to >= 1
        let (page, total) = p.select(names());
        assert_eq!(total, 4);
        assert_eq!(page.len(), 1);
    }
}
