// helpers.js — Shared config and tagged HTTP helpers for Kotatsu load tests.
//
// Kotatsu is read-only: every endpoint is a GET. Helpers tag each request with an
// `op` so the summary/Grafana break latency down per endpoint. No auth — Kotatsu
// reaches its object store (rustfs in the test env, AWS S3 in prod) directly.

import http from 'k6/http';

export const BASE = __ENV.KOTATSU_URL || 'http://localhost:8080';
export const CLUSTER = __ENV.KOTATSU_CLUSTER || 'demo';

// Build a query string from an object, skipping undefined/null/'' values.
function qs(params) {
  const parts = [];
  for (const k in params) {
    const v = params[k];
    if (v !== undefined && v !== null && v !== '') parts.push(`${k}=${encodeURIComponent(v)}`);
  }
  return parts.length ? `?${parts.join('&')}` : '';
}

// -- Tagged read helpers --

export function health() {
  return http.get(`${BASE}/api/health`, { tags: { op: 'health', name: 'GET /api/health' } });
}

// Pure config since #109 — no object-store call, so this measures the HTTP path
// only. `sourceStatus` is the one that costs an S3 round-trip.
export function source() {
  return http.get(`${BASE}/api/source`, { tags: { op: 'source', name: 'GET /api/source' } });
}

export function sourceStatus() {
  return http.get(`${BASE}/api/source/status`, {
    tags: { op: 'source_status', name: 'GET /api/source/status' },
  });
}

export function clusters() {
  return http.get(`${BASE}/api/clusters`, { tags: { op: 'clusters', name: 'GET /api/clusters' } });
}

export function clusterStats(cluster = CLUSTER) {
  return http.get(`${BASE}/api/clusters/${cluster}`, {
    tags: { op: 'cluster_stats', name: 'GET /api/clusters/{cluster}' },
  });
}

export function topicTree(cluster = CLUSTER, params = {}) {
  return http.get(`${BASE}/api/clusters/${cluster}/topic-tree${qs(params)}`, {
    tags: { op: 'topic_tree', name: 'GET /api/clusters/{cluster}/topic-tree' },
  });
}

export function listTopics(cluster = CLUSTER, params = {}) {
  return http.get(`${BASE}/api/clusters/${cluster}/topics${qs(params)}`, {
    tags: { op: 'list_topics', name: 'GET /api/clusters/{cluster}/topics' },
  });
}

export function topicDetail(topic, cluster = CLUSTER) {
  return http.get(`${BASE}/api/clusters/${cluster}/topics/${topic}`, {
    tags: { op: 'topic_detail', name: 'GET /api/clusters/{cluster}/topics/{topic}' },
  });
}

export function messages(topic, cluster = CLUSTER, params = { offset: 'earliest', limit: 20 }) {
  return http.get(`${BASE}/api/clusters/${cluster}/topics/${topic}/messages${qs(params)}`, {
    tags: { op: 'messages', name: 'GET /api/clusters/{cluster}/topics/{topic}/messages' },
  });
}

export function groups(cluster = CLUSTER) {
  return http.get(`${BASE}/api/clusters/${cluster}/groups`, {
    tags: { op: 'groups', name: 'GET /api/clusters/{cluster}/groups' },
  });
}

export function groupDetail(group, cluster = CLUSTER) {
  return http.get(`${BASE}/api/clusters/${cluster}/groups/${group}`, {
    tags: { op: 'group_detail', name: 'GET /api/clusters/{cluster}/groups/{group}' },
  });
}

export function listSchemas(params = {}) {
  return http.get(`${BASE}/api/schemas${qs(params)}`, {
    tags: { op: 'list_schemas', name: 'GET /api/schemas' },
  });
}

export function schemaDetail(subject) {
  return http.get(`${BASE}/api/schemas/${subject}`, {
    tags: { op: 'schema_detail', name: 'GET /api/schemas/{subject}' },
  });
}

// Best-effort array length of a paginated list response ({items: [...]}) or a
// bare array, used by setup() to discover what data exists.
export function itemsOf(res) {
  try {
    const b = res.json();
    if (Array.isArray(b)) return b;
    if (b && Array.isArray(b.items)) return b.items;
    if (b && Array.isArray(b.clusters)) return b.clusters;
  } catch (_) { /* ignore */ }
  return [];
}
