// smoke.js — Baseline validation: 1 VU, 30s.
// Drives the full read journey to verify every endpoint works and establish
// baseline latencies. Run 3 times before tightening thresholds for load/scale.
//
// Data-agnostic: setup() discovers the topics/schemas/groups that exist, so this
// works on the local `demo` stack today and on a 15k-topic seed later.
//
//   KOTATSU_URL      base URL (default http://localhost:8080)
//   KOTATSU_CLUSTER  cluster id (default demo)

import { check, sleep } from 'k6';
import {
  health, source, sourceStatus, clusters, clusterStats, topicTree, listTopics, topicDetail,
  messages, groups, groupDetail, listSchemas, schemaDetail, itemsOf, CLUSTER,
} from '../helpers.js';

export const options = {
  vus: 1,
  duration: '30s',
  setupTimeout: '30s',
  thresholds: {
    http_req_failed: ['rate==0'],
    http_req_duration: ['p(95)<500'],
    'http_req_duration{op:list_topics}': ['p(95)<200'],
    'http_req_duration{op:topic_tree}': ['p(95)<300'],
    'http_req_duration{op:topic_detail}': ['p(95)<200'],
    'http_req_duration{op:messages}': ['p(95)<500'], // includes batch decode + Avro resolve
    'http_req_duration{op:list_schemas}': ['p(95)<200'],
  },
};

export function setup() {
  // Discover what data exists so the journey targets real topics/schemas/groups.
  const topics = itemsOf(listTopics()).map((t) => t.name).filter(Boolean);
  const subjects = itemsOf(listSchemas());
  const grps = itemsOf(groups()).map((g) => (typeof g === 'string' ? g : g.group_id || g.name)).filter(Boolean);
  return { topics, subjects, groups: grps };
}

export default function (data) {
  // 1. Service + source + cluster overview
  check(health(), { 'health: ok': (r) => r.status === 200 });
  check(source(), { 'source: 200': (r) => r.status === 200 });
  // The only read whose cost is an S3 round-trip rather than a config lookup (#109).
  check(sourceStatus(), { 'source status: 200': (r) => r.status === 200 });
  check(clusters(), { 'clusters: 200': (r) => r.status === 200 });
  check(clusterStats(), { 'cluster_stats: 200': (r) => r.status === 200 });

  // 2. Browse: tree + flat list
  check(topicTree(CLUSTER, {}), { 'topic_tree: 200': (r) => r.status === 200 });
  check(listTopics(), { 'list_topics: 200': (r) => r.status === 200 });

  // 3. A topic: detail + messages (read-back → batch decode + Avro resolve)
  if (data.topics.length) {
    const topic = data.topics[Math.floor(Math.random() * data.topics.length)];
    check(topicDetail(topic), { 'topic_detail: 200': (r) => r.status === 200 });
    check(messages(topic), { 'messages: 200': (r) => r.status === 200 });
  }

  // 4. Consumer groups
  check(groups(), { 'groups: 200': (r) => r.status === 200 });
  if (data.groups.length) {
    check(groupDetail(data.groups[0]), { 'group_detail: 200': (r) => r.status === 200 });
  }

  // 5. Schemas: list + one subject (Kora resolve)
  check(listSchemas(), { 'list_schemas: 200': (r) => r.status === 200 });
  if (data.subjects.length) {
    const subj = data.subjects[Math.floor(Math.random() * data.subjects.length)];
    check(schemaDetail(subj), { 'schema_detail: 200': (r) => r.status === 200 });
  }

  sleep(0.5);
}
