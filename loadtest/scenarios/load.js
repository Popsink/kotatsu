// load.js — Nominal read load on Kotatsu's browse path (QA).
//
// The topic/message path needs data produced into Tansu; on an env with no
// topics this focuses on the paths that HAVE data: schemas (resolved via Kora),
// topic-tree, cluster stats, health. ~50 VUs for a few minutes.
//
//   KOTATSU_URL      base URL (e.g. https://kotatsu.ppsk.localhost:8443)
//   KOTATSU_CLUSTER  cluster id (QA: tansu)

import { check } from 'k6';
import {
  health, clusterStats, topicTree, listTopics, listSchemas, schemaDetail,
  itemsOf, CLUSTER,
} from '../helpers.js';

export const options = {
  scenarios: {
    browse: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 50 },
        { duration: '2m', target: 50 },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    // The schema list is the heavy path (fetches the full subject set from Kora).
    'http_req_duration{op:list_schemas}': ['p(99)<3000'],
    'http_req_duration{op:topic_tree}': ['p(99)<500'],
  },
};

export function setup() {
  // Sample up to 500 subjects for schema_detail (registry may hold 100k+).
  const subjects = itemsOf(listSchemas()).slice(0, 500);
  return { subjects };
}

export default function (data) {
  const roll = Math.random();
  if (roll < 0.40) {
    check(listSchemas(), { 'list_schemas 200': (r) => r.status === 200 });
  } else if (roll < 0.70 && data.subjects.length) {
    const s = data.subjects[Math.floor(Math.random() * data.subjects.length)];
    check(schemaDetail(s), { 'schema_detail 200': (r) => r.status === 200 });
  } else if (roll < 0.85) {
    check(topicTree(CLUSTER, {}), { 'topic_tree 200': (r) => r.status === 200 });
  } else if (roll < 0.95) {
    check(clusterStats(), { 'cluster_stats 200': (r) => r.status === 200 });
  } else {
    check(health(), { 'health 200': (r) => r.status === 200 });
    check(listTopics(), { 'list_topics 200': (r) => r.status === 200 });
  }
}
