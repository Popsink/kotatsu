#!/usr/bin/env bash
# Seed the demo cluster with fictitious data for the e2e smoke.
# Intended for a FRESH stack: topic creation is idempotent (--if-not-exists),
# but the produce steps APPEND, so re-running duplicates records and breaks the
# smoke's exact counts. For a clean slate run `docker compose down -v` first.
# All data is fictitious.
#
# Env:
#   NETWORK   docker network the Tansu broker is on (default: kotatsu_default;
#             in CI with COMPOSE_PROJECT_NAME=kt it is kt_default)
#   BOOTSTRAP kafka bootstrap server (default: tansu:9092, reachable on NETWORK)
#   KORA_URL  schema registry URL reachable on NETWORK (default: http://kora:8080)
#
# Topics: orders, events, spread, empty-topic, avro-orders, truncated (3 records with the
# first 2 deleted) and acme.prod.db2.dbz_config (compacted, so the broker routes it
# under its own name) — the last two exist to exercise the storage contract the
# #92–#97 sweep found Kotatsu had drifted from.
set -euo pipefail

NETWORK="${NETWORK:-kotatsu_default}"
BOOTSTRAP="${BOOTSTRAP:-tansu:9092}"
KORA_URL="${KORA_URL:-http://kora:8080}"
KAFKA_IMG="apache/kafka:latest"
AVRO_IMG="confluentinc/cp-schema-registry:7.6.0"

kafka() { docker run --rm --network "$NETWORK" "$KAFKA_IMG" "$@"; }
kafka_stdin() { docker run -i --rm --network "$NETWORK" "$KAFKA_IMG" "$@"; }

echo "→ waiting for Tansu to be reachable…"
for i in $(seq 1 30); do
  if kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

echo "→ creating topics (idempotent)…"
create_topic() {
  local topic="$1" partitions="$2"
  shift 2
  kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" --partitions "$partitions" \
    --replication-factor 1 "$@"
}
create_topic orders 1
create_topic events 3
create_topic empty-topic 1
create_topic avro-orders 1
create_topic truncated 1
# A compacted topic with ≥ 3 dotted components: the broker pins its routing prefix
# to its own full name rather than to the connector prefix `acme.prod.db2`, so this
# is the shape that rendered as an empty topic until #92. Nothing else in the seed
# exercises the pin — every other topic is its own prefix.
create_topic acme.prod.db2.dbz_config 1 --config cleanup.policy=compact
# Keyed records over 3 partitions: the only topic in the seed whose records are
# actually spread, which is what a cross-partition search needs (#102). `events`
# has 3 partitions but keyless records, so the sticky partitioner puts them all
# in one.
create_topic spread 3

echo "→ producing orders (3 keyed JSON records)…"
printf 'key-1:{"id":1,"item":"widget"}\nkey-2:{"id":2,"item":"gadget"}\nkey-3:{"id":3,"item":"gizmo"}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic orders --property parse.key=true --property key.separator=:

echo "→ producing events (6 keyless JSON records)…"
printf '{"n":1}\n{"n":2}\n{"n":3}\n{"n":4}\n{"n":5}\n{"n":6}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic events

echo "→ producing spread (12 keyed records over 3 partitions)…"
printf 'k-1:{"n":1}\nk-2:{"n":2}\nk-3:{"n":3}\nk-4:{"n":4}\nk-5:{"n":5}\nk-6:{"n":6}\nk-7:{"n":7}\nk-8:{"n":8}\nk-9:{"n":9}\nk-10:{"n":10}\nk-11:{"n":11}\nk-12:{"n":12}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic spread --property parse.key=true --property key.separator=:

echo "→ producing avro-orders (2 Confluent-framed Avro records)…"
printf '{"id":1,"item":"widget"}\n{"id":2,"item":"gadget"}\n' | \
  docker run -i --rm --network "$NETWORK" "$AVRO_IMG" \
  kafka-avro-console-producer --bootstrap-server "$BOOTSTRAP" --topic avro-orders \
  --property schema.registry.url="$KORA_URL" \
  --property value.schema='{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"item","type":"string"}]}'

echo "→ producing acme.prod.db2.dbz_config (2 keyed records, compacted topic)…"
printf 'cfg-a:{"connector":"db2","state":"RUNNING"}\ncfg-b:{"connector":"db2","state":"PAUSED"}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic acme.prod.db2.dbz_config --property parse.key=true --property key.separator=:

echo "→ producing truncated (3 records) then deleting below offset 2…"
printf '{"n":1}\n{"n":2}\n{"n":3}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic truncated
# `DeleteRecords` is logical: the records stay physically in the shared segment and
# only `watermark.json`'s `truncate` says they are gone, so a reader working from
# segments alone still saw them until #95. The offset file has to be a real file
# for the tool, hence the mount.
DR_DIR="$(mktemp -d)"
trap 'rm -rf "$DR_DIR"' EXIT
printf '{"partitions":[{"topic":"truncated","partition":0,"offset":2}],"version":1}\n' \
  >"$DR_DIR/delete-records.json"
# `mktemp -d` is mode 700 and the kafka image runs as a non-root user, so without
# this the tool reads the mount as AccessDenied. (Docker Desktop's file sharing
# hides it — this only fails on a native Linux daemon, i.e. in CI.)
chmod 755 "$DR_DIR"
chmod 644 "$DR_DIR/delete-records.json"
docker run --rm --network "$NETWORK" -v "$DR_DIR:/dr" "$KAFKA_IMG" \
  /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server "$BOOTSTRAP" \
  --offset-json-file /dr/delete-records.json

echo "→ creating consumer group qa-group (consume orders from beginning)…"
kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic orders --from-beginning --group qa-group --max-messages 3 --timeout-ms 10000 || true

echo "✓ seed complete"
