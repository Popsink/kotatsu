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
  kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}
create_topic orders 1
create_topic events 3
create_topic empty-topic 1
create_topic avro-orders 1

echo "→ producing orders (3 keyed JSON records)…"
printf 'key-1:{"id":1,"item":"widget"}\nkey-2:{"id":2,"item":"gadget"}\nkey-3:{"id":3,"item":"gizmo"}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic orders --property parse.key=true --property key.separator=:

echo "→ producing events (6 keyless JSON records)…"
printf '{"n":1}\n{"n":2}\n{"n":3}\n{"n":4}\n{"n":5}\n{"n":6}\n' | \
  kafka_stdin /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic events

echo "→ producing avro-orders (2 Confluent-framed Avro records)…"
printf '{"id":1,"item":"widget"}\n{"id":2,"item":"gadget"}\n' | \
  docker run -i --rm --network "$NETWORK" "$AVRO_IMG" \
  kafka-avro-console-producer --bootstrap-server "$BOOTSTRAP" --topic avro-orders \
  --property schema.registry.url="$KORA_URL" \
  --property value.schema='{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"item","type":"string"}]}'

echo "→ creating consumer group qa-group (consume orders from beginning)…"
kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic orders --from-beginning --group qa-group --max-messages 3 --timeout-ms 10000 || true

echo "✓ seed complete"
