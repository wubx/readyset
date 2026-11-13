#!/bin/bash
# Phase 1 Docker + least-privilege end-to-end check for the readyset-databend image.
#
# Creates a NEW readyset_gateway_dk_biz_* database, a temporary role/user (dropped at the
# end) and a container + volume this script owns. Needs docker, curl, python3 and bendsql.
#
#   DATABEND_HOST=192.168.191.96 DATABEND_PORT=8000 IMAGE=readyset-databend:phase1 \
#     bash readyset-databend/docker/e2e.sh
#
# Environment:
#   DATABEND_HOST / DATABEND_PORT   Databend reachable from this host AND from the container
#   DATABEND_ADMIN_USER / _PASSWORD account that may create a database, role and user (root, "")
#   IMAGE                           image under test (readyset-databend:phase1)
#   BENDSQL                         bendsql binary (bendsql on PATH)
#   LOCAL_PORT                      host port published for the container (18101)
set -u
: "${DATABEND_HOST:?set DATABEND_HOST}"
DATABEND_PORT=${DATABEND_PORT:-8000}
B=${BENDSQL:-bendsql}
IMAGE=${IMAGE:-readyset-databend:phase1}
LP=${LOCAL_PORT:-18101}
admin_dsn="databend://${DATABEND_ADMIN_USER:-root}:${DATABEND_ADMIN_PASSWORD:-}@$DATABEND_HOST:$DATABEND_PORT/default?sslmode=disable"
ts=$(date +%Y%m%d_%H%M%S)
biz=readyset_gateway_dk_biz_$ts
user=readyset_dk_$ts; role=readyset_dk_role_$ts; name=readyset-dk-$ts
work=$(mktemp -d /tmp/readyset-dk.XXXXXX); chmod 700 "$work"
head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' > "$work/pw"; pw=$(cat "$work/pw")
root() { BENDSQL_DSN="$admin_dsn" $B --output tsv 2>&1; }
via() { BENDSQL_DSN="databend://$user:$pw@127.0.0.1:$LP/$biz?sslmode=disable" $B --output tsv 2>&1; }
auth="$user:$pw"
health() { curl -s -u "$auth" http://127.0.0.1:$LP/health; }
vol=""
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1
  [ -n "$vol" ] && docker volume rm "$vol" >/dev/null 2>&1
  printf "DROP USER IF EXISTS %s;\nDROP ROLE IF EXISTS %s;\n" "$user" "$role" | root >/dev/null
  rm -rf "$work"
}
trap cleanup EXIT
fail() { echo "FAIL $*"; docker logs --tail 40 "$name" 2>&1 | sed 's/^/  log: /'; exit 1; }
pass() { echo "PASS $*"; }
echo "biz=$biz user=$user container=$name image=$IMAGE"
root <<SQL >/dev/null
CREATE DATABASE $biz;
CREATE TABLE $biz.source_rows (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL);
INSERT INTO $biz.source_rows SELECT CAST(number AS BIGINT), concat('p-', to_string(number)), CAST(number % 10 AS BIGINT), IF(number % 2 = 0, NULL, 'n') FROM numbers(100);
CREATE ROLE $role;
GRANT SELECT ON $biz.* TO ROLE $role;
GRANT CREATE ON $biz.* TO ROLE $role;
CREATE USER $user IDENTIFIED BY '$pw' WITH DEFAULT_ROLE = '$role';
GRANT ROLE $role TO $user;
SQL
cat > "$work/readyset-databend.toml" <<EOF
confirm_create = true
[upstream]
url = "http://$DATABEND_HOST:$DATABEND_PORT"
user = "$user"
[listen]
http = "0.0.0.0:8001"
[replication]
tables = ["$biz.source_rows"]
poll_interval = "1s"
max_staleness = "10s"
[storage]
dir = "/var/lib/readyset-databend"
EOF
printf "DATABEND_PASSWORD=%s\n" "$pw" > "$work/env"; chmod 600 "$work/env"
chmod 644 "$work/readyset-databend.toml"
docker run -d --name "$name" --env-file "$work/env" -p 127.0.0.1:$LP:8001 \
  -v "$work/readyset-databend.toml:/etc/readyset-databend/readyset-databend.toml:ro" \
  "$IMAGE" >/dev/null || fail "docker run"
vol=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/lib/readyset-databend"}}{{.Name}}{{end}}{{end}}' "$name")
for i in $(seq 1 120); do
  curl -fsS http://127.0.0.1:$LP/readyz >/dev/null 2>&1 && break
  [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ] || fail "container exited"
  sleep 1
done
curl -fsS http://127.0.0.1:$LP/readyz >/dev/null || fail "not ready in 120s"
pass "least-privilege container bootstrapped and ready (${i}s)"
[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$LP/health)" = 401 ] && pass "authenticated endpoints require credentials"
health | python3 -c "import json,sys; h=json.load(sys.stdin); assert h['single_process'] and h['checkpoint']>=1, h; print('PASS health', {k:h[k] for k in ['checkpoint','rows','instance_id']})" || fail "health"
printf "SELECT 'stream', name FROM system.streams WHERE database='%s';\nSELECT 'tables', name FROM system.tables WHERE database='%s' ORDER BY name;\nSELECT 'databases', count(*) FROM system.databases WHERE name LIKE 'readyset_meta%%';\n" "$biz" "$biz" | root
echo "CREATE CACHE by_id FROM SELECT id, payload, score, note FROM source_rows WHERE id = ?;" | via >/dev/null || fail "create cache"
echo "CREATE CACHE by_score FROM SELECT id, payload FROM source_rows WHERE score IN (?, ?);" | via >/dev/null || fail "create cache 2"
before=$(health | python3 -c "import json,sys; print(json.load(sys.stdin)['counters']['readyset'])")
out=$(echo "SELECT id, payload, score, note FROM source_rows WHERE id = 7;" | via)
[ "$out" = "$(printf '7\tp-7\t7\tn')" ] || fail "point result: $out"
n=$(echo "SELECT id, payload FROM source_rows WHERE score IN (3, 4);" | via | wc -l)
[ "$n" = 20 ] || fail "IN rows: $n"
after=$(health | python3 -c "import json,sys; print(json.load(sys.stdin)['counters']['readyset'])")
[ $((after - before)) = 2 ] && pass "bendsql reads served locally (point + IN, 20 rows)" || fail "local counter $before -> $after"
printf "INSERT INTO %s.source_rows VALUES (1000, 'fresh', 3, 'w');\nUPDATE %s.source_rows SET payload='moved' WHERE id = 7;\n" "$biz" "$biz" | root >/dev/null
cp0=$(health | python3 -c "import json,sys; print(json.load(sys.stdin)['checkpoint'])")
for i in $(seq 1 30); do
  cp=$(health | python3 -c "import json,sys; h=json.load(sys.stdin); print(h['checkpoint'] if h['ready'] else 0)")
  [ "$cp" -gt "$cp0" ] && break; sleep 0.5
done
out=$(echo "SELECT id, payload, score, note FROM source_rows WHERE id = 7;" | via)
[ "$out" = "$(printf '7\tmoved\t7\tn')" ] && pass "external UPDATE replicated" || fail "update: $out"
n=$(echo "SELECT id, payload FROM source_rows WHERE score IN (3, 4);" | via | wc -l)
[ "$n" = 21 ] && pass "external INSERT replicated into IN cache" || fail "insert IN rows: $n"
echo "INSERT INTO source_rows VALUES (2000, 'denied', 1, NULL);" | via | grep -q "Permission denied" && pass "writes through the gateway keep Databend permissions (INSERT denied)" || fail "write not denied"
docker stop -t 30 "$name" >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$name")
[ "$code" = 0 ] && pass "SIGTERM clean exit" || fail "exit code $code"
# Write while the container is down, then restart with the same volume (same instance_id):
# snapshot AT the Stream offset + the pending Stream changes must include that write.
printf "INSERT INTO %s.source_rows VALUES (3000, 'while-down', 5, NULL);\nDELETE FROM %s.source_rows WHERE id = 1000;\nUPDATE %s.source_rows SET id = 4000 WHERE id = 8;\n" "$biz" "$biz" "$biz" | root >/dev/null
docker start "$name" >/dev/null || fail "restart"
for i in $(seq 1 120); do curl -fsS http://127.0.0.1:$LP/readyz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://127.0.0.1:$LP/readyz >/dev/null || fail "not ready after restart"
streams=$(printf "SELECT count(*) FROM system.streams WHERE database='%s';\n" "$biz" | root)
[ "$streams" = 1 ] && pass "restart reused its own Stream (still exactly 1 Stream, SELECT+CREATE role)" || fail "streams after restart: $streams"
echo "CREATE CACHE by_id FROM SELECT id, payload, score, note FROM source_rows WHERE id = ?;" | via >/dev/null || fail "create cache after restart"
out=$(echo "SELECT id, payload, score, note FROM source_rows WHERE id = 3000;" | via)
[ "$out" = "$(printf '3000\twhile-down\t5\tNULL')" ] && pass "write made while down is present after restart" || fail "after restart 3000: $out"
out=$(echo "SELECT id, payload, score, note FROM source_rows WHERE id = 1000;" | via)
[ -z "$out" ] && pass "delete made while down is applied after restart" || fail "after restart 1000: $out"
out=$(echo "SELECT id, payload, score, note FROM source_rows WHERE id = 7;" | via)
[ "$out" = "$(printf '7\tmoved\t7\tn')" ] && pass "earlier replicated update survives restart" || fail "after restart 7: $out"
out=$(printf "SELECT id, payload FROM source_rows WHERE id = 8;\nSELECT id, payload FROM source_rows WHERE id = 4000;\n" | via)
[ "$out" = "$(printf '4000\tp-8')" ] && pass "primary-key change made while down is applied after restart (8 -> 4000)" || fail "after restart 8/4000: $out"
docker stop -t 30 "$name" >/dev/null
docker run --rm --entrypoint /bin/sh -v "$vol":/data "$IMAGE" -c 'echo "instance_id=$(cat /data/instance_id)"; cat /data/runs/*/final_status.json' 2>/dev/null | head -24
echo "DONE"
