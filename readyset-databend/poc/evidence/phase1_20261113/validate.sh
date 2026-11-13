#!/bin/bash
set -u
export PATH="$HOME/.cargo/bin:$PATH" LIBCLANG_PATH=/tmp/readyset-m1-libclang CARGO_TARGET_DIR=/data1/vmbox/readyset/target
cd /data1/vmbox/readyset-single-binary
out=/data1/vmbox/readyset-phase1-results-$(date +%Y%m%d-%H%M%S); mkdir -p $out; echo "RESULTS=$out"
B=/data1/vmbox/readyset/target/debug; E=http://192.168.191.96:8000; V=readyset-databend/poc/gateway_verify.py
step() { name=$1; shift; s=$(date +%s); "$@" > $out/$name.log 2>&1; rc=$?; printf "%-26s exit=%s %ss" $name $rc $(( $(date +%s) - s )); grep -q "^Report:" $out/$name.log && printf " checks_pass=%s" $(grep -c "^PASS" $out/$name.log); grep -h "test result" $out/$name.log | awk '{p+=$4; f+=$6; i+=$8} END {if (NR) printf " tests passed=%d failed=%d ignored=%d", p, f, i}'; grep -m1 "^Ran " $out/$name.log | tr '\n' ' '; echo; }
step fmt cargo fmt --check -p readyset-databend -p readyset-server
step clippy cargo clippy --locked -p readyset-databend -p readyset-server --all-targets --no-deps -j 8 -- -D warnings
step build cargo build --locked -p readyset-databend --bins -j 8
step rust_databend cargo test --locked -p readyset-databend -j 8
step rust_server_smoke cargo test --locked -p readyset-server --lib -j 8 -- integration::it_works
step python python3 -B -m unittest discover -s readyset-databend/poc -p "test_*.py"
step m2_gateway_inrange python3 -B $V --endpoint $E --user root --binary $B/databend-m2 --confirm-create --sql-declarations --native-queries --in-range-queries
step m2_gateway_legacy python3 -B $V --endpoint $E --user root --binary $B/databend-m2 --confirm-create
step m2_live python3 -B readyset-databend/poc/m2.py --endpoint $E --user root --rows 1000 --binary $B/databend-m2 --confirm-create
step m2_engine_budget python3 -B readyset-databend/poc/in_range_engine.py --binary $B/databend-m2
step single_inrange python3 -B $V --endpoint $E --user root --gateway-binary $B/readyset-databend --confirm-create --sql-declarations --native-queries --in-range-queries
step single_legacy python3 -B $V --endpoint $E --user root --gateway-binary $B/readyset-databend --confirm-create
step single_clients env GATEWAY_BINARY=$B/readyset-databend python3 -B -u /data1/vmbox/readyset-client-compat/client_compat.py bendsql python-0.34.0 python-0.34.3 jdbc-0.4.6 jdbc-0.4.8
cp /tmp/readyset-sb-logs/docker-e2e.log $out/docker_e2e.log
echo "docker_e2e (earlier run)    pass=$(grep -c ^PASS $out/docker_e2e.log) fail=$(grep -c ^FAIL $out/docker_e2e.log)"
grep -E "^(PASS|FAIL)" $out/single_clients.log | awk '{print $1, $2}' | sort | uniq -c
