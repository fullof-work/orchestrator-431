#!/usr/bin/env bash
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
# ---- (5) auto-resume THROUGH the proxy ------------------------------------
echo "==> pause $SID, then reuse the same exec KAT through the Proxy"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
if [ "$code" = "204" ]; then
    exec_argv_denied_through_proxy "$SID" "$EXEC_TOKEN"
    wait_traffic_stats "$SID" paused \
        || { dump_logs; fail "condition-denied Proxy exec changed paused state or traffic"; }
    RESUME_MARK="PROXY_EXEC_RESUME_$RANDOM"
    exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK" 40
    ok=""
    for _ in $(seq 1 40); do
        code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
        { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
        sleep 0.5
    done
    if [ -n "$ok" ]; then echo "==> PASS: same KAT woke the paused sandbox through Proxy (envd code=$code)"
    else dump_logs; fail "auto-resume via proxy did not complete, last code=$code"; fi
else dump_logs; fail "pause returned $code (want 204 before auto-resume check)"; fi
unset EXEC_TOKEN

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
# ---- full node pressure / Snapshot / adoption / Proxy recovery ------------
# Build while the case still uses its ordinary static Builder configuration.
# The pressure phase starts only after that Build has completed and owns just
# these two primary guest workloads; it never changes host services/resources.
build_image_template bare
stop_orchestrator
write_orchestrator_config unset controller
python3 - "$WORK/config.yaml" <<'PY_CONFIG'
from pathlib import Path
import sys
path = Path(sys.argv[1]); text = path.read_text()
text = text.replace("physical_memory: auto", "physical_memory: 1536MiB", 1)
text = text.replace("host_reserved: { memory: 1GiB, cpu: 0.5 }", "host_reserved: { memory: 512MiB, cpu: 0.5 }", 1)
needle = "  admission: { rate: 50,"
assert text.count(needle) == 1
text = text.replace(needle, """  watermarks: { low_factor: 0.70, high_factor: 0.85, emergency_factor: 0.05, operational_margin_factor: 0.125, startup_factor: 0.40 }
  pressure: { interval: 2s, failure_interval: 500ms, critical_after_rounds: 3, pause_after_rounds: 3, critical_exit_hold: 5s, red_to_yellow_hold: 5s, yellow_to_green_hold: 5s, minimum_run_time: 2s }
""" + needle, 1)
path.write_text(text)
PY_CONFIG
start_orchestrator "$WORK/orch-pressure.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"

pressure_status() {
    "$BIN/node-ctl" resource pressure --socket "$WORK/node-ctl.socket" > "$WORK/pressure-status.json"
}
pressure_create() { # primary workload, no dependence on a surviving exec
    local amount="$1" body
    body=$(python3 - "$TEMPLATE" "$SCRIPT_DIR/workload.py" "$amount" <<'PY_CREATE'
import json, sys
from pathlib import Path
program = Path(sys.argv[2]).read_text()
print(json.dumps(dict(templateID=sys.argv[1], timeout=600, metadata={
    "kuasar-sandbox.resource": json.dumps(dict(capacity=dict(cpu=1, memory="1GiB"), allocatable=dict(cpu=1, memory="64MiB"), startup=dict(memory="256MiB"))),
    "kuasar-sandbox.launch": json.dumps(dict(exec="/bin/sh", args=["-c", "exec python3 -u -c \"$1\"", "pressure", program], restart="never", env={"WL_MODE":"hold", "WL_DURATION":"600", "WL_RMAX_MIB":sys.argv[3], "WL_START_GATE":"/tmp/pressure.start", "WL_START_GATE_TIMEOUT":"120"}))
})))
PY_CREATE
    )
    code=$(req POST /sandboxes "$AK" "$body")
    [ "$code" = 201 ] || fail "pressure create=$code"
    PRESSURE_SID=$(json_field "$WORK/resp.body" sandboxID)
    wait_sandbox_state "$PRESSURE_SID" running 600 || fail "pressure primary did not start"
}
pressure_guest_state() {
    "$BIN/sandbox-ctl" exec --run-root "$WORK/run/sandboxes" --sandbox-id "$1" -- /bin/cat /tmp/pressure-state.json
}
pressure_create 384
FIRST=$PRESSURE_SID
FIRST_KAT=$(issue_exec_session "$FIRST" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")
"$BIN/sandbox-ctl" exec --run-root "$WORK/run/sandboxes" --sandbox-id "$FIRST" -- /bin/touch /tmp/pressure.start
ready=""
for _ in $(seq 1 300); do
    if pressure_guest_state "$FIRST" > "$WORK/pressure-before.json" 2>/dev/null; then ready=1; break; fi
    sleep 0.1
done
[ -n "$ready" ] || fail "first held-memory workload did not become ready"
# Hold the existing capture boundary to observe Q before any memory is released.
printf '%s\n' "$FIRST" > "$PAUSE_BARRIER_TARGET"
rm -f "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
pressure_create 576
SECOND=$PRESSURE_SID
"$BIN/sandbox-ctl" exec --run-root "$WORK/run/sandboxes" --sandbox-id "$SECOND" -- /bin/touch /tmp/pressure.start
for _ in $(seq 1 600); do [ ! -e "$PAUSE_BARRIER_REACHED" ] || break; sleep 0.1; done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail "sustained demand did not select the longest running guest"
pressure_status
python3 - "$WORK/pressure-status.json" <<'PY_CAPTURE'
import json, sys
p=json.load(open(sys.argv[1]))
assert p["zone"] == "critical" and p["capturing"] == 1 and p["pending_recoveries"] == 1, p
assert p["reserved_memory"] > 0, p
PY_CAPTURE
# Drain only this test controller while capture is already accepted, so slow
# cleanup cannot let automatic recovery win the adoption assertion.
"$BIN/node-ctl" resource drain --socket "$WORK/sandbox-resource.sock" >/dev/null
: > "$PAUSE_BARRIER_RELEASE"
wait_sandbox_state "$FIRST" paused 600 || fail "resource capture did not finish"
# The lifecycle fence serializes adoption with cleanup. Drain postpones only
# background launch acceptance; adoption performs no capture or new RunID.
SNAPSHOT_PAIR=$(checkpoint_pair "$FIRST" snapshot "$WORK/lib/sandboxes/$FIRST/checkpoint/$FIRST.snapshot")
code=$(req POST "/sandboxes/$FIRST/pause" "$AK")
[ "$code" = 204 ] || fail "resource Pause adoption=$code"
pressure_status
python3 - "$WORK/pressure-status.json" "$FIRST" <<'PY_ADOPT'
import json, sys
p=json.load(open(sys.argv[1])); row=next(r for r in p["sandboxes"] if r["sandbox_id"]==sys.argv[2])
assert p["zone"]=="critical" and p["pending_recoveries"]==0, p
assert row["state"]=="paused" and row["pause_reason"]=="explicit" and not row["recovery_obligation"], row
PY_ADOPT
wait_paused_cleanup "$FIRST" 600 || fail "resource Pause cleanup incomplete"
[ "$(checkpoint_pair "$FIRST" snapshot "$WORK/lib/sandboxes/$FIRST/checkpoint/$FIRST.snapshot")" = "$SNAPSHOT_PAIR" ] || fail "adoption changed Snapshot source"
"$BIN/node-ctl" resource drain --disable --socket "$WORK/sandbox-resource.sock" >/dev/null
code=$(req POST "/sandboxes/$FIRST/pause" "$AK")
[ "$code" = 409 ] || fail "ordinary repeated Pause=$code"
# A real request remains parked across the critical policy refusal. It is sent
# once; there is no replay of a delivered operation or a killed exec session.
exec_through_proxy_connect "$FIRST" "$FIRST_KAT" "PRESSURE_RESTORED_$RANDOM" 1 100 &
PRESSURE_EXEC_PID=$!
PIDS+=("$PRESSURE_EXEC_PID")
wait_proxy_traffic_stats "$FIRST" parking || fail "ordinary critical request did not park"
code=$(req POST "/sandboxes/$SECOND/pause" "$AK")
[ "$code" = 204 ] || fail "second explicit Pause=$code"
wait "$PRESSURE_EXEC_PID" || fail "parked Proxy Wake did not recover adopted Snapshot"
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$PRESSURE_EXEC_PID" ] || PIDS[$i]=""; done
wait_sandbox_state "$FIRST" running 600 || fail "adopted Snapshot did not resume"
restored=""
for _ in $(seq 1 100); do
    pressure_guest_state "$FIRST" > "$WORK/pressure-after.json"
    if python3 - "$WORK/pressure-before.json" "$WORK/pressure-after.json" <<'PY_MEMORY'
import json, sys
before,after=[json.load(open(p)) for p in sys.argv[1:]]
assert before["nonce"]==after["nonce"] and before["held"]==after["held"]==384*(1<<20)
raise SystemExit(0 if after["tick"]>before["tick"] else 1)
PY_MEMORY
    then restored=1; break; fi
    sleep 0.2
done
[ -n "$restored" ] || fail "primary held memory/identity did not survive Snapshot"
# The saved baseline exceeds the ordinary startup pool; runtime admission must
# have restored the full Snapshot budget through the serialized saved lane.
grep -Eq 'restore BudgetAtSnapshot reserved=' "$WORK/orch-pressure.log" ||     journalctl KUASAR_SANDBOX_ID="$FIRST" --no-pager -o cat > "$WORK/pressure-restore.journal"
python3 - "$WORK/orch-pressure.log" "$WORK/pressure-restore.journal" <<'PY_BUDGET'
import re, sys
from pathlib import Path
text="\n".join(Path(p).read_text() for p in sys.argv[1:] if Path(p).exists())
budgets=[int(n) for n in re.findall(r'restore BudgetAtSnapshot reserved=(\d+)',text)]
assert any(n>int(896*(1<<20)*.4) and n<=896*(1<<20) for n in budgets), budgets
PY_BUDGET
for sid in "$FIRST" "$SECOND"; do
    code=$(req DELETE "/sandboxes/$sid" "$AK"); [ "$code" = 204 ] || fail "pressure delete=$code"
    wait_sandbox_state "$sid" missing 600 || fail "pressure delete finalization"
done
pressure_status
cat "$WORK/pressure-status.json" > "$WORK/pressure-final.log"
stop_proxy_master
echo "PASS orchestrator.proxy-wake.sh"
