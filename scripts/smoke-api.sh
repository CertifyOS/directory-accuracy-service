#!/bin/bash
# Live smoke test of the deployed api group. Exit 0: every case held. Exit 2: the service is up but
# a dependency grant is still pending (labelled, never silent). Exit 1: a real failure.
#
# Usage: [TOKEN_FILE=/path/to/token] [LB_IP=1.2.3.4] [TENANT=tenant-id-001] scripts/smoke-api.sh
#   TOKEN_FILE  file holding a user's Auth0 access token for the staging tenant (audience
#               https://ng-api-stg.certifyos.com/). Without it only the unauthenticated cases run.
#   LB_IP       reach the load balancer by IP with the Host header until the DNS record exists;
#               the managed certificate is not valid yet then, so TLS verification is skipped.
#   HOST        hostname (default directory-accuracy.internal.certifyos.com)
#   TENANT      tenant-id header for the authenticated cases (default tenant-id-001)
# Never prints the token.
set -uo pipefail
HOST=${HOST:-directory-accuracy.internal.certifyos.com}
TENANT=${TENANT:-tenant-id-001}
BASE="https://$HOST"
CURL=(curl -s -m 20)
if [ -n "${LB_IP:-}" ]; then CURL+=(-k --resolve "$HOST:443:$LB_IP"); fi
fail=0; pending=0
json() { python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get(sys.argv[1])))' "$1" 2>/dev/null; }

echo "== 1. liveness through the load balancer"
code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$BASE/q/health/live")
echo "GET /q/health/live -> $code (expect 200)"; [ "$code" = "200" ] || fail=1

echo "== 2. readiness names the pending hook-ups"
body=$("${CURL[@]}" -w '\n%{http_code}' "$BASE/q/health/ready"); code=$(echo "$body" | tail -1); body=$(echo "$body" | sed '$d')
echo "GET /q/health/ready -> $code"
echo "$body" | python3 -c '
import sys,json
d=json.load(sys.stdin)
for c in d.get("checks",[]): print("   ", c.get("status"), c.get("name"), c.get("data") or "")
down=[c["name"] for c in d.get("checks",[]) if c.get("status")!="UP"]
sys.exit(0 if down==["api-layer"] or down==[] else 1)' || { echo "FAIL: a check other than api-layer is DOWN"; fail=1; }

echo "== 3. no token is 401 on an operator endpoint"
code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -H "tenant-id: $TENANT" "$BASE/v1/vendor-exports/schedules")
echo "GET /v1/vendor-exports/schedules (no token) -> $code (expect 401)"; [ "$code" = "401" ] || fail=1

if [ -z "${TOKEN_FILE:-}" ]; then
  echo "== 4-5. skipped: set TOKEN_FILE for the authenticated cases"
else
  TOKEN=$(tr -d '\n' < "$TOKEN_FILE")
  H=(-H "Authorization: Bearer $TOKEN" -H "tenant-id: $TENANT")
  echo "== 4. a tenant member reads an empty schedule list"
  r=$("${CURL[@]}" "${H[@]}" -w '\n%{http_code}' "$BASE/v1/vendor-exports/schedules"); code=$(echo "$r" | tail -1); rbody=$(echo "$r" | sed '$d')
  case "$code" in
    200) echo "GET schedules -> 200 $rbody"; [ "$(echo "$rbody" | json items)" = "[]" ] || { echo "FAIL: items not empty"; fail=1; } ;;
    503) echo "GET schedules -> 503 $(echo "$rbody" | json code) (DAL IAP grant pending, ask A2)"; pending=1 ;;
    *)   echo "FAIL: GET schedules -> $code $rbody"; fail=1 ;;
  esac
  echo "== 5. the tick runs inline"
  r=$("${CURL[@]}" "${H[@]}" -w '\n%{http_code}' -X POST "$BASE/v1/vendor-exports/tick"); code=$(echo "$r" | tail -1); rbody=$(echo "$r" | sed '$d')
  case "$code" in
    200) echo "POST tick -> 200 schedulesDue=$(echo "$rbody" | json schedulesDue) batchesCreated=$(echo "$rbody" | json batchesCreated)" ;;
    503) echo "POST tick -> 503 $(echo "$rbody" | json code) (DAL IAP grant pending, ask A2)"; pending=1 ;;
    *)   echo "FAIL: POST tick -> $code $rbody"; fail=1 ;;
  esac
fi

if [ "$fail" = 1 ]; then echo "RESULT: FAIL"; exit 1; fi
if [ "$pending" = 1 ]; then echo "RESULT: UP, grant pending"; exit 2; fi
echo "RESULT: PASS"
