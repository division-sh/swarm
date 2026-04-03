#!/usr/bin/env bash
set -euo pipefail

CONTRACTS_ROOT="${CONTRACTS_ROOT:-/Users/youmew/swarm/empire/contracts}"
HEALTH_ADDR="${HEALTH_ADDR:-127.0.0.1:8081}"
HEALTH_URL="http://${HEALTH_ADDR}/healthz"
API_HEALTH_URL="http://${HEALTH_ADDR}/api/health"

SWARM_DB_HOST="${SWARM_DB_HOST:-127.0.0.1}"
SWARM_DB_PORT="${SWARM_DB_PORT:-5432}"
SWARM_DB_NAME="${SWARM_DB_NAME:-swarm}"
SWARM_DB_USER="${SWARM_DB_USER:-postgres}"
SWARM_DB_PASSWORD="${SWARM_DB_PASSWORD:-postgres}"

LOG_FILE="${LOG_FILE:-/tmp/swarm-empire.log}"
PID_FILE="${PID_FILE:-/tmp/swarm-empire.pid}"
START_TIMEOUT="${START_TIMEOUT:-60}"

DIRECTIVE_AGENT="${DIRECTIVE_AGENT:-}"
DIRECTIVE_MESSAGE="${DIRECTIVE_MESSAGE:-}"
CORPUS_PATH="${CORPUS_PATH:-/data/test-signals-25.jsonl}"
CORPUS_MODE="${CORPUS_MODE:-corpus}"
CORPUS_GEOGRAPHY="${CORPUS_GEOGRAPHY:-US}"

echo "Stopping local Swarm processes..."
pkill -f 'go run ./cmd/swarm' >/dev/null 2>&1 || true
pkill -f '/tmp/go-build.*/exe/swarm' >/dev/null 2>&1 || true
if [[ -f "${PID_FILE}" ]]; then
  kill "$(cat "${PID_FILE}")" >/dev/null 2>&1 || true
  rm -f "${PID_FILE}"
fi

echo "Stopping running Docker containers..."
container_ids="$(docker ps -q 2>/dev/null || true)"
if [[ -n "${container_ids}" ]]; then
  docker stop ${container_ids} >/dev/null
fi

echo "Clearing database ${SWARM_DB_NAME}..."
PGPASSWORD="${SWARM_DB_PASSWORD}" psql \
  -h "${SWARM_DB_HOST}" \
  -p "${SWARM_DB_PORT}" \
  -U "${SWARM_DB_USER}" \
  -d "${SWARM_DB_NAME}" \
  -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
    EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
  END LOOP;
END $$;
SQL

echo "Starting Swarm with contracts ${CONTRACTS_ROOT}..."
: > "${LOG_FILE}"
nohup go run ./cmd/swarm -contracts "${CONTRACTS_ROOT}" -health-addr "${HEALTH_ADDR}" >"${LOG_FILE}" 2>&1 &
echo $! > "${PID_FILE}"

ready=0
for _ in $(seq 1 "${START_TIMEOUT}"); do
  health_code="$(curl -s -o /tmp/swarm-healthz.json -w '%{http_code}' "${HEALTH_URL}" || true)"
  api_code="$(curl -s -o /tmp/swarm-api-health.json -w '%{http_code}' "${API_HEALTH_URL}" || true)"
  if [[ "${health_code}" == "200" && "${api_code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Swarm failed to become ready. Current log:"
  tail -n 200 "${LOG_FILE}"
  exit 1
fi

echo "Swarm ready at http://${HEALTH_ADDR}"

run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
echo "Starting default corpus run ${run_id}..."
run_response="$(curl -sS "http://${HEALTH_ADDR}/api/rpc" \
  -H 'content-type: application/json' \
  --data-binary "{\"jsonrpc\":\"2.0\",\"id\":\"run-clear\",\"method\":\"run.start\",\"params\":{\"run_id\":\"${run_id}\",\"inputs\":{\"scan.requested\":{\"mode\":\"${CORPUS_MODE}\",\"geography\":\"${CORPUS_GEOGRAPHY}\",\"corpus_path\":\"${CORPUS_PATH}\"}}}}")"
echo "${run_response}"
if ! grep -q '"status":"started"' <<<"${run_response}"; then
  echo "Corpus run failed to start. Current log:"
  tail -n 200 "${LOG_FILE}"
  exit 1
fi

if [[ -n "${DIRECTIVE_AGENT}" && -n "${DIRECTIVE_MESSAGE}" ]]; then
  echo "Sending directive to ${DIRECTIVE_AGENT}..."
  ruby -rjson -e 'print JSON.generate({message: ARGV[0], kill_previous: true})' "${DIRECTIVE_MESSAGE}" | \
    curl -sS "http://${HEALTH_ADDR}/api/agents/${DIRECTIVE_AGENT}/actions/directive" \
      -H 'content-type: application/json' \
      --data-binary @-
  echo
fi
