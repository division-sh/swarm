#!/usr/bin/env bash
set -euo pipefail

CONTRACTS_ROOT="${CONTRACTS_ROOT:-/Users/youmew/swarm/empire/contracts}"
HEALTH_ADDR="${HEALTH_ADDR:-0.0.0.0:8081}"
HEALTH_PORT="${HEALTH_ADDR##*:}"
HEALTH_HOST="${HEALTH_ADDR%:*}"
if [[ "${HEALTH_HOST}" == "${HEALTH_ADDR}" ]]; then
  HEALTH_HOST="127.0.0.1"
fi
if [[ -z "${HEALTH_HOST}" || "${HEALTH_HOST}" == "0.0.0.0" || "${HEALTH_HOST}" == "::" ]]; then
  HEALTH_HOST="127.0.0.1"
fi
HOST_HTTP_ADDR="${HEALTH_HOST}:${HEALTH_PORT}"
HEALTH_URL="http://${HOST_HTTP_ADDR}/healthz"
READY_URL="http://${HOST_HTTP_ADDR}/readyz"
API_HEALTH_URL="http://${HOST_HTTP_ADDR}/api/health"

SWARM_DB_HOST="${SWARM_DB_HOST:-127.0.0.1}"
SWARM_DB_PORT="${SWARM_DB_PORT:-5432}"
SWARM_DB_NAME="${SWARM_DB_NAME:-swarm}"
SWARM_DB_USER="${SWARM_DB_USER:-postgres}"
SWARM_DB_PASSWORD="${SWARM_DB_PASSWORD:-postgres}"

LOG_FILE="${LOG_FILE:-/tmp/swarm-empire.log}"
PID_FILE="${PID_FILE:-/tmp/swarm-empire.pid}"
BINARY_PATH="${BINARY_PATH:-/tmp/swarm-empire-bin/swarm}"
START_TIMEOUT="${START_TIMEOUT:-60}"

DIRECTIVE_AGENT="${DIRECTIVE_AGENT:-}"
DIRECTIVE_MESSAGE="${DIRECTIVE_MESSAGE:-}"
CORPUS_PATH="${CORPUS_PATH:-/data/test-signals-25.jsonl}"
CORPUS_GEOGRAPHY="${CORPUS_GEOGRAPHY:-US}"
RUN_CLEAR_INPUT_EVENT="${RUN_CLEAR_INPUT_EVENT:-scan.corpus_file_requested}"
RUN_CLEAR_INPUT_PAYLOAD_JSON="${RUN_CLEAR_INPUT_PAYLOAD_JSON:-}"
SWARM_OPERATOR_AUTH_TOKEN="${SWARM_OPERATOR_AUTH_TOKEN:-$(uuidgen | tr '[:upper:]' '[:lower:]')}"
SWARM_BUILDER_AUTH_TOKEN="${SWARM_BUILDER_AUTH_TOKEN:-$(uuidgen | tr '[:upper:]' '[:lower:]')}"
if [[ -n "${SWARM_TOOL_GATEWAY_URL:-}" && -n "${SWARM_TOOL_GATEWAY_CONTAINER_URL:-}" ]]; then
  SWARM_TOOL_GATEWAY_URL="${SWARM_TOOL_GATEWAY_URL}"
  SWARM_TOOL_GATEWAY_CONTAINER_URL="${SWARM_TOOL_GATEWAY_CONTAINER_URL}"
else
  SWARM_TOOL_GATEWAY_URL="http://${HOST_HTTP_ADDR}"
  SWARM_TOOL_GATEWAY_CONTAINER_URL="http://host.docker.internal:${HEALTH_PORT}"
fi

export SWARM_OPERATOR_AUTH_TOKEN
export SWARM_BUILDER_AUTH_TOKEN
export SWARM_TOOL_GATEWAY_URL
export SWARM_TOOL_GATEWAY_CONTAINER_URL

kill_swarm_processes() {
  local pids=""
  pids+=" $(pgrep -f 'go run ./cmd/swarm' 2>/dev/null || true)"
  pids+=" $(pgrep -f '/tmp/go-build.*/exe/swarm' 2>/dev/null || true)"
  pids+=" $(pgrep -f '/Library/Caches/go-build/.*/swarm' 2>/dev/null || true)"
  pids+=" $(pgrep -x 'swarm' 2>/dev/null || true)"
  pids+=" $(pgrep -f '(^|[ /])swarm([[:space:]]|$)' 2>/dev/null || true)"
  pids+=" $(lsof -tiTCP:${HEALTH_PORT} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -f "${PID_FILE}" ]]; then
    pids+=" $(cat "${PID_FILE}" 2>/dev/null || true)"
    rm -f "${PID_FILE}"
  fi
  pids="$(printf '%s\n' ${pids} | awk 'NF {print $1}' | sort -u)"
  if [[ -z "${pids}" ]]; then
    return
  fi
  echo "Killing Swarm PIDs: ${pids//$'\n'/ }"
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    kill "${pid}" >/dev/null 2>&1 || true
  done <<<"${pids}"
  sleep 1
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  done <<<"${pids}"
  sleep 1
  if pgrep -x 'swarm' >/dev/null 2>&1; then
    echo "Failed to stop all swarm processes:"
    pgrep -alf '(^|[ /])swarm([[:space:]]|$)' || true
    exit 1
  fi
}

echo "Stopping local Swarm processes..."
kill_swarm_processes

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

echo "Building Swarm binary at ${BINARY_PATH}..."
mkdir -p "$(dirname "${BINARY_PATH}")"
go build -o "${BINARY_PATH}" ./cmd/swarm

echo "Starting Swarm with contracts ${CONTRACTS_ROOT}..."
: > "${LOG_FILE}"
launcher_pid="$(
  LOG_FILE="${LOG_FILE}" BINARY_PATH="${BINARY_PATH}" CONTRACTS_ROOT="${CONTRACTS_ROOT}" HEALTH_ADDR="${HEALTH_ADDR}" SWARM_OPERATOR_AUTH_TOKEN="${SWARM_OPERATOR_AUTH_TOKEN}" SWARM_BUILDER_AUTH_TOKEN="${SWARM_BUILDER_AUTH_TOKEN}" python3 - <<'PY'
import os
import subprocess
import sys

log_file = os.environ["LOG_FILE"]
binary_path = os.environ["BINARY_PATH"]
contracts_root = os.environ["CONTRACTS_ROOT"]
health_addr = os.environ["HEALTH_ADDR"]

with open(os.devnull, "rb", buffering=0) as devnull, open(log_file, "ab", buffering=0) as out:
    proc = subprocess.Popen(
        [binary_path, "-contracts", contracts_root, "-health-addr", health_addr],
        stdin=devnull,
        stdout=out,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
print(proc.pid)
PY
)"
echo "${launcher_pid}" > "${PID_FILE}"

launcher_process_state() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] || return 1
  ps -o state= -p "${pid}" 2>/dev/null | head -n 1 | sed 's/^ *//'
}

launcher_process_identity() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] || return 1
  local start command
  start="$(ps -o lstart= -p "${pid}" 2>/dev/null | head -n 1 | sed 's/^ *//')"
  command="$(ps -o command= -p "${pid}" 2>/dev/null | head -n 1 | sed 's/^ *//')"
  if [[ -z "${start}" || -z "${command}" ]]; then
    return 1
  fi
  printf '%s\t%s\n' "${start}" "${command}"
}

launcher_identity="$(launcher_process_identity "${launcher_pid}" || true)"

launcher_exited_before_ready() {
  local pid="${1:-}"
  local expected_identity="${2:-}"
  [[ -n "${pid}" ]] || return 1
  local current_state current_identity
  current_state="$(launcher_process_state "${pid}" || true)"
  if [[ -z "${current_state}" ]]; then
    return 0
  fi
  if [[ "${current_state}" == *Z* ]]; then
    return 0
  fi
  if [[ -z "${expected_identity}" ]]; then
    return 1
  fi
  current_identity="$(launcher_process_identity "${pid}" || true)"
  if [[ -z "${current_identity}" ]]; then
    return 0
  fi
  if [[ "${current_identity}" != "${expected_identity}" ]]; then
    return 0
  fi
  return 1
}

ready=0
for _ in $(seq 1 "${START_TIMEOUT}"); do
  if launcher_exited_before_ready "${launcher_pid}" "${launcher_identity}"; then
    echo "Swarm exited before becoming ready. Current log:"
    tail -n 200 "${LOG_FILE}"
    exit 1
  fi
  health_code="$(curl -s -o /tmp/swarm-healthz.json -w '%{http_code}' "${HEALTH_URL}" || true)"
  ready_code="$(curl -s -o /tmp/swarm-readyz.json -w '%{http_code}' "${READY_URL}" || true)"
  api_code="$(curl -s -o /tmp/swarm-api-health.json -w '%{http_code}' "${API_HEALTH_URL}" \
    -H "Authorization: Bearer ${SWARM_OPERATOR_AUTH_TOKEN}" || true)"
  if [[ "${health_code}" == "200" && "${ready_code}" == "200" && "${api_code}" == "200" ]]; then
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

serving_pid="$(lsof -tiTCP:${HEALTH_PORT} -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
if [[ -n "${serving_pid}" ]]; then
  echo "${serving_pid}" > "${PID_FILE}"
fi

echo "Swarm ready at http://${HOST_HTTP_ADDR}"

run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
echo "Starting default corpus run ${run_id}..."
if [[ -z "${RUN_CLEAR_INPUT_PAYLOAD_JSON}" ]]; then
  if [[ "${RUN_CLEAR_INPUT_EVENT}" != "scan.corpus_file_requested" ]]; then
    echo "RUN_CLEAR_INPUT_PAYLOAD_JSON is required when RUN_CLEAR_INPUT_EVENT is ${RUN_CLEAR_INPUT_EVENT}"
    exit 1
  fi
  RUN_CLEAR_INPUT_PAYLOAD_JSON="$(ruby -rjson -e 'print JSON.generate({request: {geography: ARGV[0]}, corpus_path: ARGV[1]})' "${CORPUS_GEOGRAPHY}" "${CORPUS_PATH}")"
fi
run_start_body="$(ruby -rjson -e 'print JSON.generate({jsonrpc: "2.0", id: "run-clear", method: "run.start", params: {run_id: ARGV[0], inputs: {ARGV[1] => JSON.parse(ARGV[2])}}})' "${run_id}" "${RUN_CLEAR_INPUT_EVENT}" "${RUN_CLEAR_INPUT_PAYLOAD_JSON}")"
run_response="$(curl -sS "http://${HOST_HTTP_ADDR}/api/rpc" \
  -H 'content-type: application/json' \
  -H "Authorization: Bearer ${SWARM_BUILDER_AUTH_TOKEN}" \
  --data-binary "${run_start_body}")"
echo "${run_response}"
if ! grep -q '"status":"started"' <<<"${run_response}"; then
  echo "Corpus run failed to start. Current log:"
  tail -n 200 "${LOG_FILE}"
  exit 1
fi

if [[ -n "${DIRECTIVE_AGENT}" && -n "${DIRECTIVE_MESSAGE}" ]]; then
  echo "Sending directive to ${DIRECTIVE_AGENT}..."
  ruby -rjson -e 'print JSON.generate({message: ARGV[0], kill_previous: true})' "${DIRECTIVE_MESSAGE}" | \
    curl -sS "http://${HOST_HTTP_ADDR}/api/agents/${DIRECTIVE_AGENT}/actions/directive" \
      -H 'content-type: application/json' \
      -H "Authorization: Bearer ${SWARM_OPERATOR_AUTH_TOKEN}" \
      --data-binary @-
  echo
fi
