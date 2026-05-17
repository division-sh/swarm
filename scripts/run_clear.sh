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
API_RPC_URL="http://${HOST_HTTP_ADDR}/v1/rpc"

MODE="${1:-run-clear}"
LOG_FILE="${LOG_FILE:-/tmp/swarm-empire.log}"
PID_FILE="${PID_FILE:-/tmp/swarm-empire.pid}"
TOKEN_FILE="${TOKEN_FILE:-/tmp/swarm-empire.token}"
BINARY_PATH="${BINARY_PATH:-/tmp/swarm-empire-bin/swarm}"
START_TIMEOUT="${START_TIMEOUT:-60}"
RUN_CLEAR_RESET_IDEMPOTENCY_KEY="${RUN_CLEAR_RESET_IDEMPOTENCY_KEY:-}"

DIRECTIVE_AGENT="${DIRECTIVE_AGENT:-}"
DIRECTIVE_MESSAGE="${DIRECTIVE_MESSAGE:-}"
CORPUS_PATH="${CORPUS_PATH:-/data/test-signals-25.jsonl}"
CORPUS_GEOGRAPHY="${CORPUS_GEOGRAPHY:-US}"
RUN_CLEAR_INPUT_EVENT="${RUN_CLEAR_INPUT_EVENT:-scan.corpus_file_requested}"
RUN_CLEAR_INPUT_PAYLOAD_JSON="${RUN_CLEAR_INPUT_PAYLOAD_JSON:-}"
SWARM_API_TOKEN="${SWARM_API_TOKEN:-}"
if [[ -n "${SWARM_TOOL_GATEWAY_URL:-}" && -n "${SWARM_TOOL_GATEWAY_CONTAINER_URL:-}" ]]; then
  SWARM_TOOL_GATEWAY_URL="${SWARM_TOOL_GATEWAY_URL}"
  SWARM_TOOL_GATEWAY_CONTAINER_URL="${SWARM_TOOL_GATEWAY_CONTAINER_URL}"
else
  SWARM_TOOL_GATEWAY_URL="http://${HOST_HTTP_ADDR}"
  SWARM_TOOL_GATEWAY_CONTAINER_URL="http://host.docker.internal:${HEALTH_PORT}"
fi

export SWARM_TOOL_GATEWAY_URL
export SWARM_TOOL_GATEWAY_CONTAINER_URL

load_persisted_api_token() {
  if [[ -n "${SWARM_API_TOKEN}" ]]; then
    return
  fi
  if [[ -f "${TOKEN_FILE}" ]]; then
    SWARM_API_TOKEN="$(tr -d '[:space:]' < "${TOKEN_FILE}")"
  fi
}

ensure_api_token_for_reset() {
  load_persisted_api_token
  if [[ -z "${SWARM_API_TOKEN}" ]]; then
    SWARM_API_TOKEN="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  fi
  mkdir -p "$(dirname "${TOKEN_FILE}")"
  printf '%s\n' "${SWARM_API_TOKEN}" > "${TOKEN_FILE}"
  export SWARM_API_TOKEN
}

ensure_api_token_for_existing_runtime() {
  load_persisted_api_token
  if [[ -z "${SWARM_API_TOKEN}" ]]; then
    echo "SWARM_API_TOKEN is required for ${MODE}; run reset-dev first or set SWARM_API_TOKEN."
    exit 2
  fi
  export SWARM_API_TOKEN
}

validate_directive_inputs() {
  if [[ -z "${DIRECTIVE_AGENT}" || -z "${DIRECTIVE_MESSAGE}" ]]; then
    echo "DIRECTIVE_AGENT and DIRECTIVE_MESSAGE are required for ${MODE}."
    exit 2
  fi
}

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

bundle_fingerprint=""

wait_for_api_reachable() {
  local launcher_pid="${1:-}"
  local launcher_identity="${2:-}"
  local reachable=0
  local api_health_body='{"jsonrpc":"2.0","id":"run-clear-health","method":"health.check","params":{}}'
  for _ in $(seq 1 "${START_TIMEOUT}"); do
    if [[ -n "${launcher_pid}" ]] && launcher_exited_before_ready "${launcher_pid}" "${launcher_identity}"; then
      echo "Swarm exited before API became reachable. Current log:"
      tail -n 200 "${LOG_FILE}"
      exit 1
    fi
    health_code="$(curl -s -o /tmp/swarm-healthz.json -w '%{http_code}' "${HEALTH_URL}" || true)"
    api_code="$(curl -s -o /tmp/swarm-api-health.json -w '%{http_code}' "${API_RPC_URL}" \
      -H 'content-type: application/json' \
      -H "Authorization: Bearer ${SWARM_API_TOKEN}" \
      --data-binary "${api_health_body}" || true)"
    if [[ "${health_code}" == "200" && "${api_code}" == "200" ]]; then
      if ruby -rjson -e 'doc = JSON.parse(File.read(ARGV[0])); abort("health.check returned error") if doc["error"]; result = doc["result"] || {}; exit(result["alive"] == true && result["db_ok"] == true ? 0 : 1)' /tmp/swarm-api-health.json 2>/dev/null; then
        reachable=1
        break
      fi
    fi
    sleep 1
  done

  if [[ "${reachable}" -ne 1 ]]; then
    echo "Swarm API failed to become reachable for runtime.nuke. Current log:"
    tail -n 200 "${LOG_FILE}"
    exit 1
  fi

  echo "Swarm API reachable at http://${HOST_HTTP_ADDR}"
}

wait_for_ready() {
  local launcher_pid="${1:-}"
  local launcher_identity="${2:-}"
  local ready=0
  local api_health_body='{"jsonrpc":"2.0","id":"run-clear-health","method":"health.check","params":{}}'
  for _ in $(seq 1 "${START_TIMEOUT}"); do
    if [[ -n "${launcher_pid}" ]] && launcher_exited_before_ready "${launcher_pid}" "${launcher_identity}"; then
      echo "Swarm exited before becoming ready. Current log:"
      tail -n 200 "${LOG_FILE}"
      exit 1
    fi
    health_code="$(curl -s -o /tmp/swarm-healthz.json -w '%{http_code}' "${HEALTH_URL}" || true)"
    ready_code="$(curl -s -o /tmp/swarm-readyz.json -w '%{http_code}' "${READY_URL}" || true)"
    api_code="$(curl -s -o /tmp/swarm-api-health.json -w '%{http_code}' "${API_RPC_URL}" \
      -H 'content-type: application/json' \
      -H "Authorization: Bearer ${SWARM_API_TOKEN}" \
      --data-binary "${api_health_body}" || true)"
    if [[ "${health_code}" == "200" && "${ready_code}" == "200" && "${api_code}" == "200" ]]; then
      bundle_fingerprint="$(ruby -rjson -e 'doc = JSON.parse(File.read(ARGV[0])); abort("health.check returned error") if doc["error"]; result = doc["result"] || {}; abort("health.check not ready") unless result["ready"] == true && result["db_ok"] == true && result["runtime_ok"] == true; fingerprint = result.dig("bundle", "fingerprint").to_s; abort("health.check missing bundle fingerprint") if fingerprint.empty?; print fingerprint' /tmp/swarm-api-health.json 2>/dev/null || true)"
      if [[ -n "${bundle_fingerprint}" ]]; then
        ready=1
        break
      fi
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
}

reset_runtime_with_nuke() {
  local reset_idempotency_key="${RUN_CLEAR_RESET_IDEMPOTENCY_KEY}"
  if [[ -z "${reset_idempotency_key}" ]]; then
    reset_idempotency_key="run-clear:reset-dev:$(uuidgen | tr '[:upper:]' '[:lower:]')"
  fi

  echo "Resetting runtime with /v1/rpc runtime.nuke..."
  runtime_nuke_body="$(ruby -rjson -e 'print JSON.generate({jsonrpc: "2.0", id: "run-clear-reset-dev", method: "runtime.nuke", params: {dry_run: false, idempotency_key: ARGV[0]}})' "${reset_idempotency_key}")"
  runtime_nuke_response="$(curl -sS "${API_RPC_URL}" \
    -H 'content-type: application/json' \
    -H "Authorization: Bearer ${SWARM_API_TOKEN}" \
    --data-binary "${runtime_nuke_body}")"
  echo "${runtime_nuke_response}"
  if ! ruby -rjson -e 'doc = JSON.parse(STDIN.read); if doc["error"]; warn "runtime.nuke error: #{doc["error"]}"; exit 1; end; result = doc["result"] || {}; exit(result["ok"] == true && result["status"].to_s == "completed" && result["dry_run"] == false && result["partial_failure"] != true ? 0 : 1)' <<<"${runtime_nuke_response}"; then
    echo "runtime.nuke reset failed. Current log:"
    tail -n 200 "${LOG_FILE}"
    exit 1
  fi
}

reset_dev() {
  ensure_api_token_for_reset

  echo "Stopping local Swarm processes..."
  kill_swarm_processes

  echo "Building Swarm binary at ${BINARY_PATH}..."
  mkdir -p "$(dirname "${BINARY_PATH}")"
  go build -o "${BINARY_PATH}" ./cmd/swarm

  echo "Starting Swarm with contracts ${CONTRACTS_ROOT}..."
  : > "${LOG_FILE}"
  launcher_pid="$(
    LOG_FILE="${LOG_FILE}" BINARY_PATH="${BINARY_PATH}" CONTRACTS_ROOT="${CONTRACTS_ROOT}" HEALTH_ADDR="${HEALTH_ADDR}" SWARM_API_TOKEN="${SWARM_API_TOKEN}" python3 - <<'PY'
import os
import subprocess

log_file = os.environ["LOG_FILE"]
binary_path = os.environ["BINARY_PATH"]
contracts_root = os.environ["CONTRACTS_ROOT"]
health_addr = os.environ["HEALTH_ADDR"]

with open(os.devnull, "rb", buffering=0) as devnull, open(log_file, "ab", buffering=0) as out:
    proc = subprocess.Popen(
        [binary_path, "serve", "--contracts", contracts_root, "--health-addr", health_addr],
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
  launcher_identity="$(launcher_process_identity "${launcher_pid}" || true)"
  wait_for_api_reachable "${launcher_pid}" "${launcher_identity}"
  reset_runtime_with_nuke
  bundle_fingerprint=""
  wait_for_ready "${launcher_pid}" "${launcher_identity}"
}

start_corpus_run() {
  ensure_api_token_for_existing_runtime
  if [[ -z "${bundle_fingerprint}" ]]; then
    wait_for_ready
  fi

  run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  echo "Starting default corpus run ${run_id}..."
  if [[ -z "${RUN_CLEAR_INPUT_PAYLOAD_JSON}" ]]; then
    if [[ "${RUN_CLEAR_INPUT_EVENT}" != "scan.corpus_file_requested" ]]; then
      echo "RUN_CLEAR_INPUT_PAYLOAD_JSON is required when RUN_CLEAR_INPUT_EVENT is ${RUN_CLEAR_INPUT_EVENT}"
      exit 1
    fi
    RUN_CLEAR_INPUT_PAYLOAD_JSON="$(ruby -rjson -e 'print JSON.generate({request: {geography: ARGV[0]}, corpus_path: ARGV[1]})' "${CORPUS_GEOGRAPHY}" "${CORPUS_PATH}")"
  fi
  run_start_body="$(ruby -rjson -e 'print JSON.generate({jsonrpc: "2.0", id: "run-clear", method: "run.start", params: {run_id: ARGV[0], event_name: ARGV[1], payload: JSON.parse(ARGV[2]), bundle_ref: {fingerprint: ARGV[3]}, idempotency_key: "run-clear:#{ARGV[0]}"}})' "${run_id}" "${RUN_CLEAR_INPUT_EVENT}" "${RUN_CLEAR_INPUT_PAYLOAD_JSON}" "${bundle_fingerprint}")"
  run_response="$(curl -sS "${API_RPC_URL}" \
    -H 'content-type: application/json' \
    -H "Authorization: Bearer ${SWARM_API_TOKEN}" \
    --data-binary "${run_start_body}")"
  echo "${run_response}"
  if ! ruby -rjson -e 'doc = JSON.parse(STDIN.read); exit 1 if doc["error"]; result = doc["result"] || {}; exit(result["run_id"].to_s == ARGV[0] && result["status"].to_s == "running" ? 0 : 1)' "${run_id}" <<<"${run_response}"; then
    echo "Corpus run failed to start. Current log:"
    tail -n 200 "${LOG_FILE}"
    exit 1
  fi
}

send_directive() {
  validate_directive_inputs
  ensure_api_token_for_existing_runtime
  wait_for_ready

  echo "Sending directive to ${DIRECTIVE_AGENT}..."
  directive_body="$(ruby -rjson -e 'print JSON.generate({jsonrpc: "2.0", id: "run-clear-directive", method: "agent.send_directive", params: {agent_id: ARGV[0], directive: ARGV[1]}})' "${DIRECTIVE_AGENT}" "${DIRECTIVE_MESSAGE}")"
  directive_response="$(curl -sS "${API_RPC_URL}" \
    -H 'content-type: application/json' \
    -H "Authorization: Bearer ${SWARM_API_TOKEN}" \
    --data-binary "${directive_body}")"
  echo "${directive_response}"
  if ! ruby -rjson -e 'doc = JSON.parse(STDIN.read); exit 1 if doc["error"]; result = doc["result"] || {}; exit(result["ok"] == true ? 0 : 1)' <<<"${directive_response}"; then
    echo "Directive failed. Current log:"
    tail -n 200 "${LOG_FILE}"
    exit 1
  fi
}

case "${MODE}" in
  reset-dev)
    reset_dev
    ;;
  run-corpus)
    start_corpus_run
    ;;
  run-directive)
    send_directive
    ;;
  run-clear)
    if [[ -n "${DIRECTIVE_AGENT}" || -n "${DIRECTIVE_MESSAGE}" ]]; then
      echo "DIRECTIVE_AGENT/DIRECTIVE_MESSAGE no longer change run-clear; use run-clear-directed."
      exit 2
    fi
    reset_dev
    start_corpus_run
    ;;
  run-clear-directed)
    validate_directive_inputs
    reset_dev
    send_directive
    ;;
  *)
    echo "Unknown run_clear mode: ${MODE}"
    echo "Supported modes: reset-dev, run-corpus, run-directive, run-clear, run-clear-directed"
    exit 2
    ;;
esac
