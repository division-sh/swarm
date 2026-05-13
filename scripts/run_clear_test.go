package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type runClearConfig struct {
	apiToken         string
	operatorToken    string
	builderToken     string
	directiveAgent   string
	directiveMessage string
	healthAddr       string
	hostGatewayURL   string
	containerURL     string
	launcherMode     string
	launcherLogText  string
	psMode           string
	readyCode        string
	apiHealthCode    string
	startTimeout     string
}

func TestRunClear_ProvisionsBuilderAuthTokenAndUsesBearerForRPC(t *testing.T) {
	result := runRunClear(t, runClearConfig{})

	if strings.TrimSpace(result.operatorToken) == "" {
		t.Fatal("expected helper to provision SWARM_OPERATOR_AUTH_TOKEN")
	}
	if strings.TrimSpace(result.builderToken) == "" {
		t.Fatal("expected helper to provision SWARM_BUILDER_AUTH_TOKEN")
	}
	wantOperatorHeader := "Authorization: Bearer " + result.operatorToken
	if !strings.Contains(result.healthHeaders, wantOperatorHeader) {
		t.Fatalf("api health headers = %q, want %q", result.healthHeaders, wantOperatorHeader)
	}
	if got := strings.TrimSpace(result.healthURL); got != "http://127.0.0.1:8081/api/health" {
		t.Fatalf("api health url = %q, want helper /api/health readiness gate", got)
	}
	wantHeader := "Authorization: Bearer " + result.builderToken
	if !strings.Contains(result.rpcHeaders, wantHeader) {
		t.Fatalf("rpc headers = %q, want %q", result.rpcHeaders, wantHeader)
	}
	if got := strings.TrimSpace(result.rpcURL); got != "http://127.0.0.1:8081/api/rpc" {
		t.Fatalf("rpc url = %q, want helper /api/rpc alias", got)
	}
	if !strings.Contains(result.rpcBody, `"method":"run.start"`) {
		t.Fatalf("rpc body = %q, want run.start request", result.rpcBody)
	}
	if !strings.Contains(result.rpcBody, `"scan.corpus_file_requested"`) {
		t.Fatalf("rpc body = %q, want typed corpus root input", result.rpcBody)
	}
	if strings.Contains(result.rpcBody, `"scan.requested"`) {
		t.Fatalf("rpc body = %q, want no retired scan.requested input", result.rpcBody)
	}
	if !strings.Contains(result.rpcBody, `"request":{"geography":"US"}`) {
		t.Fatalf("rpc body = %q, want typed request geography", result.rpcBody)
	}
	if !strings.Contains(result.rpcBody, `"corpus_path":"/data/test-signals-25.jsonl"`) {
		t.Fatalf("rpc body = %q, want corpus_path payload", result.rpcBody)
	}
	if !strings.Contains(result.stdout, `"status":"started"`) {
		t.Fatalf("stdout = %q, want started response", result.stdout)
	}
}

func TestRunClear_UsesConfiguredBuilderAuthTokenForRPC(t *testing.T) {
	const explicitOperatorToken = "operator-explicit-token"
	const explicitBuilderToken = "builder-explicit-token"
	result := runRunClear(t, runClearConfig{
		operatorToken: explicitOperatorToken,
		builderToken:  explicitBuilderToken,
	})

	if got := strings.TrimSpace(result.operatorToken); got != explicitOperatorToken {
		t.Fatalf("operator token = %q, want %q", got, explicitOperatorToken)
	}
	if got := strings.TrimSpace(result.builderToken); got != explicitBuilderToken {
		t.Fatalf("builder token = %q, want %q", got, explicitBuilderToken)
	}
	wantOperatorHeader := "Authorization: Bearer " + explicitOperatorToken
	if !strings.Contains(result.healthHeaders, wantOperatorHeader) {
		t.Fatalf("api health headers = %q, want %q", result.healthHeaders, wantOperatorHeader)
	}
	wantBuilderHeader := "Authorization: Bearer " + explicitBuilderToken
	if !strings.Contains(result.rpcHeaders, wantBuilderHeader) {
		t.Fatalf("rpc headers = %q, want %q", result.rpcHeaders, wantBuilderHeader)
	}
}

func TestRunClear_BuildsAndLaunchesBinaryDirectly(t *testing.T) {
	result := runRunClear(t, runClearConfig{})

	if got := strings.TrimSpace(result.builtBinaryPath); got == "" {
		t.Fatal("expected helper to build a swarm binary")
	}
	if got, want := strings.TrimSpace(result.launchedBinaryPath), strings.TrimSpace(result.builtBinaryPath); got != want {
		t.Fatalf("launched binary path = %q, want %q", got, want)
	}
	if got := filepath.Base(strings.TrimSpace(result.builtBinaryPath)); got != "swarm" {
		t.Fatalf("built binary basename = %q, want swarm", got)
	}
	if got := strings.TrimSpace(result.launchedHealthAddr); got != "0.0.0.0:8081" {
		t.Fatalf("launched health addr = %q, want WLAN-reachable default bind", got)
	}
}

func TestRunClear_DoesNotTreatLauncherStateChangeAsStartupFailure(t *testing.T) {
	result := runRunClear(t, runClearConfig{psMode: "state_flip"})

	if strings.Contains(result.stdout, "Swarm exited before becoming ready. Current log:") {
		t.Fatalf("stdout = %q, want readiness success despite launcher state change", result.stdout)
	}
	if !strings.Contains(result.stdout, "Swarm ready at http://127.0.0.1:8081") {
		t.Fatalf("stdout = %q, want readiness success", result.stdout)
	}
	if !strings.Contains(result.stdout, `"status":"started"`) {
		t.Fatalf("stdout = %q, want run.start request after readiness", result.stdout)
	}
}

func TestRunClear_UsesV1RPCForAgentDirective(t *testing.T) {
	const explicitAPIToken = "api-explicit-token"
	const explicitOperatorToken = "operator-explicit-token"
	result := runRunClear(t, runClearConfig{
		apiToken:         explicitAPIToken,
		operatorToken:    explicitOperatorToken,
		directiveAgent:   "agent-7",
		directiveMessage: "hello from test",
	})

	if got := strings.TrimSpace(result.apiToken); got != explicitAPIToken {
		t.Fatalf("api token = %q, want %q", got, explicitAPIToken)
	}
	wantHeader := "Authorization: Bearer " + explicitAPIToken
	if !strings.Contains(result.directiveHeaders, wantHeader) {
		t.Fatalf("directive headers = %q, want %q", result.directiveHeaders, wantHeader)
	}
	if got := strings.TrimSpace(result.directiveURL); got != "http://127.0.0.1:8081/v1/rpc" {
		t.Fatalf("directive url = %q, want v1 rpc endpoint", got)
	}

	var body struct {
		JSONRPC string         `json:"jsonrpc"`
		ID      string         `json:"id"`
		Method  string         `json:"method"`
		Params  map[string]any `json:"params"`
	}
	if err := json.Unmarshal([]byte(result.directiveBody), &body); err != nil {
		t.Fatalf("decode directive body %q: %v", result.directiveBody, err)
	}
	if body.JSONRPC != "2.0" || body.ID != "run-clear-directive" || body.Method != "agent.send_directive" {
		t.Fatalf("directive body = %#v, want JSON-RPC agent.send_directive envelope", body)
	}
	if body.Params["agent_id"] != "agent-7" {
		t.Fatalf("directive params = %#v, want agent_id", body.Params)
	}
	if body.Params["directive"] != "hello from test" {
		t.Fatalf("directive params = %#v, want directive message", body.Params)
	}
	if body.Params["kill_previous"] != true {
		t.Fatalf("directive params = %#v, want kill_previous true", body.Params)
	}
	if _, ok := body.Params["message"]; ok {
		t.Fatalf("directive params = %#v, want no legacy message field", body.Params)
	}
}

func TestRunClear_DerivesExplicitGatewayURLsByDefault(t *testing.T) {
	result := runRunClear(t, runClearConfig{})

	if got := strings.TrimSpace(result.hostGatewayURL); got != "http://127.0.0.1:8081" {
		t.Fatalf("host gateway url = %q, want explicit host loopback default", got)
	}
	if got := strings.TrimSpace(result.containerGatewayURL); got != "http://host.docker.internal:8081" {
		t.Fatalf("container gateway url = %q, want explicit container host alias default", got)
	}
}

func TestRunClear_PreservesExplicitGatewayURLOverrides(t *testing.T) {
	result := runRunClear(t, runClearConfig{
		hostGatewayURL: "http://127.0.0.1:18090",
		containerURL:   "http://orchestrator:18090",
	})

	if got := strings.TrimSpace(result.hostGatewayURL); got != "http://127.0.0.1:18090" {
		t.Fatalf("host gateway url = %q, want explicit override", got)
	}
	if got := strings.TrimSpace(result.containerGatewayURL); got != "http://orchestrator:18090" {
		t.Fatalf("container gateway url = %q, want explicit override", got)
	}
}

func TestRunClear_IgnoresPartialGatewayOverrideAndDerivesCoherentPair(t *testing.T) {
	result := runRunClear(t, runClearConfig{
		hostGatewayURL: "http://host.docker.internal:18090",
	})

	if got := strings.TrimSpace(result.hostGatewayURL); got != "http://127.0.0.1:8081" {
		t.Fatalf("host gateway url = %q, want derived local default when pair is incomplete", got)
	}
	if got := strings.TrimSpace(result.containerGatewayURL); got != "http://host.docker.internal:8081" {
		t.Fatalf("container gateway url = %q, want derived local default when pair is incomplete", got)
	}
}

func TestRunClear_FailsFastWhenSwarmExitsBeforeReady(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		launcherMode:    "exit_fast",
		launcherLogText: "workspace validation failed: workspace image is required",
		readyCode:       "503",
		apiHealthCode:   "503",
		startTimeout:    "5",
	})
	if err == nil {
		t.Fatalf("run_clear.sh err = nil, want startup failure\n%s", result.stdout)
	}
	if !strings.Contains(result.stdout, "Swarm exited before becoming ready. Current log:") {
		t.Fatalf("stdout = %q, want early-exit startup failure text", result.stdout)
	}
	if !strings.Contains(result.stdout, "workspace validation failed: workspace image is required") {
		t.Fatalf("stdout = %q, want startup failure log detail", result.stdout)
	}
	if strings.TrimSpace(result.rpcBody) != "" {
		t.Fatalf("rpc body = %q, want no run.start after startup failure", result.rpcBody)
	}
}

type runClearResult struct {
	stdout              string
	apiToken            string
	operatorToken       string
	builderToken        string
	hostGatewayURL      string
	containerGatewayURL string
	builtBinaryPath     string
	launchedBinaryPath  string
	launchedHealthAddr  string
	healthHeaders       string
	healthURL           string
	rpcHeaders          string
	rpcBody             string
	rpcURL              string
	directiveHeaders    string
	directiveBody       string
	directiveURL        string
}

func runRunClear(t *testing.T, cfg runClearConfig) runClearResult {
	t.Helper()
	result, err := runRunClearResult(t, cfg)
	if err != nil {
		t.Fatalf("run_clear.sh failed: %v\n%s", err, result.stdout)
	}
	return result
}

func runRunClearResult(t *testing.T, cfg runClearConfig) (runClearResult, error) {
	t.Helper()

	scriptDir := testScriptDir(t)
	binDir := t.TempDir()
	logFile := filepath.Join(t.TempDir(), "swarm.log")
	pidFile := filepath.Join(t.TempDir(), "swarm.pid")
	operatorTokenSink := filepath.Join(t.TempDir(), "operator-token.txt")
	apiTokenSink := filepath.Join(t.TempDir(), "api-token.txt")
	builderTokenSink := filepath.Join(t.TempDir(), "builder-token.txt")
	hostGatewayURLSink := filepath.Join(t.TempDir(), "host-gateway-url.txt")
	containerGatewayURLSink := filepath.Join(t.TempDir(), "container-gateway-url.txt")
	goBuildOutputSink := filepath.Join(t.TempDir(), "go-build-output.txt")
	pythonBinaryPathSink := filepath.Join(t.TempDir(), "python-binary-path.txt")
	pythonHealthAddrSink := filepath.Join(t.TempDir(), "python-health-addr.txt")
	psStateCountSink := filepath.Join(t.TempDir(), "ps-state-count.txt")
	healthHeadersSink := filepath.Join(t.TempDir(), "api-health-headers.txt")
	healthURLSink := filepath.Join(t.TempDir(), "api-health-url.txt")
	rpcHeadersSink := filepath.Join(t.TempDir(), "rpc-headers.txt")
	bodySink := filepath.Join(t.TempDir(), "rpc-body.txt")
	urlSink := filepath.Join(t.TempDir(), "rpc-url.txt")
	directiveHeadersSink := filepath.Join(t.TempDir(), "directive-headers.txt")
	directiveBodySink := filepath.Join(t.TempDir(), "directive-body.txt")
	directiveURLSink := filepath.Join(t.TempDir(), "directive-url.txt")

	writeExecutable(t, binDir, "pgrep", "#!/usr/bin/env bash\nexit 1\n")
	writeExecutable(t, binDir, "lsof", "#!/usr/bin/env bash\nexit 1\n")
	writeExecutable(t, binDir, "ps", `#!/usr/bin/env bash
set -euo pipefail
if [[ "${PS_MODE:-real}" == "real" ]]; then
  exec /bin/ps "$@"
fi
format=""
while (($#)); do
  case "$1" in
    -o)
      format="$2"
      shift 2
      ;;
    -p)
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
case "${PS_MODE:-}" in
  state_flip)
    case "$format" in
      state=)
        count=0
        if [[ -f "${PS_STATE_COUNT_SINK}" ]]; then
          count="$(cat "${PS_STATE_COUNT_SINK}")"
        fi
        count=$((count + 1))
        printf '%s' "$count" > "${PS_STATE_COUNT_SINK}"
        if (( count == 1 )); then
          printf 'R\n'
        else
          printf 'S\n'
        fi
        ;;
      lstart=)
        printf 'Wed Apr 15 23:54:13 2026\n'
        ;;
      command=)
        printf '%s -contracts %s -health-addr %s\n' "${BINARY_PATH}" "${CONTRACTS_ROOT}" "${HEALTH_ADDR}"
        ;;
      *)
        exit 1
        ;;
    esac
    ;;
  *)
    echo "unexpected PS_MODE: ${PS_MODE:-}" >&2
    exit 1
    ;;
esac
`)
	writeExecutable(t, binDir, "docker", "#!/usr/bin/env bash\nif [[ \"${1:-}\" == \"ps\" ]]; then exit 0; fi\nif [[ \"${1:-}\" == \"stop\" ]]; then exit 0; fi\nexit 0\n")
	writeExecutable(t, binDir, "psql", "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, binDir, "uuidgen", "#!/usr/bin/env bash\nprintf '11111111-1111-1111-1111-111111111111\\n'\n")
	writeExecutable(t, binDir, "go", `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != "build" ]]; then
  echo "unexpected go subcommand: ${1:-}" >&2
  exit 1
fi
out=""
while (($#)); do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [[ -z "$out" ]]; then
  echo "missing build output" >&2
  exit 1
fi
printf '%s' "$out" > "${GO_BUILD_OUTPUT_SINK}"
mkdir -p "$(dirname "$out")"
cat > "$out" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
case "${GO_BUILD_LAUNCHER_MODE:-steady}" in
  exit_fast)
    if [[ -n "${GO_BUILD_LAUNCHER_LOG_TEXT:-}" ]]; then
      printf '%s\n' "${GO_BUILD_LAUNCHER_LOG_TEXT}"
    fi
    exit 1
    ;;
  *)
    sleep 30 >/dev/null 2>&1 </dev/null
    ;;
esac
EOS
chmod +x "$out"
`)
	writeExecutable(t, binDir, "python3", `#!/usr/bin/env bash
set -euo pipefail
printf '%s' "${SWARM_OPERATOR_AUTH_TOKEN:-}" > "${PYTHON_OPERATOR_TOKEN_SINK}"
printf '%s' "${SWARM_API_TOKEN:-}" > "${PYTHON_API_TOKEN_SINK}"
printf '%s' "${SWARM_BUILDER_AUTH_TOKEN:-}" > "${PYTHON_ENV_SINK}"
printf '%s' "${SWARM_TOOL_GATEWAY_URL:-}" > "${PYTHON_HOST_GATEWAY_URL_SINK}"
printf '%s' "${SWARM_TOOL_GATEWAY_CONTAINER_URL:-}" > "${PYTHON_CONTAINER_GATEWAY_URL_SINK}"
printf '%s' "${BINARY_PATH:-}" > "${PYTHON_BINARY_PATH_SINK}"
printf '%s' "${HEALTH_ADDR:-}" > "${PYTHON_HEALTH_ADDR_SINK}"
(
  exec "${BINARY_PATH}" -contracts "${CONTRACTS_ROOT}" -health-addr "${HEALTH_ADDR}" >> "${LOG_FILE}" 2>&1 < /dev/null
) &
printf '%s\n' "$!"
`)
	writeExecutable(t, binDir, "curl", `#!/usr/bin/env bash
set -euo pipefail
out=""
url=""
body=""
headers=()
while (($#)); do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    -w)
      shift 2
      ;;
    -H)
      headers+=("$2")
      shift 2
      ;;
    --data-binary)
      body="$2"
      if [[ "$body" == "@-" ]]; then
        body="$(cat)"
      fi
      shift 2
      ;;
    -s|-S|-sS)
      shift
      ;;
    http://*|https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
if [[ "$url" == *"/healthz" ]]; then
  if [[ -n "$out" ]]; then
    printf '{}' > "$out"
  fi
  printf '%s' "${CURL_HEALTHZ_CODE:-200}"
  exit 0
fi
if [[ "$url" == *"/readyz" ]]; then
  if [[ -n "$out" ]]; then
    printf '{}' > "$out"
  fi
  printf '%s' "${CURL_READYZ_CODE:-200}"
  exit 0
fi
if [[ "$url" == *"/api/health" ]]; then
  printf '%s' "$url" > "${CURL_API_HEALTH_URL_SINK}"
  if ((${#headers[@]})); then
    printf '%s\n' "${headers[@]}" > "${CURL_API_HEALTH_HEADERS_SINK}"
  else
    : > "${CURL_API_HEALTH_HEADERS_SINK}"
  fi
  want="Authorization: Bearer ${SWARM_OPERATOR_AUTH_TOKEN}"
  for header in "${headers[@]}"; do
    if [[ "$header" == "$want" ]]; then
      if [[ -n "$out" ]]; then
        printf '{}' > "$out"
      fi
      printf '%s' "${CURL_API_HEALTH_CODE:-200}"
      exit 0
    fi
  done
  if [[ -n "$out" ]]; then
    printf '{"error":"operator authentication is not configured"}' > "$out"
  fi
  printf '401'
  exit 0
fi
if [[ "$url" == *"/api/rpc" ]]; then
  printf '%s' "$url" > "${CURL_URL_SINK}"
  if ((${#headers[@]})); then
    printf '%s\n' "${headers[@]}" > "${CURL_HEADERS_SINK}"
  else
    : > "${CURL_HEADERS_SINK}"
  fi
  printf '%s' "$body" > "${CURL_BODY_SINK}"
  want="Authorization: Bearer ${SWARM_BUILDER_AUTH_TOKEN}"
  for header in "${headers[@]}"; do
    if [[ "$header" == "$want" ]]; then
      printf '{"jsonrpc":"2.0","id":"run-clear","result":{"status":"started"}}'
      exit 0
    fi
  done
  printf '{"jsonrpc":"2.0","id":"run-clear","error":{"message":"missing authorization bearer token"}}'
  exit 0
fi
if [[ "$url" == *"/v1/rpc" ]]; then
  printf '%s' "$url" > "${CURL_DIRECTIVE_URL_SINK}"
  if ((${#headers[@]})); then
    printf '%s\n' "${headers[@]}" > "${CURL_DIRECTIVE_HEADERS_SINK}"
  else
    : > "${CURL_DIRECTIVE_HEADERS_SINK}"
  fi
  printf '%s' "$body" > "${CURL_DIRECTIVE_BODY_SINK}"
  want="Authorization: Bearer ${SWARM_API_TOKEN}"
  for header in "${headers[@]}"; do
    if [[ "$header" == "$want" ]]; then
      printf '{"jsonrpc":"2.0","id":"run-clear-directive","result":{"ok":true,"response":"accepted"}}'
      exit 0
    fi
  done
  printf '{"jsonrpc":"2.0","id":"run-clear-directive","error":{"code":-32000,"message":"missing authorization bearer token","data":{"code":"UNAUTHORIZED"}}}'
  exit 0
fi
printf '{}'
`)

	cmd := exec.Command("bash", filepath.Join(scriptDir, "run_clear.sh"))
	cmd.Env = append(filteredEnv(
		"SWARM_OPERATOR_AUTH_TOKEN",
		"SWARM_API_TOKEN",
		"SWARM_BUILDER_AUTH_TOKEN",
		"SWARM_TOOL_GATEWAY_URL",
		"SWARM_TOOL_GATEWAY_CONTAINER_URL",
		"RUN_CLEAR_INPUT_EVENT",
		"RUN_CLEAR_INPUT_PAYLOAD_JSON",
	), []string{
		"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"PYTHON_OPERATOR_TOKEN_SINK=" + operatorTokenSink,
		"PYTHON_API_TOKEN_SINK=" + apiTokenSink,
		"PYTHON_ENV_SINK=" + builderTokenSink,
		"PYTHON_HOST_GATEWAY_URL_SINK=" + hostGatewayURLSink,
		"PYTHON_CONTAINER_GATEWAY_URL_SINK=" + containerGatewayURLSink,
		"PYTHON_BINARY_PATH_SINK=" + pythonBinaryPathSink,
		"PYTHON_HEALTH_ADDR_SINK=" + pythonHealthAddrSink,
		"GO_BUILD_OUTPUT_SINK=" + goBuildOutputSink,
		"GO_BUILD_LAUNCHER_MODE=" + defaultString(cfg.launcherMode, "steady"),
		"GO_BUILD_LAUNCHER_LOG_TEXT=" + cfg.launcherLogText,
		"PS_MODE=" + defaultString(cfg.psMode, "real"),
		"PS_STATE_COUNT_SINK=" + psStateCountSink,
		"CURL_API_HEALTH_HEADERS_SINK=" + healthHeadersSink,
		"CURL_API_HEALTH_URL_SINK=" + healthURLSink,
		"CURL_HEALTHZ_CODE=200",
		"CURL_READYZ_CODE=" + defaultString(cfg.readyCode, "200"),
		"CURL_API_HEALTH_CODE=" + defaultString(cfg.apiHealthCode, "200"),
		"CURL_HEADERS_SINK=" + rpcHeadersSink,
		"CURL_BODY_SINK=" + bodySink,
		"CURL_URL_SINK=" + urlSink,
		"CURL_DIRECTIVE_HEADERS_SINK=" + directiveHeadersSink,
		"CURL_DIRECTIVE_BODY_SINK=" + directiveBodySink,
		"CURL_DIRECTIVE_URL_SINK=" + directiveURLSink,
		"CONTRACTS_ROOT=/tmp/contracts",
		"HEALTH_ADDR=" + defaultString(cfg.healthAddr, "0.0.0.0:8081"),
		"BINARY_PATH=" + filepath.Join(t.TempDir(), "swarm"),
		"LOG_FILE=" + logFile,
		"PID_FILE=" + pidFile,
		"START_TIMEOUT=" + defaultString(cfg.startTimeout, "1"),
	}...)
	if cfg.operatorToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_OPERATOR_AUTH_TOKEN="+cfg.operatorToken)
	}
	if cfg.apiToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_API_TOKEN="+cfg.apiToken)
	}
	if cfg.builderToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_BUILDER_AUTH_TOKEN="+cfg.builderToken)
	}
	if cfg.directiveAgent != "" {
		cmd.Env = append(cmd.Env, "DIRECTIVE_AGENT="+cfg.directiveAgent)
	}
	if cfg.directiveMessage != "" {
		cmd.Env = append(cmd.Env, "DIRECTIVE_MESSAGE="+cfg.directiveMessage)
	}
	if cfg.hostGatewayURL != "" {
		cmd.Env = append(cmd.Env, "SWARM_TOOL_GATEWAY_URL="+cfg.hostGatewayURL)
	}
	if cfg.containerURL != "" {
		cmd.Env = append(cmd.Env, "SWARM_TOOL_GATEWAY_CONTAINER_URL="+cfg.containerURL)
	}

	out, err := cmd.CombinedOutput()
	if pid := readFileTrimmedOptional(t, pidFile); strings.TrimSpace(pid) != "" {
		_ = exec.Command("kill", pid).Run()
	}

	return runClearResult{
		stdout:              string(out),
		apiToken:            readFileTrimmed(t, apiTokenSink),
		operatorToken:       readFileTrimmed(t, operatorTokenSink),
		builderToken:        readFileTrimmed(t, builderTokenSink),
		hostGatewayURL:      readFileTrimmed(t, hostGatewayURLSink),
		containerGatewayURL: readFileTrimmed(t, containerGatewayURLSink),
		builtBinaryPath:     readFileTrimmed(t, goBuildOutputSink),
		launchedBinaryPath:  readFileTrimmed(t, pythonBinaryPathSink),
		launchedHealthAddr:  readFileTrimmed(t, pythonHealthAddrSink),
		healthHeaders:       readFileTrimmedOptional(t, healthHeadersSink),
		healthURL:           readFileTrimmedOptional(t, healthURLSink),
		rpcHeaders:          readFileTrimmedOptional(t, rpcHeadersSink),
		rpcBody:             readFileTrimmedOptional(t, bodySink),
		rpcURL:              readFileTrimmedOptional(t, urlSink),
		directiveHeaders:    readFileTrimmedOptional(t, directiveHeadersSink),
		directiveBody:       readFileTrimmedOptional(t, directiveBodySink),
		directiveURL:        readFileTrimmedOptional(t, directiveURLSink),
	}, err
}

func filteredEnv(removeKeys ...string) []string {
	base := os.Environ()
	remove := map[string]struct{}{}
	for _, key := range removeKeys {
		remove[key] = struct{}{}
	}
	filtered := make([]string, 0, len(base))
	for _, item := range base {
		key := item
		if idx := strings.IndexByte(item, '='); idx >= 0 {
			key = item[:idx]
		}
		if _, drop := remove[key]; drop {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func testScriptDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func writeExecutable(t *testing.T, dir, name, body string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", name, err)
	}
}

func readFileTrimmed(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.TrimSpace(string(data))
}

func readFileTrimmedOptional(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.TrimSpace(string(data))
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
