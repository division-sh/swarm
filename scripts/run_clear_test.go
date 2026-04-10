package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type runClearConfig struct {
	operatorToken    string
	builderToken     string
	directiveAgent   string
	directiveMessage string
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

func TestRunClear_UsesOperatorBearerForDirective(t *testing.T) {
	const explicitOperatorToken = "operator-explicit-token"
	result := runRunClear(t, runClearConfig{
		operatorToken:    explicitOperatorToken,
		directiveAgent:   "agent-7",
		directiveMessage: "hello from test",
	})

	wantHeader := "Authorization: Bearer " + explicitOperatorToken
	if !strings.Contains(result.directiveHeaders, wantHeader) {
		t.Fatalf("directive headers = %q, want %q", result.directiveHeaders, wantHeader)
	}
	if got := strings.TrimSpace(result.directiveURL); got != "http://127.0.0.1:8081/api/agents/agent-7/actions/directive" {
		t.Fatalf("directive url = %q, want helper directive endpoint", got)
	}
	if !strings.Contains(result.directiveBody, `"message":"hello from test"`) {
		t.Fatalf("directive body = %q, want directive message payload", result.directiveBody)
	}
	if !strings.Contains(result.directiveBody, `"kill_previous":true`) {
		t.Fatalf("directive body = %q, want kill_previous payload", result.directiveBody)
	}
}

type runClearResult struct {
	stdout           string
	operatorToken    string
	builderToken     string
	healthHeaders    string
	healthURL        string
	rpcHeaders       string
	rpcBody          string
	rpcURL           string
	directiveHeaders string
	directiveBody    string
	directiveURL     string
}

func runRunClear(t *testing.T, cfg runClearConfig) runClearResult {
	t.Helper()

	scriptDir := testScriptDir(t)
	binDir := t.TempDir()
	logFile := filepath.Join(t.TempDir(), "swarm.log")
	pidFile := filepath.Join(t.TempDir(), "swarm.pid")
	operatorTokenSink := filepath.Join(t.TempDir(), "operator-token.txt")
	builderTokenSink := filepath.Join(t.TempDir(), "builder-token.txt")
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
	writeExecutable(t, binDir, "docker", "#!/usr/bin/env bash\nif [[ \"${1:-}\" == \"ps\" ]]; then exit 0; fi\nif [[ \"${1:-}\" == \"stop\" ]]; then exit 0; fi\nexit 0\n")
	writeExecutable(t, binDir, "psql", "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, binDir, "uuidgen", "#!/usr/bin/env bash\nprintf '11111111-1111-1111-1111-111111111111\\n'\n")
	writeExecutable(t, binDir, "python3", `#!/usr/bin/env bash
set -euo pipefail
printf '%s' "${SWARM_OPERATOR_AUTH_TOKEN:-}" > "${PYTHON_OPERATOR_TOKEN_SINK}"
printf '%s' "${SWARM_BUILDER_AUTH_TOKEN:-}" > "${PYTHON_ENV_SINK}"
printf '4242\n'
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
if [[ "$url" == *"/healthz" || "$url" == *"/readyz" ]]; then
  if [[ -n "$out" ]]; then
    printf '{}' > "$out"
  fi
  printf '200'
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
      printf '200'
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
if [[ "$url" == *"/api/agents/"*"/actions/directive" ]]; then
  printf '%s' "$url" > "${CURL_DIRECTIVE_URL_SINK}"
  if ((${#headers[@]})); then
    printf '%s\n' "${headers[@]}" > "${CURL_DIRECTIVE_HEADERS_SINK}"
  else
    : > "${CURL_DIRECTIVE_HEADERS_SINK}"
  fi
  printf '%s' "$body" > "${CURL_DIRECTIVE_BODY_SINK}"
  want="Authorization: Bearer ${SWARM_OPERATOR_AUTH_TOKEN}"
  for header in "${headers[@]}"; do
    if [[ "$header" == "$want" ]]; then
      printf '{"status":"accepted"}'
      exit 0
    fi
  done
  printf '{"error":"missing authorization bearer token"}'
  exit 0
fi
printf '{}'
`)

	cmd := exec.Command("bash", filepath.Join(scriptDir, "run_clear.sh"))
	cmd.Env = append(filteredEnv(
		"SWARM_OPERATOR_AUTH_TOKEN",
		"SWARM_BUILDER_AUTH_TOKEN",
	), []string{
		"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"PYTHON_OPERATOR_TOKEN_SINK=" + operatorTokenSink,
		"PYTHON_ENV_SINK=" + builderTokenSink,
		"CURL_API_HEALTH_HEADERS_SINK=" + healthHeadersSink,
		"CURL_API_HEALTH_URL_SINK=" + healthURLSink,
		"CURL_HEADERS_SINK=" + rpcHeadersSink,
		"CURL_BODY_SINK=" + bodySink,
		"CURL_URL_SINK=" + urlSink,
		"CURL_DIRECTIVE_HEADERS_SINK=" + directiveHeadersSink,
		"CURL_DIRECTIVE_BODY_SINK=" + directiveBodySink,
		"CURL_DIRECTIVE_URL_SINK=" + directiveURLSink,
		"CONTRACTS_ROOT=/tmp/contracts",
		"HEALTH_ADDR=127.0.0.1:8081",
		"LOG_FILE=" + logFile,
		"PID_FILE=" + pidFile,
		"START_TIMEOUT=1",
	}...)
	if cfg.operatorToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_OPERATOR_AUTH_TOKEN="+cfg.operatorToken)
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

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run_clear.sh failed: %v\n%s", err, out)
	}

	return runClearResult{
		stdout:           string(out),
		operatorToken:    readFileTrimmed(t, operatorTokenSink),
		builderToken:     readFileTrimmed(t, builderTokenSink),
		healthHeaders:    readFileTrimmed(t, healthHeadersSink),
		healthURL:        readFileTrimmed(t, healthURLSink),
		rpcHeaders:       readFileTrimmed(t, rpcHeadersSink),
		rpcBody:          readFileTrimmed(t, bodySink),
		rpcURL:           readFileTrimmed(t, urlSink),
		directiveHeaders: readFileTrimmedOptional(t, directiveHeadersSink),
		directiveBody:    readFileTrimmedOptional(t, directiveBodySink),
		directiveURL:     readFileTrimmedOptional(t, directiveURLSink),
	}
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
