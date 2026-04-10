package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunClear_ProvisionsBuilderAuthTokenAndUsesBearerForRPC(t *testing.T) {
	result := runRunClear(t, "")

	if strings.TrimSpace(result.builderToken) == "" {
		t.Fatal("expected helper to provision SWARM_BUILDER_AUTH_TOKEN")
	}
	wantHeader := "Authorization: Bearer " + result.builderToken
	if !strings.Contains(result.headers, wantHeader) {
		t.Fatalf("headers = %q, want %q", result.headers, wantHeader)
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
	const explicitToken = "builder-explicit-token"
	result := runRunClear(t, explicitToken)

	if got := strings.TrimSpace(result.builderToken); got != explicitToken {
		t.Fatalf("builder token = %q, want %q", got, explicitToken)
	}
	wantHeader := "Authorization: Bearer " + explicitToken
	if !strings.Contains(result.headers, wantHeader) {
		t.Fatalf("headers = %q, want %q", result.headers, wantHeader)
	}
}

type runClearResult struct {
	stdout       string
	builderToken string
	headers      string
	rpcBody      string
	rpcURL       string
}

func runRunClear(t *testing.T, builderToken string) runClearResult {
	t.Helper()

	scriptDir := testScriptDir(t)
	binDir := t.TempDir()
	logFile := filepath.Join(t.TempDir(), "swarm.log")
	pidFile := filepath.Join(t.TempDir(), "swarm.pid")
	builderTokenSink := filepath.Join(t.TempDir(), "builder-token.txt")
	headersSink := filepath.Join(t.TempDir(), "rpc-headers.txt")
	bodySink := filepath.Join(t.TempDir(), "rpc-body.txt")
	urlSink := filepath.Join(t.TempDir(), "rpc-url.txt")

	writeExecutable(t, binDir, "pgrep", "#!/usr/bin/env bash\nexit 1\n")
	writeExecutable(t, binDir, "lsof", "#!/usr/bin/env bash\nexit 1\n")
	writeExecutable(t, binDir, "docker", "#!/usr/bin/env bash\nif [[ \"${1:-}\" == \"ps\" ]]; then exit 0; fi\nif [[ \"${1:-}\" == \"stop\" ]]; then exit 0; fi\nexit 0\n")
	writeExecutable(t, binDir, "psql", "#!/usr/bin/env bash\nexit 0\n")
	writeExecutable(t, binDir, "uuidgen", "#!/usr/bin/env bash\nprintf '11111111-1111-1111-1111-111111111111\\n'\n")
	writeExecutable(t, binDir, "python3", `#!/usr/bin/env bash
set -euo pipefail
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
if [[ "$url" == *"/healthz" || "$url" == *"/readyz" || "$url" == *"/api/health" ]]; then
  if [[ -n "$out" ]]; then
    printf '{}' > "$out"
  fi
  printf '200'
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
  printf '{"jsonrpc":"2.0","id":"run-clear","result":{"status":"started"}}'
  exit 0
fi
printf '{}'
`)

	cmd := exec.Command("bash", filepath.Join(scriptDir, "run_clear.sh"))
	cmd.Env = append(filteredEnv(
		"SWARM_BUILDER_AUTH_TOKEN",
	), []string{
		"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"PYTHON_ENV_SINK=" + builderTokenSink,
		"CURL_HEADERS_SINK=" + headersSink,
		"CURL_BODY_SINK=" + bodySink,
		"CURL_URL_SINK=" + urlSink,
		"CONTRACTS_ROOT=/tmp/contracts",
		"HEALTH_ADDR=127.0.0.1:8081",
		"LOG_FILE=" + logFile,
		"PID_FILE=" + pidFile,
		"START_TIMEOUT=1",
	}...)
	if builderToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_BUILDER_AUTH_TOKEN="+builderToken)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run_clear.sh failed: %v\n%s", err, out)
	}

	return runClearResult{
		stdout:       string(out),
		builderToken: readFileTrimmed(t, builderTokenSink),
		headers:      readFileTrimmed(t, headersSink),
		rpcBody:      readFileTrimmed(t, bodySink),
		rpcURL:       readFileTrimmed(t, urlSink),
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
