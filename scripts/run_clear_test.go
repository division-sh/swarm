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

const runClearTestBundleFingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type runClearConfig struct {
	mode                  string
	apiToken              string
	directiveAgent        string
	directiveMessage      string
	healthAddr            string
	hostGatewayURL        string
	containerURL          string
	launcherMode          string
	launcherLogText       string
	psMode                string
	readyCode             string
	apiHealthCode         string
	apiHealthReady        string
	apiHealthDBOK         string
	apiHealthRuntime      string
	preResetReadyCode     string
	preResetHealthReady   string
	preResetHealthDBOK    string
	preResetHealthRuntime string
	startTimeout          string
	inputEvent            string
	inputPayloadJSON      string
	resetIDKey            string
	nukeError             string
	nukeOK                string
	nukeStatus            string
	nukePartial           string
}

func TestRunClear_UsesV1RPCHealthCheckAndRunStart(t *testing.T) {
	result := runRunClear(t, runClearConfig{})

	if strings.TrimSpace(result.apiToken) == "" {
		t.Fatal("expected helper to provision SWARM_API_TOKEN")
	}
	if strings.TrimSpace(result.operatorToken) != "" || strings.TrimSpace(result.builderToken) != "" {
		t.Fatalf("legacy tokens should not be provisioned: operator=%q builder=%q", result.operatorToken, result.builderToken)
	}
	wantAPIHeader := "Authorization: Bearer " + result.apiToken
	if !strings.Contains(result.healthHeaders, wantAPIHeader) {
		t.Fatalf("health.check headers = %q, want %q", result.healthHeaders, wantAPIHeader)
	}
	if got := strings.TrimSpace(result.healthURL); got != "http://127.0.0.1:8081/v1/rpc" {
		t.Fatalf("health.check url = %q, want v1 rpc endpoint", got)
	}
	health := decodeJSONRPCBody(t, result.healthBody)
	if health.Method != "health.check" {
		t.Fatalf("health body = %#v, want health.check", health)
	}
	if !strings.Contains(result.rpcHeaders, wantAPIHeader) {
		t.Fatalf("run.start headers = %q, want %q", result.rpcHeaders, wantAPIHeader)
	}
	if got := strings.TrimSpace(result.rpcURL); got != "http://127.0.0.1:8081/v1/rpc" {
		t.Fatalf("run.start url = %q, want v1 rpc endpoint", got)
	}
	start := decodeJSONRPCBody(t, result.rpcBody)
	if start.JSONRPC != "2.0" || start.ID != "run-clear" || start.Method != "run.start" {
		t.Fatalf("run.start body = %#v, want JSON-RPC run.start envelope", start)
	}
	if start.Params["run_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("run.start params = %#v, want run_id", start.Params)
	}
	if start.Params["event_name"] != "scan.corpus_file_requested" {
		t.Fatalf("run.start params = %#v, want typed corpus root input", start.Params)
	}
	if _, ok := start.Params["inputs"]; ok {
		t.Fatalf("run.start params = %#v, want no legacy inputs envelope", start.Params)
	}
	bundleRef := mapParam(t, start.Params, "bundle_ref")
	if bundleRef["fingerprint"] != runClearTestBundleFingerprint {
		t.Fatalf("bundle_ref = %#v, want health.check fingerprint", bundleRef)
	}
	if start.Params["idempotency_key"] != "run-clear:11111111-1111-1111-1111-111111111111" {
		t.Fatalf("run.start params = %#v, want per-run idempotency key", start.Params)
	}
	payload := mapParam(t, start.Params, "payload")
	request := mapParam(t, payload, "request")
	if request["geography"] != "US" {
		t.Fatalf("payload = %#v, want typed request geography", payload)
	}
	if payload["corpus_path"] != "/data/test-signals-25.jsonl" {
		t.Fatalf("payload = %#v, want corpus_path", payload)
	}
	if strings.Contains(result.rpcBody, `"scan.requested"`) {
		t.Fatalf("run.start body = %q, want no retired scan.requested input", result.rpcBody)
	}
	if !strings.Contains(result.stdout, `"status":"running"`) {
		t.Fatalf("stdout = %q, want v1 running response", result.stdout)
	}
	if strings.Contains(result.stdout, `"status":"started"`) {
		t.Fatalf("stdout = %q, want no legacy started response", result.stdout)
	}
	assertEventOrder(t, result.events, "serve_start", "health.check", "runtime.nuke", "health.check", "run.start")
}

func TestRunClear_UsesConfiguredAPITokenForHealthCheckAndRunStart(t *testing.T) {
	const explicitAPIToken = "api-explicit-token"
	result := runRunClear(t, runClearConfig{
		apiToken: explicitAPIToken,
	})

	if got := strings.TrimSpace(result.apiToken); got != explicitAPIToken {
		t.Fatalf("api token = %q, want %q", got, explicitAPIToken)
	}
	if strings.TrimSpace(result.operatorToken) != "" || strings.TrimSpace(result.builderToken) != "" {
		t.Fatalf("legacy tokens should not be forwarded: operator=%q builder=%q", result.operatorToken, result.builderToken)
	}
	wantAPIHeader := "Authorization: Bearer " + explicitAPIToken
	if !strings.Contains(result.healthHeaders, wantAPIHeader) {
		t.Fatalf("health.check headers = %q, want %q", result.healthHeaders, wantAPIHeader)
	}
	if !strings.Contains(result.rpcHeaders, wantAPIHeader) {
		t.Fatalf("run.start headers = %q, want %q", result.rpcHeaders, wantAPIHeader)
	}
}

func TestRunClear_MapsConfiguredInputEventAndPayloadToV1RunStart(t *testing.T) {
	result := runRunClear(t, runClearConfig{
		inputEvent:       "scan.signals_requested",
		inputPayloadJSON: `{"source":"manual","count":2}`,
	})

	start := decodeJSONRPCBody(t, result.rpcBody)
	if start.Method != "run.start" {
		t.Fatalf("run.start body = %#v, want run.start", start)
	}
	if start.Params["event_name"] != "scan.signals_requested" {
		t.Fatalf("run.start params = %#v, want configured event_name", start.Params)
	}
	if _, ok := start.Params["inputs"]; ok {
		t.Fatalf("run.start params = %#v, want no legacy inputs envelope", start.Params)
	}
	payload := mapParam(t, start.Params, "payload")
	if payload["source"] != "manual" {
		t.Fatalf("payload = %#v, want configured source", payload)
	}
	if payload["count"] != float64(2) {
		t.Fatalf("payload = %#v, want configured count", payload)
	}
}

func TestRunClear_DoesNotStartRunWhenHealthCheckIsNotReady(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		apiHealthReady:     "false",
		apiHealthDBOK:      "false",
		apiHealthRuntime:   "true",
		preResetHealthDBOK: "true",
		startTimeout:       "1",
	})

	if err == nil {
		t.Fatal("expected helper to fail readiness when health.check is not ready")
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start when health.check is not ready", got)
	}
	if got := strings.TrimSpace(result.rpcURL); got != "" {
		t.Fatalf("run.start url = %q, want no run.start when health.check is not ready", got)
	}
	if !strings.Contains(result.stdout, "Swarm failed to become ready. Current log:") {
		t.Fatalf("stdout = %q, want readiness failure", result.stdout)
	}
}

func TestRunClear_ResetDevDoesNotStartRunOrDirective(t *testing.T) {
	result := runRunClear(t, runClearConfig{mode: "reset-dev"})

	if got := strings.TrimSpace(result.builtBinaryPath); got == "" {
		t.Fatal("expected reset-dev to build a swarm binary")
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start in reset-dev", got)
	}
	if got := strings.TrimSpace(result.directiveBody); got != "" {
		t.Fatalf("directive body = %q, want no directive in reset-dev", got)
	}
	if health := decodeJSONRPCBody(t, result.healthBody); health.Method != "health.check" {
		t.Fatalf("health body = %#v, want health.check", health)
	}
	if !strings.Contains(result.resetHeaders, "Authorization: Bearer "+result.apiToken) {
		t.Fatalf("runtime.nuke headers = %q, want API bearer token", result.resetHeaders)
	}
	if got := strings.TrimSpace(result.resetURL); got != "http://127.0.0.1:8081/v1/rpc" {
		t.Fatalf("runtime.nuke url = %q, want v1 rpc endpoint", got)
	}
	reset := decodeJSONRPCBody(t, result.resetBody)
	if reset.JSONRPC != "2.0" || reset.ID != "run-clear-reset-dev" || reset.Method != "runtime.nuke" {
		t.Fatalf("runtime.nuke body = %#v, want JSON-RPC runtime.nuke envelope", reset)
	}
	if reset.Params["dry_run"] != false {
		t.Fatalf("runtime.nuke params = %#v, want apply mode", reset.Params)
	}
	if reset.Params["idempotency_key"] != "run-clear:reset-dev:11111111-1111-1111-1111-111111111111" {
		t.Fatalf("runtime.nuke params = %#v, want per-reset idempotency key", reset.Params)
	}
	if got := strings.TrimSpace(result.dockerCalls); got != "" {
		t.Fatalf("docker calls = %q, want no raw docker reset in reset-dev", got)
	}
	if got := strings.TrimSpace(result.psqlCalls); got != "" {
		t.Fatalf("psql calls = %q, want no raw database reset in reset-dev", got)
	}
	assertEventOrder(t, result.events, "serve_start", "health.check", "runtime.nuke", "health.check")
}

func TestRunClear_RuntimeNukeDoesNotRequireReadyBeforeReset(t *testing.T) {
	result := runRunClear(t, runClearConfig{
		mode:                  "reset-dev",
		preResetReadyCode:     "503",
		preResetHealthReady:   "false",
		preResetHealthRuntime: "false",
	})

	if got := strings.TrimSpace(result.resetBody); got == "" {
		t.Fatal("expected runtime.nuke request before full readiness")
	}
	assertEventOrder(t, result.events, "serve_start", "health.check", "runtime.nuke", "health.check")
	if !strings.Contains(result.stdout, "Swarm API reachable at http://127.0.0.1:8081") {
		t.Fatalf("stdout = %q, want API-reachable pre-reset gate", result.stdout)
	}
	if !strings.Contains(result.stdout, "Swarm ready at http://127.0.0.1:8081") {
		t.Fatalf("stdout = %q, want full readiness after runtime.nuke", result.stdout)
	}
}

func TestRunClear_UsesConfiguredResetIdempotencyKey(t *testing.T) {
	result := runRunClear(t, runClearConfig{
		mode:       "reset-dev",
		apiToken:   "api-explicit-token",
		resetIDKey: "reset-dev-test-key",
	})

	reset := decodeJSONRPCBody(t, result.resetBody)
	if reset.Params["idempotency_key"] != "reset-dev-test-key" {
		t.Fatalf("runtime.nuke params = %#v, want configured reset idempotency key", reset.Params)
	}
	if !strings.Contains(result.resetHeaders, "Authorization: Bearer api-explicit-token") {
		t.Fatalf("runtime.nuke headers = %q, want explicit API bearer token", result.resetHeaders)
	}
}

func TestRunClear_RuntimeNukeFailureStopsCompositeBeforeRunStart(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		nukeError: "jsonrpc",
	})

	if err == nil {
		t.Fatal("expected runtime.nuke error to fail run-clear")
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start after failed runtime.nuke", got)
	}
	if !strings.Contains(result.stdout, "runtime.nuke reset failed. Current log:") {
		t.Fatalf("stdout = %q, want runtime.nuke failure message", result.stdout)
	}
	if got := strings.TrimSpace(result.dockerCalls); got != "" {
		t.Fatalf("docker calls = %q, want no raw docker fallback after runtime.nuke failure", got)
	}
	if got := strings.TrimSpace(result.psqlCalls); got != "" {
		t.Fatalf("psql calls = %q, want no raw DB fallback after runtime.nuke failure", got)
	}
}

func TestRunClear_RuntimeNukePartialFailureStopsCompositeBeforeRunStart(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		nukeOK:      "false",
		nukeStatus:  "partial_failure",
		nukePartial: "true",
	})

	if err == nil {
		t.Fatal("expected runtime.nuke partial failure to fail run-clear")
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start after partial runtime.nuke", got)
	}
	if !strings.Contains(result.stdout, `"status":"partial_failure"`) {
		t.Fatalf("stdout = %q, want partial runtime.nuke response", result.stdout)
	}
}

func TestRunClear_RunCorpusStartsOnlyCorpusRun(t *testing.T) {
	const explicitAPIToken = "api-explicit-token"
	result := runRunClear(t, runClearConfig{
		mode:     "run-corpus",
		apiToken: explicitAPIToken,
	})

	if got := strings.TrimSpace(result.builtBinaryPath); got != "" {
		t.Fatalf("built binary path = %q, want no build in run-corpus", got)
	}
	if got := strings.TrimSpace(result.directiveBody); got != "" {
		t.Fatalf("directive body = %q, want no directive in run-corpus", got)
	}
	if !strings.Contains(result.rpcHeaders, "Authorization: Bearer "+explicitAPIToken) {
		t.Fatalf("run.start headers = %q, want explicit API token", result.rpcHeaders)
	}
	start := decodeJSONRPCBody(t, result.rpcBody)
	if start.Method != "run.start" {
		t.Fatalf("run.start body = %#v, want run.start", start)
	}
}

func TestRunClear_RunClearRejectsHiddenDirectiveEnv(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		directiveAgent:   "agent-7",
		directiveMessage: "hello from test",
	})

	if err == nil {
		t.Fatal("expected run-clear with DIRECTIVE_* to fail closed")
	}
	if !strings.Contains(result.stdout, "DIRECTIVE_AGENT/DIRECTIVE_MESSAGE no longer change run-clear; use run-clear-directed.") {
		t.Fatalf("stdout = %q, want migration message", result.stdout)
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start on rejected hidden directive path", got)
	}
	if got := strings.TrimSpace(result.directiveBody); got != "" {
		t.Fatalf("directive body = %q, want no directive on rejected hidden directive path", got)
	}
	if got := strings.TrimSpace(result.builtBinaryPath); got != "" {
		t.Fatalf("built binary path = %q, want no reset/start on rejected hidden directive path", got)
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
	wantCommand := strings.TrimSpace(result.builtBinaryPath) + " serve --contracts /tmp/contracts --health-addr 0.0.0.0:8081"
	if got := strings.TrimSpace(result.launchedCommand); got != wantCommand {
		t.Fatalf("launched command = %q, want %q", got, wantCommand)
	}
	if strings.Contains(result.launchedCommand, " -contracts ") || strings.Contains(result.launchedCommand, " -health-addr ") {
		t.Fatalf("launched command = %q, want Cobra serve command with long flags", result.launchedCommand)
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
	if !strings.Contains(result.stdout, `"status":"running"`) {
		t.Fatalf("stdout = %q, want run.start request after readiness", result.stdout)
	}
}

func TestRunClear_UsesV1RPCForAgentDirective(t *testing.T) {
	const explicitAPIToken = "api-explicit-token"
	result := runRunClear(t, runClearConfig{
		mode:             "run-directive",
		apiToken:         explicitAPIToken,
		directiveAgent:   "agent-7",
		directiveMessage: "hello from test",
	})

	wantHeader := "Authorization: Bearer " + explicitAPIToken
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start in directive mode", got)
	}
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
	if _, ok := body.Params["kill_previous"]; ok {
		t.Fatalf("directive params = %#v, want no kill_previous", body.Params)
	}
	if _, ok := body.Params["message"]; ok {
		t.Fatalf("directive params = %#v, want no legacy message field", body.Params)
	}
}

func TestRunClear_RunClearDirectedResetsAndSendsDirectiveOnly(t *testing.T) {
	const explicitAPIToken = "api-explicit-token"
	result := runRunClear(t, runClearConfig{
		mode:             "run-clear-directed",
		apiToken:         explicitAPIToken,
		directiveAgent:   "agent-7",
		directiveMessage: "hello from test",
	})

	if got := strings.TrimSpace(result.builtBinaryPath); got == "" {
		t.Fatal("expected run-clear-directed to build a swarm binary")
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start in run-clear-directed", got)
	}
	body := decodeJSONRPCBody(t, result.directiveBody)
	if body.Method != "agent.send_directive" {
		t.Fatalf("directive body = %#v, want agent.send_directive", body)
	}
	if _, ok := body.Params["kill_previous"]; ok {
		t.Fatalf("directive params = %#v, want no kill_previous", body.Params)
	}
	assertEventOrder(t, result.events, "serve_start", "health.check", "runtime.nuke", "health.check", "agent.send_directive")
}

func TestRunClear_RunDirectiveRequiresExplicitTargetAndMessage(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		mode:     "run-directive",
		apiToken: "api-explicit-token",
	})

	if err == nil {
		t.Fatal("expected missing directive target/message to fail")
	}
	if !strings.Contains(result.stdout, "DIRECTIVE_AGENT and DIRECTIVE_MESSAGE are required for run-directive.") {
		t.Fatalf("stdout = %q, want missing directive message", result.stdout)
	}
	if got := strings.TrimSpace(result.healthBody); got != "" {
		t.Fatalf("health body = %q, want no readiness probe before directive arg validation", got)
	}
}

func TestRunClear_RunClearDirectedRequiresInputsBeforeReset(t *testing.T) {
	result, err := runRunClearResult(t, runClearConfig{
		mode: "run-clear-directed",
	})

	if err == nil {
		t.Fatal("expected missing directive target/message to fail")
	}
	if !strings.Contains(result.stdout, "DIRECTIVE_AGENT and DIRECTIVE_MESSAGE are required for run-clear-directed.") {
		t.Fatalf("stdout = %q, want missing directive message", result.stdout)
	}
	if got := strings.TrimSpace(result.builtBinaryPath); got != "" {
		t.Fatalf("built binary path = %q, want no reset/build before directive arg validation", got)
	}
	if got := strings.TrimSpace(result.healthBody); got != "" {
		t.Fatalf("health body = %q, want no readiness probe before directive arg validation", got)
	}
	if got := strings.TrimSpace(result.rpcBody); got != "" {
		t.Fatalf("run.start body = %q, want no run.start before directive arg validation", got)
	}
	if got := strings.TrimSpace(result.directiveBody); got != "" {
		t.Fatalf("directive body = %q, want no directive before directive arg validation", got)
	}
}

func TestRunClearMakefileDefinesSplitTargets(t *testing.T) {
	makefilePath := filepath.Join(filepath.Dir(testScriptDir(t)), "Makefile")
	data, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	makefile := string(data)
	for _, target := range []string{"reset-dev:", "run-corpus:", "run-directive:", "run-clear:", "run-clear-directed:"} {
		if !strings.Contains(makefile, "\n"+target) {
			t.Fatalf("Makefile missing target %s", target)
		}
	}
	if !strings.Contains(makefile, "./scripts/run_clear.sh run-clear-directed") {
		t.Fatalf("Makefile = %q, want run-clear-directed script mode", makefile)
	}
	if !strings.Contains(makefile, "RUN_CLEAR_RESET_IDEMPOTENCY_KEY") {
		t.Fatalf("Makefile = %q, want reset-dev idempotency override surface", makefile)
	}
	if strings.Contains(makefile, "SWARM_DB_HOST") || strings.Contains(makefile, "SWARM_DB_PASSWORD") {
		t.Fatalf("Makefile = %q, want no reset-dev DB credential surface", makefile)
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
	if !strings.Contains(result.stdout, "Swarm exited before API became reachable. Current log:") {
		t.Fatalf("stdout = %q, want early API startup failure text", result.stdout)
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
	launchedCommand     string
	healthHeaders       string
	healthBody          string
	healthURL           string
	resetHeaders        string
	resetBody           string
	resetURL            string
	rpcHeaders          string
	rpcBody             string
	rpcURL              string
	directiveHeaders    string
	directiveBody       string
	directiveURL        string
	dockerCalls         string
	psqlCalls           string
	events              string
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
	tokenFile := filepath.Join(t.TempDir(), "swarm.token")
	operatorTokenSink := filepath.Join(t.TempDir(), "operator-token.txt")
	apiTokenSink := filepath.Join(t.TempDir(), "api-token.txt")
	builderTokenSink := filepath.Join(t.TempDir(), "builder-token.txt")
	hostGatewayURLSink := filepath.Join(t.TempDir(), "host-gateway-url.txt")
	containerGatewayURLSink := filepath.Join(t.TempDir(), "container-gateway-url.txt")
	goBuildOutputSink := filepath.Join(t.TempDir(), "go-build-output.txt")
	pythonBinaryPathSink := filepath.Join(t.TempDir(), "python-binary-path.txt")
	pythonHealthAddrSink := filepath.Join(t.TempDir(), "python-health-addr.txt")
	pythonCommandSink := filepath.Join(t.TempDir(), "python-command.txt")
	psStateCountSink := filepath.Join(t.TempDir(), "ps-state-count.txt")
	healthHeadersSink := filepath.Join(t.TempDir(), "api-health-headers.txt")
	healthBodySink := filepath.Join(t.TempDir(), "api-health-body.txt")
	healthURLSink := filepath.Join(t.TempDir(), "api-health-url.txt")
	resetHeadersSink := filepath.Join(t.TempDir(), "reset-headers.txt")
	resetBodySink := filepath.Join(t.TempDir(), "reset-body.txt")
	resetURLSink := filepath.Join(t.TempDir(), "reset-url.txt")
	rpcHeadersSink := filepath.Join(t.TempDir(), "rpc-headers.txt")
	bodySink := filepath.Join(t.TempDir(), "rpc-body.txt")
	urlSink := filepath.Join(t.TempDir(), "rpc-url.txt")
	directiveHeadersSink := filepath.Join(t.TempDir(), "directive-headers.txt")
	directiveBodySink := filepath.Join(t.TempDir(), "directive-body.txt")
	directiveURLSink := filepath.Join(t.TempDir(), "directive-url.txt")
	dockerCallsSink := filepath.Join(t.TempDir(), "docker-calls.txt")
	psqlCallsSink := filepath.Join(t.TempDir(), "psql-calls.txt")
	eventsSink := filepath.Join(t.TempDir(), "events.txt")

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
		printf '%s serve --contracts %s --health-addr %s\n' "${BINARY_PATH}" "${CONTRACTS_ROOT}" "${HEALTH_ADDR}"
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
	writeExecutable(t, binDir, "docker", "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${DOCKER_CALLS_SINK}\"\nif [[ \"${1:-}\" == \"ps\" ]]; then exit 0; fi\nif [[ \"${1:-}\" == \"stop\" ]]; then exit 0; fi\nexit 0\n")
	writeExecutable(t, binDir, "psql", "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${PSQL_CALLS_SINK}\"\ncat >/dev/null\nexit 0\n")
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
printf 'serve_start\n' >> "${RUN_CLEAR_EVENTS_SINK}"
printf '%s' "${SWARM_OPERATOR_AUTH_TOKEN:-}" > "${PYTHON_OPERATOR_TOKEN_SINK}"
printf '%s' "${SWARM_API_TOKEN:-}" > "${PYTHON_API_TOKEN_SINK}"
printf '%s' "${SWARM_BUILDER_AUTH_TOKEN:-}" > "${PYTHON_ENV_SINK}"
printf '%s' "${SWARM_TOOL_GATEWAY_URL:-}" > "${PYTHON_HOST_GATEWAY_URL_SINK}"
printf '%s' "${SWARM_TOOL_GATEWAY_CONTAINER_URL:-}" > "${PYTHON_CONTAINER_GATEWAY_URL_SINK}"
printf '%s' "${BINARY_PATH:-}" > "${PYTHON_BINARY_PATH_SINK}"
printf '%s' "${HEALTH_ADDR:-}" > "${PYTHON_HEALTH_ADDR_SINK}"
printf '%s serve --contracts %s --health-addr %s' "${BINARY_PATH:-}" "${CONTRACTS_ROOT:-}" "${HEALTH_ADDR:-}" > "${PYTHON_COMMAND_SINK}"
(
  exec "${BINARY_PATH}" serve --contracts "${CONTRACTS_ROOT}" --health-addr "${HEALTH_ADDR}" >> "${LOG_FILE}" 2>&1 < /dev/null
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
  if [[ ! -s "${CURL_RESET_BODY_SINK}" ]]; then
    printf '%s' "${CURL_PRE_RESET_READYZ_CODE:-${CURL_READYZ_CODE:-200}}"
    exit 0
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
  if [[ -n "$out" ]]; then
    printf '{"error":"legacy api health is not canonical"}' > "$out"
  fi
  printf '500'
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
  printf '{"jsonrpc":"2.0","id":"run-clear","error":{"message":"legacy /api/rpc run.start is not canonical"}}'
  exit 0
fi
if [[ "$url" == *"/v1/rpc" ]]; then
  if [[ "$body" == *'"method":"health.check"'* ]]; then
    printf 'health.check\n' >> "${RUN_CLEAR_EVENTS_SINK}"
    if [[ ! -s "${CURL_RESET_BODY_SINK}" ]]; then
      health_ready="${RUN_CLEAR_TEST_PRE_RESET_HEALTH_READY}"
      health_db_ok="${RUN_CLEAR_TEST_PRE_RESET_HEALTH_DB_OK}"
      health_runtime_ok="${RUN_CLEAR_TEST_PRE_RESET_HEALTH_RUNTIME_OK}"
    else
      health_ready="${RUN_CLEAR_TEST_HEALTH_READY}"
      health_db_ok="${RUN_CLEAR_TEST_HEALTH_DB_OK}"
      health_runtime_ok="${RUN_CLEAR_TEST_HEALTH_RUNTIME_OK}"
    fi
    printf '%s' "$url" > "${CURL_API_HEALTH_URL_SINK}"
    printf '%s' "$body" > "${CURL_API_HEALTH_BODY_SINK}"
    if ((${#headers[@]})); then
      printf '%s\n' "${headers[@]}" > "${CURL_API_HEALTH_HEADERS_SINK}"
    else
      : > "${CURL_API_HEALTH_HEADERS_SINK}"
    fi
    want="Authorization: Bearer ${SWARM_API_TOKEN}"
    for header in "${headers[@]}"; do
      if [[ "$header" == "$want" ]]; then
        if [[ -n "$out" ]]; then
          printf '{"jsonrpc":"2.0","id":"run-clear-health","result":{"alive":true,"ready":%s,"db_ok":%s,"runtime_ok":%s,"bundle":{"workflow_name":"empire","workflow_version":"test","fingerprint":"%s"}}}' "${health_ready}" "${health_db_ok}" "${health_runtime_ok}" "${RUN_CLEAR_TEST_BUNDLE_FINGERPRINT}" > "$out"
        fi
        printf '%s' "${CURL_API_HEALTH_CODE:-200}"
        exit 0
      fi
    done
    if [[ -n "$out" ]]; then
      printf '{"jsonrpc":"2.0","id":"run-clear-health","error":{"code":-32000,"message":"missing authorization bearer token","data":{"code":"UNAUTHORIZED"}}}' > "$out"
    fi
    printf '401'
    exit 0
  fi
  if [[ "$body" == *'"method":"runtime.nuke"'* ]]; then
    printf 'runtime.nuke\n' >> "${RUN_CLEAR_EVENTS_SINK}"
    printf '%s' "$url" > "${CURL_RESET_URL_SINK}"
    if ((${#headers[@]})); then
      printf '%s\n' "${headers[@]}" > "${CURL_RESET_HEADERS_SINK}"
    else
      : > "${CURL_RESET_HEADERS_SINK}"
    fi
    printf '%s' "$body" > "${CURL_RESET_BODY_SINK}"
    want="Authorization: Bearer ${SWARM_API_TOKEN}"
    authorized=0
    for header in "${headers[@]}"; do
      if [[ "$header" == "$want" ]]; then
        authorized=1
      fi
    done
    if [[ "$authorized" != "1" ]]; then
      printf '{"jsonrpc":"2.0","id":"run-clear-reset-dev","error":{"code":-32000,"message":"missing authorization bearer token","data":{"code":"UNAUTHORIZED"}}}'
      exit 0
    fi
    if [[ "${RUN_CLEAR_TEST_NUKE_ERROR:-}" == "jsonrpc" ]]; then
      printf '{"jsonrpc":"2.0","id":"run-clear-reset-dev","error":{"code":-32000,"message":"runtime nuke failed","data":{"code":"RUNTIME_NUKE_IN_PROGRESS"}}}'
      exit 0
    fi
    printf '{"jsonrpc":"2.0","id":"run-clear-reset-dev","result":{"ok":%s,"status":"%s","dry_run":false,"operation_name":"runtime.destructive_reset","partial_failure":%s}}' "${RUN_CLEAR_TEST_NUKE_OK}" "${RUN_CLEAR_TEST_NUKE_STATUS}" "${RUN_CLEAR_TEST_NUKE_PARTIAL}"
    exit 0
  fi
  if [[ "$body" == *'"method":"run.start"'* ]]; then
    printf 'run.start\n' >> "${RUN_CLEAR_EVENTS_SINK}"
    printf '%s' "$url" > "${CURL_URL_SINK}"
    if ((${#headers[@]})); then
      printf '%s\n' "${headers[@]}" > "${CURL_HEADERS_SINK}"
    else
      : > "${CURL_HEADERS_SINK}"
    fi
    printf '%s' "$body" > "${CURL_BODY_SINK}"
    want="Authorization: Bearer ${SWARM_API_TOKEN}"
    for header in "${headers[@]}"; do
      if [[ "$header" == "$want" ]]; then
        printf '{"jsonrpc":"2.0","id":"run-clear","result":{"run_id":"11111111-1111-1111-1111-111111111111","status":"running"}}'
        exit 0
      fi
    done
    printf '{"jsonrpc":"2.0","id":"run-clear","error":{"code":-32000,"message":"missing authorization bearer token","data":{"code":"UNAUTHORIZED"}}}'
    exit 0
  fi
  printf '%s' "$url" > "${CURL_DIRECTIVE_URL_SINK}"
  printf 'agent.send_directive\n' >> "${RUN_CLEAR_EVENTS_SINK}"
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

	args := []string{filepath.Join(scriptDir, "run_clear.sh")}
	if cfg.mode != "" {
		args = append(args, cfg.mode)
	}
	cmd := exec.Command("bash", args...)
	cmd.Env = append(filteredEnv(
		"SWARM_OPERATOR_AUTH_TOKEN",
		"SWARM_API_TOKEN",
		"SWARM_BUILDER_AUTH_TOKEN",
		"SWARM_TOOL_GATEWAY_URL",
		"SWARM_TOOL_GATEWAY_CONTAINER_URL",
		"RUN_CLEAR_INPUT_EVENT",
		"RUN_CLEAR_INPUT_PAYLOAD_JSON",
		"RUN_CLEAR_RESET_IDEMPOTENCY_KEY",
	), []string{
		"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"PYTHON_OPERATOR_TOKEN_SINK=" + operatorTokenSink,
		"PYTHON_API_TOKEN_SINK=" + apiTokenSink,
		"PYTHON_ENV_SINK=" + builderTokenSink,
		"PYTHON_HOST_GATEWAY_URL_SINK=" + hostGatewayURLSink,
		"PYTHON_CONTAINER_GATEWAY_URL_SINK=" + containerGatewayURLSink,
		"PYTHON_BINARY_PATH_SINK=" + pythonBinaryPathSink,
		"PYTHON_HEALTH_ADDR_SINK=" + pythonHealthAddrSink,
		"PYTHON_COMMAND_SINK=" + pythonCommandSink,
		"GO_BUILD_OUTPUT_SINK=" + goBuildOutputSink,
		"GO_BUILD_LAUNCHER_MODE=" + defaultString(cfg.launcherMode, "steady"),
		"GO_BUILD_LAUNCHER_LOG_TEXT=" + cfg.launcherLogText,
		"PS_MODE=" + defaultString(cfg.psMode, "real"),
		"PS_STATE_COUNT_SINK=" + psStateCountSink,
		"CURL_API_HEALTH_HEADERS_SINK=" + healthHeadersSink,
		"CURL_API_HEALTH_BODY_SINK=" + healthBodySink,
		"CURL_API_HEALTH_URL_SINK=" + healthURLSink,
		"CURL_RESET_HEADERS_SINK=" + resetHeadersSink,
		"CURL_RESET_BODY_SINK=" + resetBodySink,
		"CURL_RESET_URL_SINK=" + resetURLSink,
		"CURL_HEALTHZ_CODE=200",
		"CURL_READYZ_CODE=" + defaultString(cfg.readyCode, "200"),
		"CURL_PRE_RESET_READYZ_CODE=" + defaultString(cfg.preResetReadyCode, defaultString(cfg.readyCode, "200")),
		"CURL_API_HEALTH_CODE=" + defaultString(cfg.apiHealthCode, "200"),
		"RUN_CLEAR_TEST_HEALTH_READY=" + defaultString(cfg.apiHealthReady, "true"),
		"RUN_CLEAR_TEST_HEALTH_DB_OK=" + defaultString(cfg.apiHealthDBOK, "true"),
		"RUN_CLEAR_TEST_HEALTH_RUNTIME_OK=" + defaultString(cfg.apiHealthRuntime, "true"),
		"RUN_CLEAR_TEST_PRE_RESET_HEALTH_READY=" + defaultString(cfg.preResetHealthReady, defaultString(cfg.apiHealthReady, "true")),
		"RUN_CLEAR_TEST_PRE_RESET_HEALTH_DB_OK=" + defaultString(cfg.preResetHealthDBOK, defaultString(cfg.apiHealthDBOK, "true")),
		"RUN_CLEAR_TEST_PRE_RESET_HEALTH_RUNTIME_OK=" + defaultString(cfg.preResetHealthRuntime, defaultString(cfg.apiHealthRuntime, "true")),
		"RUN_CLEAR_TEST_BUNDLE_FINGERPRINT=" + runClearTestBundleFingerprint,
		"RUN_CLEAR_TEST_NUKE_ERROR=" + cfg.nukeError,
		"RUN_CLEAR_TEST_NUKE_OK=" + defaultString(cfg.nukeOK, "true"),
		"RUN_CLEAR_TEST_NUKE_STATUS=" + defaultString(cfg.nukeStatus, "completed"),
		"RUN_CLEAR_TEST_NUKE_PARTIAL=" + defaultString(cfg.nukePartial, "false"),
		"CURL_HEADERS_SINK=" + rpcHeadersSink,
		"CURL_BODY_SINK=" + bodySink,
		"CURL_URL_SINK=" + urlSink,
		"CURL_DIRECTIVE_HEADERS_SINK=" + directiveHeadersSink,
		"CURL_DIRECTIVE_BODY_SINK=" + directiveBodySink,
		"CURL_DIRECTIVE_URL_SINK=" + directiveURLSink,
		"DOCKER_CALLS_SINK=" + dockerCallsSink,
		"PSQL_CALLS_SINK=" + psqlCallsSink,
		"RUN_CLEAR_EVENTS_SINK=" + eventsSink,
		"CONTRACTS_ROOT=/tmp/contracts",
		"HEALTH_ADDR=" + defaultString(cfg.healthAddr, "0.0.0.0:8081"),
		"BINARY_PATH=" + filepath.Join(t.TempDir(), "swarm"),
		"LOG_FILE=" + logFile,
		"PID_FILE=" + pidFile,
		"TOKEN_FILE=" + tokenFile,
		"START_TIMEOUT=" + defaultString(cfg.startTimeout, "1"),
	}...)
	if cfg.apiToken != "" {
		cmd.Env = append(cmd.Env, "SWARM_API_TOKEN="+cfg.apiToken)
	}
	if cfg.directiveAgent != "" {
		cmd.Env = append(cmd.Env, "DIRECTIVE_AGENT="+cfg.directiveAgent)
	}
	if cfg.directiveMessage != "" {
		cmd.Env = append(cmd.Env, "DIRECTIVE_MESSAGE="+cfg.directiveMessage)
	}
	if cfg.inputEvent != "" {
		cmd.Env = append(cmd.Env, "RUN_CLEAR_INPUT_EVENT="+cfg.inputEvent)
	}
	if cfg.inputPayloadJSON != "" {
		cmd.Env = append(cmd.Env, "RUN_CLEAR_INPUT_PAYLOAD_JSON="+cfg.inputPayloadJSON)
	}
	if cfg.resetIDKey != "" {
		cmd.Env = append(cmd.Env, "RUN_CLEAR_RESET_IDEMPOTENCY_KEY="+cfg.resetIDKey)
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
		apiToken:            readFileTrimmedOptional(t, apiTokenSink),
		operatorToken:       readFileTrimmedOptional(t, operatorTokenSink),
		builderToken:        readFileTrimmedOptional(t, builderTokenSink),
		hostGatewayURL:      readFileTrimmedOptional(t, hostGatewayURLSink),
		containerGatewayURL: readFileTrimmedOptional(t, containerGatewayURLSink),
		builtBinaryPath:     readFileTrimmedOptional(t, goBuildOutputSink),
		launchedBinaryPath:  readFileTrimmedOptional(t, pythonBinaryPathSink),
		launchedHealthAddr:  readFileTrimmedOptional(t, pythonHealthAddrSink),
		launchedCommand:     readFileTrimmedOptional(t, pythonCommandSink),
		healthHeaders:       readFileTrimmedOptional(t, healthHeadersSink),
		healthBody:          readFileTrimmedOptional(t, healthBodySink),
		healthURL:           readFileTrimmedOptional(t, healthURLSink),
		resetHeaders:        readFileTrimmedOptional(t, resetHeadersSink),
		resetBody:           readFileTrimmedOptional(t, resetBodySink),
		resetURL:            readFileTrimmedOptional(t, resetURLSink),
		rpcHeaders:          readFileTrimmedOptional(t, rpcHeadersSink),
		rpcBody:             readFileTrimmedOptional(t, bodySink),
		rpcURL:              readFileTrimmedOptional(t, urlSink),
		directiveHeaders:    readFileTrimmedOptional(t, directiveHeadersSink),
		directiveBody:       readFileTrimmedOptional(t, directiveBodySink),
		directiveURL:        readFileTrimmedOptional(t, directiveURLSink),
		dockerCalls:         readFileTrimmedOptional(t, dockerCallsSink),
		psqlCalls:           readFileTrimmedOptional(t, psqlCallsSink),
		events:              readFileTrimmedOptional(t, eventsSink),
	}, err
}

type jsonRPCRequestBody struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

func decodeJSONRPCBody(t *testing.T, raw string) jsonRPCRequestBody {
	t.Helper()
	var body jsonRPCRequestBody
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode JSON-RPC body %q: %v", raw, err)
	}
	if body.Params == nil {
		body.Params = map[string]any{}
	}
	return body
}

func mapParam(t *testing.T, values map[string]any, name string) map[string]any {
	t.Helper()
	value, ok := values[name]
	if !ok {
		t.Fatalf("missing %s in %#v", name, values)
	}
	mapped, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want object", name, value)
	}
	return mapped
}

func assertEventOrder(t *testing.T, events string, want ...string) {
	t.Helper()
	seen := strings.Fields(events)
	next := 0
	for _, event := range seen {
		if next < len(want) && event == want[next] {
			next++
		}
	}
	if next != len(want) {
		t.Fatalf("events = %v, want order %v", seen, want)
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

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
