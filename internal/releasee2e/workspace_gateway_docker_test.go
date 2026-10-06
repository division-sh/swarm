package releasee2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const realDockerProxyArgument = "--test-real-workspace-docker-proxy"

type workspaceDockerObservation struct {
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Container    string    `json:"container,omitempty"`
	Mode         string    `json:"mode"`
	Tool         string    `json:"tool,omitempty"`
	Failure      string    `json:"failure,omitempty"`
	Cancellation string    `json:"cancellation,omitempty"`
	Disconnect   bool      `json:"disconnect,omitempty"`
	Model        bool      `json:"model,omitempty"`
}

// This observer always executes the real Docker command. Its only fault is an
// exact network disconnect after a successful, real read_flow_data call. It
// never evaluates a model, handles a tool, or fabricates a Docker response.
func runWorkspaceDockerProxy(args []string) int {
	started := time.Now().UTC()
	docker, root := os.Getenv("RELEASE_E2E_REAL_DOCKER"), os.Getenv("RELEASE_E2E_WORKSPACE_DOCKER_ROOT")
	if !filepath.IsAbs(docker) || root == "" {
		return 2
	}
	input := io.Reader(os.Stdin)
	var ownedInput io.ReadCloser
	var nativeReader *bufio.Reader
	var rawRequest []byte
	var request struct {
		Mode string `json:"mode"`
		Tool string `json:"tool"`
	}
	container := ""
	for i, arg := range args {
		if arg == "/opt/swarm/bin/swarm" && i > 0 && i+1 < len(args) && strings.HasPrefix(args[i+1], releaseWorkerArgument+"=") {
			container = args[i-1]
			var err error
			ownedInput, err = releaseWorkerInput(os.Stdin)
			if err != nil {
				return 2
			}
			defer ownedInput.Close()
			nativeReader = bufio.NewReader(ownedInput)
			raw, err := readReleaseWorkerFrame(nativeReader)
			if err != nil || json.Unmarshal(raw, &request) != nil {
				return 2
			}
			rawRequest = raw
			break
		}
	}
	row := workspaceDockerObservation{StartedAt: started, Container: container, Mode: request.Mode, Tool: request.Tool}
	if os.Getenv("RELEASE_E2E_FORBID_PROVIDER_LAUNCH") == "1" && slices.Contains(args, "--output-format") {
		// A negative live-serve test may prove pre-model refusal, never buy a
		// model turn if the implementation regresses. This is a failing sentinel,
		// not a simulated provider response or transport success.
		_ = os.WriteFile(filepath.Join(root, "unexpected-provider-launch"), []byte("provider launch attempted\n"), 0o600)
		return 2
	}
	if request.Mode == "probe" && os.Getenv("RELEASE_E2E_WORKSPACE_DISCONNECT") == "1" {
		if _, err := os.Stat(filepath.Join(root, "read-succeeded")); err == nil {
			if err := exec.Command(docker, "network", "disconnect", "mas_default", container).Run(); err != nil {
				fmt.Fprintln(os.Stderr, "exact test-owned workspace disconnect failed")
				return 2
			}
			row.Disconnect = true
		}
	}
	var output bytes.Buffer
	cmd := exec.Command(docker, args...)
	cmd.Stderr = os.Stderr
	var err error
	if nativeReader == nil {
		cmd.Stdin, cmd.Stdout = input, &output
		err = cmd.Run()
	} else {
		stdin, pipeErr := cmd.StdinPipe()
		if pipeErr != nil {
			return 2
		}
		defer stdin.Close()
		stdout, pipeErr := cmd.StdoutPipe()
		if pipeErr != nil {
			return 2
		}
		defer stdout.Close()
		if cmd.Start() != nil {
			return 2
		}
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			defer stdin.Close()
			if _, err := stdin.Write(rawRequest); err == nil {
				_, _ = io.Copy(stdin, nativeReader)
			}
		}()
		reader := bufio.NewReader(stdout)
		ready, readyErr := readReleaseWorkerFrame(reader)
		if readyErr == nil {
			_, readyErr = os.Stdout.Write(ready)
		}
		if readyErr == nil {
			_, readyErr = io.Copy(&output, io.LimitReader(reader, releaseWorkerMaxBytes+1))
		}
		err = cmd.Wait()
		_ = ownedInput.Close()
		_ = os.Stdin.Close()
		<-joined
		if readyErr != nil || output.Len() > releaseWorkerMaxBytes {
			return 2
		}
	}
	if request.Mode != "" {
		row.FinishedAt = time.Now().UTC()
		var result struct {
			ModelStarted bool            `json:"model_started"`
			Cancellation string          `json:"cancellation"`
			ToolResult   json.RawMessage `json:"tool_result"`
			Failure      *struct {
				Detail struct {
					Code string `json:"code"`
				} `json:"detail"`
			} `json:"failure"`
		}
		if json.Unmarshal(output.Bytes(), &result) != nil {
			return 2
		}
		row.Model = result.ModelStarted
		row.Cancellation = result.Cancellation
		if result.Failure != nil {
			row.Failure = result.Failure.Detail.Code
		}
		if request.Mode == "call" && request.Tool == "read_flow_data" && result.Failure == nil {
			var wire struct {
				IsError bool `json:"isError"`
			}
			if json.Unmarshal(result.ToolResult, &wire) != nil || wire.IsError {
				return 2
			}
			if err := os.WriteFile(filepath.Join(root, "read-succeeded"), []byte("real HTTP read acknowledged\n"), 0o600); err != nil {
				return 2
			}
		}
		log, openErr := os.OpenFile(filepath.Join(root, "observations.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if openErr != nil {
			return 2
		}
		writeErr := json.NewEncoder(log).Encode(row)
		closeErr := log.Close()
		if writeErr != nil || closeErr != nil {
			return 2
		}
	}
	if _, copyErr := io.Copy(os.Stdout, &output); copyErr != nil {
		return 2
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return exit.ExitCode()
	}
	if err != nil {
		return 2
	}
	return 0
}

func TestCompiledWorkspaceWorkerIdentityEntry(t *testing.T) {
	t.Run("observer framing", func(t *testing.T) {
		frame := strings.Repeat("x", releaseWorkerMaxBytes) + "\n"
		if raw, err := readReleaseWorkerFrame(bufio.NewReader(strings.NewReader(frame + "next\n"))); err != nil || string(raw) != frame {
			t.Fatalf("exact bounded observer frame: len=%d err=%v", len(raw), err)
		}
		for _, invalid := range []string{strings.Repeat("x", releaseWorkerMaxBytes+1) + "\n", "unterminated"} {
			if _, err := readReleaseWorkerFrame(bufio.NewReader(strings.NewReader(invalid))); err == nil {
				t.Fatal("unbounded or unterminated observer frame accepted")
			}
		}
	})
	t.Run("observer forwarding descriptor joins", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("Linux pollable forwarding descriptor control")
		}
		input, sender, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer input.Close()
		defer sender.Close()
		owned, err := releaseWorkerInput(input)
		if err != nil {
			t.Fatal(err)
		}
		defer owned.Close()
		joined := make(chan error, 1)
		go func() {
			var frame [1]byte
			_, err := owned.Read(frame[:])
			joined <- err
		}()
		select {
		case err := <-joined:
			t.Fatalf("held observer input returned without disposal: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		if err := owned.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-joined:
			if err == nil {
				t.Fatal("disposed observer descriptor fabricated input")
			}
		case <-time.After(time.Second):
			t.Fatal("disposed observer descriptor left forwarding read alive")
		}
	})
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, releaseWorkerArgument)
	cmd.Env = goldenProcessEnv(t, root, "", 0)
	cmd.Stdin = strings.NewReader(`{"mode":"identity"}`)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var result struct {
		Identity struct {
			ABI string `json:"abi"`
		} `json:"identity"`
		Failure      json.RawMessage `json:"failure"`
		Cancellation json.RawMessage `json:"cancellation"`
	}
	decodeErr := json.Unmarshal(stdout.Bytes(), &result)
	if err != nil || decodeErr != nil || len(result.Failure) != 0 || len(result.Cancellation) != 0 || result.Identity.ABI != releaseWorkerABI {
		t.Fatalf("compiled native worker entry: exit=%v decode=%v stdout=%q stderr=%q", err, decodeErr, stdout.String(), stderr.String())
	}
}

func TestWorkspaceMCPCompiledHostConformance(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	source := filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp")
	result := runReleaseCommand(t, time.Minute, root, env, "", binary, "test", source, "tests/transport.yaml", "--timeout", "20s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("compiled host native worker/MCP/read/emit: %v\n%s", result.err, result.output)
	}
}

func TestWorkspaceMCPCompiledStaticDoctorProofCredit(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	for _, asJSON := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%v", asJSON), func(t *testing.T) {
			args := []string{"doctor", "--backend", "claude_cli", "--workspace-backend", "host", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0"}
			if asJSON {
				args = append(args, "--json")
			}
			result := runReleaseCommand(t, 20*time.Second, root, env, "", binary, args...)
			if result.err != nil {
				exit, ok := result.err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 1 {
					t.Fatalf("static doctor failed outside prerequisite admission: %v\n%s", result.err, result.output)
				}
			}
			if !strings.Contains(result.output, "not checked from inside a container") || !strings.Contains(result.output, "credential validity not probed") {
				t.Fatalf("static doctor manufactured network or credential proof: %s", result.output)
			}
			if strings.Contains(result.output, "workspace_gateway_probed") {
				t.Fatalf("static doctor claimed a live gateway observation: %s", result.output)
			}
			if asJSON {
				var report struct {
					Mode, Backend string
					Findings      []struct {
						Category, Code, Status, Severity string
					}
				}
				if err := json.Unmarshal([]byte(result.output), &report); err != nil || report.Mode != "doctor" || report.Backend != "claude_cli" {
					t.Fatalf("static doctor's public JSON is not its prerequisite report: %+v, %v\n%s", report, err, result.output)
				}
				var unprobed int
				for _, finding := range report.Findings {
					if finding.Code == "workspace_gateway_not_probed" {
						unprobed++
						if finding.Category != "gateway_prerequisite" || finding.Status != "skipped" || finding.Severity != "info" {
							t.Fatalf("static gateway finding granted execution credit: %+v", finding)
						}
					}
				}
				if unprobed != 1 {
					t.Fatalf("static doctor did not report exactly one unprobed gateway: %+v", report)
				}
			}
		})
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(entry.Name(), "swarm-test-session-") || (!entry.IsDir() && filepath.Ext(path) == ".db") {
			t.Errorf("static doctor acquired test execution or retained state: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceMCPCompiledDockerConformance(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Linux Docker proof runs in the workspace-image CI job; no Docker proof credit from a skip")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native Linux workspace proof requires Linux")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	docker, err = filepath.Abs(docker)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, docker, "image", "inspect", "swarm-workspace:latest").Run(); err != nil {
		t.Fatalf("workspace-image owner did not provision its image: %v", err)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	observerBinary := buildOwnedWorkspaceInspectionBinary(t, root)
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "real-docker-bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	writeReleaseFile(t, filepath.Join(bin, "docker"), "#!/bin/sh\nexec "+workspaceDockerShellQuote(helper)+" "+realDockerProxyArgument+" \"$@\"\n")
	if err := os.Chmod(filepath.Join(bin, "docker"), 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp")
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprintf("disconnect=%v", disconnect), func(t *testing.T) {
			owned := t.TempDir()
			env := goldenProcessEnv(t, owned, "", 0)
			env = slices.DeleteFunc(env, func(v string) bool { return strings.HasPrefix(v, "PATH=") || strings.HasPrefix(v, "TMPDIR=") })
			tmp := filepath.Join(owned, "private-sessions")
			if err := os.MkdirAll(tmp, 0o700); err != nil {
				t.Fatal(err)
			}
			env = append(env, "PATH="+bin, "TMPDIR="+tmp, "RELEASE_E2E_REAL_DOCKER="+docker, "RELEASE_E2E_WORKSPACE_DOCKER_ROOT="+owned)
			if disconnect {
				env = append(env, "RELEASE_E2E_WORKSPACE_DISCONNECT=1")
			}
			stopInspection := startWorkspacePrivateStoreObserver(t, observerBinary, tmp)
			t.Cleanup(func() {
				if !t.Failed() {
					return
				}
				for _, name := range []string{"observations.jsonl", "workspace-observer-last-snapshot.json", "workspace-observer-last-snapshot.json.timing.json"} {
					data, err := os.ReadFile(filepath.Join(owned, name))
					t.Logf("failure evidence %s (read error=%v):\n%s", name, err, data)
				}
			})
			args := []string{"test", source, "tests/transport.yaml", "--workspace-backend", "docker", "--timeout", "10s", "--poll-interval", "25ms"}
			result := runReleaseCommand(t, time.Minute, root, env, "", binary, args...)
			observed := stopInspection()
			if !disconnect && (result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1")) {
				t.Fatalf("real Docker public read/emit/store assertion: %v\nselected-store observation=%+v\n%s", result.err, observed, result.output)
			}
			if disconnect && result.err == nil {
				t.Fatalf("disconnected successor reported success: %s", result.output)
			}
			data, err := os.ReadFile(filepath.Join(owned, "observations.jsonl"))
			if err != nil {
				t.Fatalf("missing real container evidence: %v\n%s", err, result.output)
			}
			var models, reads, emits, refusals int
			for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
				var row workspaceDockerObservation
				if err := json.Unmarshal(line, &row); err != nil {
					t.Fatal(err)
				}
				if row.Mode == "model" && row.Model {
					models++
				}
				if row.Mode == "call" && row.Tool == "read_flow_data" && row.Failure == "" {
					reads++
				}
				if row.Mode == "call" && row.Tool == "emit_work_completed" && row.Failure == "" {
					emits++
				}
				if row.Mode == "probe" && row.Disconnect && row.Failure == "workspace_gateway_unreachable" {
					refusals++
				}
			}
			if reads != 1 || (!disconnect && (models != 2 || emits != 1 || refusals != 0)) || (disconnect && (models != 1 || emits != 0 || refusals < 1)) {
				t.Fatalf("transport cardinality models=%d reads=%d emits=%d refusals=%d\n%s\n%s", models, reads, emits, refusals, data, result.output)
			}
			if disconnect && (observed.AgentDeliveries != 1 || observed.Delivered != 0 || observed.Emitted != 0) {
				t.Fatalf("disconnected turn lost selected-store refusal proof: %+v", observed)
			}
			entries, err := os.ReadDir(tmp)
			if err != nil || len(entries) != 0 {
				t.Fatalf("private session cleanup: %v entries=%v", err, entries)
			}
		})
	}
}

func workspaceDockerShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func TestWorkspaceMCPCompiledDockerDoctorAndLiveBootRefusal(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real workspace-image proof; no Docker credit from a skip")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native Linux workspace proof requires Linux")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	docker, err = filepath.Abs(docker)
	if err != nil {
		t.Fatal(err)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "real-docker-bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(bin, "docker")
	writeReleaseFile(t, wrapper, "#!/bin/sh\nexec "+workspaceDockerShellQuote(helper)+" "+realDockerProxyArgument+" \"$@\"\n")
	if err := os.Chmod(wrapper, 0o700); err != nil {
		t.Fatal(err)
	}
	env := goldenProcessEnv(t, root, "", 0)
	env = slices.DeleteFunc(env, func(v string) bool { return strings.HasPrefix(v, "PATH=") })
	env = append(env, "PATH="+bin, "RELEASE_E2E_REAL_DOCKER="+docker, "RELEASE_E2E_WORKSPACE_DOCKER_ROOT="+root, "RELEASE_E2E_FORBID_PROVIDER_LAUNCH=1")
	configArgs := workspaceDockerProofConfig(t, root)
	for _, probe := range []struct {
		name, address, code string
		args                []string
		failed              bool
	}{
		{"static", "0.0.0.0:0", "workspace_gateway_not_probed", nil, false},
		{"inside_container", "0.0.0.0:0", "workspace_gateway_probed", []string{"--gateway-probe"}, false},
		{"explicit_loopback", "127.0.0.1:0", "workspace_gateway_unreachable", []string{"--gateway-probe"}, true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			args := []string{"doctor", "--workspace-backend", "docker", "--mcp-listen-addr", probe.address, "--api-listen-addr", "127.0.0.1:0", "--json"}
			args = append(args, probe.args...)
			args = append(args, configArgs...)
			result := runReleaseCommand(t, 30*time.Second, root, env, "", binary, args...)
			if (result.err != nil) != probe.failed || !strings.Contains(result.output, `"code": "`+probe.code+`"`) {
				t.Fatalf("doctor target evidence: %v\n%s", result.err, result.output)
			}
			if !strings.Contains(result.output, "credential validity") {
				t.Fatalf("doctor gave undifferentiated credit: %s", result.output)
			}
		})
	}
	config := filepath.Join(root, "live-workspace.yaml")
	image := buildWorkspaceBootRefusalImage(t, docker)
	writeReleaseFile(t, config, "llm:\n  backend: claude_cli\nworkspace:\n  backend: docker\n  image: "+image+"\n")
	source := filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp")
	credential := runReleaseCommand(t, 15*time.Second, root, env, "pre-model-refusal-test-not-a-provider-credential\n", binary, "secrets", "set", "CLAUDE_CODE_OAUTH_TOKEN", "--stdin")
	if credential.err != nil {
		t.Fatalf("public credential setup for pre-model refusal: %v\n%s", credential.err, credential.output)
	}
	contracts := filepath.Join(root, "contracts")
	copyReleaseTree(t, source, contracts)
	result := runReleaseCommand(t, time.Minute, root, env, "", binary, "serve", contracts, "--dev", "--config", config, "--workspace-backend", "docker", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
	if result.err == nil || !strings.Contains(result.output, "workspace_gateway_unreachable") || strings.Contains(result.output, "[22/22] ready") {
		t.Fatalf("live Docker boot did not refuse before model/readiness: %v\n%s", result.err, result.output)
	}
	if _, err := os.Stat(filepath.Join(root, "unexpected-provider-launch")); !os.IsNotExist(err) {
		t.Fatalf("live negative reached provider launch: %v", err)
	}
}

func buildWorkspaceBootRefusalImage(t *testing.T, docker string) string {
	t.Helper()
	root := t.TempDir()
	image := fmt.Sprintf("swarm-workspace:boot-refusal-%d-%d", os.Getpid(), time.Now().UnixNano())
	// The provider is never called. Its executable prerequisite must be real
	// inside the image, so missing Claude cannot mask the gateway refusal.
	writeReleaseFile(t, filepath.Join(root, "claude"), "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '2.1.87 (boot-refusal fixture)\\n'; exit 0; fi\nprintf 'forbidden provider launch\\n' >&2\nexit 86\n")
	writeReleaseFile(t, filepath.Join(root, "Dockerfile"), "FROM swarm-workspace:latest\nUSER root\nCOPY claude /usr/local/bin/claude\nRUN chmod 755 /usr/local/bin/claude\nUSER agent\n")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, docker, "build", "--network=none", "-t", image, root).CombinedOutput(); err != nil {
		t.Fatalf("build exact preflight-only fixture image: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, docker, "image", "rm", image).CombinedOutput(); err != nil {
			t.Errorf("remove exact fixture image: %v\n%s", err, output)
		}
	})
	return image
}

func workspaceDockerProofConfig(t *testing.T, root string) []string {
	t.Helper()
	network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK")
	if network == "" {
		t.Log("workspace_network=implicit Linux default; no hardened-host override")
		return nil
	}
	// Local hardened hosts may opt into an explicit authored topology. The
	// hosted Ubuntu job supplies no override and must prove the Linux default.
	t.Logf("workspace_network=explicit %q; no implicit-default proof credit", network)
	value, err := json.Marshal(network)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "explicit-workspace-network.yaml")
	writeReleaseFile(t, path, "workspace:\n  network: "+string(value)+"\n")
	return []string{"--config", path}
}

type workspacePrivateStoreObservation struct {
	Observed        bool `json:"observed"`
	Finished        bool `json:"finished"`
	AgentDeliveries int  `json:"agent_deliveries"`
	Delivered       int  `json:"delivered"`
	Emitted         int  `json:"emitted"`
}

func startWorkspacePrivateStoreObserver(t *testing.T, binary, root string) func() workspacePrivateStoreObservation {
	t.Helper()
	cmd := exec.Command(binary, "-test.run=^TestWorkspaceInvocationReadOnlyObserverChild$")
	cmd.Env = append(os.Environ(), "SWARM_TEST_WORKSPACE_OBSERVER_CHILD=1", "SWARM_TEST_WORKSPACE_OBSERVER_ROOT="+root,
		"SWARM_TEST_WORKSPACE_OBSERVER_DIAGNOSTICS="+filepath.Join(filepath.Dir(root), "workspace-observer-last-snapshot.json"))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	type joinedResult struct {
		observation workspacePrivateStoreObservation
		err         error
	}
	ready := make(chan error, 1)
	joined := make(chan joinedResult, 1)
	go func() {
		decoder := json.NewDecoder(io.LimitReader(stdout, 4096))
		decoder.DisallowUnknownFields()
		var greeting struct {
			Ready bool `json:"ready"`
		}
		err := decoder.Decode(&greeting)
		if err == nil && !greeting.Ready {
			err = errors.New("workspace observer did not become ready")
		}
		ready <- err
		var observation workspacePrivateStoreObservation
		for err == nil {
			err = decoder.Decode(&observation)
			if err == nil && !observation.Observed {
				err = errors.New("workspace observer returned unobserved counts")
			}
			if err != nil || observation.Finished {
				break
			}
		}
		if err == nil {
			var trailing any
			if tail := decoder.Decode(&trailing); tail != io.EOF {
				err = errors.New("workspace observer returned trailing protocol data")
			}
		}
		joined <- joinedResult{observation, errors.Join(err, cmd.Wait())}
	}()
	var once sync.Once
	var result joinedResult
	stop := func() workspacePrivateStoreObservation {
		t.Helper()
		once.Do(func() {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
				result.err = err
			}
			select {
			case observed := <-joined:
				result.observation, result.err = observed.observation, errors.Join(result.err, observed.err)
			case <-time.After(5 * time.Second):
				killErr := cmd.Process.Kill()
				observed := <-joined
				result.err = errors.Join(result.err, errors.New("workspace observer did not join graceful stop"), killErr, observed.err)
			}
		})
		if result.err != nil || !result.observation.Observed {
			t.Fatalf("independent workspace observation failed: %v\n%s", result.err, stderr.String())
		}
		return result.observation
	}
	t.Cleanup(func() { stop() })
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("workspace observer startup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workspace observer did not acknowledge startup")
	}
	return stop
}
