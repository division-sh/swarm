package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/google/uuid"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

func TestWorkerRealDockerIdentityReuseAndCancellationJoin(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real workspace-image proof; no Docker credit from a skip")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native Linux workspace proof requires Linux")
	}
	manager := NewDockerManager()
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = "none"
	manager.SetConfig(cfg)
	name := "agent-g-workspace-worker-" + uuid.NewString()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := manager.RunDocker(ctx, "rm", "--force", name); err != nil {
			t.Errorf("dispose exact proof container: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := manager.EnsureContainerRunning(ctx, name, []string{"--entrypoint", "sleep", cfg.WorkspaceImage, "infinity"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureContainerRunning(ctx, name, nil); err != nil {
		t.Fatalf("real immutable input reuse: %v", err)
	}
	target := &Target{Backend: BackendDocker, Container: name, Workdir: "/"}
	result, err := RunWorker(ctx, target, manager.DockerBin(), worker.Request{Mode: "identity"})
	identity, identityErr := worker.ExecutableIdentity()
	if err != nil || identityErr != nil || result.Identity != identity {
		var execution *WorkerExecutionError
		if errors.As(err, &execution) {
			if joined, ok := execution.Err.(interface{ Unwrap() []error }); ok {
				for _, cause := range joined.Unwrap() {
					t.Logf("worker join cause: %v; underlying: %v", cause, errors.Unwrap(cause))
					var exit *exec.ExitError
					if errors.As(cause, &exit) {
						t.Logf("process observation stderr: %q", exit.Stderr)
					}
				}
			}
		}
		t.Fatalf("real target bytes/ABI/interpreter/version: %+v %v %v", result.Identity, err, identityErr)
	}
	source := []byte("def handle(input):\n    while True:\n        pass\n")
	sum := sha256.Sum256(source)
	model := &pythonmodule.Request{
		ModuleID: "bounded-cancellation-proof", RowID: "one-owned-invocation",
		Digest: "sha256:" + hex.EncodeToString(sum[:]), Entry: mockperformance.EntryHandle,
		Source: source, Input: []byte("{}"), Fuel: mockperformance.ExecutionFuel,
		MemoryPages: mockperformance.ExecutionMemoryPages, OutputBytes: mockperformance.ExecutionOutputBytes,
	}
	cancelCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	_, err = RunWorker(cancelCtx, target, manager.DockerBin(), worker.Request{Mode: "model", Module: model})
	var execution *WorkerExecutionError
	if err == nil || !errors.As(err, &execution) || !execution.Started {
		t.Fatalf("bounded target cancellation lost execution facts: %v", err)
	}
	top, err := manager.RunDocker(ctx, "top", name, "-eo", "pid,args")
	if err != nil || strings.Contains(top, worker.Argument) {
		t.Fatalf("returned before exact worker joined: %q, %v", top, err)
	}
}

func TestWorkerNativeHostRefusalRetainsLaunchFacts(t *testing.T) {
	target := &Target{Backend: BackendHost, Workdir: t.TempDir()}
	identity, err := worker.ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	result, err := RunWorker(context.Background(), target, "", worker.Request{Mode: "identity"})
	if err != nil || result.Identity != identity {
		t.Fatalf("native identity = %+v: %v", result, err)
	}
	result, err = RunWorker(context.Background(), target, "", worker.Request{Mode: "model"})
	var execution *WorkerExecutionError
	if !errors.As(err, &execution) || !execution.Started || !execution.Observed || execution.ModelStarted || result.Failure == nil {
		t.Fatalf("pre-model bounds refusal lost launch facts: %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = RunWorker(ctx, target, "", worker.Request{Mode: "model"})
	if !errors.Is(err, context.Canceled) || !errors.As(err, &execution) || execution.Started {
		t.Fatalf("canceled admission started worker: %v", err)
	}
}

func TestWorkerNativeHostModelUsesCapturedSourceAndPinnedBounds(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	root := t.TempDir()
	target := &Target{Backend: BackendHost, Workdir: root}
	captured := []byte("def handle(input):\n    return {'captured': input['marker']}\n")
	sum := sha256.Sum256(captured)
	module := pythonmodule.Request{
		ModuleID: "captured.py", RowID: "one-captured-model",
		Digest: "sha256:" + hex.EncodeToString(sum[:]), Entry: mockperformance.EntryHandle,
		Source: captured, Input: []byte(`{"marker":"captured-only"}`),
		Fuel: mockperformance.ExecutionFuel, MemoryPages: mockperformance.ExecutionMemoryPages,
		OutputBytes: mockperformance.ExecutionOutputBytes,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The target contains a conflicting source file; only the captured bytes
	// and pinned interpreter may produce a completion.
	for _, ambient := range []string{
		"raise RuntimeError('ambient source must not run')\n",
		"def handle(input):\n    return {'captured': 'changed-file'}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, module.ModuleID), []byte(ambient), 0o600); err != nil {
			t.Fatal(err)
		}
		result, err := RunWorker(ctx, target, "", worker.Request{Mode: "model", Module: &module})
		identity := pythonmodule.RuntimeIdentity()
		if err != nil || !result.ModelStarted || result.Module == nil {
			t.Fatalf("captured native model failed: %+v %v", result, err)
		}
		if string(result.Module.Output) != `{"captured":"captured-only"}` || result.Module.SourceHash != module.Digest || result.Module.InterpreterSHA != identity.InterpreterDigest || result.Module.Interpreter != identity.Interpreter || result.Module.SnapshotHash != identity.SnapshotDigest || result.Module.HarnessABI != identity.HarnessABI || result.Module.Engine != identity.Engine {
			t.Fatalf("native model escaped captured/pinned input: %+v", result.Module)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*pythonmodule.Request)
		bounds bool
	}{
		{"wrong_digest", func(m *pythonmodule.Request) { m.Digest = "sha256:" + strings.Repeat("0", 64) }, false},
		{"denied_import", func(m *pythonmodule.Request) {
			m.Source = []byte("import os\n\ndef handle(input):\n    return {'cwd': os.getcwd()}\n")
			sum := sha256.Sum256(m.Source)
			m.Digest = "sha256:" + hex.EncodeToString(sum[:])
		}, false},
		{"output_cap", func(m *pythonmodule.Request) {
			m.Source = []byte("def handle(input):\n    return {'text': 'x' * " + strconv.Itoa(mockperformance.ExecutionOutputBytes+1) + "}\n")
			sum := sha256.Sum256(m.Source)
			m.Digest = "sha256:" + hex.EncodeToString(sum[:])
		}, false},
		{"fuel_override", func(m *pythonmodule.Request) { m.Fuel++ }, true},
		{"memory_override", func(m *pythonmodule.Request) { m.MemoryPages++ }, true},
		{"output_override", func(m *pythonmodule.Request) { m.OutputBytes++ }, true},
		{"entry_override", func(m *pythonmodule.Request) { m.Entry = "foreign" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := module
			tc.change(&candidate)
			result, err := RunWorker(ctx, target, "", worker.Request{Mode: "model", Module: &candidate})
			var execution *WorkerExecutionError
			if !errors.As(err, &execution) || !execution.Started || !execution.Observed || result.Failure == nil || result.Module != nil {
				t.Fatalf("native model input admitted: %+v %v", result, err)
			}
			if tc.bounds && (result.ModelStarted || result.Failure.Detail.Code != "mock_worker_bounds_invalid") {
				t.Fatalf("native fixed-bound refusal lost pre-model facts: %+v %v", result, err)
			}
		})
	}
}

func TestWorkerNativeHostCanceledCommittedCallIsUncertainAndNotReplayed(t *testing.T) {
	entered, retired := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Method != "tools/call" {
			http.Error(w, "invalid proof request", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		close(entered)
		<-r.Context().Done()
		close(retired)
	}))
	defer server.Close()
	outer, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx, stop := context.WithCancel(outer)
	defer stop()
	target := &Target{Backend: BackendHost, Workdir: t.TempDir()}
	completed := make(chan error, 1)
	go func() {
		_, err := RunWorker(ctx, target, "", worker.Request{Mode: "call", Tool: "commit_once", Occurrence: "one-host-call", Arguments: json.RawMessage(`{}`), Gateway: toolgateway.HTTPObservation{
			URL: server.URL, Headers: map[string]string{"Authorization": "Bearer offline-host", "X-SWARM-Context-Token": "offline-host-context"},
		}})
		completed <- err
	}()
	select {
	case <-entered:
	case err := <-completed:
		t.Fatalf("cancellation never reached a launched call: %v", err)
	case <-outer.Done():
		t.Fatal("host call checkpoint was not reached")
	}
	stop()
	err := <-completed
	var execution *WorkerExecutionError
	if !errors.As(err, &execution) || !execution.Started || !execution.Observed || !errors.Is(err, context.Canceled) || failures.FromError(err, "test", "cancel").Failure.Class != failures.ClassOutcomeUncertain {
		t.Fatalf("canceled launched call lost its uncertain outcome or cause: %v", err)
	}
	select {
	case <-retired:
	default:
		t.Fatal("native child returned before its held HTTP request retired")
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled possibly committed call was replayed: %d", calls.Load())
	}
	if _, err := RunWorker(outer, target, "", worker.Request{Mode: "identity"}); err != nil {
		t.Fatalf("host workspace cannot execute after joined cancellation: %v", err)
	}
}

func TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{})
}

func TestWorkerRealDockerLostClientJoinsGatewayRequest(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{lostClient: true})
}

func TestWorkerRealDockerLostClientAfterToolCommitDoesNotReplay(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{lostClient: true, committedCall: true})
}

func TestWorkerRealDockerCallDeadlineKeepsCommittedOutcomeUncertain(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{committedCall: true})
}

func TestWorkerRealDockerLostClientRetainsCleanupFailure(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{lostClient: true, cleanupFailure: true})
}

func TestWorkerRealDockerLostClientPreservesSiblingAndSource(t *testing.T) {
	proveWorkerRealDockerHTTPJoin(t, workerHTTPJoinProof{lostClient: true, sibling: true})
}

type workerHTTPJoinProof struct {
	lostClient, committedCall, cleanupFailure, sibling bool
}

func proveWorkerRealDockerHTTPJoin(t *testing.T, proof workerHTTPJoinProof) {
	t.Helper()
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Docker request lifetime proof; a skip earns no credit")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native Linux workspace proof requires Linux")
	}
	entered, retired := make(chan struct{}), make(chan struct{})
	siblingEntered, siblingRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseSibling := func() { releaseOnce.Do(func() { close(siblingRelease) }) }
	var committed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
				Meta struct {
					Occurrence string `json:"claudecode/toolUseId"`
				} `json:"_meta"`
			} `json:"params"`
		}
		if r.Header.Get("Authorization") != "Bearer offline-join-proof" || json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid proof request", http.StatusBadRequest)
			return
		}
		if request.Method == "initialize" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}`))
			return
		}
		if proof.sibling && r.Header.Get("X-SWARM-Context-Token") == "sibling-proof-context" && request.Method == "tools/list" {
			close(siblingEntered)
			select {
			case <-siblingRelease:
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}`))
			case <-r.Context().Done():
			}
			return
		}
		method := "tools/list"
		if proof.committedCall {
			method = "tools/call"
		}
		if request.Method != method {
			http.Error(w, "unexpected proof operation", http.StatusBadRequest)
			return
		}
		if proof.committedCall {
			if request.Params.Name != "commit_once" || request.Params.Meta.Occurrence != "one-owned-call" {
				http.Error(w, "foreign call occurrence", http.StatusBadRequest)
				return
			}
			committed.Add(1)
		}
		close(entered)
		<-r.Context().Done()
		close(retired)
	}))
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	t.Cleanup(releaseSibling)
	manager := NewDockerManager()
	cfg := DefaultDockerConfig()
	if network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK"); network != "" {
		cfg.WorkspaceNetwork = network
	}
	manager.SetConfig(cfg)
	name := "agent-g-workspace-http-" + uuid.NewString()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := manager.RunDocker(ctx, "rm", "--force", name); err != nil {
			t.Errorf("dispose exact HTTP proof container: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := manager.EnsureContainerRunning(ctx, name, []string{"--entrypoint", "sleep", cfg.WorkspaceImage, "infinity"}); err != nil {
		t.Fatal(err)
	}
	target := &Target{Backend: BackendDocker, Container: name, Workdir: "/"}
	containerID, err := manager.RunDocker(ctx, "inspect", "--format", "{{.Id}}", name)
	if err != nil {
		t.Fatal(err)
	}
	gateway := toolgateway.HTTPObservation{
		URL:     "http://host.docker.internal:" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port) + "/mcp",
		Headers: map[string]string{"Authorization": "Bearer offline-join-proof", "X-SWARM-Context-Token": "owned-proof-context"},
	}
	var siblingCompleted chan error
	if proof.sibling {
		siblingCompleted = make(chan error, 1)
		observation := gateway
		observation.Headers = map[string]string{"Authorization": "Bearer offline-join-proof", "X-SWARM-Context-Token": "sibling-proof-context"}
		go func() {
			_, err := RunWorker(ctx, target, manager.DockerBin(), worker.Request{Mode: "probe", Gateway: observation})
			siblingCompleted <- err
			close(siblingCompleted)
		}()
		t.Cleanup(func() {
			releaseSibling()
			<-siblingCompleted
		})
		select {
		case <-siblingEntered:
		case err := <-siblingCompleted:
			t.Fatalf("sibling never reached its held request: %v", err)
		case <-ctx.Done():
			t.Fatal("sibling checkpoint was not reached")
		}
	}
	dockerBin := manager.DockerBin()
	var clientPID, argumentFile string
	if proof.lostClient {
		binary, err := exec.LookPath(dockerBin)
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		clientPID, dockerBin = filepath.Join(root, "client.pid"), filepath.Join(root, "owned-docker-client")
		argumentFile = filepath.Join(root, "worker.argument")
		script := "#!/bin/sh\n"
		if proof.cleanupFailure {
			script += "if [ \"$1\" = top ]; then exit 73; fi\n"
		}
		script += "if [ \"$1\" = exec ]; then\nprintf '%s' $$ > " + strconv.Quote(clientPID) + "\nfor argument do :; done\nprintf '%s' \"$argument\" > " + strconv.Quote(argumentFile) + "\nfi\nexec " + strconv.Quote(binary) + " \"$@\"\n"
		if err := os.WriteFile(dockerBin, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	deadlineCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	completed := make(chan error, 1)
	request := worker.Request{Mode: "probe", Gateway: gateway}
	if proof.committedCall {
		request.Mode, request.Tool, request.Occurrence = "call", "commit_once", "one-owned-call"
		request.Arguments = json.RawMessage(`{"value":1}`)
	}
	go func() {
		_, err := RunWorker(deadlineCtx, target, dockerBin, request)
		completed <- err
	}()
	select {
	case <-entered:
	case err := <-completed:
		t.Fatalf("nondiscriminating cancellation: target never reached held list: %v", err)
	case <-ctx.Done():
		t.Fatal("held list was not reached")
	}
	var argument string
	if proof.lostClient {
		data, err := os.ReadFile(argumentFile)
		if err != nil {
			t.Fatal(err)
		}
		argument = string(data)
		data, err = os.ReadFile(clientPID)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(string(data))
		if err != nil {
			t.Fatal(err)
		}
		client, err := os.FindProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	err = <-completed
	var execution *WorkerExecutionError
	if proof.lostClient {
		if !errors.As(err, &execution) || !execution.Started || execution.Observed || execution.RemoteCleanupUnproven != proof.cleanupFailure {
			t.Fatalf("lost client failure fabricated an observed worker result: %v", err)
		}
		var clientExit *exec.ExitError
		if !errors.As(err, &clientExit) || clientExit.ExitCode() != -1 {
			t.Fatalf("original killed-client failure was discarded: %v", err)
		}
		if failures.FromError(err, "test", "join").Failure.Class != failures.ClassOutcomeUncertain {
			t.Fatalf("lost launched response permitted a known/retryable outcome: %v", err)
		}
		if proof.cleanupFailure {
			joined, ok := errors.Unwrap(execution.Err).(interface{ Unwrap() []error })
			if !ok {
				t.Fatal("client and cleanup failures were not retained independently")
			}
			var cleanupExit *exec.ExitError
			if !errors.As(joined.Unwrap()[len(joined.Unwrap())-1], &cleanupExit) || cleanupExit.ExitCode() != 73 {
				t.Fatalf("independent cleanup failure was discarded: %v", err)
			}
		}
	} else {
		deadline := errors.Is(err, context.DeadlineExceeded)
		if !deadline {
			deadline = failures.FromError(err, "test", "join").Failure.Detail.Code == "workspace_gateway_unreachable"
		}
		if !errors.As(err, &execution) || !execution.Started || !execution.Observed || execution.ModelStarted || execution.RemoteCleanupUnproven || !deadline {
			t.Fatalf("target deadline lost exact pre-model outcome: %v", err)
		}
		if proof.committedCall && failures.FromError(err, "test", "join").Failure.Class != failures.ClassOutcomeUncertain {
			t.Fatalf("canceled committed call was downgraded to a known failure: %v", err)
		}
	}
	if proof.cleanupFailure {
		// This injected failure must not claim a join. The proof separately owns
		// eventual disposal, without weakening the successful-join controls.
		if err := observeDockerWorkerGone(manager.DockerBin(), strings.TrimSpace(containerID), argument, true); err != nil {
			t.Fatalf("proof could not join its uncertain remote worker: %v", err)
		}
		select {
		case <-retired:
		case <-ctx.Done():
			t.Fatal("proof-owned disposal left the uncertain HTTP request alive")
		}
	}
	select {
	case <-retired:
	default:
		top, inspectErr := manager.RunDocker(ctx, "top", name, "-eo", "pid,args")
		t.Fatalf("owned child left its HTTP request live; execution=%+v top=%q inspect=%v", execution, top, inspectErr)
	}
	top, err := manager.RunDocker(ctx, "top", name, "-eo", "pid,args")
	present := strings.Contains(top, worker.Argument)
	if argument != "" {
		var observationErr error
		present, observationErr = dockerWorkerPresent([]byte(top), argument)
		err = errors.Join(err, observationErr)
	}
	if err != nil || present {
		t.Fatalf("returned before exact HTTP worker joined: %q %v", top, err)
	}
	if proof.committedCall && committed.Load() != 1 {
		t.Fatalf("possibly committed call was replayed: %d", committed.Load())
	}
	if proof.sibling {
		select {
		case err := <-siblingCompleted:
			t.Fatalf("lost-client cleanup retired sibling work: %v", err)
		default:
		}
		releaseSibling()
		if err := <-siblingCompleted; err != nil {
			t.Fatalf("sibling could not complete normally: %v", err)
		}
	}
	survivorID, err := manager.RunDocker(ctx, "inspect", "--format", "{{.Id}}", name)
	if err != nil || survivorID != containerID {
		t.Fatalf("source container was replaced: %q -> %q %v", containerID, survivorID, err)
	}
	if _, err := RunWorker(ctx, target, manager.DockerBin(), worker.Request{Mode: "identity"}); err != nil {
		t.Fatalf("ordinary source cannot continue after exact worker cleanup: %v", err)
	}
}
