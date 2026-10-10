package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/store/storetest"
)

const issue2353H2ControlledEnv = "SWARM_TEST_ISSUE2353_H2_CONTROLLED"
const issue2353H2OracleEnv = "SWARM_TEST_ISSUE2353_H2_ORACLE"

type issue2353H2ProcessRequest struct {
	Source, Config, Backend, Location string
	Controlled                        bool
}

type issue2353H2Command struct {
	Action, RunID, Entity, Instance, Declaration string
}

type issue2353H2Message struct {
	Kind, Error string
	Signal      lifecycleprobe.Signal
}

type issue2353H2Gate struct {
	t           *testing.T
	process     context.Context
	request     issue2353H2ProcessRequest
	mu          sync.Mutex
	target      issue2353H2Command
	reader      storetest.Issue2564WorkloadReader
	held, crash bool
	crashArmed  bool
	release     chan struct{}
	unblock     func()
	outputMu    sync.Mutex
	output      *json.Encoder
}

func (g *issue2353H2Gate) report(message issue2353H2Message) {
	g.outputMu.Lock()
	defer g.outputMu.Unlock()
	if err := g.output.Encode(message); err != nil {
		g.t.Errorf("controlled H2 report: %v", err)
	}
}

func (g *issue2353H2Gate) controls(input io.Reader) {
	defer g.unblock()
	decoder := json.NewDecoder(input)
	for {
		var command issue2353H2Command
		if err := decoder.Decode(&command); err != nil {
			if err != io.EOF && g.process.Err() == nil {
				g.report(issue2353H2Message{Kind: "error", Error: err.Error()})
			}
			return
		}
		switch command.Action {
		case "arm":
			observation := storetest.OpenIssue2564WorkloadObservation(g.t, g.request.Backend, g.request.Location)
			g.mu.Lock()
			g.target, g.reader = command, observation.Reader
			g.mu.Unlock()
			g.report(issue2353H2Message{Kind: "armed"})
		case "release":
			g.unblock()
			g.report(issue2353H2Message{Kind: "released"})
		case "crash":
			g.mu.Lock()
			g.crashArmed = true
			g.mu.Unlock()
			g.report(issue2353H2Message{Kind: "crash_armed"})
		default:
			g.report(issue2353H2Message{Kind: "error", Error: "unknown H2 gate command"})
			return
		}
	}
}

func (g *issue2353H2Gate) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.EventPersisted && signal.EventType == "hub.bump" {
		fmt.Fprintf(os.Stdout, "H2_DURABLE_ADMISSION event=%s observed_at=%s\n", signal.EventID, time.Now().UTC().Format(time.RFC3339Nano))
	}
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != "platform.stage_timer" {
		return
	}
	g.mu.Lock()
	if g.crashArmed && !g.crash {
		g.crash = true
		g.mu.Unlock()
		g.report(issue2353H2Message{Kind: "crash_cut", Signal: signal})
		<-g.process.Done()
		return
	}
	target, reader, alreadyHeld := g.target, g.reader, g.held
	g.mu.Unlock()
	if reader == nil || alreadyHeld {
		return
	}
	evidence, err := storetest.ObserveH2WorkloadSnapshot(ctx, reader, target.RunID)
	if err != nil {
		g.report(issue2353H2Message{Kind: "error", Error: err.Error()})
		return
	}
	for _, event := range evidence.Events {
		if event.ID != signal.EventID || event.Instance != target.Instance {
			continue
		}
		occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.Task)
		if !valid || occurrence.Activation.DeclarationKey != target.Declaration {
			return
		}
		for _, timer := range evidence.Timers {
			if timer.ID != occurrence.Activation.ActivationID || timer.Entity != target.Entity || timer.Instance != target.Instance {
				continue
			}
			g.mu.Lock()
			if g.held {
				g.mu.Unlock()
				return
			}
			g.held = true
			g.mu.Unlock()
			g.report(issue2353H2Message{Kind: "held", Signal: signal})
			// A test-owned process pause, not a replacement execution context.
			// The original ten-second callback deadline still expires normally;
			// dispatch receives that original context after release and must recover.
			select {
			case <-g.release:
			case <-g.process.Done():
			}
			g.report(issue2353H2Message{Kind: "joined", Signal: signal, Error: fmt.Sprint(ctx.Err())})
			return
		}
	}
}

func TestIssue2353H2ControlledServeProcessHelper(t *testing.T) {
	raw := os.Getenv(issue2353H2ControlledEnv)
	if raw == "" {
		t.Skip("parent-owned H2 diagnostic process")
	}
	var request issue2353H2ProcessRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath = request.Source, request.Config
	opts.StoreMode, opts.StoreModeSet = request.Backend, true
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.SelfCheck, opts.Verbose, opts.Dev = true, true, false
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if request.Controlled {
		input, output := os.NewFile(3, "h2-control"), os.NewFile(4, "h2-evidence")
		defer output.Close()
		gate := &issue2353H2Gate{t: t, process: ctx, request: request, release: make(chan struct{}), output: json.NewEncoder(output)}
		gate.unblock = sync.OnceFunc(func() { close(gate.release) })
		done := make(chan struct{})
		go func() { defer close(done); gate.controls(input) }()
		defer func() { gate.unblock(); _ = input.Close(); <-done }()
		opts.TestLifecycleProbe = gate
	}
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("controlled non-dev H2 serve exit=%d", code)
	}
}

type issue2353H2Control struct {
	input   *json.Encoder
	output  *json.Decoder
	pipe    *os.File
	backlog map[string]issue2353H2Message
}

func (c *issue2353H2Control) wait(t *testing.T, kind string) issue2353H2Message {
	t.Helper()
	if err := c.pipe.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if message, found := c.backlog[kind]; found {
		delete(c.backlog, kind)
		return message
	}
	for {
		var message issue2353H2Message
		if err := c.output.Decode(&message); err != nil {
			t.Fatalf("controlled H2 %s evidence: %v", kind, err)
		}
		if message.Kind == "error" {
			t.Fatalf("controlled H2 gate: %s", message.Error)
		}
		if message.Kind == kind {
			return message
		}
		c.backlog[message.Kind] = message
	}
}

func issue2353H2Harness(t *testing.T, backend, root string) (func(bool) (*channelOnboardingCrashServeProcess, issue2564H2Fixture), *issue2353H2Control, issue2353H2ProcessRequest) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	request := issue2353H2ProcessRequest{Source: root, Backend: backend}
	if backend == "postgres" {
		request.Location = storetest.PostgresFixtureLocation(t)
		request.Config = writeChannelOnboardingPostgresRuntimeConfig(t, request.Location)
	} else {
		request.Location = filepath.Join(t.TempDir(), "h2.sqlite")
		request.Config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, request.Location, channelOnboardingHostWorkspaceFields())
	}
	setServeRuntimeRecovery(t, request.Config, false, true)
	inputR, inputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outputR, outputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{inputR, inputW, outputR, outputW} {
		t.Cleanup(func() { _ = file.Close() })
	}
	control := &issue2353H2Control{input: json.NewEncoder(inputW), output: json.NewDecoder(outputR), pipe: outputR, backlog: map[string]issue2353H2Message{}}
	temporary := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.WalkDir(temporary, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return err
		}); err != nil {
			t.Error(err)
		}
	})
	start := func(controlled bool) (*channelOnboardingCrashServeProcess, issue2564H2Fixture) {
		request.Controlled = controlled
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		process := startServedCrashProcess(t, "TestIssue2353H2ControlledServeProcessHelper", []string{issue2353H2ControlledEnv + "=" + string(raw), "TMPDIR=" + temporary}, inputR, outputW)
		reader := storetest.OpenIssue2564WorkloadObservation(t, backend, request.Location)
		return process, issue2564H2Fixture{Endpoint: process.endpoint(t) + "/v1/rpc", Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root), selected: reader.Reader, cuts: map[issue2564H2CutCoordinate]issue2564H2CutWitness{}}
	}
	t.Cleanup(func() { _ = control.input.Encode(issue2353H2Command{Action: "release"}) })
	return start, control, request
}
