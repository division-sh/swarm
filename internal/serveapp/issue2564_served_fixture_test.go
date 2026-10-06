package serveapp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564ServedObservationOwner interface {
	operatorread.EntityReader
	operatorread.RunReader
	operatorread.ObservabilityReader
	pipeline.WorkflowPersistenceOwner
	SummarizeRun(context.Context, string) (deliverylifecycle.RunSummary, error)
}

type issue2564ServedFixture struct {
	Endpoint, Backend, BundleHash string
	selected                      issue2564ServedObservationOwner
	persistence                   serveRuntimePersistence
	Runtime                       *runtimepkg.Runtime
}

// This boots the real public serve owner directly from its native selected
// location. The mock variant retains the existing mailbox lifecycle purpose.
func issue2564ServeHarness(t *testing.T, backend, root string, mock bool) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	if backend == "sqlite" {
		if mock {
			opts.ConfigPath = writeMockAgentRuntimeConfig(t, backend, filepath.Join(t.TempDir(), "completion.sqlite"))
		} else {
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, backend, filepath.Join(t.TempDir(), "lifecycle.sqlite"))
		}
	} else {
		dsn := storetest.PostgresFixtureLocation(t)
		original := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			owner, err := storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
			if err != nil {
				return nil, err
			}
			if _, err := initializeServePlatformStateStores(ctx, owner.Schema(), filepath.Join(repoRootForTest(), defaultPlatformSpecPath)); err != nil {
				return nil, errors.Join(err, owner.CloseUnactivated())
			}
			return owner, nil
		}
		t.Cleanup(func() { buildStoresForServe = original })
		if mock {
			opts.ConfigPath = writeMockAgentRuntimeConfig(t, backend, "")
		} else {
			opts.ConfigPath = writeServeRuntimeTestConfig(t)
		}
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	var persistence serveRuntimePersistence
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { persistence = p })
	retained := t.TempDir()
	return opts, func() (*serveRuntimeTestProcess, issue2564ServedFixture) {
		var process *serveRuntimeTestProcess
		if mock {
			t.Log("proof_surface=H in-process retained mock lifecycle with real authenticated HTTP/WS; not public serve/test")
			process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), *opts, func(ctx context.Context, repo string, opts cliapp.ServeOptions) int {
				code, err := runOwnedLifecycle(ctx, repo, retained, opts, apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "internal-lifecycle-parent"}, executionposture.MockOnly, "")
				if err != nil {
					fmt.Fprintf(opts.ErrorOutput, "internal mailbox lifecycle setup: %v\n", err)
				}
				return code
			})
		} else {
			process = startServeRuntimeTestProcess(t, *opts)
		}
		process.waitForReadyLine()
		selected, ok := persistence.deps.EventStore.(issue2564ServedObservationOwner)
		if !ok {
			t.Fatalf("selected serve construction lacks exact observation owners: %T", persistence.deps.EventStore)
		}
		fixture := issue2564ServedFixture{Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc", Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, opts.SourceRoot), selected: selected, persistence: persistence}
		if mock {
			fixture.Runtime = servedTestProcessRuntime(t, process)
		}
		return process, fixture
	}
}

func (f issue2564ServedFixture) waitEntityStage(t *testing.T, runID, entityID, stage string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		state := storetest.ObserveWriterStage(t, context.Background(), f.selected, runID, entityID, stage)
		if state.State == stage && state.EntityID != "" {
			return state.EntityID
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("entity %s/%s did not reach %s", runID, entityID, stage)
	return ""
}

func (f issue2564ServedFixture) waitDeliveries(t *testing.T, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	stable := 0
	for time.Now().Before(deadline) {
		summary, err := f.selected.SummarizeRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Pending+summary.InProgress == 0 {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run delivery quiescence timed out: %s", runID)
}

func (f issue2564ServedFixture) events(t *testing.T, runID, name string) []operatorread.OperatorEventFull {
	t.Helper()
	options := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: name}, Limit: 100}
	var out []operatorread.OperatorEventFull
	for {
		page, err := f.selected.ListOperatorEvents(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Events...)
		if page.NextCursor == "" {
			return out
		}
		options.Cursor = page.NextCursor
	}
}

func (f issue2564ServedFixture) debug(t *testing.T, runID string) string {
	t.Helper()
	report, err := f.selected.LoadRunDebugReport(context.Background(), runID, operatorread.RunDebugQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", report)
}
