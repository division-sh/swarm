package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

var errSourceRelease = errors.New("source release rejected")

func TestSelectedSourceCleanupRetainsOwnership(t *testing.T) {
	for _, phase := range []string{"retained", "selected_validation", "original_validation", "original_compilation", "original_compiled"} {
		t.Run(phase, func(t *testing.T) {
			owner, process, ctx := contextLifetimeOwner(t)
			repaired, calls := false, 0
			defer func() {
				repaired = true
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			release := func() error {
				calls++
				if !repaired {
					return errSourceRelease
				}
				return nil
			}
			var err error
			if phase == "retained" {
				p := contextLifetimePreparation(t, owner, ctx)
				bindContextLifetimePreparation(t, owner, p)
				p.loadedSource.Cleanup = release
				if err := owner.retainPrepared(p); err != nil {
					t.Fatal(err)
				}
				err = p.Close()
			} else {
				selection := testContractSelection()
				selected := testLoadedSelectedSource(selection)
				selected.Module = nil // Stop after successful original compilation, before any store mutation.
				original := *originalActivationSourceFixture(t)
				req := SelectedContractExecutionRequest{ContractSelection: selection, SourceArtifactFact: selected.SourceArtifactFact}
				if phase == "selected_validation" {
					selected.Cleanup = release
					req.SourceArtifactFact = testEphemeralSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				} else {
					original.Cleanup = release
					if phase == "original_validation" {
						original.SourceArtifactFact = correlation.SourceArtifactFact{}
					}
					if phase == "original_compilation" {
						original.Source = nil
					}
				}
				req.SourceLoader = &fakeSelectedContractSourceLoader{loaded: selected, original: &original}
				_, err = owner.Prepare(ctx, req)
			}
			if !errors.Is(err, errSourceRelease) || process.ActiveCount() == 0 || calls != 1 {
				t.Fatalf("cleanup lost responsibility: calls=%d active=%d error=%v", calls, process.ActiveCount(), err)
			}
			if err := owner.RetireSelectedContexts(context.Background()); !errors.Is(err, errSourceRelease) || calls != 2 {
				t.Fatalf("persistent release was not retried: calls=%d error=%v", calls, err)
			}
			process.Retire()
			expired, cancel := context.WithCancel(context.Background())
			cancel()
			if receipt, err := process.Join(expired); receipt != nil || err == nil {
				t.Fatal("unsettled source permitted store-release receipt")
			}
			repaired = true
			if err := owner.RetireSelectedContexts(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 3 || process.ActiveCount() != 0 {
				t.Fatalf("eventual cleanup: calls=%d active=%d", calls, process.ActiveCount())
			}
			if err := owner.RetireSelectedContexts(context.Background()); err != nil || calls != 3 {
				t.Fatalf("successful resource was released again: %d %v", calls, err)
			}
			if _, err := process.Join(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type sourceCleanupProbe struct {
	SelectedContractSourceLoader
	faultAt, loads int
	phase          string
	repaired       bool
	calls          [2]int
	roots          [2]string
	cancel         context.CancelFunc
}

func (p *sourceCleanupProbe) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	loaded, err := p.SelectedContractSourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, req)
	if err != nil {
		return loaded, err
	}
	i := p.loads
	p.loads++
	p.roots[i] = filepath.Dir(loaded.RuntimeProjection.PrivateRoot())
	release := loaded.Cleanup
	loaded.Cleanup = func() error {
		p.calls[i]++
		if i == p.faultAt && !p.repaired && (p.phase != "fail_once" || p.calls[i] == 1) {
			if p.phase == "panic" {
				panic(errSourceRelease)
			}
			return errSourceRelease
		}
		return release()
	}
	if i == p.faultAt {
		switch p.phase {
		case "rejected":
			loaded.SourceArtifactFact = correlation.SourceArtifactFact{}
		case "compilation":
			loaded.Source = nil
		case "load_error":
			return loaded, errors.New("loader failed after acquiring source")
		case "cancelled":
			p.cancel()
		}
	}
	return loaded, nil
}

type sourceCleanupWorkspace struct {
	workspace.Lifecycle
	calls int
}

func (w *sourceCleanupWorkspace) ReleaseSourceProjection(context.Context) error {
	w.calls++
	if w.calls == 1 {
		return errWorkspaceCleanup
	}
	return nil
}

func TestSelectedSourceCleanupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range []struct {
			phase string
			at    int
		}{
			{"rejected", 0}, {"rejected", 1}, {"load_error", 0}, {"load_error", 1},
			{"compilation", 1}, {"cancelled", 0}, {"cancelled", 1},
			{"prepared", 0}, {"prepared", 1}, {"panic", 0}, {"panic", 1},
			{"fail_once", 0}, {"fail_once", 1},
			{"workspace_first", 0}, {"workspace_first", 1}, {"retained_stop", 0}, {"retained_stop", 1},
		} {
			t.Run(backend+"/"+tc.phase+"/"+[]string{"selected", "original"}[tc.at], func(t *testing.T) {
				var selected startupownership.Store
				var db *sql.DB
				var owner SelectedContractExecutionOwner
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
				}
				ctx, cancel := context.WithCancel(runForkTestContext(t))
				defer cancel()
				process, _ := worklifetime.ProcessFromContext(ctx)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				repo := runForkExecutionRepoRoot(t)
				fixture := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
				loaded, err := fixture.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				runID, eventID := uuid.NewString(), uuid.NewString()
				seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, runID, eventID, uuid.NewString())
				base := SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				sibling, err := base.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{SourceRunID: runID, Selection: runforkadmission.SelectedContractSelection(loaded.Source)})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := sibling.Cleanup(); err != nil {
						t.Error(err)
					}
				}()
				probe := &sourceCleanupProbe{SelectedContractSourceLoader: base, phase: tc.phase, faultAt: tc.at, cancel: cancel}
				defer func() {
					probe.repaired = true
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				request := SelectedContractExecutionRequest{SourceRunID: runID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: probe,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}}
				before := selectedPreparationDatabaseSnapshot(t, db, backend)
				baseline := process.ActiveCount()
				var p *PreparedSelectedFork
				var workspaceProbe *sourceCleanupWorkspace
				if tc.phase == "retained_stop" {
					result, executeErr := ExecuteSelectedContractRunFork(ctx, request)
					if executeErr != nil || !result.Activation.Activated {
						t.Fatalf("execution: %v", executeErr)
					}
					beforeStop := selectedPreparationDatabaseSnapshot(t, db, backend)
					_, selected, stopErr := owner.StopSelectedFork(ctx, runcontrol.TransitionRequest{RunID: result.Materialization.ForkRunID})
					if !selected {
						t.Fatal("stop lost exact selected context")
					}
					if afterStop := selectedPreparationDatabaseSnapshot(t, db, backend); !reflect.DeepEqual(beforeStop, afterStop) {
						t.Fatal("failed cleanup advanced terminal mutation or executed work")
					}
					err = stopErr
				} else {
					p, err = owner.Prepare(ctx, request)
					if p != nil {
						if err != nil {
							t.Fatal(err)
						}
						if tc.phase == "workspace_first" {
							workspaceProbe = &sourceCleanupWorkspace{Lifecycle: workspace.NewHostManager()}
							p.agentRuntime.workspaceProjection = &selectedContractWorkspaceProjection{lifecycle: workspaceProbe}
							if err := p.Close(); !errors.Is(err, errWorkspaceCleanup) || probe.calls != [2]int{} {
								t.Fatalf("workspace failure advanced source release: %v %v", err, probe.calls)
							}
						}
						func() {
							defer func() {
								if got := recover(); got != nil {
									if tc.phase != "panic" || got != errSourceRelease {
										panic(got)
									}
									err = p.closeErr
								}
							}()
							err = p.Close()
						}()
					}
					if after := selectedPreparationDatabaseSnapshot(t, db, backend); !reflect.DeepEqual(before, after) {
						t.Fatal("preparation/cleanup mutated domain state")
					}
				}
				if err == nil || process.ActiveCount() <= baseline || probe.calls[tc.at] != 1 {
					t.Fatalf("lost source owner: calls=%v active=%d err=%v", probe.calls, process.ActiveCount(), err)
				}
				if _, err := os.Stat(probe.roots[tc.at]); err != nil {
					t.Fatalf("failed resource vanished: %v", err)
				}
				if _, err := os.Stat(sibling.RuntimeProjection.PrivateRoot()); err != nil {
					t.Fatalf("sibling changed: %v", err)
				}
				if tc.phase != "panic" && tc.phase != "fail_once" {
					if err := owner.RetireSelectedContexts(context.Background()); !errors.Is(err, errSourceRelease) || probe.calls[tc.at] != 2 {
						t.Fatalf("persistent retry: %v %v", probe.calls, err)
					}
				}
				if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
					t.Fatal(err)
				}
				process.Retire()
				expired, stop := context.WithCancel(context.Background())
				stop()
				if receipt, err := process.Join(expired); receipt != nil || err == nil {
					t.Fatal("failed cleanup permitted store-release receipt")
				}
				probe.repaired = tc.phase != "fail_once"
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := process.Join(context.Background()); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < probe.loads; i++ {
					want := 1
					if i == tc.at {
						want = 3
						if tc.phase == "panic" || tc.phase == "fail_once" {
							want = 2
						}
					}
					if probe.calls[i] != want {
						t.Fatalf("resource%d releases=%d want%d", i, probe.calls[i], want)
					}
					if _, err := os.Stat(probe.roots[i]); !os.IsNotExist(err) {
						t.Fatalf("resource%d remains: %v", i, err)
					}
				}
				if workspaceProbe != nil && workspaceProbe.calls != 2 {
					t.Fatalf("successful workspace repeated: %d", workspaceProbe.calls)
				}
				if _, err := os.Stat(sibling.RuntimeProjection.PrivateRoot()); err != nil {
					t.Fatalf("cleanup retired sibling: %v", err)
				}
			})
		}
	}
}

func TestSelectedSourceStagedCleanupOwnership(t *testing.T) {
	for _, phase := range []string{"non_selected_compilation", "non_selected_activated", "selected_validation", "selected_original_compilation"} {
		t.Run(phase, func(t *testing.T) {
			owner, process, ctx := contextLifetimeOwner(t)
			store := &fakeSelectedContractActivationStore{originalRunID: uuid.NewString(), activation: runfork.RunForkActivation{Activated: true}}
			binding := testSelectedContractBinding(uuid.NewString())
			selected := testLoadedSelectedSource(binding.ContractSelection)
			original := *originalActivationSourceFixture(t)
			calls, repaired := 0, false
			defer func() {
				repaired = true
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			release := func() error {
				calls++
				if !repaired {
					return errSourceRelease
				}
				return nil
			}
			if phase == "selected_validation" {
				selected.Cleanup = release
				selected.SourceArtifactFact = correlation.SourceArtifactFact{}
			} else {
				original.Cleanup = release
				if phase != "non_selected_activated" {
					original.Source = nil
				}
			}
			if phase == "selected_validation" || phase == "selected_original_compilation" {
				store.binding, store.bindingOK = binding, true
				store.bundleAvailability = testSelectedContractBundleAvailability(binding.ForkRunID)
			}
			result, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{ForkRunID: binding.ForkRunID, Store: store, ExecutionOwner: owner, SourceLoader: &fakeSelectedContractSourceLoader{loaded: selected, original: &original}})
			if !errors.Is(err, errSourceRelease) || calls != 1 || process.ActiveCount() != 1 {
				t.Fatalf("staged cleanup lost ownership: calls=%d active=%d err=%v", calls, process.ActiveCount(), err)
			}
			if (phase == "non_selected_activated") != result.Activated || store.activateCalled != result.Activated {
				t.Fatal("activation evidence changed during cleanup")
			}
			if err := owner.RetireSelectedContexts(context.Background()); !errors.Is(err, errSourceRelease) || calls != 2 {
				t.Fatalf("staged retry: calls=%d err=%v", calls, err)
			}
			repaired = true
			if err := owner.RetireSelectedContexts(context.Background()); err != nil || calls != 3 || process.ActiveCount() != 0 {
				t.Fatalf("staged settlement: calls=%d active=%d err=%v", calls, process.ActiveCount(), err)
			}
		})
	}
}

func TestSelectedSourceFilesystemCleanupRetainsOwnership(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires filesystem permission enforcement")
	}
	owner, process, ctx := contextLifetimeOwner(t)
	parent := t.TempDir()
	t.Setenv("TMPDIR", parent)
	artifact, err := sourceartifact.AdmitDirectory(filepath.Join(runForkExecutionRepoRoot(t), "tests/tier1-primitives/test-emits-multiple"))
	if err != nil {
		t.Fatal(err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(projection.PrivateRoot())
	p := contextLifetimePreparation(t, owner, ctx)
	p.loadedSource = LoadedSelectedContractSource{RuntimeProjection: projection, Cleanup: projection.Release}
	defer func() {
		if err := os.Chmod(parent, 0o700); err != nil {
			t.Error(err)
		}
	}()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err == nil || process.ActiveCount() == 0 {
		t.Fatal("real deletion failure released ownership")
	}
	if err := owner.RetireSelectedContexts(context.Background()); err == nil {
		t.Fatal("persistent deletion failure reported completion")
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := owner.RetireSelectedContexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("projection survived owner retry: %v", err)
	}
}
