package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedForkPreparationProcessBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s := storetest.StartPostgresRuntimeStore(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			repo := runForkExecutionRepoRoot(t)
			fixtureLoader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
			loaded, err := fixtureLoader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, eventID := uuid.NewString(), uuid.NewString()
			seedSelectedOperationSource(t, ctx, selected, loaded, sourceRun, eventID)
			request := SelectedContractExecutionRequest{
				Owner: owner, SourceRunID: sourceRun, At: eventID,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				SourceLoader:      SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(SourceArtifactSelectedContractSourceStore)},
				AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
			}
			before := selectedPreparationDatabaseSnapshot(t, selected)
			baseline := process.ActiveCount()
			for _, test := range []struct {
				name string
				cap  startupownership.ProcessCapability
			}{
				{"missing", nil},
				{"foreign_lease", crossedSelectedProcessCapability{ProcessCapability: capability, cross: func(a *startupownership.Authority) { a.AuthorityID = uuid.NewString() }}},
				{"foreign_generation", crossedSelectedProcessCapability{ProcessCapability: capability, cross: func(a *startupownership.Authority) { a.AuthorityGeneration++ }}},
			} {
				t.Run(test.name, func(t *testing.T) {
					attempt := request
					attempt.AgentRuntime.ProcessCapability = test.cap
					prepared, err := owner.Prepare(ctx, attempt)
					if prepared != nil || err == nil || !strings.Contains(err.Error(), "process capability") {
						if prepared != nil {
							_ = prepared.Close()
						}
						t.Fatalf("wrong preparation disposition: prepared=%v err=%v", prepared, err)
					}
					if !reflect.DeepEqual(before, selectedPreparationDatabaseSnapshot(t, selected)) || process.ActiveCount() != baseline {
						t.Fatal("rejected preparation changed persisted facts or leaked process ownership")
					}
				})
			}
			prepared, err := owner.Prepare(ctx, request)
			if err != nil {
				t.Fatalf("exact process preparation: %v", err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, selectedPreparationDatabaseSnapshot(t, selected)) || process.ActiveCount() != baseline {
				t.Fatal("non-executable preparation mutated the store or retained process work")
			}
		})
	}
}

func selectedPreparationDatabaseSnapshot(t testing.TB, selected any) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	out, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
