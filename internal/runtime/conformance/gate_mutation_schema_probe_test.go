package conformance

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func constructConformanceGateMutationSubject(t *testing.T, selected fanInBarrierConformanceStore) (context.Context, string, string) {
	t.Helper()
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "schema.yaml", "name: mutation-proof\nstages:\n  queued: {}\n")
	writeConformanceSnapshotFixture(t, root, "entities.yaml", "default: {}\n")
	source := loadConformanceWorkflowFixtureModule(t, root).source
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("gate mutation constraint subject requires its admitted source")
	}
	fact := conformanceSourceArtifactFact(t, source)
	run := uuid.NewString()
	ctx := effects.WithExecutionMode(correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), fact), run), executionmode.Live)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: run, Artifact: bundle.SourceArtifact})
	native := newFanInBarrierRuntimeForSource(t, selected, source, fact, 1)
	plan, err := native.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: flowidentity.Stored(source, ".", run, run, run, ""), OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("prepare gate mutation constraint subject: %v", err)
	}
	committed, err := native.bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct gate mutation constraint subject: %+v err=%v", committed, err)
	}
	if err := native.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatal(err)
	}
	return ctx, run, run
}

func TestGateMutationEmptyPathProbeRequiresActualSchemaRefusalAndPreservesHistoryBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) fanInBarrierConformanceStore
	}{
		{"sqlite", func(t *testing.T) fanInBarrierConformanceStore { return storetest.StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) fanInBarrierConformanceStore { return storetest.StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx, run, entity := constructConformanceGateMutationSubject(t, selected)
			before, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, selected, run, entity)
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			if err := storetest.ProbeWorkflowGateEmptyPath(ctx, selected, run, entity); err == nil || !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
				t.Fatalf("gate-empty-path write did not receive an actual database constraint refusal: %v", err)
			}
			if receipt := probe.Snapshot(); receipt.Total.WriteCommits != 0 || receipt.Total.RollbackAttempts != 1 || receipt.Active != 0 {
				t.Fatalf("refused gate probe bypassed original rollback or retained work: %+v", receipt)
			}
			if err := storetest.ProbeWorkflowGateEmptyPath(ctx, selected, uuid.NewString(), entity); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("foreign run probe masqueraded as a schema refusal: %v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := storetest.ProbeWorkflowGateEmptyPath(cancelled, selected, run, entity); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled probe masqueraded as a schema refusal: %v", err)
			}
			var nilPG *store.PostgresStore
			var nilSQLite *store.SQLiteRuntimeStore
			for _, owner := range []any{nil, nilPG, nilSQLite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if err := storetest.ProbeWorkflowGateEmptyPath(ctx, owner, run, entity); err == nil {
					t.Fatalf("foreign %T provided a successful probe", owner)
				}
			}
			if after, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, selected, run, entity); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("gate schema probe changed exact entity or mutation history: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}
