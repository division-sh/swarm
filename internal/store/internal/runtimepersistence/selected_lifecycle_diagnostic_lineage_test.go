package runtimepersistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// This is the selected grant/lifecycle/projection component boundary. It does
// not substitute for materialization, agent execution or fork activation proof.
func TestSelectedGrantLifecycleDiagnosticCausalLineageBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, kind := range []string{"explicit", "subject", "missing"} {
			t.Run(fmt.Sprintf("sqlite=%t/%s", sqlite, kind), func(t *testing.T) {
				selected, db := newLifecycleDiagnosticTestStore(t, sqlite)
				ctx := testAuthorActivityContext()
				identity, grant := selectedReceiverClaimGrant(t, ctx, selected.(agentFixtureFlowStore))
				t.Cleanup(func() {
					if err := grant.Retire(context.Background()); err != nil {
						t.Error(err)
					}
				})
				evidence, err := grant.Evidence()
				if err != nil || evidence.SelectedFork == nil || evidence.SelectedFork.ForkRunID != identity.RunID {
					t.Fatalf("requires exact selected grant: %+v %v", evidence, err)
				}
				ctx = runtimecorrelation.WithRunID(ctx, identity.RunID)
				logger := runtimepkg.NewRuntimeLogger(selected, executionposture.Live, storeTestPayloadAdmitter)
				if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{Level: diaglog.LevelInfo, Component: "lineage", Action: "selected-parent"}); err != nil {
					t.Fatal(err)
				}
				var parent string
				if err := db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='platform.runtime_log'`, identity.RunID).Scan(&parent); err != nil {
					t.Fatal(err)
				}
				lineage := runtimecorrelation.RuntimeLineage{
					Owner: runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
					RunID: identity.RunID, SelectedForkContext: true,
					Classification: runtimecorrelation.RuntimeLineageClassificationForkLocal,
				}
				if kind == "subject" {
					lineage.SubjectEventID = parent
				} else {
					lineage.ParentEventID = parent
					if kind == "missing" {
						lineage.ParentEventID = uuid.NewString()
					}
				}
				state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
				if err != nil || !found || state.ProcessBinding.GenerationGrantID != evidence.GrantID {
					t.Fatalf("selected lifecycle authority: %+v %t %v", state, found, err)
				}
				op := uuid.NewString()
				if _, err := grant.CommitAgentLifecycleTransition(runtimecorrelation.WithRuntimeLineage(ctx, lineage), manager.AgentLifecycleTransition{
					OperationID: op, OperationKind: "stop", RequestHash: op,
					Identity: identity, AgentID: identity.AgentID(), Trigger: "selected-causal-termination",
					ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
					TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation + 1, TargetPhase: manager.AgentLifecycleTerminated,
					ConfigRevision: state.ConfigRevision, RunMode: manager.AgentRunModeStopped, Topology: state.Topology, Now: time.Now().UTC(),
				}); err != nil {
					t.Fatalf("selected lifecycle producer: %v", err)
				}
				pending, err := selected.ListPendingAgentLifecycleDiagnostics(ctx, 100)
				if err != nil {
					t.Fatal(err)
				}
				var item diaglog.LifecycleDiagnostic
				for _, candidate := range pending {
					if candidate.OperationID == op {
						item = candidate
					}
				}
				if item.OutboxID == "" {
					t.Fatal("selected producer did not commit its diagnostic")
				}
				consumer := runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString()})
				for attempt := 0; attempt < 2; attempt++ {
					err := logger.ProjectLifecycleDiagnostic(consumer, item)
					if kind == "missing" {
						if err == nil || diagnosticLogCount(t, db, item.OutboxID) != 0 {
							t.Fatalf("missing selected cause was accepted: %v", err)
						}
						after, listErr := selected.ListPendingAgentLifecycleDiagnostics(ctx, 100)
						if listErr != nil || len(after) != len(pending) {
							t.Fatalf("rejected selected diagnostic was acknowledged: %d %v", len(after), listErr)
						}
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					run, cause, disposition := diagnosticProjectionLineage(t, db, sqlite, item)
					if run != identity.RunID || cause != parent || disposition != "causal_"+kind || diagnosticLogCount(t, db, item.OutboxID) != 1 {
						t.Fatalf("selected producer lineage changed: %s/%s/%s", run, cause, disposition)
					}
				}
			})
		}
	}
}
