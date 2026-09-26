package runforkpersistence

import (
	"context"
	"testing"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedLifecycleDiagnosticUsesExactForkPointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_bindings (
					binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
					fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL, fork_event_id TEXT, bundle_hash TEXT)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (
					execution_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, fork_run_id TEXT NOT NULL,
					source_run_id TEXT NOT NULL, fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL,
					fork_event_id TEXT, generation BIGINT NOT NULL, admission_fingerprint TEXT NOT NULL,
					container_plan_fingerprint TEXT NOT NULL, actor_census_fingerprint TEXT NOT NULL,
					effective_config_fingerprint TEXT NOT NULL, state TEXT NOT NULL, lease_expires_at TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			for _, point := range []runfork.RunForkPoint{
				{Kind: runfork.RunForkPointEvent, Revision: 2, EventID: uuid.NewString()},
				{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3},
			} {
				sourceID, forkID, bindingID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
				const bundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				if _, err := db.ExecContext(ctx, `INSERT INTO runs VALUES ($1,$2)`, forkID, bundleHash); err != nil {
					t.Fatal(err)
				}
				var eventID any
				if point.EventID != "" {
					eventID = point.EventID
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,$3,$4,$5,$6,NULL)`,
					bindingID, forkID, sourceID, point.Kind, point.Revision, eventID); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_runtime_executions
					(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,
					 generation,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
					 effective_config_fingerprint,state)
					VALUES ($1,$2,$3,$4,$5,$6,$7,1,'admission','container','actors','config','running')`,
					executionID, bindingID, forkID, sourceID, point.Kind, point.Revision, eventID); err != nil {
					t.Fatal(err)
				}
				origin := runtimemanager.LifecycleDiagnosticOrigin{
					Owner:       runtimemanager.LifecycleDiagnosticSelectedFork,
					Causality:   runtimemanager.LifecycleDiagnosticObservation,
					SourceRunID: sourceID, ForkPoint: point, ForkEventID: point.EventID,
					SelectedFork: runtimeeffects.SelectedContractForkAuthority{
						ExecutionID: executionID, ForkRunID: forkID, Generation: 1,
						AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
						ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
					},
				}
				result := runtimemanager.AgentLifecycleTransitionResult{
					Identity:             runtimeagentidentity.Identity{RunID: forkID},
					ProcessBinding:       runtimemanager.ProcessExecutionBinding{BundleHash: bundleHash, RuntimeGeneration: 1},
					DiagnosticProvenance: runtimemanager.LifecycleDiagnosticProvenance{Origin: origin, BindingID: bindingID},
				}
				check := func(wantOK bool) {
					t.Helper()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					got, err := validateLifecycleDiagnosticOriginTx(ctx, tx, result, true)
					if wantOK && (err != nil || got != bindingID) {
						t.Fatalf("exact point rejected: binding=%q err=%v", got, err)
					}
					if !wantOK && err == nil {
						t.Fatal("contradictory point accepted")
					}
				}
				check(true)
				result.DiagnosticProvenance.Origin.ForkPoint.Revision++
				check(false)
				result.DiagnosticProvenance.Origin.ForkPoint.Revision--
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions
					SET fork_revision=fork_revision+1 WHERE execution_id=$1`, executionID); err != nil {
					t.Fatal(err)
				}
				check(false)
			}
		})
	}
}
