package runtimepersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/google/uuid"
)

// Deliberately incomplete reset references are a corruption probe, not a
// replacement reset writer or evidence of semantic cohort admission.
func proveCompletionMembershipCannotBecomeLiveEvidence(t *testing.T, ctx context.Context, fixture selectedCompletionFixture, execution, attempt string, surface managedcapabilities.Surface) {
	t.Helper()
	physical, err := ReadCompletionAttemptStorageForTest(ctx, fixture.store, "openai_responses", attempt)
	if err != nil || len(physical) != 1 || len(physical[0].Targets) != 1 {
		t.Fatalf("original retained completion observation: %+v err=%v", physical, err)
	}
	persistence, ok := fixture.store.(managedcapabilities.Persistence)
	if !ok {
		t.Fatal("original native store has no managed capability persistence owner")
	}
	if err := persistence.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("attached closed completion surface lost idempotent persistence: %v", err)
	}
	before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
	reset := uuid.NewString()
	clean := func() {
		t.Helper()
		tx, err := fixture.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for _, query := range []string{
			`DELETE FROM run_fork_selected_contract_turn_detachments WHERE execution_id=$1`,
			`DELETE FROM run_fork_selected_contract_execution_detachments WHERE execution_id=$1`,
		} {
			if _, err := tx.ExecContext(ctx, query, execution); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_reset_operations WHERE operation_id=$1`, reset); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	defer clean()
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_reset_operations (operation_id,actor_token_id,revision,phase,active_slot,record)
		VALUES ($1,'invalid-reference-probe',1,'completed',NULL,'{}')`, reset); err != nil {
		t.Fatal(err)
	}
	fingerprint := "sha256:" + strings.Repeat("f", 64)
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_execution_detachments
		(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,execution_state,
		reset_operation_id,reset_revision,planned_runs_fingerprint,execution_fingerprint,manifest_fingerprint,manifest)
		SELECT execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,'closed',
		$2,1,$3,$3,$3,'{"invalid_reference_probe":true}' FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, execution, reset, fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_turn_detachments
		(execution_id,turn_id,fork_run_id,completion_attempt_id,capability_surface_id,row_fingerprint)
		SELECT $1,turn_id,run_id,completion_attempt_id,capability_surface_id,$3 FROM agent_turns WHERE completion_attempt_id=$2`, execution, attempt, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	corrupt := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
	if got, err := ReadCompletionAttemptStorageForTest(ctx, fixture.store, "openai_responses", attempt); err == nil || got != nil || !strings.Contains(err.Error(), "live-parent association") {
		t.Fatalf("simultaneous live and retained completion membership became readable live evidence: %+v err=%v", got, err)
	}
	proveRetainedSurfaceCannotBecomeWritable(t, ctx, persistence, surface)
	historicalContextRequireUnchanged(t, corrupt, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
	clean()
	historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
	if got, err := ReadCompletionAttemptStorageForTest(ctx, fixture.store, "openai_responses", attempt); err != nil || len(got) != 1 || len(got[0].Targets) != 1 {
		t.Fatalf("original completion evidence was not preserved after removing the invalid probe: %+v err=%v", got, err)
	}
	if err := persistence.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("original attached surface was not preserved after removing the invalid probe: %v", err)
	}
}

func proveRetainedSurfaceCannotBecomeWritable(t *testing.T, ctx context.Context, persistence managedcapabilities.Persistence, surface managedcapabilities.Surface) {
	t.Helper()
	advanced, err := surface.ObserveMismatch(managedcapabilities.DeliveryMismatch{
		BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: "completion", Kind: "unavailable",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := advanced.CanAdvanceFrom(surface); err != nil || advanced.IntegrityHash == surface.IntegrityHash {
		t.Fatalf("surface advancement control is invalid: %v", err)
	}
	nextAuthority := surface.Authority
	nextAuthority.ID = uuid.NewString()
	nextAuthority.TurnOrdinal++
	fresh, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: surface.ActorIdentity, ActorPlan: surface.ActorPlan,
		RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport,
		ProviderContract: surface.ProviderContract, Authority: nextAuthority, CreatedAt: surface.CreatedAt,
	})
	if err != nil || fresh.ID == surface.ID {
		t.Fatalf("fresh selected surface control is invalid: %v", err)
	}
	for _, candidate := range []struct {
		name    string
		surface managedcapabilities.Surface
	}{
		{name: "idempotent", surface: surface},
		{name: "advance", surface: advanced},
		{name: "fresh", surface: fresh},
	} {
		if err := persistence.SaveManagedCapabilitySurface(ctx, candidate.surface); err == nil || !strings.Contains(err.Error(), "selected execution live-parent association") {
			t.Errorf("%s retained selected surface accepted live persistence or lost exact refusal: %v", candidate.name, err)
		}
	}
}
