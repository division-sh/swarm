package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func TestProviderResumeAdmissionRetainsOriginalLogicalAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, _ := openProviderJournalFixture(t, backend, agentmemory.Plan{})
			ctx, journal, frame, _, body := providerJournalAdmissionInputs(t, fixture)
			original, err := effects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := original.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			grant, takeover, _ := prepareProviderTakeover(t, fixture, nil)
			if _, err := grant.CommitAgentLifecycleTransition(ctx, takeover); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			if err := MakeDeliveryRecoveryRetryEligibleForTest(ctx, fixture.store, fixture.origin.DeliveryID()); err != nil {
				t.Fatal(err)
			}
			snapshot, err := fixture.store.Snapshot(ctx, fixture.origin.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			event := loadCompletionOriginEvent(t, fixture, snapshot.EventID)
			claimResult, err := fixture.store.ClaimDelivery(ctx, snapshot.Authority, event, snapshot.Route)
			if err != nil {
				t.Fatal(err)
			}
			claimed, ok := claimResult.Acquired()
			if !ok {
				t.Fatalf("retained continuation claim=%s", claimResult.Disposition)
			}
			successor := original.Attempt().Authority
			successor.Normal.RuntimeEpoch = takeover.TargetEpoch
			successor.Normal.Generation = takeover.TargetGeneration
			successor.FenceGeneration = takeover.TargetGeneration
			successor.ExecutionOwner = "provider-resume-successor"
			resumeCtx := effects.WithAuthority(effects.WithLifecycleToken(ctx, successor.Normal), successor)
			resumeCtx = deliverylifecycle.WithClaim(managedExecutionStoreTestContext(t, resumeCtx), claimed.Claim)
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := effects.BeginJournaledManagedCompletion(resumeCtx, "anthropic_api", body, frame, journal, nil)
			if err != nil {
				t.Fatalf("exact retained continuation was not admitted: %v", err)
			}
			if !resumed.Attempt().AuthorizationAcknowledged || resumed.Attempt().OperationID != original.Attempt().OperationID ||
				resumed.Attempt().AttemptID == original.Attempt().AttemptID || resumed.Attempt().Ordinal != 2 {
				t.Fatal("resume did not admit one distinct physical attempt under its original operation")
			}
			if err := resumed.MarkLaunched(resumeCtx); err != nil {
				t.Fatal(err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			physical := providerResumeSnapshotRow(t, after, "runtime_external_effect_attempts", "attempt_id", resumed.Attempt().AttemptID)
			if parent, _ := physical["resume_from_attempt_id"].Text(); parent != original.Attempt().AttemptID {
				t.Fatal("resumed physical attempt lacks the exact immutable predecessor link")
			}
			oldOperation := providerResumeSnapshotRow(t, before, "runtime_external_effect_operations", "operation_id", original.Attempt().OperationID)
			operation := providerResumeSnapshotRow(t, after, "runtime_external_effect_operations", "operation_id", original.Attempt().OperationID)
			for _, column := range []string{"authority_evidence", "runtime_epoch", "generation", "request_fingerprint", "agent_frame_bytes", "lineage"} {
				if _, found := oldOperation[column]; !found || !reflect.DeepEqual(oldOperation[column], operation[column]) {
					t.Fatalf("resume rewrote original operation %s", column)
				}
			}
			oldClocks, err := DecodeSelectedForkSnapshotRowsForTest(before["runtime_agent_turn_lifetimes"])
			if err != nil || len(oldClocks) != 1 {
				t.Fatal("original logical turn clock is missing")
			}
			clocks, err := DecodeSelectedForkSnapshotRowsForTest(after["runtime_agent_turn_lifetimes"])
			if err != nil || len(clocks) != 1 {
				t.Fatal("resume duplicated the logical turn clock")
			}
			for _, column := range []string{"first_attempt_id", "first_launched_at", "bound_ns", "bound_emit", "timeout_event_id"} {
				if _, found := oldClocks[0][column]; !found || !reflect.DeepEqual(oldClocks[0][column], clocks[0][column]) {
					t.Fatalf("resume reset the logical turn %s", column)
				}
			}
			delivery := providerResumeSnapshotRow(t, after, "event_deliveries", "delivery_id", fixture.origin.DeliveryID())
			var retries int
			if err := json.Unmarshal(delivery["retry_count"].Value, &retries); err != nil || retries != deliverylifecycle.AgentMaxRetries {
				t.Fatal("child admission charged a second retry or lost the first charge")
			}
		})
	}
}
