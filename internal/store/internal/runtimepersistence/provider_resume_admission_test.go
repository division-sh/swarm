package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestProviderResumeAdmissionRetainsOriginalLogicalAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"uncapped", "retained_spend_cap", "commit_refused", "commit_ack_lost"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				fixture, control := openProviderJournalFixture(t, backend, agentmemory.Plan{})
				capped := phase == "retained_spend_cap"
				if !capped {
					fixture.authority.BudgetScopes = nil
					fixture.context = effects.WithAuthority(fixture.context, fixture.authority)
				}
				ctx, journal, frame, surface, body := providerJournalAdmissionInputs(t, fixture)
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
				origin, err := effects.DeliveryCompletionOrigin(claimed.Claim)
				if err != nil {
					t.Fatal(err)
				}
				attemptID, err := effects.AttemptID(original.Attempt().OperationID, 2)
				if err != nil {
					t.Fatal(err)
				}
				request := effects.AuthorizeRequest{
					OperationID: original.Attempt().OperationID, AttemptID: attemptID,
					Kind: effects.KindProviderTurn, Class: effects.EffectWriteOrUnknown, Adapter: "anthropic_api", Transport: "http",
					CompletionRequest: successor.CompletionRequest, RequestFingerprint: effects.Fingerprint(body),
					AgentFrame: &frame, CapabilitySurface: &surface, ProviderRequestJournal: &journal,
					Origin: origin, Now: time.Now().UTC(),
				}
				if phase == "uncapped" {
					proveProviderResumeAdmissionRefusals(t, resumeCtx, fixture, successor, request, original.Attempt(), before)
				}
				if phase == "commit_refused" || phase == "commit_ack_lost" {
					control.phase, control.failure = phase, errors.New("provider resume admission commit observation")
					control.statement = func(query string) bool {
						if !strings.HasPrefix(strings.Join(strings.Fields(strings.ToUpper(query)), " "), "INSERT INTO RUNTIME_EXTERNAL_EFFECT_ATTEMPTS (") {
							return false
						}
						control.enabled.Store(true)
						return true
					}
				}
				resumed, err := fixture.store.AuthorizeExternalAttempt(resumeCtx, successor, request)
				control.enabled.Store(false)
				if phase == "commit_refused" || phase == "commit_ack_lost" {
					if !errors.Is(err, control.failure) || resumed.AttemptID != "" || control.writes.Load() != 1 {
						t.Fatalf("native admission cut was not reached without launch authority: %v", err)
					}
					after, readErr := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
					if readErr != nil {
						t.Fatal(readErr)
					}
					if phase == "commit_refused" {
						if !reflect.DeepEqual(before, after) {
							t.Fatal("refused admission retained partial child, clock or budget rows")
						}
						return
					}
					child := providerResumeSnapshotRow(t, after, "runtime_external_effect_attempts", "attempt_id", attemptID)
					parent, _ := child["resume_from_attempt_id"].Text()
					state, _ := child["state"].Text()
					if parent != original.Attempt().AttemptID || state != "authorized" || string(child["launched_at"].Value) != "null" {
						t.Fatal("unacknowledged admission lost the exact parent or launched a request")
					}
					if _, err := fixture.store.AuthorizeExternalAttempt(resumeCtx, successor, request); err == nil {
						t.Fatal("ordinary replay converted an unacknowledged child into launch authority")
					}
					replayed, err := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
					if err != nil || !reflect.DeepEqual(after, replayed) {
						t.Fatalf("unacknowledged child replay changed durable rows: %v", err)
					}
					return
				}
				if capped {
					failure, typed := failures.EnvelopeFromError(err)
					if !typed || failure.Detail.Code != "completion_spend_budget_exhausted" || resumed.AttemptID != "" {
						t.Fatalf("retained unknown-spend cap was bypassed: %v", err)
					}
					after, readErr := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
					if readErr != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("spend refusal retained partial resume rows: %v", readErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("exact retained continuation was not admitted: %v", err)
				}
				if !resumed.AuthorizationAcknowledged || resumed.OperationID != original.Attempt().OperationID ||
					resumed.AttemptID == original.Attempt().AttemptID || resumed.Ordinal != 2 {
					t.Fatal("resume did not admit one distinct physical attempt under its original operation")
				}
				if _, err := fixture.store.MarkExternalAttemptLaunched(resumeCtx, resumed, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				physical := providerResumeSnapshotRow(t, after, "runtime_external_effect_attempts", "attempt_id", resumed.AttemptID)
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
				if _, err := fixture.store.AuthorizeExternalAttempt(resumeCtx, successor, request); err == nil {
					t.Fatal("repeated child admission created another physical request")
				}
				if err := original.MarkResponseObserved(resumeCtx, map[string]any{"provider_resume_late_response": true}); err == nil {
					t.Fatal("fenced predecessor accepted a late response after successor admission")
				}
				repeated, err := ReadSelectedForkApplicationStorageSnapshotForTest(resumeCtx, fixture.store)
				if err != nil || !reflect.DeepEqual(after, repeated) {
					t.Fatalf("repeated admission or predecessor response changed durable evidence: %v", err)
				}
			})
		}
	}
}

func proveProviderResumeAdmissionRefusals(t *testing.T, ctx context.Context, fixture completionSettlementFixture, authority effects.Authority, request effects.AuthorizeRequest, previous effects.Attempt, expected map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	for _, scenario := range []string{"stale_generation", "original_claim", "foreign_operation", "missing_journal", "logical_identity", "semantic_destination", "body_fingerprint"} {
		t.Run("refuse_"+scenario, func(t *testing.T) {
			changed, changedAuthority := request, authority
			journal := *request.ProviderRequestJournal
			changed.ProviderRequestJournal = &journal
			switch scenario {
			case "stale_generation":
				changedAuthority = previous.Authority
			case "original_claim":
				changed.Origin = previous.Origin
			case "foreign_operation":
				changed.OperationID = uuid.NewString()
			case "missing_journal":
				changed.ProviderRequestJournal = nil
			case "logical_identity":
				journal.LogicalOperationIdentity = "another-logical-provider-call"
			case "semantic_destination":
				journal.Destination += "/foreign"
			case "body_fingerprint":
				changed.RequestFingerprint = effects.Fingerprint([]byte("another physical request"))
			}
			if attempt, err := fixture.store.AuthorizeExternalAttempt(ctx, changedAuthority, changed); err == nil || attempt.AttemptID != "" {
				t.Fatal("inexact retained provider evidence granted a child attempt")
			}
			actual, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(expected, actual) {
				t.Fatalf("resume refusal changed durable rows: %v", err)
			}
		})
	}
}
