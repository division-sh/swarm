package runtimepersistence

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestProviderResumeTakeoverRetainsOneBudgetedContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "commit_refused", "commit_ack_lost"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				fixture, control := openProviderJournalFixture(t, backend, agentmemory.Plan{})
				ctx, journal, frame, _, body := providerJournalAdmissionInputs(t, fixture)
				handle, err := effects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := handle.MarkLaunched(ctx); err != nil {
					t.Fatal(err)
				}
				grant, request, process := prepareProviderTakeover(t, fixture, nil)
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				control.phase, control.failure = phase, errors.New("provider resume recovery commit observation")
				control.statement = func(query string) bool {
					if !strings.HasPrefix(strings.Join(strings.Fields(strings.ToUpper(query)), " "), "UPDATE RUNTIME_EXTERNAL_EFFECT_ATTEMPTS SET STATE") {
						return false
					}
					control.enabled.Store(true)
					return true
				}
				_, commitErr := grant.CommitAgentLifecycleTransition(ctx, request)
				control.enabled.Store(false)
				if phase == "healthy" && commitErr != nil {
					t.Fatal(commitErr)
				}
				if phase != "healthy" && !errors.Is(commitErr, control.failure) {
					t.Fatalf("native recovery cut not reached: %v", commitErr)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "commit_refused" {
					if !reflect.DeepEqual(before, after) {
						t.Fatal("refused recovery retained a retry charge or partial disposition")
					}
					return
				}
				delivery := providerResumeSnapshotRow(t, after, "event_deliveries", "delivery_id", handle.Attempt().Origin.Delivery.DeliveryID())
				status, _ := delivery["status"].Text()
				var retries int
				if err := json.Unmarshal(delivery["retry_count"].Value, &retries); err != nil {
					t.Fatal(err)
				}
				if status != "failed" || retries != 1 {
					t.Fatalf("restart must retain one budgeted continuation: status=%s retries=%d", status, retries)
				}
				attempt := providerResumeSnapshotRow(t, after, "runtime_external_effect_attempts", "attempt_id", handle.Attempt().AttemptID)
				state, _ := attempt["state"].Text()
				if state != "outcome_uncertain" {
					t.Fatalf("old physical attempt disposition=%s", state)
				}
				for table, want := range map[string]int{"agent_turns": 0, "spend_ledger": 1, "runtime_provider_attempt_drains": 1} {
					rows, err := DecodeSelectedForkSnapshotRowsForTest(after[table])
					if err != nil || len(rows) != want {
						t.Fatalf("%s rows=%d want=%d error=%v", table, len(rows), want, err)
					}
				}
				if !reflect.DeepEqual(before["runtime_agent_turn_lifetimes"], after["runtime_agent_turn_lifetimes"]) {
					t.Fatal("recovery reset or settled the first-launch clock")
				}
				if phase == "commit_ack_lost" {
					grant, request, _ = prepareProviderTakeover(t, fixture, process)
				}
				if _, err := grant.CommitAgentLifecycleTransition(ctx, request); err != nil {
					t.Fatal(err)
				}
				repeated, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				for _, table := range []string{"runtime_external_effect_operations", "runtime_external_effect_attempts", "event_deliveries", "event_delivery_attempts", "runtime_provider_attempt_drains", "agent_turns", "spend_ledger", "runtime_agent_turn_lifetimes"} {
					if !reflect.DeepEqual(after[table], repeated[table]) {
						t.Fatalf("reopen/replay changed the retained %s disposition", table)
					}
				}
			})
		}
	}
}

func TestProviderResumeTakeoverPreservesTerminalFallbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"legacy_no_journal", "prelaunch", "retry_exhausted"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				fixture, _ := openProviderJournalFixture(t, backend, agentmemory.Plan{})
				if scenario == "retry_exhausted" {
					failure := failures.New(failures.ClassDependencyUnavailable, "provider_resume_prior_retry", "resume-proof", "settle", nil).(*failures.Error).Failure
					if _, err := fixture.store.(deliverylifecycle.Store).SettleFailure(fixture.context, fixture.origin, deliverylifecycle.Settlement{
						Disposition: deliverylifecycle.FailureRetry, Failure: &failure,
						RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
					}); err != nil {
						t.Fatal(err)
					}
					if err := MakeDeliveryRecoveryRetryEligibleForTest(fixture.context, fixture.store, fixture.origin.DeliveryID()); err != nil {
						t.Fatal(err)
					}
					snapshot, err := fixture.store.Snapshot(fixture.context, fixture.origin.DeliveryID())
					if err != nil {
						t.Fatal(err)
					}
					event := loadCompletionOriginEvent(t, fixture, snapshot.EventID)
					result, err := fixture.store.ClaimDelivery(fixture.context, snapshot.Authority, event, snapshot.Route)
					if err != nil {
						t.Fatal(err)
					}
					claimed, ok := result.Acquired()
					if !ok {
						t.Fatalf("prior retry did not acquire: %s", result.Disposition)
					}
					fixture.origin = claimed.Claim
					fixture.context = deliverylifecycle.WithClaim(fixture.context, fixture.origin)
				}
				ctx, journal, frame, _, body := providerJournalAdmissionInputs(t, fixture)
				var handle *effects.Handle
				var err error
				if scenario == "legacy_no_journal" {
					handle, err = effects.BeginManagedCompletion(ctx, "anthropic_api", body, frame, nil)
				} else {
					handle, err = effects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				if scenario != "prelaunch" {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatal(err)
					}
				}
				grant, request, _ := prepareProviderTakeover(t, fixture, nil)
				if _, err := grant.CommitAgentLifecycleTransition(ctx, request); err != nil {
					t.Fatal(err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				delivery := providerResumeSnapshotRow(t, after, "event_deliveries", "delivery_id", fixture.origin.DeliveryID())
				status, _ := delivery["status"].Text()
				var retries int
				if err := json.Unmarshal(delivery["retry_count"].Value, &retries); err != nil {
					t.Fatal(err)
				}
				wantRetries := 0
				if scenario == "retry_exhausted" {
					wantRetries = deliverylifecycle.AgentMaxRetries
				}
				if status != "dead_letter" || retries != wantRetries {
					t.Fatalf("unsupported continuation changed terminal fallback: status=%s retries=%d", status, retries)
				}
				turns, err := DecodeSelectedForkSnapshotRowsForTest(after["agent_turns"])
				wantTurns := 1
				if scenario == "prelaunch" {
					wantTurns = 0
				}
				if err != nil || len(turns) != wantTurns {
					t.Fatalf("terminal turn count=%d want=%d error=%v", len(turns), wantTurns, err)
				}
			})
		}
	}
}

func TestProviderResumeTakeoverPreservesOriginalLogicalLineageBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, _ := openProviderJournalFixture(t, backend, agentmemory.Plan{})
			ctx, journal, frame, _, body := providerJournalAdmissionInputs(t, fixture)
			lineage := map[string]string{"provider_call": "original-call-coordinate"}
			handle, err := effects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, lineage)
			if err != nil {
				t.Fatal(err)
			}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			original := providerResumeSnapshotRow(t, before, "runtime_external_effect_operations", "operation_id", handle.Attempt().OperationID)
			grant, request, _ := prepareProviderTakeover(t, fixture, nil)
			if _, err := grant.CommitAgentLifecycleTransition(ctx, request); err != nil {
				t.Fatal(err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			delivery := providerResumeSnapshotRow(t, after, "event_deliveries", "delivery_id", fixture.origin.DeliveryID())
			if status, _ := delivery["status"].Text(); status != "failed" {
				t.Fatal("exact original nonempty lineage did not retain its budgeted continuation")
			}
			retained := providerResumeSnapshotRow(t, after, "runtime_external_effect_operations", "operation_id", handle.Attempt().OperationID)
			for _, column := range []string{"authority_evidence", "lineage", "request_fingerprint", "agent_frame_bytes", "bundle_hash", "runtime_epoch", "generation"} {
				if _, exists := original[column]; !exists || !reflect.DeepEqual(original[column], retained[column]) {
					t.Fatalf("startup rewrote original logical operation %s", column)
				}
			}
		})
	}
}

func providerResumeSnapshotRow(t *testing.T, snapshot map[string]SelectedForkStorageTableSnapshot, table, column, id string) map[string]SelectedForkSnapshotCell {
	t.Helper()
	rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot[table])
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if value, _ := row[column].Text(); value == id {
			return row
		}
	}
	t.Fatalf("exact %s row absent", table)
	return nil
}
