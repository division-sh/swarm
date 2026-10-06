package runtimepersistence

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestSelectedForkAuthoredMutationRetainsEveryCoordinateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			claim, foreign := seedDeliveryRecoveryClaim(t, f, ctx), seedDeliveryRecoveryClaim(t, f, ctx)
			entity, mutation := uuid.NewString(), uuid.NewString()
			if _, err := f.db.ExecContext(ctx, `INSERT INTO entity_mutations
				(mutation_id,run_id,entity_id,domain,path,new_value,caused_by_event,writer_type,writer_id,handler_step)
				VALUES ($1,$2,$3,'authored_field','processed_token',$4,$5,'platform','workflow_engine','mutate')`,
				mutation, claim.Claim.RunID(), entity, `"receiver-proof"`, claim.Snapshot.EventID); err != nil {
				t.Fatal(err)
			}
			read := func(run, entity, event string) (string, error) {
				return ReadSelectedForkAuthoredMutationForTest(ctx, f.store, run, entity, event)
			}
			for _, cut := range []struct {
				name, domain, path, writer, writerID, step, event string
			}{
				{"complete", "authored_field", "processed_token", "platform", "workflow_engine", "mutate", claim.Snapshot.EventID},
				{"domain", "bookkeeping", "processed_token", "platform", "workflow_engine", "mutate", claim.Snapshot.EventID},
				{"path", "authored_field", "another_field", "platform", "workflow_engine", "mutate", claim.Snapshot.EventID},
				{"writer-type", "authored_field", "processed_token", "agent", "workflow_engine", "mutate", claim.Snapshot.EventID},
				{"writer-id", "authored_field", "processed_token", "platform", "another-writer", "mutate", claim.Snapshot.EventID},
				{"handler", "authored_field", "processed_token", "platform", "workflow_engine", "another-step", claim.Snapshot.EventID},
				{"foreign-cause", "authored_field", "processed_token", "platform", "workflow_engine", "mutate", foreign.Snapshot.EventID},
			} {
				t.Run(cut.name, func(t *testing.T) {
					if _, err := f.db.ExecContext(ctx, `UPDATE entity_mutations SET domain=$1,path=$2,writer_type=$3,writer_id=$4,handler_step=$5,caused_by_event=$6 WHERE mutation_id=$7`, cut.domain, cut.path, cut.writer, cut.writerID, cut.step, cut.event, mutation); err != nil {
						t.Fatal(err)
					}
					value, err := read(claim.Claim.RunID(), entity, claim.Snapshot.EventID)
					if cut.name == "complete" {
						if err != nil || value != `"receiver-proof"` {
							t.Fatalf("exact mutation value lost: %s %v", value, err)
						}
						for _, ids := range [][3]string{{foreign.Claim.RunID(), entity, claim.Snapshot.EventID}, {claim.Claim.RunID(), uuid.NewString(), claim.Snapshot.EventID}, {claim.Claim.RunID(), entity, foreign.Snapshot.EventID}} {
							if value, err := read(ids[0], ids[1], ids[2]); !errors.Is(err, sql.ErrNoRows) || value != "" {
								t.Fatalf("foreign coordinates acquired mutation: %s %v", value, err)
							}
						}
					} else if !errors.Is(err, sql.ErrNoRows) || value != "" {
						t.Fatalf("partial metadata admitted: %s %v", value, err)
					}
				})
			}
		})
	}
}

func TestVersionOneDeliveredSettlementCountRetainsOutcomeAndVersionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			claim := seedDeliveryRecoveryClaim(t, f, ctx)
			read := func(id string, want int) {
				t.Helper()
				if got, err := ReadVersionOneDeliveredSettlementCountForTest(ctx, f.store, id); err != nil || got != want {
					t.Fatalf("exact settled version-one delivered count=%d want=%d err=%v", got, want, err)
				}
			}
			read(claim.Claim.DeliveryID(), 0)
			if _, err := f.store.SettleSuccess(ctx, claim.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			read(claim.Claim.DeliveryID(), 1)
			read(uuid.NewString(), 0)
			retry := seedDeliveryRecoveryClaim(t, f, ctx)
			failure := runtimefailures.FromError(errors.New("retry proof"), "storage-proof", "dispatch")
			settled, err := f.store.SettleFailure(ctx, retry.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureRetry, ReasonCode: "retry_proof", Failure: &failure.Failure,
				RetryBase: time.Millisecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
			})
			if err != nil {
				t.Fatal(err)
			}
			read(retry.Claim.DeliveryID(), 0)
			if !settled.RetryScheduled || settled.NextEligibleAt.IsZero() {
				t.Fatal("retry proof lacks the owner's durable eligibility coordinate")
			}
			time.Sleep(time.Until(settled.NextEligibleAt))
			event, err := LoadCanonicalEventRecordForTest(ctx, f.store, retry.Snapshot.EventID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.store.ClaimDelivery(ctx, retry.Snapshot.Authority, event, retry.Snapshot.Route)
			if err != nil {
				t.Fatal(err)
			}
			next, ok := result.Acquired()
			if !ok || next.Claim.Version() != 2 {
				t.Fatalf("retry failed to mint version two: %+v", result)
			}
			if _, err := f.store.SettleSuccess(ctx, next.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			read(next.Claim.DeliveryID(), 0)
			read(claim.Claim.DeliveryID(), 1)
		})
	}
}
