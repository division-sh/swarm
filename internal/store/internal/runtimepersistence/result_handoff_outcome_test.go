package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

// Native acknowledgement loss is injected after real COMMIT, not simulated
// wire loss. A failed live handoff is a separate acknowledged-outcome case.
func TestResultSiblingsPreserveAcknowledgedHandoffOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []string{"publication", "delivery", "candidate"} {
			for _, phase := range []string{"handoff_failed", "commit_ack_lost"} {
				if operation == "publication" && phase == "handoff_failed" {
					// Existing-run ingress does not request a standalone-run
					// completion candidate. It is an acknowledged control.
					phase = "healthy"
				}
				t.Run(backend+"/"+operation+"/"+phase, func(t *testing.T) {
					fixture, control := openCompletionOutcomeFixture(t, backend)
					ctx, runID := fixture.context, fixture.authority.Target.RunID
					var run func() (bool, error)
					switch operation {
					case "delivery":
						run = func() (bool, error) {
							result, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, delivery.NotApplicableHandlerRuleSelection())
							return result.DeliveryID != "", err
						}
					case "candidate":
						run = func() (bool, error) {
							result, err := fixture.store.(runlifecycle.OperationOwner).RequestCompletionCandidate(ctx, runlifecycle.ImmediateCandidate(runID))
							return result != "", err
						}
					case "publication":
						event := eventtest.ExistingRunRootIngress(uuid.NewString(), "outcome.publication", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
						admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
						if err != nil {
							t.Fatal(err)
						}
						var release func()
						ctx, release, err = semanticEventFixtureContext(ctx, fixture.store, event)
						if err != nil {
							t.Fatal(err)
						}
						defer release()
						owner := pipelineObligationOwnerForFixture(fixture.store)
						claim, err := owner.ClaimPublication(ctx, event.ID())
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if err := owner.Release(context.WithoutCancel(ctx), claim); err != nil {
								t.Error(err)
							}
						}()
						ledger, err := events.NewConnectEvaluationLedger(nil)
						if err != nil {
							t.Fatal(err)
						}
						settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
						if err != nil {
							t.Fatal(err)
						}
						scope, _ := authoractivity.ScopeFromContext(ctx)
						disposition := pipelineobligation.Acknowledged("processed")
						command := runtimebus.PublicationCommand{
							Commit:      runtimebus.CommitPublishRequest{Event: admitted, RouteSettlement: settlement, ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: claim, Disposition: &disposition},
							AuthorScope: scope, HasAuthorScope: true,
							AuthorDescriptor: authoractivity.EventDescriptor{EventType: string(event.Type()), Disposition: authoractivity.StoryDifferent}, HasAuthorDescriptor: true,
						}
						run = func() (bool, error) {
							result, err := fixture.store.(publicationRevisionProofStore).CommitPublication(ctx, command)
							return result.AppendOutcome == runtimebus.EventAppendInserted, err
						}
					}
					query := "SELECT bundle_hash FROM runs WHERE run_id=?"
					if backend == "postgres" {
						query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
					}
					var bundle string
					if err := fixture.db.QueryRow(query, runID).Scan(&bundle); err != nil {
						t.Fatal(err)
					}
					failure := errors.New("independent " + phase)
					submits := 0
					registration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: bundle}, &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
						submits++
						if candidate.RunID != runID {
							t.Errorf("foreign candidate: %+v", candidate)
						}
						return failure
					}})
					if err != nil {
						t.Fatal(err)
					}
					defer registration.Release()
					control.phase, control.failure = phase, failure
					control.enabled.Store(true)
					result, err := run()
					control.enabled.Store(false)
					acknowledged := phase != "commit_ack_lost"
					validError := errors.Is(err, failure)
					if phase == "healthy" {
						validError = err == nil
					}
					if !validError || result != acknowledged {
						t.Fatalf("result present=%t acknowledged=%t err=%v", result, acknowledged, err)
					}
					wantSubmits := 0
					if phase == "handoff_failed" {
						wantSubmits = 1
					}
					if submits != wantSubmits {
						t.Fatalf("submissions=%d want=%d", submits, wantSubmits)
					}
				})
			}
		}
	}
}
