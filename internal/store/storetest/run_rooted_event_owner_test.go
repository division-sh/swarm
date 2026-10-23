package storetest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1EventRunFixturesRetainExplicitRootBothStores(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			rooted, _ := sourceartifactfixture.Rooted(t, "event-source", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
			fact := sourceartifactfixture.FactFor(rooted.Artifact())
			base := semanticFixtureContext(context.Background(), fact)
			ctx := correlation.WithRootAdmission(base, rooted.Admission())
			sourceartifactfixture.RequireArtifact(t, ctx, selected, rooted.Artifact())
			foreign, _ := sourceartifactfixture.Rooted(t, "foreign", sourceartifactfixture.New("schema.yaml", []byte("required_agents: []\n")))
			reader := selected.(interface {
				LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
			})
			at := time.Now().UTC().Truncate(time.Microsecond)
			for _, kind := range []string{"scenario", "event"} {
				t.Run(kind, func(t *testing.T) {
					eventID := uuid.NewString()
					create := func(ctx context.Context, runID string) error {
						if kind == "scenario" {
							return EnsureEphemeralRun(ctx, selected, runID, at)
						}
						event := eventtest.RunCreatingRootIngress(eventID, "fixture.created", "fixture", "", nil, 0, runID, "", events.EventEnvelope{}, at)
						return EnsureRunForAdmittedEvent(ctx, selected, AdmitNativeDeliveryEvent(t, event), at)
					}
					runID := uuid.NewString()
					for i := 0; i < 2; i++ {
						if err := create(ctx, runID); err != nil {
							t.Fatalf("create/repeat with explicit root: %v", err)
						}
						loaded, err := reader.LoadRunRootedSource(ctx, runID)
						if err != nil || !loaded.Admission().Equal(rooted.Admission()) {
							t.Fatalf("run fixture lost exact root: %+v, %v", loaded.Admission().Reference(), err)
						}
					}
					collector := CollectTransactions(t, selected, TransactionProbeOptions{})
					for _, refusal := range []struct {
						name, message string
						ctx           context.Context
						cause         error
					}{
						{"missing", "", base, sourceartifact.ErrRootAdmissionMissing},
						{"cleared", "", correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{}), sourceartifact.ErrRootAdmissionMissing},
						{"contradictory", "rooted context does not match its content fact", correlation.WithRootAdmission(ctx, foreign.Admission()), nil},
					} {
						t.Run(refusal.name, func(t *testing.T) {
							id := uuid.NewString()
							if err := create(refusal.ctx, id); err == nil || !strings.Contains(err.Error(), refusal.message) || refusal.cause != nil && !errors.Is(err, refusal.cause) {
								t.Fatalf("invalid root did not refuse at fixture admission: %v", err)
							}
							if err := selected.RequirePresentRun(ctx, id); err == nil {
								t.Fatal("refused root created a run")
							}
						})
					}
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if err := create(cancelled, uuid.NewString()); !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled fixture lost cancellation: %v", err)
					}
					if proof := collector.Snapshot(); proof.Active != 0 || proof.Total.WriteCommits != 0 {
						t.Fatalf("invalid event fixture retained writes or transaction authority: %+v", proof)
					}
				})
			}
		})
	}
}
