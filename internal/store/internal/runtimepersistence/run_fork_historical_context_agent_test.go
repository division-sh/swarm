package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// H22 uses real event/delivery publication, claim, settlement and revision
// writers. The foreign agent identity remains well-formed and its route hash is
// recomputed: rejection must establish owning-run consistency, not hash damage.
func TestRunForkHistoricalContextNestedAgentOwningRunBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, geometry := range []string{"root_agent", "nested_agent"} {
				t.Run(geometry, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					opened := backend.open(t)
					runID, foreignID := uuid.NewString(), uuid.NewString()
					seedAuthorActivityReceiptRun(t, opened, ctx, runID)
					seedAuthorActivityReceiptRun(t, opened, ctx, foreignID)
					publishCompleteRunForkRevisionBaseline(t, ctx, opened.db, backend.name == "postgres", runID)
					name, err := agentidentity.RuntimeName("history-agent", "store-test-fixture")
					if err != nil {
						t.Fatal(err)
					}
					agentRoute := agentidentity.RootRoute()
					target := events.RouteIdentity{FlowID: "root", FlowInstance: "root"}
					if geometry == "nested_agent" {
						agentRoute, err = agentidentity.PresentRoute("outer/inner", "nested-instance", "outer/one/inner/nested-instance")
						if err != nil {
							t.Fatal(err)
						}
						target = events.RouteIdentity{FlowID: "outer/inner", FlowInstance: "outer/one/inner/nested-instance"}
					}
					identity, err := agentidentity.New(runID, name, agentRoute)
					if err != nil {
						t.Fatal(err)
					}
					route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity, Target: events.MustEntitylessReceiverTarget(target)}
					at := time.Now().UTC().Truncate(time.Microsecond)
					event := eventtest.PersistedProjection(uuid.NewString(), "history.agent_ready", "history-proof", "", []byte(`{}`), 0, runID, "", events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{target}), at)
					if err := commitSemanticEventFixtureWithRoutes(ctx, opened.store, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatalf("canonical agent publication: %v", err)
					}
					live := loadDeliverySnapshotFixture(t, ctx, opened.store, event.ID(), route)
					revision := routeEvidenceHead(t, ctx, opened.db, runID)
					historical := routeEvidenceSnapshot(t, ctx, opened.db, runID, live.DeliveryID, revision)
					assertRouteEvidenceEqual(t, historical.Route, route)
					if historical.RunID != runID || historical.Route.AgentIdentity.RunID != runID {
						t.Fatalf("canonical source agent ownership lost: %#v", historical)
					}
					planner := opened.store.(historicalContextPlanner)
					request := runfork.RunForkPlanRequest{SourceRunID: runID, At: event.ID()}
					healthy, err := planner.PlanRunFork(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					assertRouteEvidencePlan(t, healthy, live, revision, runfork.RunForkPendingClassificationPending)
					claim, err := claimDeliveryFixture(ctx, opened.store, event, route)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := opened.store.SettleSuccess(ctx, claim.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
					later := eventtest.PersistedProjection(uuid.NewString(), "history.agent_checkpoint", "history-proof", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, at.Add(time.Second))
					if err := commitSemanticPipelineProcessedEventFixture(ctx, opened.store, later); err != nil {
						t.Fatal(err)
					}
					repeated, err := planner.PlanRunFork(ctx, request)
					if err != nil || !reflect.DeepEqual(repeated, healthy) {
						t.Fatalf("lawful later settlement changed older plan: %v", err)
					}
					var raw string
					if err := opened.db.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 AND revision=$3`, runID, live.DeliveryID, revision).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					fields := historicalContextFields(t, raw)
					foreignRoute := route
					foreignRoute.AgentIdentity, err = agentidentity.New(foreignID, name, agentRoute)
					if err != nil {
						t.Fatal(err)
					}
					hash, err := foreignRoute.Identity()
					if err != nil || hash == live.RouteIdentity {
						t.Fatalf("foreign well-formed route/hash prerequisite: %v", err)
					}
					fields["agent_identity"] = foreignRoute.AgentIdentity
					fields["route_identity"] = events.EncodeDeliveryRouteIdentity(hash)
					bad := historicalContextJSON(t, fields)
					var roundtrip struct {
						Agent agentidentity.Identity `json:"agent_identity"`
						Hash  string                 `json:"route_identity"`
						RunID string                 `json:"run_id"`
					}
					if err := json.Unmarshal([]byte(bad), &roundtrip); err != nil || roundtrip.Agent != foreignRoute.AgentIdentity || roundtrip.RunID != runID || roundtrip.Hash != events.EncodeDeliveryRouteIdentity(hash) {
						t.Fatalf("hash-consistent nested ownership injection lost exact coordinates: %#v %v", roundtrip, err)
					}
					if _, err := opened.db.ExecContext(ctx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='event_deliveries' AND fact_key=$3 AND revision=$4`, bad, runID, live.DeliveryID, revision); err != nil {
						t.Fatal(err)
					}
					requireCompleteRunForkRevision(t, ctx, opened, runID)
					before := historicalContextDatabaseRows(t, opened.db, backend.name == "postgres")
					got, err := planner.PlanRunFork(ctx, request)
					historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, opened.db, backend.name == "postgres"))
					if err == nil {
						t.Fatalf("PlanRunFork accepted source delivery %s with hash-consistent foreign nested agent owner %s (wrapper/body owner %s); pending=%#v", live.DeliveryID, foreignID, runID, got.PendingWork)
					}
					if !reflect.DeepEqual(got, runfork.RunForkPlan{}) {
						t.Fatalf("corruption returned partial plan: %#v, error=%v", got, err)
					}
					t.Logf("hash-consistent nested ownership rejected: %v; complete database unchanged", err)
				})
			}
		})
	}
}
