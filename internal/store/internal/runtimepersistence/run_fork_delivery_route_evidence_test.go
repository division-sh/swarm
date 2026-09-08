package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkDeliveryRouteEvidenceBothStores(t *testing.T) {
	root := canonicalrouting.CopyReceiverMixedAgent(t)
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	graph := runtimepinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compile mixed receiver fixture: %#v", issues)
	}
	var connect runtimepinrouting.ConnectRoutePlan
	for _, plan := range graph.Plans() {
		if plan.ReceiverEndpoint().Readback().ResolvedEvent == "sink/work.completed" {
			connect = plan
			break
		}
	}
	if connect.ReceiverLocalEvent() == "" {
		t.Fatal("mixed receiver fixture has no work.completed connect")
	}
	table, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	var nodeRoute, agentRoute events.DeliveryRoute
	var agentPlan agentidentity.Plan
	for _, subscriber := range table.Resolve("sink/work.completed") {
		route := events.DeliveryRoute{Recipient: subscriber.Recipient}
		if route.Recipient.IsNode() {
			nodeRoute = route
		} else if route.Recipient.IsAgent() {
			agentRoute = route
			agentPlan = subscriber.AgentPlan
		}
	}
	if nodeRoute.Recipient.Empty() || agentRoute.Recipient.Empty() || agentPlan.IsZero() {
		t.Fatal("mixed receiver fixture must derive both exact node and agent recipients")
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, cell := range []struct {
				name      string
				route     events.DeliveryRoute
				connected bool
				ownership string
			}{
				{"node_connect_existing", nodeRoute, true, "existing"},
				{"node_connect_materializing", nodeRoute, true, "materializing"},
				{"node_connect_entityless", nodeRoute, true, "entityless"},
				{"agent_connect_existing", agentRoute, true, "existing"},
				{"node_nonconnect", nodeRoute, false, "existing"},
				{"agent_nonconnect", agentRoute, false, "existing"},
			} {
				t.Run(cell.name, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					runID := uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, backend.name == "postgres", runID)
					target := events.RouteIdentity{FlowID: "sink", FlowInstance: "sink", EntityID: uuid.NewString()}
					route := cell.route
					if route.Recipient.IsAgent() {
						route.AgentIdentity, err = agentPlan.Live(runID)
						if err != nil {
							t.Fatal(err)
						}
					}
					switch cell.ownership {
					case "existing":
						route.Target = events.MustExistingEntityTarget(target)
					case "materializing":
						route.Target = events.MustMaterializingEntityTarget(target)
					case "entityless":
						target.EntityID = ""
						route.Target = events.MustEntitylessReceiverTarget(target)
					}
					route.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-" + uuid.NewString()}}
					route.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"receiver_key": "sink-key", "route_proof": cell.name})
					if err != nil {
						t.Fatal(err)
					}
					if cell.connected {
						blueprint := runtimepinrouting.ConnectDeliveryRoute{
							Recipient: route.Recipient, Target: target,
							Context: route.Context, PayloadProjection: route.PayloadProjection,
						}
						if route.Recipient.IsAgent() {
							blueprint.AgentPlan = agentPlan
						}
						if route.Recipient.IsNode() {
							blueprint.Handler = runtimepinrouting.MustConnectReceiverHandler(mustPersistenceNode("sink", "collector"))
						}
						route.ConnectClaim, err = runtimepinrouting.ConnectExecutionClaim(connect, blueprint)
						if err != nil {
							t.Fatal(err)
						}
					}
					at := time.Now().UTC().Truncate(time.Microsecond)
					event := eventtest.PersistedProjection(uuid.NewString(), "work.completed", "route-proof", "", []byte(`{"ok":true}`), 0,
						runID, "", events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{target}), at)
					beforeCommit := routeEvidenceHead(t, ctx, fixture.db, runID)
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatalf("canonical event/delivery writer: %v", err)
					}
					live := loadDeliverySnapshotFixture(t, ctx, fixture.store, event.ID(), route)
					revision := routeEvidenceHead(t, ctx, fixture.db, runID)
					if revision != beforeCommit+1 {
						t.Fatalf("event/delivery commit advanced %d -> %d, want one atomic revision", beforeCommit, revision)
					}
					for family, key := range map[string]string{"events": event.ID(), "event_deliveries": live.DeliveryID} {
						var firstRevision int64
						if err := fixture.db.QueryRowContext(ctx, `SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family=$2 AND fact_key=$3`, runID, family, key).Scan(&firstRevision); err != nil || firstRevision != revision {
							t.Fatalf("%s first revision=%d err=%v, want atomic event/delivery revision %d", family, firstRevision, err, revision)
						}
					}
					historical := routeEvidenceSnapshot(t, ctx, fixture.db, runID, live.DeliveryID, revision)
					assertRouteEvidenceEqual(t, historical.Route, route)
					if historical.RouteIdentity != live.RouteIdentity || historical.DeliveryID != live.DeliveryID || historical.EventID != event.ID() || historical.RunID != runID || historical.Status != runtimedelivery.StatusPending {
						t.Fatalf("historical/live delivery coordinates disagree: historical=%#v live=%#v", historical, live)
					}
					planner := fixture.store.(interface {
						PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
					})
					request := runfork.RunForkPlanRequest{SourceRunID: runID, At: event.ID()}
					initial, err := planner.PlanRunFork(ctx, request)
					if err != nil {
						t.Fatalf("PlanRunFork complete historical route: %v", err)
					}
					assertRouteEvidencePlan(t, initial, live, revision, runfork.RunForkPendingClassificationPending)
					if cell.connected {
						assertRouteEvidenceStampedConsumers(t, source, initial, event.ID(), route, false)
					}

					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatalf("exact duplicate writer: %v", err)
					}
					if got := routeEvidenceHead(t, ctx, fixture.db, runID); got != revision {
						t.Fatalf("exact duplicate emitted revision %d, want unchanged %d", got, revision)
					}
					proveRouteEvidenceRollback(t, ctx, fixture, event, route)

					claimed, err := claimDeliveryFixture(ctx, fixture.store, event, route)
					if err != nil {
						t.Fatal(err)
					}
					if err := claimed.Claim.Validate(); err != nil {
						t.Fatalf("real claim must carry live authority: %v", err)
					}
					claimRevision := routeEvidenceHead(t, ctx, fixture.db, runID)
					claimHistory := routeEvidenceSnapshot(t, ctx, fixture.db, runID, live.DeliveryID, claimRevision)
					assertRouteEvidenceEqual(t, claimHistory.Route, route)
					if claimHistory.Status != runtimedelivery.StatusInProgress || claimHistory.ClaimVersion != claimed.Claim.Version() || claimHistory.ClaimExpiresAt.IsZero() {
						t.Fatalf("real claim lost historical lifecycle evidence: %#v", claimHistory)
					}
					if claimHistory.Authority.Validate() == nil {
						t.Fatal("historical readback of a live claim grants execution authority")
					}
					var claimFact []byte
					if err := fixture.db.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 AND revision=$3`, runID, live.DeliveryID, claimRevision).Scan(&claimFact); err != nil {
						t.Fatal(err)
					}
					var claimFields map[string]json.RawMessage
					if err := json.Unmarshal(claimFact, &claimFields); err != nil {
						t.Fatal(err)
					}
					for _, forbidden := range []string{"claim_token", "lease_token", "execution_authority"} {
						if _, exists := claimFields[forbidden]; exists {
							t.Fatalf("real claimed delivery revision contains %s", forbidden)
						}
					}
					if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
					later := eventtest.PersistedProjection(uuid.NewString(), "route.proof_checkpoint", "route-proof", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, at.Add(time.Second))
					if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, later); err != nil {
						t.Fatal(err)
					}
					older, err := planner.PlanRunFork(ctx, request)
					if err != nil || !reflect.DeepEqual(older, initial) {
						t.Fatalf("later live settlement changed fixed older event plan: err=%v\nbefore=%#v\nafter=%#v", err, initial, older)
					}
					latest, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: later.ID()})
					if err != nil {
						t.Fatalf("plan later selected event: %v", err)
					}
					if latest.ForkPoint.Revision <= revision || latest.ForkPoint.EventID != later.ID() {
						t.Fatalf("later event did not select later revision: %#v", latest.ForkPoint)
					}
					assertRouteEvidencePlan(t, latest, live, latest.ForkPoint.Revision, runfork.RunForkPendingClassificationDeliveredCompleted)
					if cell.connected {
						assertRouteEvidenceStampedConsumers(t, source, latest, event.ID(), route, true)
					}
					foreignRun := uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, foreignRun)
					if _, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: foreignRun, At: event.ID()}); err == nil {
						t.Fatal("foreign-run event selection accepted")
					}
					requireCompleteRunForkRevision(t, ctx, fixture, runID)
				})
			}
		})
	}
}

func routeEvidenceHead(t testing.TB, ctx context.Context, db *sql.DB, runID string) int64 {
	t.Helper()
	var revision int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	return revision
}

func routeEvidenceSnapshot(t testing.TB, ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, runID, deliveryID string, revision int64) runtimedelivery.Snapshot {
	t.Helper()
	var raw []byte
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1`, runID, deliveryID, revision).Scan(&raw, &present); err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("selected delivery revision is a tombstone")
	}
	got, err := runtimedelivery.DecodeHistoricalSnapshot(raw)
	if err != nil {
		t.Fatalf("decode actual SQL revision delivery projection: %v", err)
	}
	return got
}

func assertRouteEvidenceEqual(t testing.TB, got, want events.DeliveryRoute) {
	t.Helper()
	// Compare full wire semantics as well as the canonical hash. In particular,
	// recipient-only equality deliberately cannot establish connect evidence parity.
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) || !events.SameDeliveryRouteIdentity(got, want) || !got.ConnectClaim.Equal(want.ConnectClaim) {
		t.Fatalf("complete route evidence changed:\ngot %s\nwant %s", gotJSON, wantJSON)
	}
}

func assertRouteEvidencePlan(t testing.TB, plan runfork.RunForkPlan, live runtimedelivery.Snapshot, revision int64, classification string) {
	t.Helper()
	if plan.SourceRunID != live.RunID || plan.ForkPoint.Revision != revision {
		t.Fatalf("planner selected wrong run/revision: %#v", plan.ForkPoint)
	}
	matched := 0
	for _, pending := range plan.PendingWork {
		if pending.DeliveryID != live.DeliveryID {
			continue
		}
		matched++
		if pending.EventID != live.EventID || pending.Classification != classification {
			t.Fatalf("planner delivery classification/event: %#v", pending)
		}
		assertRouteEvidenceEqual(t, pending.DeliveryRoute, live.Route)
	}
	if matched != 1 {
		t.Fatalf("planner has %d exact delivery facts, want 1: %#v", matched, plan.PendingWork)
	}
}

func assertRouteEvidenceStampedConsumers(t testing.TB, source semanticview.Source, plan runfork.RunForkPlan, eventID string, route events.DeliveryRoute, completed bool) {
	t.Helper()
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	var recipients []runfork.RunForkContractFrontierRecipient
	if completed {
		history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range history.SelectedRouteEvents {
			if event.SourceEventID == eventID {
				recipients = event.DerivedRecipients
			}
		}
	} else {
		for _, event := range frontier.FrontierEvents {
			if event.SourceEventID == eventID {
				recipients = event.DerivedRecipients
			}
		}
	}
	var expectedPlan agentidentity.Plan
	if route.Recipient.IsAgent() {
		var err error
		expectedPlan, err = route.AgentIdentity.Plan()
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(recipients) != 1 || recipients[0].Recipient != route.Recipient || recipients[0].AgentPlan != expectedPlan || recipients[0].Path != route.Target.Route().FlowInstance || recipients[0].RouteSourceCode() != "stamped_connect_claim" {
		t.Fatalf("completed=%v consumer lost exact stamped recipient: %#v, route=%#v", completed, recipients, route)
	}
}

func proveRouteEvidenceRollback(t testing.TB, ctx context.Context, fixture authorActivityReceiptFixture, event events.Event, route events.DeliveryRoute) {
	t.Helper()
	before := routeEvidenceHead(t, ctx, fixture.db, event.RunID())
	rollbackEvent := eventtest.PersistedProjection(uuid.NewString(), "route.rollback", "route-proof", "", []byte(`{}`), 0, event.RunID(), "", events.EventEnvelope{}, event.CreatedAt())
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, rollbackEvent); err != nil {
		t.Fatal(err)
	}
	baseline := routeEvidenceHead(t, ctx, fixture.db, event.RunID())
	if baseline <= before {
		t.Fatal("rollback fixture event must be committed independently")
	}
	sentinel := errors.New("rollback after complete route revision capture")
	postgres := fixture.dialect == "postgres"
	operation := func(txctx context.Context, tx *sql.Tx) error {
		adapter, dialect := sqliteDeliveryAdapter, deliveryadapter.DialectSQLite
		if postgres {
			adapter, dialect = postgresDeliveryAdapter, deliveryadapter.DialectPostgres
		}
		authority, err := deliveryFixtureAuthorityForRun(txctx, tx, dialect, event.RunID())
		if err != nil {
			return err
		}
		if _, err := adapter.CommitInitial(txctx, tx, rollbackEvent.ID(), event.RunID(), []events.DeliveryRoute{route}, authority); err != nil {
			return err
		}
		effects, err := runforkrevision.ForRun(event.RunID(), runforkrevision.FamilyEventDeliveries)
		if err != nil {
			return err
		}
		results, err := finalizeRunForkRevisionMatrix(txctx, tx, postgres, effects)
		if err != nil {
			return err
		}
		result := results[event.RunID()]
		if !result.Changed || result.Revision != baseline+1 {
			t.Fatalf("rollback transaction did not capture delivery revision: %#v", result)
		}
		deliveryID, err := runtimedelivery.DeliveryID(rollbackEvent.ID(), route)
		if err != nil {
			return err
		}
		assertRouteEvidenceEqual(t, routeEvidenceSnapshot(t, txctx, tx, event.RunID(), deliveryID, result.Revision).Route, route)
		return sentinel
	}
	var err error
	switch selected := fixture.store.(type) {
	case *PostgresStore:
		err = selected.runEventTransaction(ctx, operation)
	case *SQLiteRuntimeStore:
		err = selected.runEventTransaction(ctx, operation)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("rollback transaction error = %v, want injected post-capture failure", err)
	}
	assertDeliveryRouteEqualityCommitCounts(t, ctx, fixture, rollbackEvent.ID(), 1, 0)
	if got := routeEvidenceHead(t, ctx, fixture.db, event.RunID()); got != baseline {
		t.Fatalf("rolled-back delivery advanced revision: %d, want %d", got, baseline)
	}
	for _, table := range []string{"run_fork_revisions", "run_fork_fact_revisions"} {
		var count int
		if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1 AND revision>$2", event.RunID(), baseline).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial rollback history in %s: count=%d err=%v", table, count, err)
		}
	}
}
