package runforkadmission

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(uuid.NewString(), "runfork-admission"))
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	pendingEventID := uuid.NewString()
	completedEventID := uuid.NewString()
	sourceEntityID := uuid.NewString()
	targetEntityID := uuid.NewString()
	at := time.Unix(1700001000, 0).UTC()
	sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: sourceEntityID}
	targetRoute := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: targetEntityID}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	envelope := events.EnvelopeForTargetRoute(events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), targetRoute)
	pendingEvent := eventtest.ExistingRunRootIngress(pendingEventID, "producer/inst-1/scan.requested", "producer-node", "", []byte(`{}`), 0, runID, envelope, at)
	pendingRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "pending-source-node"}
	storetest.CommitSemanticEventWithRoutes(t, ctx, pg, pendingEvent, []events.DeliveryRoute{pendingRoute}, runtimepipelineobligation.ScopeSubscribed)

	completedEvent := eventtest.ExistingRunRootIngress(completedEventID, "producer/inst-1/scan.requested", "producer-node", "", []byte(`{}`), 0, runID, envelope, at.Add(time.Second))
	completedRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "completed-source-node"}
	storetest.CommitSemanticEventWithRoutes(t, ctx, pg, completedEvent, []events.DeliveryRoute{completedRoute}, runtimepipelineobligation.ScopeSubscribed)
	completedClaim, err := pg.ClaimNodeDelivery(ctx, completedEvent, completedRoute)
	if err != nil {
		t.Fatalf("claim completed delivery: %v", err)
	}
	if _, err := pg.SettleSuccess(ctx, completedClaim.Claim, nil, time.Second); err != nil {
		t.Fatalf("settle completed delivery: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin revision capture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatalf("capture revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit revision: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, store.RunForkPlanRequest{
		SourceRunID: runID,
		At:          completedEventID,
	})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if len(plan.PendingWork) != 2 {
		t.Fatalf("pending work = %#v, want pending and completed revision rows", plan.PendingWork)
	}
	for _, item := range plan.PendingWork {
		if item.SourceRoute != sourceRoute || item.FlowInstance != targetRoute.FlowInstance {
			t.Fatalf("revision projection = source:%#v receiver:%q, want source %#v and receiver %q", item.SourceRoute, item.FlowInstance, sourceRoute, targetRoute.FlowInstance)
		}
	}

	source := testContractFrontierTemplateConnectSource()
	selection := SelectedContractSelection(source, "/tmp/contracts-a")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if len(frontier.FrontierEvents) != 1 || len(frontier.FrontierEvents[0].DerivedRecipients) != 1 || frontier.FrontierEvents[0].DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("frontier = %#v, want producer/inst-1 source routed independently of consumer/inst-9 receiver context", frontier.FrontierEvents)
	}

	history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan: plan, Source: source, ContractSelection: selection, FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(history.SelectedRouteEvents) != 1 || len(history.SelectedRouteEvents[0].DerivedRecipients) != 1 || history.SelectedRouteEvents[0].DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("history = %#v, want revisioned producer source routed independently of receiver context", history.SelectedRouteEvents)
	}
}

func TestRunForkPointRevisionedSourceRouteDrivesSelectedHistoryMatrixPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(uuid.NewString(), "runfork-admission"))
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	type testCase struct {
		name              string
		eventName         string
		sourceRoute       events.RouteIdentity
		explicitSelector  bool
		deliveryStatus    string
		source            func() semanticview.Source
		wantFrontier      []string
		wantHistory       []string
		wantHistoryEvents int
		wantDynamic       []string
		wantFrontierCodes []string
		wantHistoryCodes  []string
		wantRouteFacts    bool
	}
	nonMutating := store.RunForkBlockerSelectedContractRouteAdmissionNonMutating
	flowHistory := store.RunForkBlockerFlowRouteHistoryUnproven
	cases := []testCase{
		{
			name:             "explicit template fork point without delivery",
			eventName:        "producer/inst-1/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
			explicitSelector: true,
			source:           testContractFrontierTemplateConnectSource,
			wantHistory:      []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:        "latest template fork point without delivery",
			eventName:   "producer/inst-1/scan.requested",
			sourceRoute: events.RouteIdentity{FlowID: " producer ", FlowInstance: " /producer/inst-1/ ", EntityID: " 11111111-1111-4111-8111-111111111111 "},
			source:      testContractFrontierTemplateConnectSource,
			wantHistory: []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "completed delivery deterministically agrees with fork point",
			eventName:        "producer/inst-1/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
			explicitSelector: true, deliveryStatus: "completed",
			source:      testContractFrontierTemplateConnectSource,
			wantHistory: []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "pending delivery remains frontier work",
			eventName:        "producer/inst-1/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
			explicitSelector: true, deliveryStatus: "pending",
			source:       testContractFrontierTemplateConnectSource,
			wantFrontier: []string{"consumer-node"}, wantDynamic: []string{"producer/inst-1"},
			wantFrontierCodes: []string{store.RunForkBlockerContractFrontierExecutionUnsupported},
			wantHistoryCodes:  []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "static source preserves static connect",
			eventName:        "producer/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: "11111111-1111-4111-8111-111111111111"},
			explicitSelector: true,
			source:           func() semanticview.Source { return testContractFrontierConnectSource("static") },
			wantHistory:      []string{"consumer-node"}, wantHistoryEvents: 1,
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "root source needs no child route identity",
			eventName:        "root.ready",
			explicitSelector: true,
			source:           testContractFrontierRootConnectSource,
			wantHistory:      []string{"consumer-node"}, wantHistoryEvents: 1,
			wantHistoryCodes: []string{nonMutating},
		},
		{
			name:              "template source without route fails closed",
			eventName:         "producer/inst-1/scan.requested",
			explicitSelector:  true,
			source:            testContractFrontierTemplateConnectSource,
			wantHistoryEvents: 1,
			wantHistoryCodes:  []string{nonMutating},
		},
		{
			name:              "conflicting template source route fails closed",
			eventName:         "producer/inst-1/scan.requested",
			sourceRoute:       events.RouteIdentity{FlowID: "foreign", FlowInstance: "foreign/inst-9", EntityID: "22222222-2222-4222-8222-222222222222"},
			explicitSelector:  true,
			source:            testContractFrontierTemplateConnectSource,
			wantHistoryEvents: 1,
			wantHistoryCodes:  []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Now().UTC()
			if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, at.Add(-time.Minute)); err != nil {
				t.Fatalf("seed run: %v", err)
			}
			if !tc.explicitSelector {
				storetest.InsertExistingRunRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres, uuid.NewString(), runID, "platform.precursor",
					eventtest.Producer(events.EventProducerExternal, "platform"), []byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, at.Add(-time.Second))
				captureRunForkRevision(t, ctx, db, runID)
			}
			eventEnvelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, tc.sourceRoute)
			event := eventtest.ExistingRunRootIngress(eventID, events.EventType(tc.eventName), "producer-node", "", []byte(`{}`), 0, runID, eventEnvelope, at)
			if tc.deliveryStatus != "" {
				route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "source-node"}
				storetest.CommitSemanticEventWithRoutes(t, ctx, pg, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				if tc.deliveryStatus == "completed" {
					claimed, err := pg.ClaimNodeDelivery(ctx, event, route)
					if err != nil {
						t.Fatalf("claim completed delivery: %v", err)
					}
					if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, time.Second); err != nil {
						t.Fatalf("settle completed delivery: %v", err)
					}
				}
			} else {
				storetest.InsertCanonicalEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres, event)
			}
			captureRunForkRevision(t, ctx, db, runID)

			selector := ""
			if tc.explicitSelector {
				selector = eventID
			}
			plan, err := pg.PlanRunFork(ctx, store.RunForkPlanRequest{SourceRunID: runID, At: selector})
			if err != nil {
				t.Fatalf("PlanRunFork: %v", err)
			}
			if plan.ForkPoint.EventID != eventID || plan.ForkPoint.Input != selector {
				t.Fatalf("fork point = %#v, want event %s selector %q", plan.ForkPoint, eventID, selector)
			}
			if got, want := plan.ForkPoint.SourceRoute, tc.sourceRoute.Normalized(); got != want {
				t.Fatalf("fork point source route = %#v, want normalized %#v", got, want)
			}

			source := tc.source()
			selection := SelectedContractSelection(source, "/tmp/contracts-a")
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if got := frontierRecipientIDs(frontier); strings.Join(got, "\x00") != strings.Join(tc.wantFrontier, "\x00") {
				t.Fatalf("frontier recipients = %v, want %v", got, tc.wantFrontier)
			}
			if got := blockerCodes(frontier.UnsupportedBlockers); strings.Join(got, "\x00") != strings.Join(tc.wantFrontierCodes, "\x00") {
				t.Fatalf("frontier blocker codes = %v, want %v", got, tc.wantFrontierCodes)
			}

			history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{Plan: plan, Source: source, ContractSelection: selection, FrontierAdmission: frontier})
			if err != nil {
				t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
			}
			if len(history.SelectedRouteEvents) != tc.wantHistoryEvents {
				t.Fatalf("selected route events = %#v, want %d", history.SelectedRouteEvents, tc.wantHistoryEvents)
			}
			if got := historyRecipientIDs(history); strings.Join(got, "\x00") != strings.Join(tc.wantHistory, "\x00") {
				t.Fatalf("history recipients = %v, want %v", got, tc.wantHistory)
			}
			for _, event := range history.SelectedRouteEvents {
				if event.SourceEventID != eventID || event.Disposition != store.RunForkSelectedContractDispositionEvidenceOnly {
					t.Fatalf("selected route event = %#v, want fork point evidence-only disposition", event)
				}
			}
			if strings.Join(history.DynamicFlowInstances, "\x00") != strings.Join(tc.wantDynamic, "\x00") {
				t.Fatalf("dynamic instances = %v, want %v", history.DynamicFlowInstances, tc.wantDynamic)
			}
			if got := blockerCodes(history.UnsupportedBlockers); strings.Join(got, "\x00") != strings.Join(tc.wantHistoryCodes, "\x00") {
				t.Fatalf("history blocker codes = %v, want %v", got, tc.wantHistoryCodes)
			}
			if history.SourceRouteFactsPresent != tc.wantRouteFacts {
				t.Fatalf("source route facts present = %v, want %v", history.SourceRouteFactsPresent, tc.wantRouteFacts)
			}
		})
	}
}

func captureRunForkRevision(t *testing.T, ctx context.Context, db interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}, runID string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin revision capture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatalf("capture revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit revision capture: %v", err)
	}
}

func frontierRecipientIDs(frontier store.RunForkContractFrontierAdmission) []string {
	var out []string
	for _, event := range frontier.FrontierEvents {
		for _, recipient := range event.DerivedRecipients {
			out = append(out, recipient.SubscriberID)
		}
	}
	return out
}

func historyRecipientIDs(history store.RunForkSelectedContractRouteAdmission) []string {
	var out []string
	for _, event := range history.SelectedRouteEvents {
		for _, recipient := range event.DerivedRecipients {
			out = append(out, recipient.SubscriberID)
		}
	}
	return out
}

func blockerCodes(blockers []store.RunForkUnsupportedBlocker) []string {
	out := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		out = append(out, blocker.Code)
	}
	return out
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return string(raw)
}
