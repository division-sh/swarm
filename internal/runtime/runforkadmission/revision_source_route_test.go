package runforkadmission

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			execution_mode, run_id, event_id, event_name, entity_id, flow_instance,
			source_route, target_route, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			('live', $1::uuid, $2::uuid, 'producer/inst-1/scan.requested', $4::uuid, 'consumer/inst-9', $5::jsonb, $6::jsonb, 'flow', '{}'::jsonb, 'producer-node', 'node', $7),
			('live', $1::uuid, $3::uuid, 'producer/inst-1/scan.requested', $4::uuid, 'consumer/inst-9', $5::jsonb, $6::jsonb, 'flow', '{}'::jsonb, 'producer-node', 'node', $8)
	`, runID, pendingEventID, completedEventID, targetEntityID, mustJSON(t, sourceRoute), mustJSON(t, targetRoute), at, at.Add(time.Second)); err != nil {
		t.Fatalf("seed routed events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id,
			status, retry_count, reason_code, delivered_at, created_at
		)
		VALUES
			($1::uuid, $2::uuid, $3::uuid, 'node', 'pending-source-node', 'pending', 0, 'matched_node_subscription', NULL, $5),
			($4::uuid, $2::uuid, $6::uuid, 'node', 'completed-source-node', 'delivered', 0, 'ok', $7, $7)
	`, uuid.NewString(), runID, pendingEventID, uuid.NewString(), at, completedEventID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'node', 'completed-source-node', 'success', 'ok', '{}'::jsonb, $2)
	`, completedEventID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed completed receipt: %v", err)
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

	plan, err := (storetest.AdmitPostgresRuntimeStore(t, db)).PlanRunFork(ctx, store.RunForkPlanRequest{
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
	ctx := context.Background()
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
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"},
			explicitSelector: true,
			source:           testContractFrontierTemplateConnectSource,
			wantHistory:      []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:        "latest template fork point without delivery",
			eventName:   "producer/inst-1/scan.requested",
			sourceRoute: events.RouteIdentity{FlowID: " producer ", FlowInstance: " /producer/inst-1/ "},
			source:      testContractFrontierTemplateConnectSource,
			wantHistory: []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "completed delivery deterministically agrees with fork point",
			eventName:        "producer/inst-1/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"},
			explicitSelector: true, deliveryStatus: "completed",
			source:      testContractFrontierTemplateConnectSource,
			wantHistory: []string{"consumer-node"}, wantHistoryEvents: 1, wantDynamic: []string{"producer/inst-1"},
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "pending delivery remains frontier work",
			eventName:        "producer/inst-1/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"},
			explicitSelector: true, deliveryStatus: "pending",
			source:       testContractFrontierTemplateConnectSource,
			wantFrontier: []string{"consumer-node"}, wantDynamic: []string{"producer/inst-1"},
			wantFrontierCodes: []string{store.RunForkBlockerContractFrontierExecutionUnsupported},
			wantHistoryCodes:  []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "static source preserves static connect",
			eventName:        "producer/scan.requested",
			sourceRoute:      events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"},
			explicitSelector: true,
			source:           func() semanticview.Source { return testContractFrontierConnectSource("static") },
			wantHistory:      []string{"consumer-node"}, wantHistoryEvents: 1,
			wantHistoryCodes: []string{nonMutating, flowHistory}, wantRouteFacts: true,
		},
		{
			name:             "root source needs no child route identity",
			eventName:        "root.ready",
			sourceRoute:      events.RouteIdentity{EntityID: "root-entity"},
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
			sourceRoute:       events.RouteIdentity{FlowID: "foreign", FlowInstance: "foreign/inst-9"},
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
				if _, err := db.ExecContext(ctx, `
					INSERT INTO events (execution_mode, run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
					VALUES ('live', $1::uuid, $2::uuid, 'platform.precursor', 'global', '{}'::jsonb, 'platform', 'platform', $3)
				`, runID, uuid.NewString(), at.Add(-time.Second)); err != nil {
					t.Fatalf("seed precursor event: %v", err)
				}
				captureRunForkRevision(t, ctx, db, runID)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO events (
					execution_mode, run_id, event_id, event_name, source_route, scope, payload,
					produced_by, produced_by_type, created_at
				)
				VALUES ('live', $1::uuid, $2::uuid, $3, $4::jsonb, 'flow', '{}'::jsonb, 'producer-node', 'node', $5)
			`, runID, eventID, tc.eventName, mustJSON(t, tc.sourceRoute), at); err != nil {
				t.Fatalf("seed fork point event: %v", err)
			}
			if tc.deliveryStatus != "" {
				deliveryID := uuid.NewString()
				status := "pending"
				var deliveredAt any
				if tc.deliveryStatus == "completed" {
					status = "delivered"
					deliveredAt = at
				}
				if _, err := db.ExecContext(ctx, `
					INSERT INTO event_deliveries (
						delivery_id, run_id, event_id, subscriber_type, subscriber_id,
						status, retry_count, reason_code, delivered_at, created_at
					) VALUES ($1::uuid, $2::uuid, $3::uuid, 'node', 'source-node', $4, 0, 'matched_node_subscription', $5, $6)
				`, deliveryID, runID, eventID, status, deliveredAt, at); err != nil {
					t.Fatalf("seed delivery: %v", err)
				}
				if tc.deliveryStatus == "completed" {
					if _, err := db.ExecContext(ctx, `
						INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at)
						VALUES ($1::uuid, 'node', 'source-node', 'success', 'ok', '{}'::jsonb, $2)
					`, eventID, at); err != nil {
						t.Fatalf("seed receipt: %v", err)
					}
				}
			}
			captureRunForkRevision(t, ctx, db, runID)

			selector := ""
			if tc.explicitSelector {
				selector = eventID
			}
			plan, err := (storetest.AdmitPostgresRuntimeStore(t, db)).PlanRunFork(ctx, store.RunForkPlanRequest{SourceRunID: runID, At: selector})
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
