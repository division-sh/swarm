package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func seedSelectedRouteRecoveryEvent(t testing.TB, ctx context.Context, db *sql.DB, runID, eventID string) {
	t.Helper()
	seedPostgresRootEventRecordFixture(
		t, ctx, db, eventID, runID, "item.received",
		events.EventProducerPlatform, "route-recovery", "", "", time.Now().UTC(),
	)
}

func TestNormalizeRunForkSelectedContractRouteRecoveryRejectsCurrentRouteOwner(t *testing.T) {
	selection := RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/contracts",
		WorkflowName:    "workflow",
		WorkflowVersion: "v1",
	}
	_, err := normalizeRunForkSelectedContractRouteRecovery(RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         uuid.NewString(),
		SourceRunID:       uuid.NewString(),
		ForkEventID:       uuid.NewString(),
		ContractSelection: selection,
		RouteTopology: RunForkSelectedContractRouteTopology{
			Owner:                         "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			NonMutating:                   true,
			ContractSelection:             selection,
			FrontierEvidenceFingerprint:   "frontier",
			RoutePersistenceSupported:     false,
			ExecutableRecipientsSupported: false,
		},
		RecipientPlanning: RunForkSelectedContractRecipientPlanning{
			Owner:                       RunForkSelectedContractRecipientPlanningOwner,
			RouteTopologyOwner:          RunForkSelectedContractRouteTopologyOwner,
			NonMutating:                 true,
			RecipientPlanningSupported:  true,
			DeliveryWritesSupported:     false,
			ContractSelection:           selection,
			FrontierEvidenceFingerprint: "frontier",
		},
	}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), RunForkSelectedContractRouteTopologyOwner) {
		t.Fatalf("normalize error = %v, want canonical route topology owner rejection", err)
	}
}

func TestRecordRunForkSelectedContractRouteRecoveryRoundTripsForkLocalEvidence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedSelectedRouteRecoveryEvent(t, ctx, db, sourceRunID, eventID)

	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)

	record, err := pg.RecordRunForkSelectedContractRouteRecovery(ctx, RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       eventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	})
	if err != nil {
		t.Fatalf("RecordRunForkSelectedContractRouteRecovery: %v", err)
	}
	if record.Owner != RunForkSelectedContractRoutePersistenceOwner ||
		record.RuntimeRecoveryOwner != RunForkSelectedContractRouteRecoveryOwner ||
		record.StaticRouteEventCount != 1 ||
		record.RecipientPlanEventCount != 1 ||
		record.RouteTopologyFingerprint == "" ||
		record.RecipientPlanningFingerprint == "" {
		t.Fatalf("record = %#v", record)
	}
	loaded, ok, err := pg.LoadRunForkSelectedContractRouteRecovery(ctx, forkRunID)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractRouteRecovery: %v", err)
	}
	if !ok {
		t.Fatal("route recovery row not found")
	}
	if loaded.RouteTopologyFingerprint != record.RouteTopologyFingerprint ||
		loaded.RecipientPlanningFingerprint != record.RecipientPlanningFingerprint ||
		!strings.Contains(string(loaded.RouteTopology), "item.received") ||
		!strings.Contains(string(loaded.RecipientPlanning), "node-a") {
		t.Fatalf("loaded = %#v", loaded)
	}
}

func TestRecordRunForkSelectedContractRouteRecoveryRoundTripsBundleHashSelection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedSelectedRouteRecoveryEvent(t, ctx, db, sourceRunID, eventID)

	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	targetHash := "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	selection = RunForkContractSelection{
		Mode:            RunForkContractSelectionModeBundleHash,
		BundleHash:      targetHash,
		WorkflowName:    selection.WorkflowName,
		WorkflowVersion: selection.WorkflowVersion,
	}
	topology.ContractSelection = selection
	planning.ContractSelection = selection

	record, err := pg.RecordRunForkSelectedContractRouteRecovery(ctx, RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       eventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	})
	if err != nil {
		t.Fatalf("RecordRunForkSelectedContractRouteRecovery: %v", err)
	}
	if record.ContractSelection.Mode != RunForkContractSelectionModeBundleHash ||
		record.ContractSelection.BundleHash != targetHash ||
		record.ContractSelection.ContractsRoot != "" {
		t.Fatalf("record selection = %#v", record.ContractSelection)
	}
	loaded, ok, err := pg.LoadRunForkSelectedContractRouteRecovery(ctx, forkRunID)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractRouteRecovery: %v", err)
	}
	if !ok {
		t.Fatal("route recovery row not found")
	}
	if loaded.ContractSelection.Mode != RunForkContractSelectionModeBundleHash ||
		loaded.ContractSelection.BundleHash != targetHash ||
		loaded.ContractSelection.ContractsRoot != "" ||
		!strings.Contains(string(loaded.RouteTopology), targetHash) ||
		!strings.Contains(string(loaded.RecipientPlanning), targetHash) {
		t.Fatalf("loaded bundle_hash route recovery = %#v", loaded)
	}
}

func TestRecordRunForkSelectedContractRouteRecoveryFeedsManagerRecoveryThroughJSONB(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedSelectedRouteRecoveryEvent(t, ctx, db, sourceRunID, eventID)
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	if _, err := pg.RecordRunForkSelectedContractRouteRecovery(ctx, RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       eventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}); err != nil {
		t.Fatalf("RecordRunForkSelectedContractRouteRecovery: %v", err)
	}
	bus := &selectedRouteRecoveryPostgresBus{store: selectedRouteRecoveryStoreWrapper{pg: pg}}
	am := runtimemanager.NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return selectedRouteRecoveryAgent{id: cfg.ID}, nil
	}, pg)

	ctx = managedExecutionStoreTestContext(t, ctx)
	if err := am.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	guard, ok := am.SelectedContractRouteRecoveryRecipientGuard(forkRunID)
	if !ok {
		t.Fatalf("missing recovered recipient guard for fork %s", forkRunID)
	}
	guard.ExpectForkEvent("00000000-0000-0000-0000-000000000991", eventID)
	evt := eventtest.PersistedProjection("00000000-0000-0000-0000-000000000991",
		events.EventType("item.received"),
		RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})

	if err := guard.Authorize(ctx, evt, runtimebus.PublishRecipientPlan{
		RoutedRecipients: []runtimebus.PublishDiagnosticRecipient{{
			Type:        "node",
			ID:          "node-a",
			Path:        "flow-a/node-a",
			RouteSource: "selected_contracts",
		}},
	}); err != nil {
		t.Fatalf("Authorize recovered JSONB recipient plan: %v", err)
	}
}

func TestRecordRunForkSelectedContractRouteRecoveryFeedsManagerRecoveryThroughBundleHashJSONB(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedSelectedRouteRecoveryEvent(t, ctx, db, sourceRunID, eventID)
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	targetHash := "bundle-v1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	selection = RunForkContractSelection{
		Mode:            RunForkContractSelectionModeBundleHash,
		BundleHash:      targetHash,
		WorkflowName:    selection.WorkflowName,
		WorkflowVersion: selection.WorkflowVersion,
	}
	topology.ContractSelection = selection
	planning.ContractSelection = selection
	if _, err := pg.RecordRunForkSelectedContractRouteRecovery(ctx, RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       eventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}); err != nil {
		t.Fatalf("RecordRunForkSelectedContractRouteRecovery: %v", err)
	}
	bus := &selectedRouteRecoveryPostgresBus{store: selectedRouteRecoveryStoreWrapper{pg: pg}}
	am := runtimemanager.NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return selectedRouteRecoveryAgent{id: cfg.ID}, nil
	}, pg)

	ctx = managedExecutionStoreTestContext(t, ctx)
	if err := am.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	guard, ok := am.SelectedContractRouteRecoveryRecipientGuard(forkRunID)
	if !ok {
		t.Fatalf("missing recovered recipient guard for fork %s", forkRunID)
	}
	guard.ExpectForkEvent("00000000-0000-0000-0000-000000000992", eventID)
	evt := eventtest.PersistedProjection("00000000-0000-0000-0000-000000000992",
		events.EventType("item.received"),
		RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})

	if err := guard.Authorize(ctx, evt, runtimebus.PublishRecipientPlan{
		RoutedRecipients: []runtimebus.PublishDiagnosticRecipient{{
			Type:        "node",
			ID:          "node-a",
			Path:        "flow-a/node-a",
			RouteSource: "selected_contracts",
		}},
	}); err != nil {
		t.Fatalf("Authorize recovered bundle_hash JSONB recipient plan: %v", err)
	}
}

func TestRecordRunForkSelectedContractRouteRecoveryRejectsJSONBTamperDuringManagerRecovery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedSelectedRouteRecoveryEvent(t, ctx, db, sourceRunID, eventID)
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	if _, err := pg.RecordRunForkSelectedContractRouteRecovery(ctx, RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       eventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}); err != nil {
		t.Fatalf("RecordRunForkSelectedContractRouteRecovery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE run_fork_selected_contract_route_recoveries
		SET recipient_planning = jsonb_set(recipient_planning, '{recipient_plan_events,0,recipients,0,subscriber_id}', '"node-tampered"'::jsonb)
		WHERE fork_run_id = $1::uuid
	`, forkRunID); err != nil {
		t.Fatalf("tamper recipient planning: %v", err)
	}
	am := runtimemanager.NewAgentManager(&selectedRouteRecoveryPostgresBus{store: selectedRouteRecoveryStoreWrapper{pg: pg}}, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return selectedRouteRecoveryAgent{id: cfg.ID}, nil
	}, pg)

	err := am.Recover(ctx)
	if err == nil || !strings.Contains(err.Error(), "recipient planning fingerprint mismatch") {
		t.Fatalf("Recover error = %v, want recipient planning fingerprint mismatch", err)
	}
}

func testSelectedRouteRecoveryEvidence(eventID string) (RunForkContractSelection, RunForkSelectedContractRouteTopology, RunForkSelectedContractRecipientPlanning) {
	selection := RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/contracts",
		WorkflowName:    "workflow",
		WorkflowVersion: "v1",
	}
	topology := RunForkSelectedContractRouteTopology{
		Owner:                         RunForkSelectedContractRouteTopologyOwner,
		RouteAdmissionOwner:           RunForkSelectedContractRouteAdmissionOwner,
		NonMutating:                   true,
		RoutePersistenceSupported:     false,
		ExecutableRecipientsSupported: false,
		ContractSelection:             selection,
		StaticTopologySupported:       true,
		DynamicTopologySupported:      true,
		FrontierAdmissionOwner:        RunForkContractFrontierAdmissionOwner,
		FrontierEventCount:            1,
		FrontierSourceEventIDs:        []string{eventID},
		FrontierEvidenceFingerprint:   "frontier-fingerprint",
		StaticRouteEvents: []RunForkSelectedContractRouteEvent{{
			SourceEventID: eventID,
			EventName:     "item.received",
			DerivedRecipients: []RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "node-a",
				Path:           "flow-a/node-a",
				RouteSource:    "selected_contracts",
			}},
			Disposition: RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	planning := RunForkSelectedContractRecipientPlanning{
		Owner:                       RunForkSelectedContractRecipientPlanningOwner,
		RouteTopologyOwner:          RunForkSelectedContractRouteTopologyOwner,
		RouteAdmissionOwner:         RunForkSelectedContractRouteAdmissionOwner,
		FutureExecutionOwner:        RunForkSelectedContractExecutionOwner,
		NonMutating:                 true,
		RecipientPlanningSupported:  true,
		DeliveryWritesSupported:     false,
		ContractSelection:           selection,
		FrontierEventCount:          1,
		FrontierSourceEventIDs:      []string{eventID},
		FrontierEvidenceFingerprint: "frontier-fingerprint",
		RecipientPlanEvents: []RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: eventID,
			EventName:     "item.received",
			Recipients: []RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "node-a",
				Path:           "flow-a/node-a",
				RouteSource:    "selected_contracts",
			}},
			Disposition: RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	return selection, topology, planning
}

type selectedRouteRecoveryPostgresBus struct {
	store runtimebus.EventStore
	logs  []runtimepipeline.RuntimeLogEntry
}

func (*selectedRouteRecoveryPostgresBus) Publish(context.Context, events.Event) error { return nil }
func (*selectedRouteRecoveryPostgresBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*selectedRouteRecoveryPostgresBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*selectedRouteRecoveryPostgresBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*selectedRouteRecoveryPostgresBus) Unsubscribe(string)        {}
func (*selectedRouteRecoveryPostgresBus) ResetInMemoryState() error { return nil }
func (b *selectedRouteRecoveryPostgresBus) Store() runtimebus.EventStore {
	return b.store
}
func (b *selectedRouteRecoveryPostgresBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.logs = append(b.logs, entry)
	return nil
}

type selectedRouteRecoveryAgent struct{ id string }

func (a selectedRouteRecoveryAgent) ID() string                      { return a.id }
func (selectedRouteRecoveryAgent) Type() string                      { return "generic" }
func (selectedRouteRecoveryAgent) Subscriptions() []events.EventType { return nil }
func (selectedRouteRecoveryAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

type selectedRouteRecoveryStoreWrapper struct {
	pg *PostgresStore
}

func (s selectedRouteRecoveryStoreWrapper) AppendEvent(context.Context, events.Event) error {
	return nil
}
func (s selectedRouteRecoveryStoreWrapper) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublishNoop(ctx, plan)
}
func (s selectedRouteRecoveryStoreWrapper) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (s selectedRouteRecoveryStoreWrapper) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s selectedRouteRecoveryStoreWrapper) SupportsPersistedReplay() bool { return false }
func (s selectedRouteRecoveryStoreWrapper) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error) {
	return s.pg.ListSelectedContractRouteRecoveryRecords(ctx)
}
