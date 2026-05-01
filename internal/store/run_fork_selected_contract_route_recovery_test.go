package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, sourceRunID, forkRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, payload, produced_by_type)
		VALUES ($1::uuid, $2::uuid, 'item.received', '{}'::jsonb, 'platform')
	`, sourceRunID, eventID); err != nil {
		t.Fatalf("seed fork point event: %v", err)
	}

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
