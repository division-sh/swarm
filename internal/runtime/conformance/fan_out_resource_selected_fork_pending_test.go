package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// A system node claims synchronously inside pipeline admission. A blocking
// ClaimDelivery would also block the pipeline receipt, so this faultpoint
// defers only that normal receiver claim using its real persisted snapshot.
type selectedDeploymentClaimGate struct {
	runtimedelivery.Store
	held     atomic.Bool
	deferred atomic.Int64
	kind     runtimedelivery.ExecutionAuthorityKind
	entered  chan string
}

type selectedDeploymentSettlementExecutor struct {
	startupownership.FanOutExecutor
	committed chan string
}

func (e *selectedDeploymentSettlementExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	return e.FanOutExecutor.ServeFanOutCandidate(ctx, selectedDeploymentSettlementOwner{FanOutObligationOwner: owner, committed: e.committed}, key)
}

type selectedDeploymentSettlementOwner struct {
	pipeline.FanOutObligationOwner
	committed chan string
}

func (o selectedDeploymentSettlementOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (runtimepipelineobligation.PublicationGroup, error) {
	group, err := o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	if group == nil || err != nil {
		return group, err
	}
	return selectedDeploymentSettlementGroup{PublicationGroup: group, committed: o.committed}, nil
}

type selectedDeploymentSettlementGroup struct {
	runtimepipelineobligation.PublicationGroup
	committed chan string
}

func (g selectedDeploymentSettlementGroup) Settle(ctx context.Context, members []runtimepipelineobligation.PublicationSettlementMember) (runtimepipelineobligation.PublicationGroupOutcome, error) {
	outcome, err := g.PublicationGroup.Settle(ctx, members)
	for _, result := range outcome.Results {
		if result.Outcome.DeliveryHandoffCommitted() {
			g.committed <- result.Claim.EventID()
		}
	}
	return outcome, err
}

func (g *selectedDeploymentClaimGate) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	kind := g.kind
	if kind == "" {
		kind = runtimedelivery.ExecutionAuthorityNormalRuntime
	}
	if !g.held.Load() || authority.Kind() != kind || event.Type() != selectedDeploymentEvent {
		return g.Store.ClaimDelivery(ctx, authority, event, route)
	}
	id, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	snapshot, err := g.Store.Snapshot(ctx, id)
	if err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	if snapshot.Status != runtimedelivery.StatusPending {
		return g.Store.ClaimDelivery(ctx, authority, event, route)
	}
	g.deferred.Add(1)
	if g.entered != nil {
		select {
		case g.entered <- event.ID():
		default:
		}
	}
	return runtimedelivery.ClaimResult{Acknowledged: true, Disposition: runtimedelivery.ClaimDeferred, Snapshot: snapshot}, nil
}

// R3b requires T after pipeline settlement and before receiver claim. A
// publication-only snapshot is R3a and must not satisfy this proof. The child
// must preserve the old pending delivery while feeding only the changed pin.
func TestDeploymentSourceFixedTPendingReceiverAndChangedPinBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			gate := &selectedDeploymentClaimGate{}
			gate.held.Store(true)
			settled := make(chan string, 16)
			f := selectedDeploymentResourceFixtureWithDelivery(t, backend, "singleton", nil, func(store runtimedelivery.Store) runtimedelivery.Store {
				gate.Store = store
				return gate
			}, func(executor startupownership.FanOutExecutor) startupownership.FanOutExecutor {
				return &selectedDeploymentSettlementExecutor{FanOutExecutor: executor, committed: settled}
			})
			t.Cleanup(func() { gate.held.Store(false) })
			server := f.operatorServer(t)
			start := func(name string, rows []byte) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, rows, 0o600); err != nil {
					t.Fatal(err)
				}
				return startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			}
			oldRows := []byte("{\"account_id\":\"old\",\"document\":{\"version\":1}}\n")
			newRows := []byte("{\"account_id\":\"new\",\"document\":{\"version\":2}}\n")
			newVersion := selectedDeploymentVersion(t, f, newRows)
			sourceRunID := start("old", oldRows)
			pointEventID := waitDeploymentForkRowEvent(t, f.ctx, f.db, sourceRunID, f.runtime.fanOutServing.Wake)
			waitCtx, cancelWait := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancelWait()
			for matched := false; !matched; {
				select {
				case eventID := <-settled:
					matched = eventID == pointEventID
				case <-waitCtx.Done():
					t.Fatalf("R3b pipeline receipt/handoff did not commit at T: %v", waitCtx.Err())
				}
			}
			var deliveryID, deliveryStatus string
			var handoffStamped bool
			var pipelineReceipts, attempts int
			err := f.db.QueryRowContext(f.ctx, `SELECT CAST(d.delivery_id AS TEXT),d.status,d.continuation_handoff_at IS NOT NULL,
					(SELECT COUNT(*) FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'),
					(SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id)
					FROM event_deliveries d WHERE d.run_id=$1 AND d.event_id=$2`, sourceRunID, pointEventID).
				Scan(&deliveryID, &deliveryStatus, &handoffStamped, &pipelineReceipts, &attempts)
			if err != nil {
				if err == sql.ErrNoRows {
					t.Fatal("R3b dispatch completed without persisted receiver delivery")
				}
				t.Fatal(err)
			}
			if deliveryID == "" || deliveryStatus != "pending" || pipelineReceipts != 1 || !handoffStamped || attempts != 0 || gate.deferred.Load() == 0 {
				t.Fatalf("R3b requires settled pipeline, stamped handoff and unclaimed receiver at T: delivery=%q status=%q receipt=%d handoff=%t attempts=%d deferred=%d", deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts, gate.deferred.Load())
			}
			point := assertDeploymentForkPendingCandidate(t, f, runfork.RunForkPlanRequest{SourceRunID: sourceRunID}, pointEventID, deliveryID, runfork.RunForkDeploymentPendingReceiver)
			if point.Kind != runfork.RunForkPointDeploymentRevision {
				t.Fatalf("R3b did not select the post-handoff deployment revision: %+v", point)
			}
			t.Logf("R3b exact T: delivery=%s status=%s receipt=%d handoff=%t attempts=%d deferred_claims=%d", deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts, gate.deferred.Load())
			_ = start("new", newRows)
			owner, _ := deploymentForkOwner(t, f)
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id":       sourceRunID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
				"data_pin_overrides": []any{map[string]any{
					"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
					"version_id":  string(newVersion.VersionID),
				}},
			}
			result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 {
				t.Fatalf("fixed-T pending receiver fork: %s", rpcErr)
			}
			if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != newVersion.VersionID {
				t.Fatalf("fixed-T fork did not bind the changed pin: %+v", result)
			}
			if result.ForkPointKind != string(runfork.RunForkPointDeploymentRevision) || result.ForkRevision != point.Revision || result.ForkEventID != "" {
				t.Fatalf("R3b fork changed its fixed post-handoff cut: %+v", result)
			}
			gate.held.Store(false)
			f.runtime.bus.SignalDeliveryContinuations()
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, result.ForkRunID, 3*time.Minute)
			page := selectedDeploymentPublicEvents(t, f.ctx, server, result.ForkRunID)
			if len(page.Events) != 2 || page.NextCursor != "" {
				t.Fatalf("fixed-T fork has %d old/new events, cursor=%q: %+v", len(page.Events), page.NextCursor, page.Events)
			}
			want := map[string]map[string]any{}
			for _, row := range [][]byte{oldRows, newRows} {
				var payload map[string]any
				if err := json.Unmarshal(bytes.TrimSpace(row), &payload); err != nil {
					t.Fatal(err)
				}
				want[payload["account_id"].(string)] = payload
			}
			seen := map[string]bool{}
			for _, event := range page.Events {
				key, _ := event.Payload["account_id"].(string)
				if seen[key] || !reflect.DeepEqual(event.Payload, want[key]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
					t.Fatalf("fixed-T fork lost exact old/new receiver settlement: %+v", event)
				}
				seen[key] = true
			}
			if !seen["old"] || !seen["new"] {
				t.Fatalf("fixed-T fork failed to retain old pending and new pin rows: %+v", seen)
			}
			var newFeedOutcomes int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, result.ForkRunID).Scan(&newFeedOutcomes); err != nil {
				t.Fatal(err)
			}
			if newFeedOutcomes != 1 {
				t.Fatalf("fixed-T fork issued %d changed-pin ordinals, want one", newFeedOutcomes)
			}
		})
	}
}
