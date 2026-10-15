package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
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

type selectedDeploymentRollbackPublication struct {
	runforkexecution.SelectedContractReplayPersistence
	selected any
	checked  atomic.Bool
	receipt  chan storetest.SelectedFanOutPublicationRollbackProof
}

func (p *selectedDeploymentRollbackPublication) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if p.checked.CompareAndSwap(false, true) {
		proof, err := storetest.ProveSelectedFanOutPublicationRollback(ctx, p.selected, req)
		if err != nil {
			return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("real continued publication rollback: %w", err)
		}
		want := storetest.SelectedFanOutPublicationCounts{Events: 1, Deliveries: len(req.Commit.DeliveryRoutes), Lineages: 1, Continued: 1}
		if proof.Drafted != want || proof.AfterRollback != (storetest.SelectedFanOutPublicationCounts{}) {
			return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("partial continued publication escaped rollback: %+v", proof)
		}
		p.receipt <- proof
	}
	return p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
}

type selectedDeploymentSettlementExecutor struct {
	startupownership.FanOutExecutor
	observed *selectedDeploymentSettlementObservation
}

func (e *selectedDeploymentSettlementExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	return e.FanOutExecutor.ServeFanOutCandidate(ctx, selectedDeploymentSettlementOwner{FanOutObligationOwner: owner, observed: e.observed}, key)
}

type selectedDeploymentSettlementOwner struct {
	pipeline.FanOutObligationOwner
	observed *selectedDeploymentSettlementObservation
}

func (o selectedDeploymentSettlementOwner) BeginFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim, validate func(pipeline.FanOutEvaluationInput) error) (pipeline.FanOutEvaluationInput, runtimepipelineobligation.PublicationGroup, error) {
	input, group, err := o.FanOutObligationOwner.BeginFanOutEvaluation(ctx, claim, validate)
	if group == nil || err != nil {
		return input, group, err
	}
	o.observed.record(claim.Key.RunID, "")
	return input, selectedDeploymentSettlementGroup{PublicationGroup: group, observed: o.observed}, nil
}

type selectedDeploymentSettlementGroup struct {
	runtimepipelineobligation.PublicationGroup
	observed *selectedDeploymentSettlementObservation
}

func (g selectedDeploymentSettlementGroup) Settle(ctx context.Context, members []runtimepipelineobligation.PublicationSettlementMember) (runtimepipelineobligation.PublicationGroupOutcome, error) {
	outcome, err := g.PublicationGroup.Settle(ctx, members)
	for _, result := range outcome.Results {
		if result.Outcome.DeliveryHandoffCommitted() {
			g.observed.record("", result.Claim.EventID())
		}
	}
	return outcome, err
}

// The journal retains every witness; the coalesced signal only wakes its reader.
// Serving never waits for an observer to consume a channel.
type selectedDeploymentSettlementObservation struct {
	mu        sync.Mutex
	evaluated []string
	committed []string
	changed   chan struct{}
}

func newSelectedDeploymentSettlementObservation() *selectedDeploymentSettlementObservation {
	return &selectedDeploymentSettlementObservation{changed: make(chan struct{}, 1)}
}

func (o *selectedDeploymentSettlementObservation) record(runID, eventID string) {
	o.mu.Lock()
	if runID != "" {
		o.evaluated = append(o.evaluated, runID)
	}
	if eventID != "" {
		o.committed = append(o.committed, eventID)
	}
	o.mu.Unlock()
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

func (o *selectedDeploymentSettlementObservation) snapshot() (evaluated, committed []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.evaluated...), append([]string(nil), o.committed...)
}

func (o *selectedDeploymentSettlementObservation) waitCommitted(ctx context.Context, eventID string) error {
	for {
		_, committed := o.snapshot()
		for _, observed := range committed {
			if observed == eventID {
				return nil
			}
		}
		select {
		case <-o.changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
		for _, changedPin := range []bool{false, true} {
			name := backend + "/same_pin"
			if changedPin {
				name = backend + "/changed_pin"
			}
			t.Run(name, func(t *testing.T) {
				gate := &selectedDeploymentClaimGate{entered: make(chan string, 16)}
				gate.held.Store(true)
				observed := newSelectedDeploymentSettlementObservation()
				f := selectedDeploymentResourceFixtureConfigured(t, backend, "singleton", true, !changedPin, nil, func(store runtimedelivery.Store) runtimedelivery.Store {
					gate.Store = store
					return gate
				}, func(executor startupownership.FanOutExecutor) startupownership.FanOutExecutor {
					return &selectedDeploymentSettlementExecutor{FanOutExecutor: executor, observed: observed}
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
				wantVersion := selectedDeploymentVersion(t, f, oldRows)
				if changedPin {
					wantVersion = newVersion
				}
				sourceRunID := start("old", oldRows)
				pointEventID := waitDeploymentForkRowEvent(t, f.ctx, f.selected, sourceRunID, f.runtime.fanOutServing.Wake)
				waitCtx, cancelWait := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancelWait()
				if err := observed.waitCommitted(waitCtx, pointEventID); err != nil {
					t.Fatalf("R3b pipeline receipt/handoff did not commit at T: %v", err)
				}
				for matched := false; !matched; {
					select {
					case eventID := <-gate.entered:
						matched = eventID == pointEventID
					case <-waitCtx.Done():
						t.Fatalf("R3b receiver claim did not reach the held gate at T: %v", waitCtx.Err())
					}
				}
				deliveryID, deliveryStatus, handoffStamped, pipelineReceipts, attempts := selectedDeploymentPendingReceiverCut(t, f.ctx, f.selected, sourceRunID, pointEventID)
				if deliveryID == "" || deliveryStatus != "pending" || pipelineReceipts != 1 || !handoffStamped || attempts != 0 || gate.deferred.Load() == 0 {
					t.Fatalf("R3b requires settled pipeline, stamped handoff and unclaimed receiver at T: delivery=%q status=%q receipt=%d handoff=%t attempts=%d deferred=%d", deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts, gate.deferred.Load())
				}
				point := assertDeploymentForkPendingCandidate(t, f, runfork.RunForkPlanRequest{SourceRunID: sourceRunID}, pointEventID, deliveryID, runfork.RunForkDeploymentPendingReceiver)
				if point.Kind != runfork.RunForkPointDeploymentRevision {
					t.Fatalf("R3b did not select the post-handoff deployment revision: %+v", point)
				}
				t.Logf("R3b exact T: delivery=%s status=%s receipt=%d handoff=%t attempts=%d deferred_claims=%d", deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts, gate.deferred.Load())
				if changedPin {
					_ = start("new", newRows)
				}
				var rollback *selectedDeploymentRollbackPublication
				var replayOwner runforkexecution.SelectedContractReplayPersistence
				if !changedPin {
					canonical, ok := f.selected.(runforkexecution.SelectedContractReplayPersistence)
					if !ok {
						t.Fatal("selected store lacks its original publication owner")
					}
					rollback = &selectedDeploymentRollbackPublication{SelectedContractReplayPersistence: canonical, selected: f.selected,
						receipt: make(chan storetest.SelectedFanOutPublicationRollbackProof, 1)}
					replayOwner = rollback
				}
				owner, _ := deploymentForkOwnerWithReplay(t, f, nil, nil, replayOwner)
				forkServer := deploymentForkServer(t, f, owner)
				params := map[string]any{
					"source_run_id":       sourceRunID,
					"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
					"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
				}
				if changedPin {
					params["data_pin_overrides"] = []any{map[string]any{
						"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
						"version_id":  string(newVersion.VersionID),
					}}
				}
				result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
				if len(rpcErr) != 0 {
					t.Fatalf("fixed-T pending receiver fork: %s", rpcErr)
				}
				if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != wantVersion.VersionID {
					t.Fatalf("fixed-T fork did not bind the exact pin: %+v", result)
				}
				if result.ForkPointKind != string(runfork.RunForkPointDeploymentRevision) || result.ForkRevision != point.Revision || result.ForkEventID != "" {
					t.Fatalf("R3b fork changed its fixed post-handoff cut: %+v", result)
				}
				defer func() {
					if t.Failed() {
						logSelectedDeploymentForkFailure(t, f.selected, result.ForkRunID)
					}
				}()
				// Activation acknowledgment precedes retained publication. Observe
				// that publication before inspecting its final ordinal settlement.
				_ = waitDeploymentForkRowEvent(t, f.ctx, f.selected, result.ForkRunID, f.runtime.fanOutServing.Wake)
				drainDeadline := time.Now().Add(3 * time.Minute)
				drainCtx, stopDrain := context.WithDeadline(f.ctx, drainDeadline)
				defer stopDrain()
				gate.held.Store(false)
				f.runtime.bus.SignalDeliveryContinuations()
				waitNotifyAllChildrenRuntimeWithin(t, f.runtime, result.ForkRunID, time.Until(drainDeadline))
				if !changedPin {
					// Ordinal settlement does not join the authored follow-up that
					// closes this finite collector or its retained executor.
					for {
						state, err := storetest.ReadSelectedExecutionStorage(drainCtx, f.selected, result.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
						if state.State == "closed" && state.RunStatus == "completed" && state.Occurrences == 1 {
							break
						}
						select {
						case <-drainCtx.Done():
							t.Fatalf("finite continued receiver did not complete and join: state=%+v error=%v", state, drainCtx.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				page := selectedDeploymentPublicEvents(t, f.ctx, server, result.ForkRunID)
				wantRows := [][]byte{oldRows}
				if changedPin {
					wantRows = append(wantRows, newRows)
				}
				if len(page.Events) != len(wantRows) || page.NextCursor != "" {
					t.Fatalf("fixed-T fork has %d old/new events, cursor=%q: %+v", len(page.Events), page.NextCursor, page.Events)
				}
				want := map[string]map[string]any{}
				for _, row := range wantRows {
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
				if !seen["old"] || seen["new"] != changedPin {
					t.Fatalf("fixed-T fork failed to retain old pending and new pin rows: %+v", seen)
				}
				kind := "continued"
				if changedPin {
					kind = "committed"
				}
				correspondence, err := storetest.ReadSelectedFanOutOutcomeCorrespondence(f.ctx, f.selected,
					result.ForkRunID, kind, pointEventID, page.Events[0].EventID, sourceRunID)
				if err != nil {
					t.Fatal(err)
				}
				newFeedOutcomes, exactContinued := correspondence.Outcomes, correspondence.ExactContinued
				if newFeedOutcomes != 1 || exactContinued != map[bool]int{false: 1, true: 0}[changedPin] {
					t.Fatalf("fixed-T fork ordinal evidence: kind=%s outcomes=%d exact continued=%d", kind, newFeedOutcomes, exactContinued)
				}
				if !changedPin {
					select {
					case proof := <-rollback.receipt:
						t.Logf("real selected publication rollback: drafted=%+v after=%+v", proof.Drafted, proof.AfterRollback)
					default:
						t.Fatal("continued publication did not exercise its native rollback cut")
					}
					before := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID)
					if err := owner.RetireSelectedContexts(f.ctx); err != nil {
						t.Fatal(err)
					}
					old := f.runtime
					join := beginServingLifetimeJoin(old, nil)
					assertServingJoinComplete(t, join, old, nil)
					f.boot(t)
					recoveredOwner, recovery := deploymentForkOwner(t, f)
					terminalPreserved := false
					for _, entry := range recovery {
						if entry.RunID == result.ForkRunID {
							terminalPreserved = entry.Disposition == runfork.SelectedForkRecoveryTerminal && entry.Continuation == nil &&
								entry.Operation != nil && entry.Operation.ForkRunID == result.ForkRunID && entry.Operation.Status == runfork.ForkOperationActivated &&
								entry.Effects.PrelaunchTerminal == 0 && entry.Effects.OutcomeUncertain == 0 && len(entry.PendingCancellations) == 0 && len(entry.CanceledTurns) == 0
						}
					}
					if !terminalPreserved {
						t.Fatalf("settled continued feed recovered executable work: %+v", recovery)
					}
					restartedServer := deploymentForkServer(t, f, recoveredOwner)
					restartedReadServer := f.operatorServer(t)
					replay, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
					if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) {
						t.Fatalf("continued feed restart changed durable fork result: first=%+v replay=%+v error=%s", result, replay, rpcErr)
					}
					if after := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID); after != before {
						t.Fatalf("continued feed restart changed its committed operation: before=%+v after=%+v", before, after)
					}
					if after := selectedDeploymentPublicEvents(t, f.ctx, restartedReadServer, result.ForkRunID); !reflect.DeepEqual(after, page) {
						t.Fatalf("continued feed restart reissued or changed publication/delivery evidence: before=%+v after=%+v", page, after)
					}
				}
			})
		}
	}
}

func selectedDeploymentPendingReceiverCut(t *testing.T, ctx context.Context, selected any, runID, eventID string) (string, string, bool, int, int) {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	rows := func(table string) []map[string]storetest.SelectedForkSnapshotCell {
		t.Helper()
		decoded, err := storetest.DecodeSelectedForkSnapshotRows(snapshot[table])
		if err != nil {
			t.Fatalf("R3b %s snapshot: %v", table, err)
		}
		return decoded
	}
	text := func(row map[string]storetest.SelectedForkSnapshotCell, column string) string {
		t.Helper()
		cell, found := row[column]
		if !found {
			t.Fatalf("R3b snapshot lacks column %s", column)
		}
		value, err := cell.Text()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var deliveries []map[string]storetest.SelectedForkSnapshotCell
	for _, row := range rows("event_deliveries") {
		if text(row, "run_id") == runID && text(row, "event_id") == eventID {
			deliveries = append(deliveries, row)
		}
	}
	if len(deliveries) != 1 {
		t.Fatalf("R3b dispatch requires exactly one persisted receiver delivery, got %d", len(deliveries))
	}
	delivery := deliveries[0]
	deliveryID, status := text(delivery, "delivery_id"), text(delivery, "status")
	handoff, found := delivery["continuation_handoff_at"]
	if !found {
		t.Fatal("R3b snapshot lacks the continuation handoff column")
	}
	receipts, attempts := 0, 0
	for _, row := range rows("event_receipts") {
		if text(row, "event_id") == eventID && text(row, "subscriber_type") == "platform" &&
			text(row, "subscriber_id") == "pipeline" && text(row, "outcome") == "success" {
			receipts++
		}
	}
	for _, row := range rows("event_delivery_attempts") {
		if text(row, "delivery_id") == deliveryID {
			attempts++
		}
	}
	return deliveryID, status, string(handoff.Value) != "null", receipts, attempts
}
