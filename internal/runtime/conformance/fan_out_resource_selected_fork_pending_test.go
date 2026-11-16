package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
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
	allow    func(events.Event) bool
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
		if proof.Drafted != want || proof.AfterRollback != (storetest.SelectedFanOutPublicationCounts{}) || proof.DraftedSettlement.Unsettled != 1 {
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
	if !g.held.Load() || authority.Kind() != kind || event.Type() != selectedDeploymentEvent || g.allow != nil && g.allow(event) {
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
		t.Run(backend+"/sixty_of_hundred", func(t *testing.T) { proveSixtyOfHundredSelectedFork(t, backend) })
	}
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
				start := func(rows []byte, expected durabledata.ExpectedHead) string {
					t.Helper()
					ref, err := durabledata.ParseDeclarationRef(".", selectedDeploymentEvent)
					if err != nil {
						t.Fatal(err)
					}
					return createDeploymentResourceFixtureRun(t, f, durabledata.RunCreationDataEnvelope{Imports: []durabledata.FusedImport{{SourceInvocationID: uuid.NewString(), Declaration: ref, ExpectedHead: expected, InputFormat: "jsonl", Input: rows}}})
				}
				oldRows := []byte("{\"account_id\":\"old\",\"document\":{\"version\":1}}\n")
				newRows := []byte("{\"account_id\":\"new\",\"document\":{\"version\":2}}\n")
				newVersion := selectedDeploymentVersion(t, f, newRows)
				oldVersion := selectedDeploymentVersion(t, f, oldRows)
				wantVersion := oldVersion
				if changedPin {
					wantVersion = newVersion
				}
				sourceRunID := start(oldRows, durabledata.AbsentHead())
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
				if point.Kind != runfork.RunForkPointStateRevision {
					t.Fatalf("R3b did not select the post-handoff deployment revision: %+v", point)
				}
				t.Logf("R3b exact T: delivery=%s status=%s receipt=%d handoff=%t attempts=%d deferred_claims=%d", deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts, gate.deferred.Load())
				if changedPin {
					pauseDeploymentFixtureRun(t, f, sourceRunID)
					_ = start(newRows, durabledata.VersionHead(oldVersion.VersionID))
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
					"source_run_id":   sourceRunID,
					"bundle_hash":     f.runtime.sourceArtifactFact.BundleHash(),
					"idempotency_key": uuid.NewString(),
				}
				if changedPin {
					params["data_pin_overrides"] = []any{map[string]any{
						"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
						"version_id":  string(newVersion.VersionID),
					}}
				}
				result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
				if len(rpcErr) != 0 {
					readAt := time.Now().UTC().Format(time.RFC3339Nano)
					select {
					case <-f.topology.capability.Done():
						terminal, available := f.topology.capability.TerminalResult()
						t.Logf("the capability's terminal state as read by the test: read_at=%s done_closed=true terminal_result=%+v terminal_result_available=%t; Done also closes on a clean release; read_at is when the test looked", readAt, terminal, available)
					default:
						t.Logf("the capability's terminal state as read by the test: read_at=%s done_closed=false; Done also closes on a clean release; read_at is when the test looked", readAt)
					}
					t.Fatalf("fixed-T pending receiver fork: %s", rpcErr)
				}
				if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != wantVersion.VersionID {
					t.Fatalf("fixed-T fork did not bind the exact pin: %+v", result)
				}
				if result.ForkPointKind != string(runfork.RunForkPointStateRevision) || result.ForkRevision != point.Revision || result.ForkEventID != "" {
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
				currentReader := f.selected.(interface {
					ReadFanOutSettlement(context.Context, string, time.Time) (fanoutobligation.RunSettlement, error)
				})
				drainDeadline := time.Now().Add(3 * time.Minute)
				drainCtx, stopDrain := context.WithDeadline(f.ctx, drainDeadline)
				defer stopDrain()
				gate.held.Store(false)
				f.runtime.bus.SignalDeliveryContinuations()
				waitNotifyAllChildrenRuntimeWithin(t, f.runtime, result.ForkRunID, time.Until(drainDeadline))
				if current, err := currentReader.ReadFanOutSettlement(f.ctx, result.ForkRunID, time.Now().UTC()); err != nil || current.BlocksCompletion() {
					t.Fatalf("continued/deployment settlement remains owed: %+v err=%v", current, err)
				}
				if !changedPin {
					// Ordinal settlement does not join the authored follow-up or the
					// selected executor retired by the live handoff.
					for {
						state, err := storetest.ReadSelectedExecutionStorage(drainCtx, f.selected, result.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
						if state.State == "closed" && state.RunStatus == "live" && state.Occurrences == 1 {
							break
						}
						select {
						case <-drainCtx.Done():
							t.Fatalf("continued receiver did not settle and join its selected owner: state=%+v error=%v", state, drainCtx.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				page := selectedDeploymentPublicEvents(t, f.ctx, f.operatorServer(t), result.ForkRunID)
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
							terminalPreserved = entry.Disposition == runfork.SelectedForkRecoveryControlOnly && entry.Continuation == nil &&
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

func proveSixtyOfHundredSelectedFork(t *testing.T, backend string) {
	t.Helper()
	gate := &selectedDeploymentClaimGate{entered: make(chan string, 128), allow: func(event events.Event) bool {
		var payload struct {
			AccountID string `json:"account_id"`
		}
		if err := json.Unmarshal(event.Payload(), &payload); err != nil {
			return false
		}
		return payload.AccountID < "item-060"
	}}
	gate.held.Store(true)
	t.Cleanup(func() { gate.held.Store(false) })
	f := selectedDeploymentResourceFixtureWithDelivery(t, backend, "root", nil, func(store runtimedelivery.Store) runtimedelivery.Store { gate.Store = store; return gate }, nil)
	var rows bytes.Buffer
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&rows, "{\"account_id\":\"item-%03d\",\"document\":{\"ordinal\":%d}}\n", i, i)
	}
	ref, err := durabledata.ParseDeclarationRef(".", selectedDeploymentEvent)
	if err != nil {
		t.Fatal(err)
	}
	runID := createDeploymentResourceFixtureRun(t, f, durabledata.RunCreationDataEnvelope{Imports: []durabledata.FusedImport{{SourceInvocationID: uuid.NewString(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: rows.Bytes()}}})
	server := f.operatorServer(t)
	deadline := time.Now().Add(20 * time.Second)
	var atCut operatorread.OperatorEventListResult
	for {
		atCut = selectedDeploymentPublicEvents(t, f.ctx, server, runID)
		delivered, pending := 0, 0
		for _, event := range atCut.Events {
			if len(event.Deliveries) != 1 {
				continue
			}
			switch event.Deliveries[0].Status {
			case "delivered":
				delivered++
			case "pending":
				pending++
			}
		}
		if len(atCut.Events) == 100 && delivered == 60 && pending == 40 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("real 60/100 cut did not form: issued=%d delivered=%d pending=%d", len(atCut.Events), delivered, pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atCut.NextCursor != "" {
		t.Fatal("hundred-item cut omitted retained rows")
	}
	planner := f.selected.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	pauseDeploymentFixtureRun(t, f, runID)
	pausePlan, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := deploymentForkOwner(t, f)
	forkServer := deploymentForkServer(t, f, owner)
	// The snapshot precedes the final plan read. A revision that lands between
	// them has no classified facts and fails closed rather than moving the cut.
	standing := selectedDeploymentPauseRevisionFacts(t, f, runID)
	plan, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedDeploymentPauseRevisionProvenance(standing, pausePlan.ForkPoint.Revision, plan.ForkPoint.Revision, atCut); err != nil {
		t.Fatal(err)
	}
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatal(err)
	}
	var owed int
	for _, obligation := range plan.FanOutObligations {
		owed += len(obligation.PendingDeployment)
	}
	if owed != 40 {
		t.Fatalf("fixed cut owes %d recipients, want40", owed)
	}
	params := map[string]any{"source_run_id": runID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(), "idempotency_key": uuid.NewString()}
	result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
	version := selectedDeploymentVersion(t, f, rows.Bytes())
	if len(rpcErr) != 0 || result.ForkRunID == "" || result.ForkRevision != plan.ForkPoint.Revision || len(result.DataPins) != 1 || result.DataPins[0].VersionID != version.VersionID {
		t.Fatalf("60/100 fork lost exact native cut/version: %+v expected_revision=%d expected_version=%s error=%s", result, plan.ForkPoint.Revision, version.VersionID, rpcErr)
	}
	gate.held.Store(false)
	f.runtime.bus.SignalDeliveryContinuations()
	waitNotifyAllChildrenRuntimeWithin(t, f.runtime, result.ForkRunID, 3*time.Minute)
	child := selectedDeploymentPublicEvents(t, f.ctx, server, result.ForkRunID)
	if child.NextCursor != "" || len(child.Events) != 40 {
		t.Fatalf("60/100 replayed settled prefix or lost suffix: %+v", child)
	}
	seen := map[string]bool{}
	for _, event := range child.Events {
		key, ok := event.Payload["account_id"].(string)
		if !ok || key < "item-060" || key > "item-099" || seen[key] || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || len(event.DeadLetters) != 0 {
			t.Fatalf("wrong continued suffix row: %+v", event)
		}
		seen[key] = true
	}
	summary, err := f.selected.(interface {
		FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error)
	}).FanOutRunSummary(f.ctx, result.ForkRunID, time.Now().UTC())
	if err != nil || summary.Cardinality != 100 || summary.Cursor != 100 || summary.Settled != 100 || summary.Unsettled != 0 || summary.Owed != 0 {
		t.Fatalf("60 retained +40 executed did not settle all100: %+v %v", summary, err)
	}
	if after := selectedDeploymentPublicEvents(t, f.ctx, server, runID); !reflect.DeepEqual(atCut, after) {
		t.Fatal("fork continuation reexecuted or changed its paused source")
	}
	before := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID)
	replay, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
	if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) || before != selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID) {
		t.Fatalf("60/100 exact operation replay mutated evidence: %+v %s", replay, rpcErr)
	}
}

type selectedDeploymentRevisionFact struct {
	family, key string
	present     bool
	value       map[string]any
}

func selectedDeploymentPauseRevisionFacts(t *testing.T, f *deploymentResourceFixture, runID string) map[int64][]selectedDeploymentRevisionFact {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.selected)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot["run_fork_fact_revisions"])
	if err != nil {
		t.Fatal(err)
	}
	readText := func(row map[string]storetest.SelectedForkSnapshotCell, column string) string {
		t.Helper()
		cell, ok := row[column]
		if !ok {
			t.Fatalf("revision fact lacks %s", column)
		}
		value, err := cell.Text()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	facts := make(map[int64][]selectedDeploymentRevisionFact)
	for _, row := range rows {
		if readText(row, "run_id") != runID {
			continue
		}
		var revision int64
		if err := json.Unmarshal(row["revision"].Value, &revision); err != nil {
			t.Fatal(err)
		}
		present, err := selectedDeploymentPresentPhysical(row["present"])
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		if err := json.Unmarshal([]byte(readText(row, "fact")), &value); err != nil {
			t.Fatalf("revision %d has unreadable fact kind: %v", revision, err)
		}
		facts[revision] = append(facts[revision], selectedDeploymentRevisionFact{
			family: readText(row, "family"), key: readText(row, "fact_key"), present: present, value: value,
		})
	}
	return facts
}

func selectedDeploymentPresentPhysical(cell storetest.SelectedForkSnapshotCell) (bool, error) {
	switch cell.Type {
	case "bool":
		var present bool
		if err := json.Unmarshal(cell.Value, &present); err != nil {
			return false, err
		}
		return present, nil
	case "int64":
		var present int64
		if err := json.Unmarshal(cell.Value, &present); err != nil {
			return false, err
		}
		if present != 0 && present != 1 {
			return false, fmt.Errorf("physical present value %d is not 0 or 1", present)
		}
		return present == 1, nil
	default:
		return false, fmt.Errorf("physical present has unsupported type %q", cell.Type)
	}
}

func TestSelectedDeploymentPresentPhysical(t *testing.T) {
	for _, test := range []struct {
		name, physicalType, value string
		want                      bool
		wantError                 bool
	}{
		{name: "bool_true", physicalType: "bool", value: "true", want: true},
		{name: "bool_false", physicalType: "bool", value: "false"},
		{name: "sqlite_true", physicalType: "int64", value: "1", want: true},
		{name: "sqlite_false", physicalType: "int64", value: "0"},
		{name: "non_boolean_integer", physicalType: "int64", value: "2", wantError: true},
		{name: "wrong_physical_type", physicalType: "string", value: `"1"`, wantError: true},
		{name: "wrong_boolean_value", physicalType: "bool", value: "1", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectedDeploymentPresentPhysical(storetest.SelectedForkSnapshotCell{
				Column: "present", Type: test.physicalType, Value: json.RawMessage(test.value),
			})
			if (err != nil) != test.wantError || (err == nil && got != test.want) {
				t.Fatalf("physical present = %t, %v, want %t, error=%t", got, err, test.want, test.wantError)
			}
		})
	}
}

func selectedDeploymentPauseRevisionProvenance(facts map[int64][]selectedDeploymentRevisionFact, paused, planned int64, cut operatorread.OperatorEventListResult) error {
	if planned < paused {
		return fmt.Errorf("paused source revision regressed: pause=%d plan=%d", paused, planned)
	}
	eventsAtCut := make(map[string]operatorread.OperatorEventFull, len(cut.Events))
	deliveriesAtCut := make(map[string]string, len(cut.Events))
	for _, event := range cut.Events {
		if len(event.Deliveries) != 1 {
			return fmt.Errorf("issued event %s lost its sole receiver", event.EventID)
		}
		eventsAtCut[event.EventID] = event
		deliveriesAtCut[event.Deliveries[0].DeliveryID] = event.EventID
	}
	priorFacts := map[string]selectedDeploymentRevisionFact{}
	knownFeeds := map[string]bool{}
	for revision := int64(1); revision <= paused; revision++ {
		recorded := facts[revision]
		for _, fact := range recorded {
			priorFacts[fact.family+"|"+fact.key] = fact
			if fact.present && fact.family == "fan_out_obligations" && fact.value["fact_kind"] == "intent" && fact.value["origin_kind"] == "deployment" {
				if feedID, ok := fact.value["deployment_feed_id"].(string); ok && feedID != "" {
					knownFeeds[feedID] = true
				}
			}
		}
	}
	for revision := paused + 1; revision <= planned; revision++ {
		recorded := facts[revision]
		if len(recorded) == 0 {
			return fmt.Errorf("paused source revision %d has no readable provenance (pause=%d plan=%d)", revision, paused, planned)
		}
		for _, fact := range recorded {
			kind := ""
			if fact.value != nil {
				kind, _ = fact.value["fact_kind"].(string)
			}
			allowed := false
			if fact.present {
				switch fact.family {
				case "event_deliveries":
					eventID, _ := fact.value["event_id"].(string)
					deliveryID, _ := fact.value["delivery_id"].(string)
					atCut, issued := eventsAtCut[eventID]
					status, _ := fact.value["status"].(string)
					allowed = issued && deliveriesAtCut[deliveryID] == eventID &&
						(status == atCut.Deliveries[0].Status || atCut.Deliveries[0].StartedAt != nil && status == "delivered")
				case "event_receipts":
					eventID, _ := fact.value["event_id"].(string)
					atCut, issued := eventsAtCut[eventID]
					allowed = issued && (atCut.Deliveries[0].Status == "delivered" || atCut.Deliveries[0].StartedAt != nil) &&
						fact.value["subscriber_type"] == "platform" && fact.value["subscriber_id"] == "pipeline" && fact.value["outcome"] == "success"
				case "fan_out_obligations":
					prior, known := priorFacts[fact.family+"|"+fact.key]
					switch kind {
					case "intent":
						allowed = known && prior.present && prior.value["fact_kind"] == "intent" &&
							fact.value["origin_kind"] == "deployment" && fact.value["deployment_feed_id"] == prior.value["deployment_feed_id"] &&
							fact.value["source_resource_version_id"] == prior.value["source_resource_version_id"] &&
							fact.value["cardinality"] == float64(100) && fact.value["cursor"] == float64(100)
					case "outcome":
						// The original event and its outcome commit together. A later
						// projection may repeat that fact; it cannot add an outcome.
						eventID, _ := fact.value["event_id"].(string)
						atCut, issued := eventsAtCut[eventID]
						feedID, _ := fact.value["deployment_feed_id"].(string)
						ordinal, ordinalOK := fact.value["ordinal"].(float64)
						allowed = known && prior.present && reflect.DeepEqual(prior.value, fact.value) && issued && knownFeeds[feedID] &&
							fact.value["origin_kind"] == "deployment" && fact.value["outcome_source_event_id"] == nil &&
							fact.value["outcome_kind"] == "committed" && ordinalOK && ordinal >= 0 && ordinal < 100 &&
							atCut.Payload["account_id"] == fmt.Sprintf("item-%03d", int(ordinal))
					}
				}
			}
			if !allowed {
				return fmt.Errorf("paused source advanced by unproved revision %d family=%s key=%s kind=%s present=%t fact=%v (pause=%d plan=%d)", revision, fact.family, fact.key, kind, fact.present, fact.value, paused, planned)
			}
			priorFacts[fact.family+"|"+fact.key] = fact
		}
	}
	return nil
}

func TestSixtyOfHundredPauseRevisionProvenance(t *testing.T) {
	cut := operatorread.OperatorEventListResult{Events: []operatorread.OperatorEventFull{{
		EventID: "issued-event", Payload: map[string]any{"account_id": "item-000"},
		Deliveries: []operatorread.OperatorEventDelivery{{DeliveryID: "issued-delivery", Status: "delivered"}},
	}}}
	intent := selectedDeploymentRevisionFact{family: "fan_out_obligations", key: "intent", present: true, value: map[string]any{
		"fact_kind": "intent", "origin_kind": "deployment", "deployment_feed_id": "original-feed",
		"cardinality": float64(100), "cursor": float64(100), "source_resource_version_id": "original-version",
	}}
	outcome := selectedDeploymentRevisionFact{family: "fan_out_obligations", key: "outcome-0", present: true, value: map[string]any{
		"fact_kind": "outcome", "origin_kind": "deployment", "deployment_feed_id": "original-feed",
		"event_id": "issued-event", "outcome_source_event_id": nil, "ordinal": float64(0), "outcome_kind": "committed",
	}}
	for _, test := range []struct {
		name      string
		facts     map[int64][]selectedDeploymentRevisionFact
		wantError bool
	}{
		{"same committed original outcome recaptured", map[int64][]selectedDeploymentRevisionFact{1: {intent, outcome}, 2: {outcome}}, false},
		{"new outcome after pause", map[int64][]selectedDeploymentRevisionFact{1: {intent}, 2: {outcome}}, true},
		{"new event after pause", map[int64][]selectedDeploymentRevisionFact{1: {intent, outcome}, 2: {{family: "events", key: "new-event", present: true, value: map[string]any{"event_id": "new-event"}}}}, true},
		{"unreadable successor", map[int64][]selectedDeploymentRevisionFact{1: {intent, outcome}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := selectedDeploymentPauseRevisionProvenance(test.facts, 1, 2, cut)
			if (err != nil) != test.wantError {
				t.Fatalf("revision classification error=%v, wantError=%t", err, test.wantError)
			}
		})
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
