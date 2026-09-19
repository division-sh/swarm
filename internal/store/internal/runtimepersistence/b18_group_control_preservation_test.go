package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
)

func TestB18PausedPreparedGroupRetainsAcceptedWorkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			raw, db, _ := newP16RaceStore(t, backend)
			fixture, ctx, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, backend, 34)
			control := raw.(runcontrol.Store)
			request := runcontrol.TransitionRequest{RunID: fixture.runID, Reason: "b18-real-pause", ControlledBy: "conformance"}
			if _, err := control.PauseRunControl(ctx, request); err != nil {
				t.Fatal(err)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, command.Claim); err != nil {
				t.Fatalf("pause revoked accepted evaluation: %v", err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
				t.Fatalf("pause revoked prepared group publication: %v", err)
			}
			if err := group.ValidateCommitted(ctx, b16Claims(members)); err != nil {
				t.Fatal(err)
			}
			out, err := group.Settle(ctx, members)
			requireB18Acknowledged(t, out, err, members)
			paused := readB16Snapshot(t, db)
			_, _, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "b18-paused-new-turn", BundleHash: fixture.bundleHash, Candidate: &command.Claim.Key, Now: time.Now().UTC(), Lease: time.Minute,
			})
			if err != nil || found {
				t.Fatalf("pause admitted new suffix work: found=%v err=%v", found, err)
			}
			requireB16Snapshot(t, db, paused)
			// Stop is a different transition: after the accepted handoff it
			// cancels only the remaining suffix, not the successful receipts.
			if _, err := control.StopRunControl(ctx, request); err != nil {
				t.Fatal(err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 32, 32)
			for _, member := range members {
				assertDirectiveReceipt(t, db, member.Claim.EventID(), "processed", nil)
			}
			after := readStopCommitEvidence(t, db, fixture.runID)
			if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 {
				t.Fatalf("stop after paused accepted group: %+v", after)
			}
		})
	}
}

// Pause through the real controller while the first routed handler is already
// accepted. Its acknowledgement survives; later group members remain queued.
func TestB18RealPauseInterruptsGroupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			handlers := lifecycleprobe.New()
			f, seen, coordinator := newMixedExecutionFixture(t, backend, handlers)
			controller := runcontrol.NewController(f.raw.(runcontrol.Store), f.bus, runcontrol.Options{})
			f.bus.SetRunDispatchGate(controller)
			var firstCalls atomic.Int32
			pauseResult := make(chan runcontrol.TransitionResult, 1)
			coordinator.SetTestWorkflowNodeHandlerStartHook(func(ctx context.Context, _ string, event events.Event) error {
				if event.ID() != f.events[0].ID() {
					return nil
				}
				if firstCalls.Add(1) != 1 {
					return errors.New("pause replayed acknowledged prefix handler")
				}
				result, err := controller.Pause(ctx, runcontrol.TransitionRequest{RunID: f.seed.runID, Reason: "b18-between-members"})
				pauseResult <- result
				return err
			})
			f.prepare(t)
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			observed := &mixedExecutionGroup{PublicationGroup: f.group}
			if err := f.bus.DispatchFanOutPublications(f.ctx, observed, committed.Publications); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-pauseResult:
				if result.RunID != f.seed.runID || result.Status != runcontrol.StatusPaused {
					t.Fatalf("actual pause result=%+v", result)
				}
			default:
				t.Fatal("real first handler never paused the run")
			}
			observed.assertAcknowledged(t, f.claims[:1])
			mixedAssertReceipt(t, f, f.events[0].ID(), "success", "pipeline_persisted")
			reader := f.raw.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			prefix, err := reader.LoadOperatorEvent(f.ctx, f.events[0].ID())
			if err != nil || len(prefix.Deliveries) != 1 || !prefix.Deliveries[0].Terminal || prefix.Deliveries[0].Status != "delivered" {
				t.Fatalf("accepted prefix did not complete through pause: %+v err=%v", prefix, err)
			}
			for _, event := range f.events[1:] {
				var receipts int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event.ID()).Scan(&receipts); err != nil || receipts != 0 {
					t.Fatalf("paused suffix acquired fabricated acknowledgement: %s count=%d err=%v", event.ID(), receipts, err)
				}
				view, err := reader.LoadOperatorEvent(f.ctx, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				for _, delivery := range view.Deliveries {
					if delivery.Terminal || delivery.StartedAt != nil {
						t.Fatalf("paused suffix dispatched: %+v", delivery)
					}
				}
			}
			select {
			case event := <-seen:
				t.Fatalf("paused agent suffix executed %s", event.ID())
			default:
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			continued, err := controller.Continue(f.ctx, runcontrol.TransitionRequest{RunID: f.seed.runID})
			if err != nil || continued.Status != runcontrol.StatusRunning || continued.Recovery.Err != nil || !continued.Recovery.Sweep.Exhausted {
				t.Fatalf("real Continue recovery=%+v err=%v", continued, err)
			}
			mixedAwaitDeliveries(t, f, 6)
			for _, event := range f.events {
				mixedAssertReceipt(t, f, event.ID(), "success", "pipeline_persisted")
				if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, []engine.EmitIntent{{Event: event}}); err != nil {
					t.Fatal(err)
				}
			}
			after, err := reader.LoadOperatorEvent(f.ctx, f.events[0].ID())
			if err != nil || !reflect.DeepEqual(prefix, after) || firstCalls.Load() != 1 {
				t.Fatalf("Continue rewrote/replayed acknowledged prefix: calls=%d err=%v", firstCalls.Load(), err)
			}
			got := map[string]int{}
			for i := 0; i < 2; i++ {
				select {
				case event := <-seen:
					got[event.ID()]++
				case <-time.After(5 * time.Second):
					t.Fatal("Continue did not execute both actual agent recipients")
				}
			}
			if got[f.events[1].ID()] != 1 || got[f.events[2].ID()] != 1 || len(got) != 2 {
				t.Fatalf("actual recovered agent recipients=%v", got)
			}
			t.Log("B07/B18 real Pause during accepted first handler: one acknowledged prefix, three unacknowledged queued members, real Continue/queue/continuation convergence to six delivered recipients, no prefix replay")
		})
	}
}

func TestB18RetiredGenerationPreservesAcknowledgedGroupPrefixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			members := f.members()
			out, err := f.group.Settle(f.ctx, members[:1])
			requireB18Acknowledged(t, out, err, members[:1])
			before := f.snapshot(t)
			if err := f.grant.Retire(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
				t.Fatalf("retirement lost immutable committed membership: %v", err)
			}
			out, err = f.group.Settle(f.ctx, members[1:])
			if err == nil || !strings.Contains(err.Error(), "run execution generation grant is no longer current") || len(out.Results) != 0 {
				t.Fatalf("retired generation settlement=%+v err=%v", out, err)
			}
			read, err := f.group.ReadPublicationSettlement(f.ctx, members)
			if err != nil || len(read.Rows) != 2 || read.Rows[0].State != pipelineobligation.PublicationSettlementSatisfied || read.Rows[1].State != pipelineobligation.PublicationSettlementPending {
				t.Fatalf("retirement lost acknowledged prefix or invented suffix: %+v err=%v", read, err)
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			for _, old := range f.claims {
				successor, err := f.store().ClaimPublication(f.ctx, old.EventID())
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store().Release(f.ctx, old); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("retired group claim reached successor: %v", err)
				}
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				if err := f.store().Release(f.ctx, successor); err != nil {
					t.Fatalf("old group cleanup stole successor: %v", err)
				}
			}
			f.unchanged(t, before)
		})
	}
}

func TestB18CommittedGroupSurvivesEnumerationLeaseExpiryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			// Replace only the unused fixture claim through its real owners, so
			// expiry is a real admitted lease, never a forged caller timestamp.
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.owner.ReleaseFanOutClaim(f.ctx, f.claim); err != nil {
				t.Fatal(err)
			}
			key := f.claim.Key
			intent, claim, found, err := f.owner.ClaimFanOutIntent(f.ctx, pipeline.FanOutClaimRequest{
				Owner: "b18-committed-expiry", BundleHash: f.seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: 2 * time.Second,
			})
			if err != nil || !found {
				t.Fatalf("short actual lease: found=%v err=%v", found, err)
			}
			f.intent, f.claim = intent, claim
			f.command = pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
			f.group, err = f.owner.BeginFanOutPublicationGroup(f.ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			if delay := time.Until(claim.LeaseUntil.Add(20 * time.Millisecond)); delay > 0 {
				time.Sleep(delay)
			}
			if err := f.group.ValidateCommitted(f.ctx, f.claims); err != nil {
				t.Fatalf("enumeration expiry revoked committed publication authority: %v", err)
			}
			members := f.members()
			out, err := f.group.Settle(f.ctx, members)
			requireB18Acknowledged(t, out, err, members)
			before := f.snapshot(t)
			if _, err := f.group.Settle(f.ctx, members); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
				t.Fatalf("expired consumed group regained authority: %v", err)
			}
			f.unchanged(t, before)
		})
	}
}

func requireB18Acknowledged(t *testing.T, out pipelineobligation.PublicationGroupOutcome, err error, members []pipelineobligation.PublicationSettlementMember) {
	t.Helper()
	if err != nil || len(out.Results) != len(members) {
		t.Fatalf("actual group acknowledgement=%+v err=%v", out, err)
	}
	for i, result := range out.Results {
		if result.Claim != members[i].Claim || !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
			t.Fatalf("missing exact acknowledged group handoff: %+v", result)
		}
	}
}
