package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanOutEvaluationSingleAdmissionRealConsumersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, _ := newMixedExecutionFixture(t, backend, nil)
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{Delay: 300 * time.Millisecond, DelayScope: transactiontest.DelayAllCommits})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			before := f.snapshot(t)
			oldInput, err := f.owner.LoadFanOutEvaluation(f.ctx, f.claim)
			if err != nil {
				t.Fatal(err)
			}
			oldGroup, err := f.owner.BeginFanOutPublicationGroup(f.ctx, f.claim)
			if err != nil {
				t.Fatal(err)
			}
			if err := oldGroup.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			old := collector.Snapshot()
			oldAdmission := old.ByOperation[transactiontest.FanOutLoad]
			if oldAdmission.ReadCommits != 2 || old.Total.WriteCommits != 0 || oldAdmission.DelayedCommits != 2 || oldAdmission.InjectedDelay != 600*time.Millisecond || oldAdmission.Failed != 0 {
				t.Fatalf("original adjacent boundaries did not commit two reads: %+v", old)
			}
			input, group, err := f.owner.BeginFanOutEvaluation(f.ctx, f.claim, func(input pipeline.FanOutEvaluationInput) error {
				if !reflect.DeepEqual(input, oldInput) {
					return errors.New("composition changed immutable input")
				}
				return input.Validate(f.intent)
			})
			if err != nil || group == nil || !reflect.DeepEqual(input, oldInput) {
				t.Fatalf("single admission: input=%+v group=%v err=%v", input, group, err)
			}
			f.group = group
			current := collector.Snapshot()
			currentAdmission := current.ByOperation[transactiontest.FanOutLoad]
			if currentAdmission.ReadCommits != oldAdmission.ReadCommits+1 || current.Total.WriteCommits != 0 || currentAdmission.DelayedCommits != oldAdmission.DelayedCommits+1 || currentAdmission.InjectedDelay != oldAdmission.InjectedDelay+300*time.Millisecond || currentAdmission.Failed != 0 {
				t.Fatalf("composition did not remove exactly one delayed read: before=%+v after=%+v", old, current)
			}
			for class, active := range current.ActiveByClass {
				if class.Operation == transactiontest.FanOutLoad && active != 0 {
					t.Fatalf("returned admission retained its transaction: %+v", current)
				}
			}
			if err := collector.StopAndJoin(f.ctx); err != nil {
				t.Fatal(err)
			}
			if joined := collector.Snapshot(); joined.Active != 0 || joined.Total.CommitFailures != 0 || joined.Total.CleanupFailures != 0 || joined.Total.Failed != 0 {
				t.Fatalf("independent background reads did not join cleanly: %+v", joined)
			}
			f.unchanged(t, before)
			f.prepare(t)
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil || len(committed.Publications) != 4 || committed.Intent.Cursor != 4 {
				t.Fatalf("mixed commit: %+v %v", committed, err)
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			mixedAwaitDeliveries(t, f, 6)
			assertFanOutCursorAndOutcomeCount(t, f.ctx, f.db, f.seed, 4, 4)
			t.Log("same exact claim/input/group and four publications/six genuine consumers; admission reads 2 -> 1, injected admission cost 600ms -> 300ms")
		})
	}
}

func TestFanOutEvaluationHydrationIntervalFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"healthy", "expiry", "cancel", "validation_error", "retired_grant", "source_head", "stop"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				if cut == "expiry" {
					if _, err := f.owner.ReleaseFanOutClaim(f.ctx, f.claim); err != nil {
						t.Fatal(err)
					}
					key := f.claim.Key
					var found bool
					var err error
					f.intent, f.claim, found, err = f.owner.ClaimFanOutIntent(f.ctx, pipeline.FanOutClaimRequest{Owner: "hydration-expiry", BundleHash: f.seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: 200 * time.Millisecond})
					if err != nil || !found {
						t.Fatalf("short actual claim: %v %v", found, err)
					}
				}
				independent := errors.New("independent hydrated-input validation failure")
				var before map[string][]string
				input, group, err := f.owner.BeginFanOutEvaluation(ctx, f.claim, func(input pipeline.FanOutEvaluationInput) error {
					if err := input.Validate(f.intent); err != nil {
						return err
					}
					switch cut {
					case "expiry":
						if remaining := time.Until(f.claim.LeaseUntil.Add(10 * time.Millisecond)); remaining > 0 {
							time.Sleep(remaining)
						}
					case "cancel":
						cancel()
					case "retired_grant":
						if err := f.grant.Retire(f.ctx); err != nil {
							t.Fatal(err)
						}
					case "source_head":
						current, found, err := f.process.CurrentSourceSet(f.ctx)
						if err != nil || !found {
							t.Fatalf("current source set: %v %v", found, err)
						}
						next, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: f.seed.bundleHash}, {BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64)}}, nil)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := f.process.RestoreSourceSet(f.ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: next}); err != nil {
							t.Fatal(err)
						}
					case "stop":
						control := f.raw.(interface {
							StopRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
						})
						outcome, err := control.StopRunControlOutcome(f.ctx, runcontrol.TransitionRequest{RunID: f.seed.runID, Now: time.Now().UTC(), Reason: "hydration-cut", ControlledBy: "test"})
						if err != nil || !outcome.Acknowledged {
							t.Fatalf("stop: %+v %v", outcome, err)
						}
					}
					before = f.snapshot(t)
					if cut == "validation_error" {
						cancel()
						return independent
					}
					return nil
				})
				if before == nil {
					t.Fatalf("did not reach actual hydration cut: %v", err)
				}
				if group != nil {
					defer func() {
						if err := group.Close(f.ctx); err != nil {
							t.Error(err)
						}
					}()
				}
				switch cut {
				case "healthy":
					if err != nil || group == nil {
						t.Fatalf("healthy composition: %v %v", group, err)
					}
					if err := group.Seal(f.ctx, input.StartOrdinal+len(input.Items), nil); err != nil {
						t.Fatal(err)
					}
					committed, err := f.owner.CommitFanOutChunk(f.ctx, rejectedFanOutChunk(f.claim, 0, len(input.Items), time.Now().UTC()))
					if err != nil || committed.Intent.Cursor != 1 {
						t.Fatalf("healthy sealed range: %+v %v", committed, err)
					}
					assertFanOutCursorAndOutcomeCount(t, f.ctx, f.db, f.seed, 1, 1)
					return
				case "expiry":
					if group != nil || !errors.Is(err, fanoutobligation.ErrStaleClaim) {
						t.Fatalf("expiry opened a group: %v %v", group, err)
					}
				case "cancel":
					if group != nil || !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel opened a group: %v %v", group, err)
					}
				case "validation_error":
					var load *pipeline.FanOutEvaluationLoadError
					if group != nil || !errors.Is(err, independent) || !errors.As(err, &load) || load.Stage != "validate_evaluation" {
						t.Fatalf("cancellation hid independent validation: %v %v", group, err)
					}
				default:
					if err != nil || group == nil {
						t.Fatalf("authority cut did not reach group admission: %v %v", group, err)
					}
					if err := group.Seal(f.ctx, input.StartOrdinal+len(input.Items), nil); err != nil {
						t.Fatal(err)
					}
					committed, err := f.owner.CommitFanOutChunk(f.ctx, rejectedFanOutChunk(f.claim, 0, len(input.Items), time.Now().UTC()))
					intended := err != nil && ((cut == "stop" && errors.Is(err, fanoutobligation.ErrStaleClaim)) || (cut == "source_head" && strings.Contains(err.Error(), "fan-out grant no longer owns the current source-set head")) || (cut == "retired_grant" && strings.Contains(err.Error(), "grant is retired")))
					if !intended || len(committed.Publications) != 0 {
						t.Fatalf("hydration authority change admitted durable effects: %+v %v", committed, err)
					}
					t.Logf("sealed range reached exact %s refusal: %v", cut, err)
				}
				f.unchanged(t, before)
			})
		}
	}
}
