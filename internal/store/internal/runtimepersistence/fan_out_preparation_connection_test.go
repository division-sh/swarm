package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type preparationReadGate struct {
	owner interface {
		RequireActiveRun(context.Context, string) error
	}
	runID string
	calls int
}

func (g *preparationReadGate) QueueableRunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	g.calls++
	if runID != g.runID || correlation.RunIDFromContext(ctx) != runID {
		return false, errors.New("dispatch gate lost the original member run")
	}
	return false, g.owner.RequireActiveRun(ctx, runID)
}

func (*preparationReadGate) QueueableRunDispatchParked(context.Context, string) (bool, error) {
	return false, nil
}

func TestFanOutCompletePreparationUsesItsOneAvailableConnectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, found := range []bool{false, true} {
			name := "absent"
			if found {
				name = "found"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				if found {
					if err := f.bus.Publish(f.ctx, f.events[0]); err != nil {
						t.Fatal(err)
					}
				}
				before := f.snapshot(t)
				gate := &preparationReadGate{owner: f.raw.(interface {
					RequireActiveRun(context.Context, string) error
				}), runID: f.seed.runID}
				f.bus.SetRunDispatchGate(gate)
				f.db.SetMaxOpenConns(4)
				var holders []*sql.Conn
				for f.db.Stats().InUse < 3 {
					conn, err := f.db.Conn(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					holders = append(holders, conn)
				}
				release := func() {
					for _, c := range holders {
						if err := c.Close(); err != nil {
							t.Error(err)
						}
					}
					holders = nil
				}
				t.Cleanup(release)
				entered, resume := make(chan struct{}), make(chan struct{})
				var once, resumeOnce sync.Once
				resumeRead := func() { resumeOnce.Do(func() { close(resume) }) }
				var deliveries int
				f.probe.set(func(phase, query string) error {
					if phase == "before_read_begin" {
						var pcs [32]uintptr
						n := runtime.Callers(2, pcs[:])
						frames := runtime.CallersFrames(pcs[:n])
						for {
							frame, more := frames.Next()
							if strings.Contains(frame.Function, "publicationGroup).WithFlowInstancePlanningSnapshot") {
								once.Do(func() { close(entered); <-resume })
								break
							}
							if !more {
								break
							}
						}
					}
					if phase == "before_query" && strings.Contains(strings.ToLower(query), "from event_deliveries") {
						deliveries++
					}
					return nil
				})
				type result struct {
					prepared []pipeline.FanOutPublicationPreparation
					err      error
				}
				done := make(chan result, 1)
				var r result
				completed := false
				waits := f.db.Stats().WaitCount
				go func() {
					prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, []pipeline.FanOutPublicationRequest{{Ordinal: 0, Intent: engine.EmitIntent{Event: f.events[0]}}})
					done <- result{prepared, err}
				}()
				defer func() {
					resumeRead()
					release()
					if !completed {
						<-done
					}
				}()
				select {
				case <-entered:
				case r = <-done:
					completed = true
					t.Fatalf("preparation ended before its read cut: %+v", r)
				case <-f.ctx.Done():
					t.Fatal(f.ctx.Err())
				}
				for f.db.Stats().InUse < 4 {
					conn, err := f.db.Conn(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					holders = append(holders, conn)
				}
				f.db.SetMaxOpenConns(4)
				if n := f.db.Stats().InUse; n != 4 {
					t.Fatalf("controlled occupancy=%d, want four", n)
				}
				resumeRead()
				deadline := time.Now().Add(5 * time.Second)
				for !completed && f.db.Stats().WaitCount == waits && time.Now().Before(deadline) {
					select {
					case r = <-done:
						completed = true
					default:
						runtime.Gosched()
					}
				}
				nested := f.db.Stats().WaitCount - waits
				release()
				if !completed {
					select {
					case r = <-done:
						completed = true
					case <-f.ctx.Done():
						t.Fatal(f.ctx.Err())
					}
				}
				f.probe.set(nil)
				if nested != 0 {
					t.Errorf("complete preparation acquired a second connection while holding its cut: wait delta=%d", nested)
				}
				if r.err != nil || len(r.prepared) != 1 || r.prepared[0].Err != nil || r.prepared[0].Publication == nil {
					t.Fatalf("preparation=%+v err=%v", r.prepared, r.err)
				}
				if found && deliveries == 0 {
					t.Fatal("found prepared publication did not exercise delivery snapshot read")
				}
				if !found && gate.calls != 1 {
					t.Fatalf("fresh dispatch gate calls=%d, want one", gate.calls)
				}
				if found && gate.calls != 0 {
					t.Fatalf("stored duplicate gained a new dispatch-gate decision: calls=%d", gate.calls)
				}
				if err := f.bus.ReleaseEnginePublications(context.Background(), []engine.DurablePublicationPlan{r.prepared[0].Publication}); err != nil {
					t.Fatal(err)
				}
				if err := f.group.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.unchanged(t, before)
			})
		}
	}
}

type preparationMetadata interface {
	bus.PreparedPublishEventReader
	bus.ActiveAgentDescriptorLister
}

func TestFanOutPreparationMetadataLifetimeAndCoordinatesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 1)
			_, bundle := fanOutMixedRouteSource(t)
			planning := f.group.(interface {
				WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
			})
			before := f.snapshot(t)
			var retained preparationMetadata
			if err := planning.WithFlowInstancePlanningSnapshot(f.ctx, semanticview.Wrap(bundle), func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				retained = reader.(preparationMetadata)
				if _, found, err := retained.LoadPreparedPublishEvent(ctx, f.events[0].ID()); err != nil || found {
					t.Fatalf("absent publication=%v err=%v", found, err)
				}
				if _, err := retained.ListActiveAgentDescriptors(ctx, f.seed.runID); err != nil {
					return err
				}
				errorsRead := make(chan error, 4)
				for i := 0; i < cap(errorsRead); i++ {
					go func() {
						_, _, err := retained.LoadPreparedPublishEvent(ctx, f.events[0].ID())
						errorsRead <- err
					}()
				}
				var joined error
				for i := 0; i < cap(errorsRead); i++ {
					joined = errors.Join(joined, <-errorsRead)
				}
				if joined != nil {
					return joined
				}
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if _, _, err := retained.LoadPreparedPublishEvent(canceled, f.events[0].ID()); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation=%v", err)
				}
				wrongRun := correlation.WithRunID(ctx, uuid.NewString())
				if _, _, err := retained.LoadPreparedPublishEvent(wrongRun, f.events[0].ID()); err == nil {
					t.Fatal("foreign context run admitted")
				}
				if _, err := retained.ListActiveAgentDescriptors(ctx, uuid.NewString()); err == nil {
					t.Fatal("foreign requested run admitted")
				}
				wrongSource := correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact("bundle-v2:sha256:"+strings.Repeat("f", 64)))
				if _, _, err := retained.LoadPreparedPublishEvent(wrongSource, f.events[0].ID()); err == nil {
					t.Fatal("foreign source admitted")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := retained.LoadPreparedPublishEvent(f.ctx, f.events[0].ID()); err == nil {
				t.Fatal("prepared-event reader escaped its lifetime")
			}
			if _, err := retained.ListActiveAgentDescriptors(f.ctx, f.seed.runID); err == nil {
				t.Fatal("agent reader escaped its lifetime")
			}
			f.unchanged(t, before)
		})
	}
}

func TestFanOutPreparationConflictingStoredEventBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 1)
			if err := f.bus.Publish(f.ctx, f.events[0]); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			event := f.events[0]
			target := event.SourceRoute()
			target.EntityID = uuid.NewString()
			hostile := eventtest.ChildForProducerWithRoutingSource(event.ID(), event.Type(), event.Producer(), event.TaskID(), event.Payload(), event.ChainDepth(), f.intent.Request.Capsule.Lineage, events.EnvelopeForTargetRoute(event.Envelope(), target), event.RoutingSource(), event.CreatedAt())
			read := false
			f.probe.set(func(phase, query string) error {
				if phase == "before_query" && strings.Contains(strings.ToLower(query), "from event_deliveries") {
					read = true
				}
				return nil
			})
			prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, []pipeline.FanOutPublicationRequest{{Ordinal: 0, Intent: engine.EmitIntent{Event: hostile}}})
			f.probe.set(nil)
			if !read || err != nil || len(prepared) != 1 || prepared[0].Err == nil || prepared[0].Publication != nil || !strings.Contains(prepared[0].Err.Error(), "durable event route facts conflict with the admitted event target") {
				t.Fatalf("conflicting stored event admitted: %+v err=%v", prepared, err)
			}
			if err := f.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.unchanged(t, before)
		})
	}
}

func TestFanOutPreparationAgentReadFailureBeforeEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, stage := range []string{"preparation", "commit"} {
			t.Run(backend+"/"+stage, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				_, bundle := fanOutMixedRouteSource(t)
				planning := f.group.(interface {
					WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
				})
				fault := errors.New("agent metadata owner read failure")
				observed := false
				arm := func() {
					f.probe.set(func(phase, query string) error {
						if phase == "before_query" && strings.Contains(strings.ToLower(query), "from agents") {
							observed = true
							return fault
						}
						return nil
					})
				}
				if stage == "preparation" {
					arm()
				}
				err := planning.WithFlowInstancePlanningSnapshot(f.ctx, semanticview.Wrap(bundle), func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
					_, err := reader.(preparationMetadata).ListActiveAgentDescriptors(ctx, f.seed.runID)
					return err
				})
				if stage == "commit" {
					if err != nil {
						t.Fatal(err)
					}
					f.prepare(t)
					f.seal(t)
					arm()
				}
				before := f.snapshot(t)
				if stage == "commit" {
					committed, commitErr := f.owner.CommitFanOutChunk(f.ctx, f.command)
					err = commitErr
					if len(committed.Publications) != 0 {
						t.Fatal("failed metadata read leaked publications")
					}
				}
				f.probe.set(nil)
				if !observed || !errors.Is(err, fault) {
					t.Fatalf("agent read failure lost: observed=%v err=%v", observed, err)
				}
				if err := f.group.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.unchanged(t, before)
			})
		}
	}
}

func TestFanOutPreparationSecondaryReadFailuresBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"event", "deliveries"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				if phase == "deliveries" {
					if err := f.bus.Publish(f.ctx, f.events[0]); err != nil {
						t.Fatal(err)
					}
				}
				before := f.snapshot(t)
				fault := errors.New("secondary preparation owner read failure")
				needle := "from events"
				if phase == "deliveries" {
					needle = "from event_deliveries"
				}
				observed := false
				f.probe.set(func(at, query string) error {
					if at == "before_query" && strings.Contains(strings.ToLower(query), needle) {
						observed = true
						return fault
					}
					return nil
				})
				prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, []pipeline.FanOutPublicationRequest{{Ordinal: 0, Intent: engine.EmitIntent{Event: f.events[0]}}})
				f.probe.set(nil)
				if !observed || err != nil || len(prepared) != 1 || !errors.Is(prepared[0].Err, fault) || prepared[0].Publication != nil {
					t.Fatalf("secondary refusal observed=%v prepared=%+v err=%v", observed, prepared, err)
				}
				if err := f.group.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.unchanged(t, before)
			})
		}
	}
}

func TestFanOutPreparationReobservesAgentInventoryBeforeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, coordinator, agentsManager := newMixedExecutionFixtureWithManager(t, backend, nil)
			if err := agentsManager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			agents, err := f.raw.(bus.ActiveAgentDescriptorLister).ListActiveAgentDescriptors(f.ctx, f.seed.runID)
			if err != nil || len(agents) == 0 {
				t.Fatalf("missing real agent inventory: %+v err=%v", agents, err)
			}
			agent := agents[0]
			route := agent.Identity.Route
			owner := flowidentity.RunScopedFlowInstance{RunID: f.seed.runID, Route: flowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)}
			fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
			request, err := pipeline.NewExactFlowInstanceLookup(coordinator.SemanticSource(), fact, owner)
			if err != nil {
				t.Fatal(err)
			}
			observation, found, err := f.raw.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, request)
			if err != nil || !found {
				t.Fatalf("agent construction found=%v err=%v", found, err)
			}
			// Retire the instance before both planning cuts. Only its agent
			// inventory changes after preparation; the captured index stays valid.
			if err := coordinator.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(observation.Identity().EntityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			planning := f.group.(interface {
				WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
			})
			var descriptors []bus.ActiveAgentDescriptor
			if err := planning.WithFlowInstancePlanningSnapshot(f.ctx, coordinator.SemanticSource(), func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				var err error
				descriptors, err = reader.(preparationMetadata).ListActiveAgentDescriptors(ctx, f.seed.runID)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			f.prepare(t)
			f.seal(t)
			if len(descriptors) == 0 {
				t.Fatal("missing real prepared agent inventory")
			}
			agentIdentity := agent.Identity
			state, found, err := f.raw.(manager.AgentLifecycleStateReader).LoadAgentLifecycleState(f.ctx, agentIdentity)
			if err != nil || !found {
				t.Fatalf("agent lifecycle state found=%v err=%v", found, err)
			}
			if _, err := f.grant.CommitAgentLifecycleTransition(f.ctx, manager.AgentLifecycleTransition{
				DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
				OperationID:      uuid.NewString(), OperationKind: "teardown", RequestHash: "preparation-inventory-terminate",
				Identity: agentIdentity, AgentID: agentIdentity.AgentID(), Trigger: "terminate",
				ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
				TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation + 1, TargetPhase: manager.AgentLifecycleTerminated,
				ConfigRevision: state.ConfigRevision, RunMode: manager.AgentRunModeStopped, Topology: state.Topology, Now: time.Now().UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			failure := failures.Normalize(err, "test", "commit")
			if err == nil || !failure.Retryable || failure.Detail.Code != "flow_instance_planning_stale" || len(committed.Publications) != 0 {
				t.Fatalf("changed agent inventory admitted: %+v err=%v", committed, err)
			}
			if !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("agent staleness leaked publication/ordinal/cursor state")
			}
		})
	}
}
