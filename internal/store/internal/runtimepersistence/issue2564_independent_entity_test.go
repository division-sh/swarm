package runtimepersistence

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type issue2564PausedEntityReader struct {
	workflowTestSelectedStore
	entityID string
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (s *issue2564PausedEntityReader) LoadWorkflowTargetPersistence(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID identity.EntityID) (pipeline.WorkflowTargetPersistenceRecord, error) {
	if entityID.String() == s.entityID {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return pipeline.WorkflowTargetPersistenceRecord{}, ctx.Err()
		}
	}
	return s.workflowTestSelectedStore.LoadWorkflowTargetPersistence(ctx, owner, entityID)
}

func TestIssue2564M28OneEntityFenceDoesNotBlockIndependentEntityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newIssue2564OperationFixture(t, backend)
			request := sqliteFlowActivationRequest(f.bundle, "operations", "independent", "", "operations/independent")
			request.OccurredAt = time.Now().UTC().Truncate(time.Microsecond)
			request.ConstructorInput, request.ResolvedKey = "construct.requested", "independent"
			request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "construct.requested", "constructor-fixture", "", []byte(`{}`), 0, f.state.Identity.RunID, events.EventEnvelope{}, request.OccurredAt)
			activation, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			constructed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, activation)
			if err != nil || !constructed.Acknowledged || !constructed.Created {
				t.Fatalf("independent canonical construction: %+v %v", constructed, err)
			}
			second, err := activation.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			firstBefore := issue2564LoadOperationInstance(t, f)
			reader := &issue2564PausedEntityReader{workflowTestSelectedStore: f.selected, entityID: f.state.EntityID, entered: make(chan struct{}), release: make(chan struct{})}
			writer := issue2564OperationCoordinator(f, reader)
			ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			var release sync.Once
			unblock := func() { release.Do(func() { close(reader.release) }) }
			type outcome struct {
				result pipeline.EntityFieldMutationResult
				err    error
			}
			firstDone := make(chan outcome, 1)
			joined := false
			go func() {
				result, err := writer.ApplyEntityFieldMutation(ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "first"}))
				firstDone <- outcome{result, err}
			}()
			defer func() {
				unblock()
				if joined {
					return
				}
				select {
				case <-firstDone:
				case <-ctx.Done():
				}
			}()
			select {
			case <-reader.entered:
			case <-ctx.Done():
				t.Fatal("first entity did not reach its locked evaluation")
			}
			command := f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "second"})
			command.Owner, command.EntityID = second.State.Identity, second.State.EntityID
			result, err := writer.ApplyEntityFieldMutation(ctx, command)
			if err != nil || !result.Acknowledged || result.Revision != 2 {
				t.Fatalf("independent entity could not commit while first fence was held: %+v %v", result, err)
			}
			if current := issue2564LoadOperationInstance(t, f); current.Revision != firstBefore.Revision || !reflect.DeepEqual(current.Fields, firstBefore.Fields) {
				t.Fatal("paused entity changed before its evaluation was released")
			}
			unblock()
			select {
			case committed := <-firstDone:
				joined = true
				if committed.err != nil || !committed.result.Acknowledged || committed.result.Revision != 2 {
					t.Fatalf("first entity failed after release: %+v %v", committed.result, committed.err)
				}
			case <-ctx.Done():
				t.Fatal("first entity did not commit after release")
			}
			for _, state := range []pipeline.WorkflowEngineStateRecord{f.state, second.State} {
				instance, found, err := f.selected.LoadWorkflowInstance(f.ctx, state.Identity)
				want := []any{"equal", "first"}
				if state.EntityID == second.State.EntityID {
					want = []any{"equal", "second"}
				}
				if err != nil || !found || instance.Revision != 2 || !reflect.DeepEqual(instance.Fields["items"], want) {
					t.Fatalf("independent canonical state %s: %+v found=%v err=%v", state.EntityID, instance, found, err)
				}
			}
			issue2564AssertOperationDeliveryUnchanged(t, f)
		})
	}
}
