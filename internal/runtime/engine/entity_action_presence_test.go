package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityActionOutputConflictRejectsBeforeRunner(t *testing.T) {
	outputs := rc.ArtifactRepoOutputSpec{RepoURL: "entity.url", CurrentRef: "entity.ref", FileManifest: "entity.manifest", Status: "entity.status", Failure: "entity.failure", LastRequestID: "entity.request", LastSourceEventID: "entity.source"}
	fields := map[string]rc.EntityFieldDecl{}
	for _, target := range outputs.Fields() {
		fields[strings.TrimPrefix(target, "entity.")] = rc.EntityFieldDecl{Type: "text", IsOptional: true}
	}
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{"work": {Fields: fields}}})
	for _, conflict := range []bool{false, true} {
		name := "control"
		if conflict {
			name = "prior_clear"
		}
		t.Run(name, func(t *testing.T) {
			runner := &stubActionRunner{}
			action := identity.NormalizeActionKey("artifact_repo_commit")
			executor, err := NewExecutor(RuntimeDependencies{
				Source: source, StateRepo: sparseSnapshotRepo{snapshot: testStateSnapshot("ready", nil, nil, nil)},
				MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
				ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]registry.ActionInstruction{action: {Key: action}}}, ActionRunner: runner,
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			handler := rc.SystemNodeEventHandler{Action: rc.ActionSpec{ID: "artifact_repo_commit", ArtifactRepo: &rc.ArtifactRepoSpec{Output: outputs}}}
			if conflict {
				handler.DataAccumulation.Writes = []rc.WorkflowDataWrite{{Operation: rc.WorkflowDataOperationClear, TargetField: "entity.url"}}
			}
			_, err = executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", Node: testRootExecutableNode(t, "worker"), Handler: handler,
				Event: eventtest.RunCreatingRootIngress("event-1", "work.received", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
			})
			if conflict {
				if err == nil || !strings.Contains(err.Error(), "set/clear conflict") || len(runner.called) != 0 {
					t.Fatalf("conflict error=%v, runner calls=%v", err, runner.called)
				}
			} else if err != nil || len(runner.called) != 1 {
				t.Fatalf("control error=%v, runner calls=%v", err, runner.called)
			}
		})
	}
}

func TestEntityEmitPrerequisitesConsumeExecutedMutationPlan(t *testing.T) {
	contract := entityruntime.Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"removed": {Type: "text", IsOptional: true}, "written": {Type: "text"}, "untouched": {Type: "text"},
	}}}
	plan, err := entityruntime.NewMutationPlan(contract, map[string]any{"removed": "old"})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []entityruntime.Mutation{{Operation: entityruntime.MutationClear, Target: "removed"}, {Target: "written", Value: ""}} {
		if err := plan.Append(mutation); err != nil {
			t.Fatal(err)
		}
	}
	frame := executionFrame{entityMutations: plan}
	frame.state.State = testStateSnapshot("ready", plan.Draft(), nil, nil)
	frame.req.Handler.DataAccumulation.Writes = []rc.WorkflowDataWrite{{TargetField: "untouched"}}
	got := (&Executor{}).emitPersistencePrerequisites(frame)
	if len(got.Fields) != 2 || got.Fields[0].Field != "removed" || got.Fields[0].Presence != EntityFieldAbsent || got.Fields[1].Field != "written" || got.Fields[1].Presence != EntityFieldPresent || got.Fields[1].Expected != "" {
		t.Fatalf("prerequisites = %#v", got)
	}
}

func TestEntityArtifactCommittedOutcomeAssignments(t *testing.T) {
	outputs := rc.ArtifactRepoOutputSpec{RepoURL: "url", CurrentRef: "ref", FileManifest: "manifest", Status: "status", Failure: "failure", LastRequestID: "request", LastSourceEventID: "source"}
	fields := map[string]rc.EntityFieldDecl{}
	for _, path := range outputs.Fields() {
		fields[path] = rc.EntityFieldDecl{Type: "text"}
	}
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{"work": {Fields: fields}}})
	typ, err := semanticview.ResolveEntityStructuralType(source, ".")
	if err != nil || typ == nil {
		t.Fatalf("missing entity type: %v", err)
	}
	analysis := &EntityAssignmentAnalysis{source: source, entity: *typ}
	for _, failureEvent := range []string{"", "artifact.failed"} {
		for _, previouslyPresent := range []bool{false, true} {
			before := entityruntime.AssignmentFacts{}
			if previouslyPresent {
				for _, path := range outputs.Fields() {
					before[path] = struct{}{}
				}
			}
			handler := rc.SystemNodeEventHandler{Action: rc.ActionSpec{ID: "artifact_repo_commit", ArtifactRepo: &rc.ArtifactRepoSpec{Output: outputs, FailureEvent: failureEvent}}}
			facts := analysis.transfer(testRootExecutableNode(t, "worker"), "work.received", handler, entityAssignmentOutcome{}, before, nil)
			for name, path := range outputs.Fields() {
				common := name == "status" || name == "failure" || name == "last_request_id" || name == "last_source_event_id"
				want := common || failureEvent == "" || previouslyPresent
				if facts.Has(path) != want {
					t.Fatalf("failure=%q prior=%v field=%s assigned=%v want=%v", failureEvent, previouslyPresent, path, facts.Has(path), want)
				}
			}
		}
	}
}
