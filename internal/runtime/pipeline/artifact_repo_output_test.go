package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const artifactRepoStateEntityYAML = `test_entity:
  repo_id: text
  namespace: text
  partition_key: text
  display_slug: text
  source_record_id: text
  repo_url: text
  current_ref: text
  file_manifest: json
  status: text
  failure: json
  last_request_id: text
  last_source_event_id: text
`

func TestArtifactRepoOutputReadbackRecoveryBothStores(t *testing.T) {
	for _, named := range []bool{false, true} {
		for _, backend := range []string{"sqlite", "postgres"} {
			name := backend + "/json"
			if named {
				name = backend + "/named"
			}
			t.Run(name, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				ctx := context.Background()
				if backend == "sqlite" {
					ctx = sqliteExactOnceRunContext(t, db)
				} else {
					ctx = testPipelineRunContext(t, db)
				}
				source := testArtifactRepoResultEventSource(t)
				bundle, _ := semanticview.Bundle(source)
				if named {
					artifactRepoNamedOutputFixture(bundle)
				}
				root := t.TempDir()
				bus := &recordingPipelineBus{}
				newCoordinator := func() *PipelineCoordinator {
					return &PipelineCoordinator{workflowStore: store, artifactRoot: root, bus: bus,
						module: handlerTestWorkflowModuleWithBundle(bundle, "artifact-repo", "artifact-node"), entityLocks: map[string]*sync.Mutex{}}
				}
				pc := newCoordinator()
				entityID := eventtest.UUID("artifact-output-receiver")
				ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineSourceNode(t, source, ".", "artifact-node")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID})})
				initial := testArtifactRepoEntityFieldsForSource(source, entityID)
				if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: artifactRepoFixtureRoute(initial), StorageRef: artifactRepoFixtureRoute(initial), EntityID: entityID,
					WorkflowName: ".", WorkflowVersion: "1.0.0", CurrentState: "ready", EntityType: "test_entity", Fields: initial,
				})); err != nil {
					t.Fatal(err)
				}
				read := func() map[string]any {
					t.Helper()
					instance, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, artifactRepoFixtureRoute(initial)))
					if err != nil || !found {
						t.Fatalf("canonical artifact state readback: found=%v err=%v", found, err)
					}
					if _, err := entityruntime.NormalizeMetadataForFlow(source, ".", instance.Fields); err != nil {
						t.Fatalf("artifact result is not readable receiver state: %v", err)
					}
					return instance.Fields
				}
				run := func(event, request, content string, invalidPath bool) error {
					t.Helper()
					action, execution := testArtifactRepoActionAndContext(entityID, read(), eventtest.UUID(event), eventtest.UUID(request), content)
					action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
					action.ArtifactRepo.SuccessPayload = map[string]contracts.ExpressionValue{"result_kind": contracts.LiteralExpression("ready")}
					if invalidPath {
						action.ArtifactRepo.Files[0].Path = contracts.LiteralExpression("../escape.yaml")
					}
					seedExactOnceEvent(t, store, ctx, execution.Request.Event)
					_, err := pc.executeNodeContractHandler(ctx, pipelineSourceNode(t, source, ".", "artifact-node"), contracts.SystemNodeEventHandler{Action: action}, workflowTriggerContext{Event: execution.Request.Event}, false)
					return err
				}
				repoPath, err := artifactRepoPath(root, initial["namespace"].(string), initial["repo_id"].(string))
				if err != nil {
					t.Fatal(err)
				}
				head := func() string {
					t.Helper()
					value, err := artifactRepoHead(ctx, repoPath)
					if err != nil {
						t.Fatal(err)
					}
					return value
				}
				requireSuccess := func() map[string]any {
					t.Helper()
					fields := read()
					failure, ok := fields["failure"].(map[string]any)
					if !ok || failure == nil || len(failure) != 0 || fields["status"] != "committed" {
						t.Fatalf("success retained invalid/stale failure: %#v", fields)
					}
					return fields
				}
				if err := run("fresh", "request-one", "name: Demo\n", false); err != nil {
					t.Fatal(err)
				}
				first := requireSuccess()
				firstRef := head()
				if bus.outboxCount() != 1 || bus.publishedCount() != 1 {
					t.Fatal("fresh success missing atomic result publication")
				}
				var successPayload map[string]any
				if err := json.Unmarshal(bus.outboxIntent(0).Event.Payload(), &successPayload); err != nil {
					t.Fatal(err)
				}
				if _, exists := successPayload["failure"]; exists {
					t.Fatal("empty state failure leaked into success event")
				}
				pc = newCoordinator()
				if err := run("fresh", "request-one", "name: Demo\n", false); err != nil {
					t.Fatalf("restart duplicate: %v", err)
				}
				if !reflect.DeepEqual(first, read()) || head() != firstRef || bus.outboxCount() != 1 || bus.publishedCount() != 1 {
					t.Fatal("restart duplicate changed state/ref/publications")
				}
				if err := run("conflict", "request-one", "name: Changed\n", false); err != nil {
					t.Fatal(err)
				}
				failed := read()
				requireArtifactRepoFailure(t, failed["failure"], failures.ClassConflictingDuplicate, "artifact_repo_request_conflict")
				if failed["status"] != "failed" || head() != firstRef {
					t.Fatal("request conflict changed Git history or lost failure status")
				}
				pc = newCoordinator()
				if err := run("recover", "request-one", "name: Demo\n", false); err != nil {
					t.Fatalf("provider history recovery: %v", err)
				}
				if requireSuccess()["current_ref"] != firstRef || head() != firstRef {
					t.Fatal("provider recovery minted a new ref")
				}
				if err := run("same-tree", "request-two", "name: Demo\n", false); err != nil {
					t.Fatal(err)
				}
				sameRef := head()
				if sameRef == firstRef || requireSuccess()["current_ref"] != sameRef {
					t.Fatal("same-tree new request lost durable operation history")
				}
				for _, failureOutcome := range []bool{true, false} {
					before, beforeHead := read(), head()
					outbox, published := bus.outboxCount(), bus.publishedCount()
					bus.outboxErr = errors.New("artifact proof outbox unavailable")
					event, request := "rollback-success", "request-three"
					if failureOutcome {
						event, request = "rollback-failure", "request-invalid"
					}
					if err := run(event, request, "name: Next\n", failureOutcome); err == nil || !strings.Contains(err.Error(), "artifact proof outbox unavailable") {
						t.Fatalf("outbox failure not preserved: %v", err)
					}
					if !reflect.DeepEqual(before, read()) || bus.outboxCount() != outbox || bus.publishedCount() != published {
						t.Fatal("outbox rollback changed state/publications")
					}
					if failureOutcome && head() != beforeHead {
						t.Fatal("invalid input touched Git")
					}
					committedProviderRef := head()
					bus.outboxErr = nil
					pc = newCoordinator()
					if err := run(event, request, "name: Next\n", failureOutcome); err != nil {
						t.Fatalf("retry after rollback: %v", err)
					}
					if !failureOutcome && (requireSuccess()["current_ref"] != committedProviderRef || head() != committedProviderRef) {
						t.Fatal("rollback recovery failed to reuse real provider commit")
					}
					if failureOutcome {
						failed := read()
						requireArtifactRepoFailure(t, failed["failure"], failures.ClassSchemaInvalid, "artifact_repo_file_invalid")
						if failed["status"] != "failed" {
							t.Fatal("failure retry lost status")
						}
					}
					if bus.outboxCount() != outbox+1 || bus.publishedCount() != published+1 {
						t.Fatal("retry did not enqueue and publish exactly one result")
					}
					if !failureOutcome {
						after := read()
						pc = newCoordinator()
						if err := run(event, request, "name: Next\n", false); err != nil {
							t.Fatalf("recovered result duplicate: %v", err)
						}
						if !reflect.DeepEqual(after, read()) || head() != committedProviderRef || bus.outboxCount() != outbox+1 || bus.publishedCount() != published+1 {
							t.Fatal("recovered result duplicate changed state/ref/publication")
						}
					}
				}
			})
		}
	}
}

func artifactRepoNamedOutputFixture(bundle *contracts.WorkflowContractBundle) {
	bundle.RootTypes.Scalars = map[string]contracts.ScalarTypeDecl{"ArtifactFailure": {Base: "json"}}
	bundle.RootTypes.Enums = map[string]contracts.EnumTypeDecl{"ArtifactStatus": {Values: []string{"committed", "failed"}, Default: "failed"}}
	entity := bundle.RootEntities["test_entity"]
	for field, typeName := range map[string]string{"file_manifest": "ArtifactManifest", "failure": "ArtifactFailure", "status": "ArtifactStatus", "last_request_id": "uuid", "last_source_event_id": "uuid"} {
		decl := entity.Fields[field]
		decl.Type = typeName
		entity.Fields[field] = decl
	}
	bundle.RootEntities["test_entity"] = entity
}

func TestArtifactRepoOutputContractRejectsBeforeProvider(t *testing.T) {
	type mutation func(*contracts.WorkflowContractBundle, *contracts.ArtifactRepoSpec)
	cases := map[string]mutation{}
	setType := func(field, typeName string) mutation {
		return func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
			entity := bundle.RootEntities["test_entity"]
			decl := entity.Fields[field]
			decl.Type = typeName
			entity.Fields[field] = decl
			bundle.RootEntities["test_entity"] = entity
		}
	}
	for _, output := range []string{"repo_url", "current_ref", "file_manifest", "status", "failure", "last_request_id", "last_source_event_id"} {
		cases[output+"_integer"] = setType(output, "integer")
	}
	cases["failure_array"] = setType("failure", "array")
	cases["failure_named_manifest"] = setType("failure", "ArtifactManifest")
	cases["failure_undeclared"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		delete(bundle.RootEntities["test_entity"].Fields, "failure")
	}
	cases["duplicate_mapping"] = func(_ *contracts.WorkflowContractBundle, spec *contracts.ArtifactRepoSpec) {
		spec.Output.Failure = spec.Output.FileManifest
	}
	cases["status_success_only"] = func(bundle *contracts.WorkflowContractBundle, spec *contracts.ArtifactRepoSpec) {
		artifactRepoNamedOutputFixture(bundle)
		bundle.RootTypes.Enums["ArtifactStatus"] = contracts.EnumTypeDecl{Values: []string{"committed"}, Default: "committed"}
	}
	cases["ref_empty_only_enum"] = func(bundle *contracts.WorkflowContractBundle, spec *contracts.ArtifactRepoSpec) {
		artifactRepoNamedOutputFixture(bundle)
		bundle.RootTypes.Enums["ArtifactRef"] = contracts.EnumTypeDecl{Values: []string{"", "not-a-ref"}, Default: "not-a-ref"}
		setType("current_ref", "ArtifactRef")(bundle, spec)
	}
	cases["ref_pattern"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		decl := bundle.RootEntities["test_entity"].Fields["current_ref"]
		decl.Refinements.Pattern = "^$"
		bundle.RootEntities["test_entity"].Fields["current_ref"] = decl
	}
	cases["ref_equality_target"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		decl := bundle.RootEntities["test_entity"].Fields["repo_url"]
		decl.Refinements.EqualTo = "current_ref"
		bundle.RootEntities["test_entity"].Fields["repo_url"] = decl
	}
	cases["manifest_wrong_member"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		artifactRepoNamedOutputFixture(bundle)
		decl := bundle.RootTypes.Types["ArtifactManifest"].Fields["namespace"]
		decl.Type = "integer"
		bundle.RootTypes.Types["ArtifactManifest"].Fields["namespace"] = decl
	}
	cases["manifest_ref_refinement"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		artifactRepoNamedOutputFixture(bundle)
		decl := bundle.RootTypes.Types["ArtifactManifest"].Fields["ref"]
		decl.Refinements.Pattern = "^$"
		bundle.RootTypes.Types["ArtifactManifest"].Fields["ref"] = decl
	}
	cases["manifest_ref_equality_target"] = func(bundle *contracts.WorkflowContractBundle, _ *contracts.ArtifactRepoSpec) {
		artifactRepoNamedOutputFixture(bundle)
		decl := bundle.RootTypes.Types["ArtifactManifest"].Fields["tree_hash"]
		decl.Refinements.EqualTo = "ref"
		bundle.RootTypes.Types["ArtifactManifest"].Fields["tree_hash"] = decl
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			source := testArtifactRepoResultEventSource(t)
			bundle, _ := semanticview.Bundle(source)
			_, store := openHandlerEntityRequirementStore(t, "sqlite")
			root := t.TempDir()
			pc := &PipelineCoordinator{workflowStore: store, artifactRoot: root, module: &pipelineFixtureWorkflowModule{source: source}}
			initial := testArtifactRepoEntityFieldsForSource(source, eventtest.UUID("hostile-artifact-entity"))
			action, execution := testArtifactRepoActionAndContext(initial["entity_id"].(string), initial, eventtest.UUID("hostile-event"), eventtest.UUID("hostile-request"), "name: Demo\n")
			mutate(bundle, action.ArtifactRepo)
			result, err := pc.commitArtifactRepo(context.Background(), action, execution)
			failure := failures.Normalize(err, "artifact-repo", "commit")
			if err == nil || failure.Detail.Code != "artifact_repo_output_schema_invalid" || result.State != nil || len(result.EmitIntents) != 0 {
				t.Fatalf("incompatible output admitted: %+v %v", result, err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("invalid output touched provider: %v %v", entries, readErr)
			}
		})
	}
}
