package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The retired selector suite also exercised real repository side effects.
// Preserve that consumer behind canonical receiver initialization, not a
// business-key lookup or a test-only alternate receiver identity.
func TestCompositionReceiverInitializationFeedsArtifactCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml": "name: artifact-receiver\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n",
				"entities.yaml": `artifact:
  repo_url: {type: text}
  current_ref: {type: text}
  file_manifest: {type: json}
  status: {type: text}
  failure: {type: json}
  last_request_id: {type: text}
  last_source_event_id: {type: text}
`,
				"events.yaml": "artifact_repo.commit_requested:\n  request_id: text\n  mvp_yaml: text\n",
				"nodes.yaml": `artifact-node:
  id: artifact-node
  execution_type: system_node
  subscribes_to: [artifact_repo.commit_requested]
  event_handlers:
    artifact_repo.commit_requested:
      create_entity: true
`,
			})
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("source bundle missing")
			}
			pc := &PipelineCoordinator{
				workflowStore: store, artifactRoot: t.TempDir(), bus: &recordingPipelineBus{},
				module:      handlerTestWorkflowModuleWithBundle(bundle, "artifact-receiver", "artifact-node"),
				entityLocks: map[string]*sync.Mutex{},
			}
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			node := pipelineSourceNode(t, source, ".", "artifact-node")
			receiverID := testPipelineRunID
			sourceID := eventtest.UUID("different-artifact-producer")
			initial := testArtifactRepoEntityFieldsForSource(source, sourceID)
			action, request := testArtifactRepoActionAndContext(sourceID, initial, eventtest.UUID("artifact-source-event"), eventtest.UUID("artifact-request"), "name: Demo\n")
			action.ArtifactRepo.RepoID = runtimecontracts.RefExpression("_entity.id")
			action.ArtifactRepo.Namespace = runtimecontracts.LiteralExpression("tenant-alpha")
			action.ArtifactRepo.PartitionKey = runtimecontracts.LiteralExpression("project-42")
			action.ArtifactRepo.DisplaySlug = runtimecontracts.LiteralExpression("Demo Artifact")
			action.ArtifactRepo.Provenance = map[string]runtimecontracts.ExpressionValue{"artifact_type": runtimecontracts.LiteralExpression("fixture")}
			action.ArtifactRepo.FailureEvent = ""
			action.ArtifactRepo.FailurePayload = nil
			owner := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: receiverID})
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: owner}
			ctx = runtimedelivery.WithRoute(ctx, route)
			seedExactOnceEvent(t, store, ctx, request.Request.Event)
			handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true, Action: action}
			result, err := pc.executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{Event: request.Request.Event}, false)
			if err != nil || !result.Handled {
				t.Fatalf("execute canonical initialization and action: handled=%t err=%v", result.Handled, err)
			}
			current, found, err := store.Load(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, testPipelineRunID))
			if err != nil || !found {
				t.Fatalf("read receiver: found=%t err=%v", found, err)
			}
			if current.EntityID != receiverID || current.EntityID == sourceID {
				t.Fatalf("action retargeted receiver: %#v", current)
			}
			if got := strings.TrimSpace(asString(current.Fields["repo_url"])); got != "swarm-artifact://repos/"+receiverID {
				t.Fatalf("repo_url=%q", got)
			}
			ref := strings.TrimSpace(asString(current.Fields["current_ref"]))
			if len(ref) != 40 {
				t.Fatalf("current_ref=%q", ref)
			}
			if got := current.Fields["status"]; got != "committed" {
				t.Fatalf("business status=%#v", got)
			}
			if current.CurrentState != "active" {
				t.Fatalf("business status overwrote lifecycle: %#v", current)
			}
			// Repeated handler execution reuses the exact canonical state and the
			// repository request, preserving the Git ref rather than creating a second repo.
			result, err = pc.executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{Event: request.Request.Event}, false)
			if err != nil || !result.Handled {
				t.Fatalf("duplicate handler: handled=%t err=%v", result.Handled, err)
			}
			repeated, found, err := store.Load(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, testPipelineRunID))
			if err != nil || !found || asString(repeated.Fields["current_ref"]) != ref {
				t.Fatalf("duplicate changed repository: %#v found=%t err=%v", repeated, found, err)
			}
		})
	}
}
