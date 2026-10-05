package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func setSuppliedPayload(req *runtimepipeline.FlowInstanceActivationRequest, payload string) {
	event := req.TriggerEvent
	req.TriggerEvent = eventtest.RunCreatingRootIngressWithMode(event.ID(), event.Type(),
		event.SourceAgent(), event.TaskID(), json.RawMessage(payload), event.ChainDepth(),
		event.RunID(), event.ParentEventID(), event.Envelope(), event.CreatedAt(), event.ExecutionMode())
}

func setSuppliedFields(t testing.TB, req *runtimepipeline.FlowInstanceActivationRequest, fields map[string]any) {
	t.Helper()
	raw, err := canonicaljson.MarshalPreservingNumberKinds(fields)
	if err != nil {
		t.Fatal(err)
	}
	setSuppliedPayload(req, string(raw))
}

// Fixture declarations are admitted through the same state loader as production.
func declareSuppliedFields(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, fields map[string]runtimecontracts.EventFieldSpec) {
	t.Helper()
	root := t.TempDir()
	var state strings.Builder
	state.WriteString("review_item:\n  instance_key: text\n")
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&state, "  %s: %s\n", key, fields[key].Type)
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: supplied-state-fixture\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "review/schema.yaml"), "name: review\ninstance: instance_key\npins: {inputs: [construction.requested]}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "review/entities.yaml"), state.String())
	repo := runtimepipeline.WorkflowRepoRoot()
	admitted, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	admitted.FlowTree, admitted.FlowSchemas, admitted.URIRegistry = bundle.FlowTree, bundle.FlowSchemas, bundle.URIRegistry
	admitted.Nodes, admitted.Events, admitted.Agents, admitted.Tools = bundle.Nodes, bundle.Events, bundle.Agents, bundle.Tools
	admitted.Policy, admitted.Semantics = bundle.Policy, bundle.Semantics
	*bundle = *admitted
	schema := bundle.FlowSchemas["review"]
	schema.Pins.Inputs.EventPins = append(schema.Pins.Inputs.EventPins, runtimecontracts.FlowInputEventPin{Event: "construction.requested"})
	bundle.FlowSchemas["review"], bundle.FlowTree.ByID["review"].Schema = schema, schema
	properties := make(map[string]runtimecontracts.EventFieldSpec, len(fields)+1)
	properties["instance_key"] = runtimecontracts.EventFieldSpec{Type: "text"}
	for key, field := range fields {
		properties[key] = field
	}
	bundle.FlowTree.ByID["review"].Events["construction.requested"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: properties, Required: keys}}
	compileFlowActivationFixture(t, bundle)
}

func TestSuppliedStateHasNoParallelConfigurationAdmission(t *testing.T) {
	if _, found := reflect.TypeOf(runtimepipeline.FlowInstanceActivationRequest{}).FieldByName("Config"); found {
		t.Fatal("parallel configuration input restored")
	}
	bundle := testFlowBundle(t, "")
	declareSuppliedFields(t, bundle, map[string]runtimecontracts.EventFieldSpec{"brief": {Type: "text"}})
	req := testActivationRequest(bundle, "review", "one", "", "review/one")
	req.ConstructorInput = "construction.requested"
	setSuppliedPayload(&req, `{"brief":"exact supplied value"}`)
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	plan, err := am.PrepareFlowInstanceActivation(testAuthorActivityContext(context.Background()), req)
	if err != nil || plan.Instance.Fields["brief"] != "exact supplied value" {
		t.Fatalf("canonical constructor was blocked by a second authority: %+v %v", plan, err)
	}
}
