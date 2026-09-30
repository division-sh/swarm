package semanticview

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func receiverInstanceSource(t *testing.T, mutation string) Source {
	t.Helper()
	root := canonicalrouting.CopySelectedForkReadiness(t, 0, "node")
	if mutation != "" {
		file := "schema.yaml"
		if mutation == "schema" {
			file = "events.yaml"
		}
		path := filepath.Join(root, file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		switch mutation {
		case "edge_removed":
			delete(doc, "connect")
		case "schema":
			doc["worker.inspect"].(map[string]any)["note"] = "text"
		case "nested":
			dir := filepath.Join(root, "worker-flow", "nested")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string]string{
				"schema.yaml": "stages:\n  idle: {initial: true}\n  done: {terminal: true}\n",
				"events.yaml": "worker.inspect.requested:\n  nested_marker: text\n",
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
		default:
			t.Fatal("unknown fixture mutation")
		}
		raw, err = yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return Wrap(bundle)
}

func TestReceiverInstanceEventProofPreservesDescendantBoundary(t *testing.T) {
	source := receiverInstanceSource(t, "nested")
	const descendant = "worker-flow/nested/worker.inspect.requested"
	for _, proof := range []FlowEventProof{
		ResolveFlowEventProof(source, "worker-flow", descendant),
		ResolveExecutableNodeEventProof(source, identitytest.FlowNode(t, "worker-flow", "worker-inspect-entry"), descendant),
	} {
		if proof.Local == "worker.inspect.requested" || proof.CatalogKey == "worker.inspect" {
			t.Fatalf("descendant declaration became parent receiver occurrence: %+v", proof)
		}
	}
	if proof := ResolveFlowEventProof(source, "worker-flow", "worker-flow/worker-001/worker.inspect.requested"); !proof.HasSchema || proof.Entry.Payload.Properties["worker_id"].Type != "text" {
		t.Fatalf("descendant shadowed valid parent instance: %+v", proof)
	}
}

func TestReceiverInstanceEventSchemaRetainsReceiverGeneratedKey(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "examples/routing/template-create-minted-key"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)
	pin, ok := source.FlowInputEventPin("validator", "validation.requested")
	if !ok {
		t.Fatal("receiver pin missing")
	}
	receiver, receiverOK := pin.ReceiverEventSchema()
	producer, producerOK := pin.ProducerEventSchema()
	if !receiverOK || !producerOK || receiver.AcceptanceSchemaDigest() == producer.AcceptanceSchemaDigest() {
		t.Fatal("fixture must have distinct producer and receiver acceptance")
	}
	for _, event := range []string{"validation.requested", "validator/validation.requested", "validator/instance-1/validation.requested"} {
		proof := ResolveEventSchema(source, "validator", event)
		if !proof.HasCompiled || proof.CompiledSchema.AcceptanceSchemaDigest() != receiver.AcceptanceSchemaDigest() {
			t.Fatalf("receiver occurrence substituted producer schema: event=%s proof=%+v", event, proof)
		}
		if _, ok := proof.Field("validation_case_id"); !ok {
			t.Fatal("receiver occurrence lost generated key")
		}
	}
}

func TestReceiverInstanceEventProofUsesCompiledRename(t *testing.T) {
	source := receiverInstanceSource(t, "")
	flow := "worker-flow"
	local := "worker.inspect.requested"
	scope, _ := source.FlowScopeByID(flow)
	if _, authored := scope.Events[local]; authored {
		t.Fatal("receiver rename must not be restated as an authored event")
	}
	pin, ok := source.FlowInputEventPin(flow, local)
	if !ok {
		t.Fatal("missing compiled receiver pin")
	}
	receiver, ok := pin.ReceiverEventSchema()
	if !ok {
		t.Fatal("missing compiled receiver schema")
	}
	declared := ResolveFlowEventProof(source, flow, local)
	for _, name := range []string{local, flow + "/" + local, flow + "/worker-001/" + local} {
		t.Run(name, func(t *testing.T) {
			proof := ResolveFlowEventProof(source, flow, name)
			if !proof.HasSchema || proof.Local != local || proof.CatalogKey != declared.CatalogKey || !reflect.DeepEqual(proof.Entry, declared.Entry) {
				t.Fatalf("receiver declaration and occurrence disagree: %+v vs %+v", proof, declared)
			}
			if name == flow+"/worker-001/"+local && (proof.Canonical != name || proof.IsAuthored(source)) {
				t.Fatalf("instance identity/classification was rewritten: %+v", proof)
			}
			nodeProof := ResolveExecutableNodeEventProof(source, identitytest.FlowNode(t, flow, "worker-inspect-entry"), name)
			if !nodeProof.HasSchema || nodeProof.Local != local || !reflect.DeepEqual(nodeProof.Entry, declared.Entry) {
				t.Fatalf("node proof differs from receiver proof: %+v", nodeProof)
			}
			schema := ResolveEventSchema(source, flow, name)
			if !schema.HasSchema || !schema.HasCompiled || !schema.HasStructural || schema.CompiledSchema.AcceptanceSchemaDigest() != receiver.AcceptanceSchemaDigest() {
				t.Fatalf("schema did not bind exact compiled receiver: %+v", schema)
			}
		})
	}
	for _, tc := range []struct{ flow, name string }{
		{"other", "worker-flow/worker-001/" + local},
		{"worker-flow/worker-001", "worker-flow/worker-001/" + local},
		{flow, "sibling/worker-001/" + local},
		{flow, "worker-flow/worker-001/not.declared"},
	} {
		if proof := ResolveFlowEventProof(source, tc.flow, tc.name); proof.HasSchema {
			t.Errorf("foreign or undeclared occurrence gained schema: %+v", proof)
		}
	}
}

func TestReceiverInstanceEventProofTracksCompiledSchemaAndEdgeRemoval(t *testing.T) {
	const name = "worker-flow/worker-001/worker.inspect.requested"
	base := ResolveEventSchema(receiverInstanceSource(t, ""), "worker-flow", name)
	changed := ResolveEventSchema(receiverInstanceSource(t, "schema"), "worker-flow", name)
	if !base.HasCompiled || !changed.HasCompiled || base.CompiledSchema.AcceptanceSchemaDigest() == changed.CompiledSchema.AcceptanceSchemaDigest() {
		t.Fatal("source schema mutation did not change concrete receiver acceptance")
	}
	if _, ok := changed.Field("note"); !ok {
		t.Fatal("concrete receiver lost the new producer field")
	}
	removed := receiverInstanceSource(t, "edge_removed")
	for _, spelling := range []string{"worker.inspect.requested", "worker-flow/worker.inspect.requested", name} {
		if proof := ResolveFlowEventProof(removed, "worker-flow", spelling); proof.HasSchema {
			t.Fatalf("removed edge retained schema authority: %+v", proof)
		}
	}
}
