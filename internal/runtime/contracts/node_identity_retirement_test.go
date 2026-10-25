package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

const retiredNodeID = `node field "id" is not supported.`

func TestNodeMapKeyIdentityAcrossRecordSources(t *testing.T) {
	scoped := &WorkflowContractBundle{scopedNodes: map[string]SystemNodeContract{}, scopedNodeSources: map[string]ContractItemSource{}}
	root := &FlowContractView{Path: ".", Paths: FlowContractPaths{FlowPath: "."}, Nodes: map[string]SystemNodeContract{"worker": {Description: "."}}}
	for _, path := range []string{".", "left", "right", "left/nested"} {
		entry := SystemNodeContract{Description: path}
		source := ContractItemSource{FlowPath: path, Family: "nodes", File: filepath.Join(path, "nodes.yaml")}
		key := contractScopeKey(source, "worker")
		scoped.scopedNodes[key], scoped.scopedNodeSources[key] = entry, source
		if path != "." {
			root.Children = append(root.Children, FlowContractView{Path: path, Paths: FlowContractPaths{FlowPath: path}, Nodes: map[string]SystemNodeContract{"worker": entry}})
		}
	}
	for _, test := range []struct {
		name   string
		bundle *WorkflowContractBundle
		count  int
	}{
		{"exported tree", &WorkflowContractBundle{FlowTree: FlowTree{Root: root}}, 4},
		{"scoped tables", scoped, 4},
		{"root-only hand-built source", &WorkflowContractBundle{Nodes: root.Nodes}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := test.bundle.ScopedNodeRecords()
			if len(records) != test.count {
				t.Fatalf("record count = %d", len(records))
			}
			keys := map[string]bool{}
			for _, record := range records {
				ref, err := record.Identity()
				if err != nil || ref.NodeID() != "worker" || ref.FlowPath() != record.Entry.Description || keys[ref.Key()] {
					t.Fatalf("identity = %v error=%v record=%#v", ref, err, record)
				}
				keys[ref.Key()] = true
				selected, ok := test.bundle.ExecutableNode(ref)
				if !ok || !reflect.DeepEqual(selected, record) {
					t.Fatalf("lookup selected another same-name declaration: %#v", selected)
				}
			}
			absent, err := identity.AdmitExecutableNodeDeclaration("missing", "worker")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := test.bundle.ExecutableNode(absent); ok {
				t.Fatal("lookup fell back to a bare local name")
			}
			if _, ok := test.bundle.ExecutableNode(identity.ExecutableNode{}); ok {
				t.Fatal("invalid identity selected a declaration")
			}
		})
	}
}

func TestNodeIDRetirementPreservesIndependentIDs(t *testing.T) {
	const body = `execution_type: system_node
subscribes_to: [task.requested, task.extra]
produces: [task.completed]
timers:
  - {id: reminder, event: timer.reminder, delay: 1m}
event_handlers:
  task.requested:
    activity: {id: send, tool: provider.send, input: {message: payload.message}}
`
	var node SystemNodeContract
	if err := decodeNodeTestYAML([]byte(body), &node); err != nil {
		t.Fatal(err)
	}
	if node.ExecutionType != "system_node" || !reflect.DeepEqual(node.SubscribesTo, []string{"task.requested", "task.extra"}) || !reflect.DeepEqual(node.Produces, []string{"task.completed"}) || node.Timers[0].ID != "reminder" || node.EventHandlers["task.requested"].Activity.ID != "send" {
		t.Fatalf("independent node behavior changed: %#v", node)
	}
	if err := decodeNodeTestYAML([]byte("id: worker\n"+body), &node); err == nil || !strings.Contains(err.Error(), retiredNodeID) {
		t.Fatalf("node ID accepted alongside legitimate IDs: %v", err)
	}
	for _, test := range []struct {
		name  string
		proof func(*testing.T)
	}{
		{"delivery join ID", TestFanOutDeliveryJoinStrictClosedGrammar},
		{"stage join identity", TestSystemNodeHandlerDecodeJoinCanonicalShape},
		{"loop identity", TestFlowSchemaDocumentDecodeBoundedLoopCanonicalSyntax},
		{"gate decision identity", TestFlowSchemaDocumentDecodeTypedStageGate},
		{"agent public name", TestAgentDeclarationIDPresenceMatrix},
	} {
		t.Run(test.name, test.proof)
	}
}

func TestNodeIdentityAdmissionAndArtifactReconstruction(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, scope := range []string{".", "left", "right", "left/nested"} {
		for _, value := range []string{"worker", "other", "worker-{instance_id}", `""`, "null", "{}", "[]"} {
			t.Run(scope+"/"+value, func(t *testing.T) {
				root := t.TempDir()
				for _, path := range []string{".", "left", "right", "left/nested"} {
					writeFixtureFile(t, filepath.Join(root, path, "schema.yaml"), "name: worker-flow\n")
				}
				file := filepath.Join(root, scope, "nodes.yaml")
				writeFixtureFile(t, file, "worker:\n  id: "+value+"\n  event_handlers: {}\n")
				if _, err := loadOptionalNodeDeclarations(file); err == nil || !strings.Contains(err.Error(), retiredNodeID) {
					t.Fatalf("file helper: %v", err)
				}
				if bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); bundle != nil || err == nil || !strings.Contains(err.Error(), retiredNodeID) || !strings.Contains(err.Error(), "nodes.yaml") {
					t.Fatalf("directory admission: bundle=%v error=%v", bundle, err)
				}
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
				if err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
				if bundle, err := LoadWorkflowContractBundleFromArtifact(repo, decoded, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{}); bundle != nil || err == nil || !strings.Contains(err.Error(), retiredNodeID) {
					t.Fatalf("reconstructed retired artifact: bundle=%v error=%v", bundle, err)
				}
			})
		}
	}
	t.Run("map identities survive source deletion", func(t *testing.T) {
		root := t.TempDir()
		for _, path := range []string{".", "left", "right", "left/nested"} {
			writeFixtureFile(t, filepath.Join(root, path, "schema.yaml"), "name: worker-flow\n")
			writeFixtureFile(t, filepath.Join(root, path, "nodes.yaml"), "worker:\n  event_handlers: {}\n")
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		baseline, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadWorkflowContractBundleFromArtifact(repo, decoded, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(baseline.ScopedNodeRecords(), loaded.ScopedNodeRecords()) {
			t.Fatal("reconstruction changed node declarations")
		}
		keys := map[string]bool{}
		for _, record := range loaded.ScopedNodeRecords() {
			ref, err := record.Identity()
			if err != nil || ref.NodeID() != "worker" || keys[ref.Key()] {
				t.Fatalf("scoped identity: %v %v", ref, err)
			}
			keys[ref.Key()] = true
		}
		if len(keys) != 4 {
			t.Fatalf("distinct declarations = %d", len(keys))
		}
	})
}

func TestSystemNodeContractRejectsRetiredIDOnPresence(t *testing.T) {
	for _, value := range []string{"worker", "other", "worker-{instance_id}", `""`, "null", "42", "true", "{}", "[]"} {
		t.Run(value, func(t *testing.T) {
			var nodes map[string]SystemNodeContract
			err := decodeNodeTestYAML([]byte("worker:\n  id: "+value+"\n  event_handlers: {}\n"), &nodes)
			if err == nil || !strings.Contains(err.Error(), retiredNodeID) {
				t.Fatalf("node.id %s: got %v, want %s", value, err, retiredNodeID)
			}
		})
	}
	t.Run("whole node alias", func(t *testing.T) {
		var node SystemNodeContract
		err := decodeNodeTestMember([]byte("definition: &node\n  id: worker\n  event_handlers: {}\nnode: *node\n"), "node", &node)
		if err == nil || !strings.Contains(err.Error(), retiredNodeID) {
			t.Fatalf("alias: %v", err)
		}
	})
	t.Run("merged retired id remains rejected", func(t *testing.T) {
		var node SystemNodeContract
		err := decodeNodeTestMember([]byte("definition: &fields\n  id: worker\nnode:\n  <<: *fields\n  event_handlers: {}\n"), "node", &node)
		if err == nil || !strings.Contains(err.Error(), retiredNodeID) {
			t.Fatalf("merged node.id must be rejected: %v", err)
		}
	})
	t.Run("omitted", func(t *testing.T) {
		var nodes map[string]SystemNodeContract
		if err := decodeNodeTestYAML([]byte("worker:\n  event_handlers: {}\n"), &nodes); err != nil {
			t.Fatal(err)
		}
	})
}
