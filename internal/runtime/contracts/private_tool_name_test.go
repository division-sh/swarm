package contracts

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func privateToolNameBody(kind string) string {
	if kind == "in_process" {
		return "  category: provider_connector\n  handler_type: in_process\n  effect_class: non_idempotent_write\n  in_process: whatsapp.send_text\n"
	}
	abi, entry := "core-json-v1", "compute"
	if kind == "python" {
		abi, entry = "python-json-v1", "handle"
	}
	return fmt.Sprintf("  handler_type: %s\n  path: modules/pinned.bin\n  abi: %s\n  entry: %s\n  digest: sha256:%s\n  input_schema: {type: object, properties: {value: {type: integer}}}\n  output_schema: {type: object, properties: {value: {type: integer}}}\n  limits: {gas: 100, memory_pages: 16, output_bytes: 1024}\n", kind, abi, entry, strings.Repeat("0", 64))
}

func TestPrivateToolNameDeclarationAdmission(t *testing.T) {
	for _, kind := range []string{"in_process", "wasm", "python"} {
		for _, name := range []string{"Read", "Write", "Edit", "Bash", "WebSearch", "WebFetch", "read_file", "private.send", "emit_probe", "emit_"} {
			for _, depth := range []int{0, 1, 2, 5} {
				id := strings.Repeat(toolidentity.RuntimeToolsMCPPrefix, depth) + name
				t.Run(fmt.Sprintf("%s/%d/%s", kind, depth, name), func(t *testing.T) {
					entries, err := admitW5Tools(t, id+":\n"+privateToolNameBody(kind))
					valid := depth == 0 && (name == "read_file" || name == "private.send")
					if valid {
						if err != nil || entries[id].AgentExposable() {
							t.Fatalf("canonical private tool was not admitted privately: %v", err)
						}
					} else if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "private tool declaration") {
						t.Fatalf("ambiguous private declaration %q admitted or lost exact teaching error: %v", id, err)
					}
				})
			}
		}
	}
}

func TestPrivateToolNameTypedCompilationRefusesEveryScope(t *testing.T) {
	for _, kind := range []string{"in_process", "wasm", "python"} {
		entries, err := admitW5Tools(t, "private.send:\n"+privateToolNameBody(kind))
		if err != nil {
			t.Fatal(err)
		}
		for _, scope := range []string{"global", "root", "child", "inherited", "package_without_flow"} {
			t.Run(kind+"/"+scope, func(t *testing.T) {
				invalid := map[string]ToolSchemaEntry{"Read": entries["private.send"]}
				bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: &FlowContractView{Paths: FlowContractPaths{FlowPath: "."},
					Children: []FlowContractView{{Path: "child", Paths: FlowContractPaths{FlowPath: "child"}}}}}}
				switch scope {
				case "global":
					bundle.Tools = invalid
				case "root", "inherited":
					bundle.FlowTree.Root.Tools = invalid
				case "child":
					bundle.FlowTree.Root.Children[0].Tools = invalid
				case "package_without_flow":
					bundle.FlowTree.Root.Children[0].Tools = invalid
					bundle.FlowTree.Root.Children[0].Paths.FlowPath = ""
				}
				if err := CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), "private tool declaration") {
					t.Fatalf("typed compilation installed invalid %s private map: %v", scope, err)
				}
				bundle.Tools, bundle.FlowTree.Root.Tools, bundle.FlowTree.Root.Children[0].Tools = nil, nil, nil
				if _, err := CompileActivityToolBindings(bundle, map[string]map[string]ToolSchemaEntry{".": invalid}); err == nil || !strings.Contains(err.Error(), "private tool declaration") {
					t.Fatalf("activity compilation installed invalid private map: %v", err)
				}
			})
		}
	}
}

func TestPrivateToolNameRetainedSourceAdmission(t *testing.T) {
	for _, label := range []string{"tools.yaml", "child/tools.yaml"} {
		t.Run(label, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: private-name-admission\n")
			if strings.HasPrefix(label, "child/") {
				writeFixtureFile(t, filepath.Join(root, "child/schema.yaml"), "name: child\n")
			}
			writeFixtureFile(t, filepath.Join(root, label), "Read:\n"+privateToolNameBody("in_process"))
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(decoded.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"tree": artifact, "stored": decoded, "retained": retained} {
				if _, err := loadOptionalToolDeclarationsFromSource(source, label); err == nil || !strings.Contains(err.Error(), "Read") || !strings.Contains(err.Error(), label) {
					t.Fatalf("%s reconstruction lost exact private declaration refusal: %v", name, err)
				}
				if _, err := LoadWorkflowContractBundleFromArtifact(repoRootForContractsTest(t), source, "", WorkflowContractLoadOptions{}); err == nil || !strings.Contains(err.Error(), "Read") || !strings.Contains(err.Error(), "private tool declaration") {
					t.Fatalf("%s startup loader accepted invalid private source or failed at a different gate: %v", name, err)
				}
			}
		})
	}
}

func TestPrivateToolNameTypedCompilationPreservesRawKeys(t *testing.T) {
	entries, err := admitW5Tools(t, "private.send:\n"+privateToolNameBody("in_process"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", " Read ", " private.send ", "\tprivate.send"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			bundle := &WorkflowContractBundle{Tools: map[string]ToolSchemaEntry{name: entries["private.send"]}}
			if err := CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", name)) {
				t.Fatalf("typed compiler normalized or dropped invalid private ID: %v", err)
			}
		})
	}
}
