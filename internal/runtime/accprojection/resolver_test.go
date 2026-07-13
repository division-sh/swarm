package accprojection

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func projectionResolverBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension": {Type: "text"},
						"score":     {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {
						Type:            "[DimensionScore]",
						MaterializeFrom: "valid-node.dimensions_received",
					},
					"bad_scores": {
						Type:            "[DimensionScore]",
						MaterializeFrom: "bad-node.missing_buffer",
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"valid-node": {
				StateSchema: runtimecontracts.NodeStateSchema{
					Fields: []runtimecontracts.NodeStateField{{Name: "dimensions_received", Type: "[DimensionScore]"}},
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
					},
				},
			},
			"bad-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.bad_dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "missing_buffer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"dimension": {Type: "text"},
					"score":     {Type: "integer"},
				}},
			},
			"score.bad_dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"dimension": {Type: "text"},
					"score":     {Type: "integer"},
				}},
			},
		},
	}
}

func TestForHandler_FiltersProjectionIssuesToActiveHandler(t *testing.T) {
	source := semanticview.Wrap(projectionResolverBundle())
	resolved := Resolve(source)
	if len(resolved.Issues) == 0 {
		t.Fatal("Resolve issues = 0, want global invalid bad-node declaration")
	}

	bindings, issues := ForHandler(source, "", "valid-node", "score.dimension_complete")
	if len(issues) != 0 {
		t.Fatalf("ForHandler valid issues = %#v, want none", issues)
	}
	if len(bindings) != 1 {
		t.Fatalf("ForHandler valid bindings = %d, want 1", len(bindings))
	}

	_, issues = ForHandler(source, "", "bad-node", "score.bad_dimension_complete")
	if !issuesContain(issues, "missing_buffer") {
		t.Fatalf("ForHandler bad issues = %#v, want missing_buffer issue", issues)
	}
}

func TestForHandler_ResolvesQualifiedRuntimeEventToLocalProjectionBinding(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/accprojection/resolver_test.go:loadProjectionFlowBundle"))
	source := semanticview.Wrap(loadProjectionFlowBundle(t))

	result := ForHandlerWithAccumulator(source, "scoring", "scoring-node", "scoring/score.dimension_complete", "dimensions_received")
	if len(result.Issues) != 0 {
		t.Fatalf("ForHandler issues = %#v, want none", result.Issues)
	}
	if len(result.Bindings) != 1 {
		t.Fatalf("ForHandler bindings = %d, want 1", len(result.Bindings))
	}
	if got := result.Bindings[0].SourceEventType; got != "score.dimension_complete" {
		t.Fatalf("binding source event = %q, want authored local event", got)
	}
	if got := result.ExpectedBindingCount; got != 1 {
		t.Fatalf("ExpectedBindingCount = %d, want 1", got)
	}
}

func TestActiveHandlerResolution_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := semanticview.Wrap(loadProjectionImportBoundaryWildcardBundle(t, ""))
	active := activeHandlerResolution(source, "worker-listener", "producer/task.done")
	if active.AccumulatorName != "" || active.AuthoredEventType != "" || active.CanonicalEventType != "" {
		t.Fatalf("active handler = %#v, want empty for ungranted sibling event", active)
	}
	if handlerEventMatches(source, "worker-listener", "**/task.done", "producer/task.done", active) {
		t.Fatal("handlerEventMatches accepted ungranted sibling event")
	}
}

func TestActiveHandlerResolution_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/accprojection/resolver_test.go:loadProjectionFlowBundle"))
	source := semanticview.Wrap(loadProjectionImportBoundaryWildcardBundle(t, "      observe:\n        - source: producer\n          events: [task.done]\n"))
	active := activeHandlerResolution(source, "worker-listener", "producer/task.done")
	if got := active.AccumulatorName; got != "tasks" {
		t.Fatalf("ActiveAccumulatorName = %q, want tasks", got)
	}
	if got := active.AuthoredEventType; got != "**/task.done" {
		t.Fatalf("ActiveAuthoredEventType = %q, want **/task.done", got)
	}
	if !handlerEventMatches(source, "worker-listener", "**/task.done", "producer/task.done", active) {
		t.Fatal("handlerEventMatches denied granted sibling event")
	}
}

func issuesContain(issues []Issue, want string) bool {
	for _, issue := range issues {
		if strings.Contains(issue.Message, want) {
			return true
		}
	}
	return false
}

func loadProjectionFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: projection-flow
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: projection-flow\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
initial_state: discovered
states: [discovered, scored]
terminal_states: [scored]
pins:
  inputs:
    events: []
  outputs:
    events:
      - score.dimension_complete
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "types.yaml"), `
types:
  DimensionScore:
    dimension: text
    score: integer
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
vertical:
  scores:
    type: "[DimensionScore]"
    initial: []
    materialize_from: scoring-node.dimensions_received
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), `
score.dimension_complete:
  dimension: text
  score: integer
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), `
scoring-node:
  id: scoring-node
  execution_type: system_node
  event_handlers:
    score.dimension_complete:
      accumulate:
        into: dimensions_received
  state_schema:
    fields:
      dimensions_received: "[DimensionScore]"
`)

	repoRoot := repoRootForProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadProjectionImportBoundaryWildcardBundle(t *testing.T, observeGrant string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	workerBind := ""
	if strings.TrimSpace(observeGrant) != "" {
		workerBind = "    bind:\n" + observeGrant
	}
	writeProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: projection-import-boundary-wildcard
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: static
`+workerBind+`  - id: producer
    flow: producer
    mode: static
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: projection-import-boundary-wildcard\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), "name: worker\nversion: \"1.0.0\"\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
initial_state: active
states: [active, done]
terminal_states: [done]
pins:
  outputs:
    events: [task.done]
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), `
task.done:
  task_id: text
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done":
      accumulate:
        into: tasks
  state_schema:
    fields:
      tasks: text
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), "name: producer\nversion: \"1.0.0\"\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
initial_state: active
states: [active, done]
terminal_states: [done]
pins:
  outputs:
    events: [task.done]
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
task.done:
  task_id: text
`)
	writeProjectionFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")

	repoRoot := repoRootForProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writeProjectionFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func repoRootForProjectionTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
