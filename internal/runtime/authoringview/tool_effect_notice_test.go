package authoringview

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDescribeAgentToolEffectNoticeUsesScopedAuthoredDeclaration(t *testing.T) {
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: effect-notice\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "tools.yaml"), `probe:
  handler_type: http
  effect_class: read_only
  http: {method: GET, url: 'https://example.invalid'}
infra.ping:
  handler_type: mcp
  effect_class: non_idempotent_write
omitted:
  handler_type: http
  http: {method: GET, url: 'https://example.invalid'}
activity_only:
  handler_type: http
  effect_class: read_only
  http: {method: GET, url: 'https://example.invalid'}
query_entities:
  handler_type: platform_builtin
  effect_class: read_only
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), `first:
  role: first
  intent: {inline: Observe the first result.}
  model: regular
  tools: [probe, infra.ping, omitted, query_entities]
second:
  role: second
  intent: {inline: Observe the second result.}
  model: regular
  tools: [probe]
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "child", "agents.yaml"), `worker:
  role: worker
  intent: {inline: Observe the child result.}
  model: regular
  tools: [probe]
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "child", "tools.yaml"), `probe:
  handler_type: http
  effect_class: idempotent_write
  http: {method: GET, url: 'https://example.invalid/child'}
`)
	repo := authoringViewRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var previous []DiagnosticView
	for iteration := 0; iteration < 2; iteration++ {
		view, err := Build(t.Context(), semanticview.Wrap(bundle), BuildOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var notices []DiagnosticView
		for _, diagnostic := range view.Diagnostics {
			if diagnostic.CheckID == "agent_tool_effect_class_not_applied" {
				notices = append(notices, diagnostic)
			}
		}
		if len(notices) != 3 {
			t.Fatalf("notices=%+v, want root probe + child probe + non-HTTP MCP, each once", notices)
		}
		files := map[string]int{}
		for _, notice := range notices {
			if notice.Severity != "info" || !strings.Contains(notice.Message, "not applied to agent calls; an interrupted call is outcome-uncertain and not redispatched") || strings.Contains(notice.Location, "activity_only") || strings.Contains(notice.Location, "omitted") {
				t.Fatalf("incorrect informational scope: %+v", notice)
			}
			files[filepath.ToSlash(notice.AuthoredLocation)]++
		}
		if files["tools.yaml"] != 2 || files["child/tools.yaml"] != 1 {
			t.Fatalf("scoped source evidence lost: %+v", notices)
		}
		if iteration > 0 && !reflect.DeepEqual(previous, notices) {
			t.Fatal("notice order changed across renders")
		}
		previous = notices
		if _, err := json.Marshal(view); err != nil {
			t.Fatal(err)
		}
	}
}
