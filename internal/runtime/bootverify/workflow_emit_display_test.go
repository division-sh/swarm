package bootverify

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestEmitCompletenessConsumesSharedRuleDisplayAndKeepsSingularJoins(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	for _, tc := range []struct {
		name   string
		source string
		labels []string
	}{
		{
			name:   "rules",
			source: "rules:\n  - when: payload.ready\n    emit: chosen\n  - else: true\n    emit: fallback",
			labels: []string{"rules[0]: when payload.ready.emit", "rules[1]: else.emit"},
		},
		{
			name:   "completion",
			source: "on_complete:\n  - condition: payload.ready\n    emit: chosen\n  - emit: fallback",
			labels: []string{"on_complete[0]: condition payload.ready.emit", "on_complete[1].emit"},
		},
		{
			name:   "join outcomes",
			source: "join:\n  id: membership-owner\n  stage: pending\n  members: {count: 1, by: payload.member_id}\n  output: payload.value\n  deadline: {after: 1h, from: stage_entry}\n  on_complete: {emit: chosen}\n  on_deadline: {emit: fallback}",
			labels: []string{"join.on_complete.emit", "join.on_deadline.emit"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: emit-display\nstages:\n  pending: {}\n  done: {final: true}\npins:\n  outputs: [chosen, fallback]\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "types.yaml"), "types:\n  JoinValue:\n    fields:\n      value: {type: text}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "request:\n  ready: {type: boolean}\n  member_id: {type: text}\n  value: {type: JoinValue}\nchosen:\nfallback:\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  execution_type: system_node\n  event_handlers:\n    request:\n      "+strings.ReplaceAll(tc.source, "\n", "\n      ")+"\n")
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			handler, err := runtimecontracts.QualifySystemNodeHandlerRuleRefsForEvent(node, "request", bundle.Nodes["worker"].EventHandlers["request"])
			if err != nil {
				t.Fatal(err)
			}
			sites := runtimecontracts.HandlerDeclarativeEmitSites(handler, nil)
			if len(sites) != len(tc.labels) {
				t.Fatalf("site census changed: %#v", sites)
			}
			for i, site := range sites {
				if got := payloadCompletenessDeclarativeSiteLabel(site); got != tc.labels[i] || !site.RuleRef.Valid() || site.Spec.EventType() != []string{"chosen", "fallback"}[i] {
					t.Fatalf("site[%d] lost display, exact identity or original output: %#v display=%q", i, site, got)
				}
			}
			if handler.Join != nil && handler.Join.EffectiveID() != "membership-owner" {
				t.Fatalf("outcome presentation changed join membership identity: %#v", handler.Join)
			}
		})
	}
}
