package semanticview

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestAuthoredEmitSites_EnumeratesRootAndFlowOwnedScopes(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:    "root-node",
		rootEmit:      "root.ready",
		rootGuardEmit: "root.escalated",
		flowNodeID:    "flow-node",
		flowEmit:      "support.ready",
		extrasNodeID:  "extras-node",
		extrasEmit:    "support.ready",
	})

	sites := AuthoredEmitSites(source)
	if countAuthoredEmitSites(sites, "", "root-node", "root.ready") != 1 {
		t.Fatalf("expected one root authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "", "root-node", "root.escalated") != 1 {
		t.Fatalf("expected one root guard escalation authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "support", "flow-node", "support.ready") != 1 {
		t.Fatalf("expected one flow authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "support", "extras-node", "support.ready") != 1 {
		t.Fatalf("expected one sole-parent package authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
}

func TestAuthoredEmitSites_GuardEscalationObjectCarriesFields(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:      "root-node",
		rootEmit:        "root.ready",
		rootGuardEmit:   "root.escalated",
		rootGuardObject: true,
	})

	sites := AuthoredEmitSites(source)
	matches := authoredEmitSitesByFlowNodeEvent(sites, "", "root-node", "root.escalated")
	if len(matches) != 1 {
		t.Fatalf("expected one guard escalation authored emit site, got %d: %#v", len(matches), authoredEmitSiteSummaries(sites))
	}
	if matches[0].Site != "handler.guard.on_fail.escalate" {
		t.Fatalf("site = %q, want handler.guard.on_fail.escalate", matches[0].Site)
	}
	if expr := matches[0].Spec.Fields["score"]; expr.Kind != runtimecontracts.ExpressionKindCEL || expr.CEL != "payload.score" {
		t.Fatalf("score field = %#v, want CEL payload.score", expr)
	}
	if expr := matches[0].Spec.Fields["reason"]; expr.Kind != runtimecontracts.ExpressionKindLiteral || expr.Literal != "score_below_threshold" {
		t.Fatalf("reason field = %#v, want literal score_below_threshold", expr)
	}
}

func TestAuthoredEmitSites_DeduplicatesPackageProjectionWithoutCollapsingDistinctSources(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:      "root-node",
		rootEmit:        "root.ready",
		flowNodeID:      "support-node",
		flowEmit:        "support.ready",
		extrasNodeID:    "support-node",
		extrasEmit:      "support.ready",
		extrasBroadcast: true,
	})

	sites := AuthoredEmitSites(source)
	matches := authoredEmitSitesByFlowNodeEvent(sites, "support", "support-node", "support.ready")
	if len(matches) != 2 {
		t.Fatalf("expected two distinct support-node authored sites, got %d: %#v", len(matches), authoredEmitSiteSummaries(matches))
	}
	keys := []string{matches[0].SourceScopeKey, matches[1].SourceScopeKey}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "extras,flows/support" {
		t.Fatalf("source scope keys = %v, want extras and flows/support; sites=%#v", keys, authoredEmitSiteSummaries(matches))
	}
}

func TestAuthoredEmitSites_OutsidePackageDoesNotSuppressGenericFlowScope(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		flowNodeID:      "flow-node",
		flowEmit:        "support.ready",
		extrasNodeID:    "extras-node",
		extrasEmit:      "support.ready",
		omitFlowPackage: true,
	})

	sites := AuthoredEmitSites(source)
	if countAuthoredEmitSites(sites, "support", "flow-node", "support.ready") != 1 {
		t.Fatalf("expected generic flow scope site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "support", "extras-node", "support.ready") != 1 {
		t.Fatalf("expected outside package site, got %#v", authoredEmitSiteSummaries(sites))
	}
}

func TestAuthoredEmitSites_NestedFlowPackageDoesNotSuppressMainFlowScope(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		flowNodeID:          "flow-node",
		flowEmit:            "support.ready",
		nestedPackageNodeID: "nested-node",
		nestedPackageEmit:   "support.ready",
		omitFlowPackage:     true,
	})

	sites := AuthoredEmitSites(source)
	if countAuthoredEmitSites(sites, "support", "flow-node", "support.ready") != 1 {
		t.Fatalf("expected main flow scope site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "support", "nested-node", "support.ready") != 1 {
		t.Fatalf("expected nested package site, got %#v", authoredEmitSiteSummaries(sites))
	}
}

type authoredEmitSiteFixture struct {
	rootNodeID          string
	rootEmit            string
	rootGuardEmit       string
	rootBroadcast       bool
	flowNodeID          string
	flowEmit            string
	flowBroadcast       bool
	extrasNodeID        string
	extrasEmit          string
	extrasBroadcast     bool
	omitFlowPackage     bool
	nestedPackageNodeID string
	nestedPackageEmit   string
	rootGuardObject     bool
}

func loadAuthoredEmitSiteFixture(t *testing.T, opts authoredEmitSiteFixture) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	nestedPackageRef := ""
	if strings.TrimSpace(opts.nestedPackageNodeID) != "" {
		nestedPackageRef = "  - path: flows/support/addon\n"
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: authored-emit-site-fixture
version: "1.0.0"
platform_version: ">=1.6.0"
packages:
  - path: extras
`+nestedPackageRef+`
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: authored-emit-site-fixture
pins:
  outputs:
    events: [root.ready]
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.start: {}
root.ready: {}
root.escalated: {}
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	rootNodeYAML := authoredEmitSiteNodeYAML(opts.rootNodeID, "root.start", opts.rootEmit, opts.rootGuardEmit, opts.rootBroadcast)
	if opts.rootGuardObject {
		rootNodeYAML = authoredEmitSiteNodeYAMLWithGuardObject(opts.rootNodeID, "root.start", opts.rootEmit, opts.rootGuardEmit, opts.rootBroadcast)
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), rootNodeYAML)
	if !opts.omitFlowPackage {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [support.start]
  outputs:
    events: [support.ready]
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support.start: {}
support.ready: {}
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), authoredEmitSiteNodeYAML(opts.flowNodeID, "support.start", opts.flowEmit, "", opts.flowBroadcast))
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "nodes.yaml"), authoredEmitSiteNodeYAML(opts.extrasNodeID, "support.start", opts.extrasEmit, "", opts.extrasBroadcast))
	if strings.TrimSpace(opts.nestedPackageNodeID) != "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "addon", "package.yaml"), `
name: support-addon
version: "1.0.0"
flows: []
`)
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "addon", "nodes.yaml"), authoredEmitSiteNodeYAML(opts.nestedPackageNodeID, "support.start", opts.nestedPackageEmit, "", false))
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}

func authoredEmitSiteNodeYAML(nodeID, trigger, eventType, guardEventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "        broadcast: true\n"
	}
	guardYAML := ""
	if strings.TrimSpace(guardEventType) != "" {
		guardYAML = `      guard:
        id: guard-escalate
        check: "false"
        on_fail: "escalate:` + guardEventType + `"
`
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
` + guardYAML + `
      emit:
        event: ` + eventType + `
` + broadcastLine
}

func authoredEmitSiteNodeYAMLWithGuardObject(nodeID, trigger, eventType, guardEventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "        broadcast: true\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      guard:
        id: guard-escalate
        check: "false"
        on_fail:
          escalate:
            event: ` + guardEventType + `
            fields:
              score: payload.score
              reason:
                literal: score_below_threshold

      emit:
        event: ` + eventType + `
` + broadcastLine
}

func countAuthoredEmitSites(sites []AuthoredEmitSite, flowID, nodeID, eventType string) int {
	return len(authoredEmitSitesByFlowNodeEvent(sites, flowID, nodeID, eventType))
}

func authoredEmitSitesByFlowNodeEvent(sites []AuthoredEmitSite, flowID, nodeID, eventType string) []AuthoredEmitSite {
	out := []AuthoredEmitSite{}
	for _, site := range sites {
		if strings.TrimSpace(site.FlowID) == flowID &&
			strings.TrimSpace(site.NodeID) == nodeID &&
			strings.TrimSpace(site.Spec.EventType()) == eventType {
			out = append(out, site)
		}
	}
	return out
}

func authoredEmitSiteSummaries(sites []AuthoredEmitSite) []string {
	out := make([]string, 0, len(sites))
	for _, site := range sites {
		out = append(out, strings.Join([]string{site.FlowID, site.SourceScopeKey, site.NodeID, site.HandlerEvent, site.Site, site.Spec.EventType()}, "|"))
	}
	sort.Strings(out)
	return out
}
