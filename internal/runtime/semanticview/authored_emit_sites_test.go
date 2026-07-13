package semanticview

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestAuthoredEmitSites_EnumeratesRootAndFlowOwnedScopes(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/authored_emit_sites_test.go:authoredEmitSiteNodeYAML"), canonicalrouting.SourceID("internal/runtime/semanticview/authored_emit_sites_test.go:authoredEmitSiteNodeYAMLWithGuardObject"), canonicalrouting.SourceID("internal/runtime/semanticview/authored_emit_sites_test.go:authoredEmitSiteRulesSuccessNodeYAML"))
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

func TestAuthoredEmitSites_EnumeratesOnSuccessEmitWithRules(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:    "root-node",
		rootRuleEmit:  "root.routed",
		rootOnSuccess: "root.audit",
	})

	sites := AuthoredEmitSites(source)
	ruleMatches := authoredEmitSitesByFlowNodeEvent(sites, "", "root-node", "root.routed")
	if len(ruleMatches) != 1 {
		t.Fatalf("expected one rules authored emit site, got %d: %#v", len(ruleMatches), authoredEmitSiteSummaries(sites))
	}
	if got := ruleMatches[0].Site; got != "handler.rules.emit" {
		t.Fatalf("rule site = %q, want handler.rules.emit", got)
	}
	successMatches := authoredEmitSitesByFlowNodeEvent(sites, "", "root-node", "root.audit")
	if len(successMatches) != 1 {
		t.Fatalf("expected one on_success authored emit site, got %d: %#v", len(successMatches), authoredEmitSiteSummaries(sites))
	}
	if got := successMatches[0].Site; got != "handler.on_success.emit" {
		t.Fatalf("success site = %q, want handler.on_success.emit", got)
	}
}

func TestAuthoredEmitSites_UsesRulesEmitTemplateEffectiveSite(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:       "root-node",
		rootTemplateEmit: true,
	})

	sites := AuthoredEmitSites(source)
	matches := authoredEmitSitesByFlowNodeEvent(sites, "", "root-node", "root.ready")
	if len(matches) != 2 {
		t.Fatalf("expected one effective template site per rule, got %d: %#v", len(matches), authoredEmitSiteSummaries(sites))
	}
	for _, match := range matches {
		if got := match.Site; got != "handler.rules.emit_template" {
			t.Fatalf("template site = %q, want handler.rules.emit_template", got)
		}
		for _, field := range []string{"shared", "bucket"} {
			if _, ok := match.Spec.Fields[field]; !ok {
				t.Fatalf("site %s missing merged field %s: %#v", match.RuleID, field, match.Spec.Fields)
			}
		}
	}
	if countAuthoredSitesWithSite(sites, "handler.emit") != 0 {
		t.Fatalf("raw handler.emit survived for template specialization: %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredSitesWithSite(sites, "handler.rules.emit") != 0 {
		t.Fatalf("raw handler.rules.emit survived for template specialization: %#v", authoredEmitSiteSummaries(sites))
	}
}

func TestAuthoredEmitSites_LowersEmitFromToCanonicalFields(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/authored_emit_sites_test.go:authoredEmitSiteRulesSuccessNodeYAML"))
	source := loadAuthoredEmitSiteLoweringFixture(t)

	sites := AuthoredEmitSites(source)
	matches := authoredEmitSitesByFlowNodeEvent(sites, "", "dispatcher", "market_research.scan_assigned")
	if len(matches) != 1 {
		t.Fatalf("expected one lowered authored emit site, got %d: %#v", len(matches), authoredEmitSiteSummaries(sites))
	}
	if matches[0].Spec.From != "" {
		t.Fatalf("semantic view retained authored emit.from = %q", matches[0].Spec.From)
	}
	if expr := matches[0].Spec.Fields["scan_id"]; expr.Kind != runtimecontracts.ExpressionKindCEL || expr.CEL != "entity.scan_id" {
		t.Fatalf("scan_id field = %#v, want CEL entity.scan_id", expr)
	}
	if expr := matches[0].Spec.Fields["geography"]; expr.Kind != runtimecontracts.ExpressionKindCEL || expr.CEL != "payload.geography" {
		t.Fatalf("geography field = %#v, want CEL payload.geography", expr)
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
	rootRuleEmit        string
	rootOnSuccess       string
	rootTemplateEmit    bool
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
platform_version: ">=0.7.0 <0.8.0"
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
root.routed: {}
root.audit: {}
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	rootNodeYAML := authoredEmitSiteNodeYAML(opts.rootNodeID, "root.start", opts.rootEmit, opts.rootGuardEmit, opts.rootBroadcast)
	if strings.TrimSpace(opts.rootRuleEmit) != "" || strings.TrimSpace(opts.rootOnSuccess) != "" {
		rootNodeYAML = authoredEmitSiteRulesSuccessNodeYAML(opts.rootNodeID, "root.start", opts.rootRuleEmit, opts.rootOnSuccess)
	}
	if opts.rootTemplateEmit {
		rootNodeYAML = authoredEmitSiteTemplateNodeYAML(opts.rootNodeID, "root.start", "root.ready")
	}
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

func loadAuthoredEmitSiteLoweringFixture(t *testing.T) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: authored-emit-site-lowering
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [scan.corpus_dispatch]
  outputs:
    events: [market_research.scan_assigned]
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), `
scan.corpus_dispatch:
  geography:
    type: string
  required: [geography]
market_research.scan_assigned:
  scan_id:
    type: string
  geography:
    type: string
  required: [scan_id, geography]
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
scan:
  scan_id:
    type: text
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
dispatcher:
  id: dispatcher
  execution_type: system_node
  event_handlers:
    scan.corpus_dispatch:
      emit:
        event: market_research.scan_assigned
        from: entity
        fields:
          geography: payload
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}

func authoredEmitSiteNodeYAML(nodeID, trigger, eventType, guardEventType string, broadcast bool) string {
	// routing-example-census: parser-only issue=none owner=semanticview.authored_emit_site proof=internal/runtime/semanticview/authored_emit_sites_test.go:TestAuthoredEmitSites_EnumeratesRootAndFlowOwnedScopes
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
	return nodeID + `:
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
	// routing-example-census: parser-only issue=none owner=semanticview.authored_emit_site proof=internal/runtime/semanticview/authored_emit_sites_test.go:TestAuthoredEmitSites_EnumeratesRootAndFlowOwnedScopes
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "        broadcast: true\n"
	}
	return nodeID + `:
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

func authoredEmitSiteRulesSuccessNodeYAML(nodeID, trigger, ruleEventType, successEventType string) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	return nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      on_success:
        emit: ` + successEventType + `
      rules:
        routed:
          condition: "else"
          emit: ` + ruleEventType + `
`
}

func authoredEmitSiteTemplateNodeYAML(nodeID, trigger, eventType string) string {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return "{}\n"
	}
	return nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      emit:
        event: ` + eventType + `
        fields:
          shared: payload.shared
      rules:
        high:
          condition: "payload.score >= 80"
          emit:
            fields:
              bucket: '"high"'
        low:
          condition: "else"
          emit:
            fields:
              bucket: '"low"'
`
}

func countAuthoredEmitSites(sites []AuthoredEmitSite, flowID, nodeID, eventType string) int {
	return len(authoredEmitSitesByFlowNodeEvent(sites, flowID, nodeID, eventType))
}

func countAuthoredSitesWithSite(sites []AuthoredEmitSite, site string) int {
	count := 0
	for _, candidate := range sites {
		if strings.TrimSpace(candidate.Site) == site {
			count++
		}
	}
	return count
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
