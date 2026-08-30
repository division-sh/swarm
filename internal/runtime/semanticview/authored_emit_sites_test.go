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
	if countAuthoredEmitSites(sites, ".", "root-node", "root.ready") != 1 {
		t.Fatalf("expected one root authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, ".", "root-node", "root.escalated") != 1 {
		t.Fatalf("expected one root guard escalation authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "support", "flow-node", "support.ready") != 1 {
		t.Fatalf("expected one flow authored emit site, got %#v", authoredEmitSiteSummaries(sites))
	}
	if countAuthoredEmitSites(sites, "extras", "extras-node", "support.ready") != 1 {
		t.Fatalf("expected one extras child-flow authored emit site, got %#v", authoredEmitSiteSummaries(sites))
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
	matches := authoredEmitSitesByFlowNodeEvent(sites, ".", "root-node", "root.escalated")
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
	ruleMatches := authoredEmitSitesByFlowNodeEvent(sites, ".", "root-node", "root.routed")
	if len(ruleMatches) != 1 {
		t.Fatalf("expected one rules authored emit site, got %d: %#v", len(ruleMatches), authoredEmitSiteSummaries(sites))
	}
	if got := ruleMatches[0].Site; got != "handler.rules.emit" {
		t.Fatalf("rule site = %q, want handler.rules.emit", got)
	}
	successMatches := authoredEmitSitesByFlowNodeEvent(sites, ".", "root-node", "root.audit")
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
	matches := authoredEmitSitesByFlowNodeEvent(sites, ".", "root-node", "root.ready")
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
	source := loadAuthoredEmitSiteLoweringFixture(t)

	sites := AuthoredEmitSites(source)
	matches := authoredEmitSitesByFlowNodeEvent(sites, ".", "dispatcher", "market_research.scan_assigned")
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

func TestAuthoredEmitSites_PreservesSameNodeIDAcrossDistinctFilesystemFlows(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID:          "root-node",
		rootEmit:            "root.ready",
		flowNodeID:          "support-node",
		flowEmit:            "support.ready",
		nestedPackageNodeID: "support-node",
		nestedPackageEmit:   "support.ready",
	})

	sites := AuthoredEmitSites(source)
	main := authoredEmitSitesByFlowNodeEvent(sites, "support", "support-node", "support.ready")
	addon := authoredEmitSitesByFlowNodeEvent(sites, "support/addon", "support-node", "support.ready")
	if len(main) != 1 || len(addon) != 1 {
		t.Fatalf("expected one authored site per filesystem flow, got main=%#v addon=%#v", authoredEmitSiteSummaries(main), authoredEmitSiteSummaries(addon))
	}
	if main[0].SourceScopeKey != "support" || addon[0].SourceScopeKey != "support/addon" {
		t.Fatalf("source scope keys = %q/%q, want exact flow paths", main[0].SourceScopeKey, addon[0].SourceScopeKey)
	}
}

type authoredEmitSiteFixture struct {
	rootNodeID          string
	rootEmit            string
	rootRuleEmit        string
	rootOnSuccess       string
	rootTemplateEmit    bool
	rootGuardEmit       string
	flowNodeID          string
	flowEmit            string
	extrasNodeID        string
	extrasEmit          string
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
	rootNodeYAML := authoredEmitSiteNodeYAML(opts.rootNodeID, "root.start", opts.rootEmit, opts.rootGuardEmit)
	if strings.TrimSpace(opts.rootRuleEmit) != "" || strings.TrimSpace(opts.rootOnSuccess) != "" {
		rootNodeYAML = authoredEmitSiteRulesSuccessNodeYAML(opts.rootNodeID, "root.start", opts.rootRuleEmit, opts.rootOnSuccess)
	}
	if opts.rootTemplateEmit {
		rootNodeYAML = authoredEmitSiteTemplateNodeYAML(opts.rootNodeID, "root.start", "root.ready")
	}
	if opts.rootGuardObject {
		rootNodeYAML = authoredEmitSiteNodeYAMLWithGuardObject(opts.rootNodeID, "root.start", opts.rootEmit, opts.rootGuardEmit)
	}
	if strings.TrimSpace(rootNodeYAML) != "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), rootNodeYAML)
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "schema.yaml"), `
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
	writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "events.yaml"), `
support.start: {}
support.ready: {}
`)
	if flowNodes := authoredEmitSiteNodeYAML(opts.flowNodeID, "support.start", opts.flowEmit, ""); strings.TrimSpace(flowNodes) != "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "nodes.yaml"), flowNodes)
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "schema.yaml"), "name: extras\nmode: static\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "events.yaml"), "extras.start: {}\n")
	if extraNodes := authoredEmitSiteNodeYAML(opts.extrasNodeID, "extras.start", opts.extrasEmit, ""); strings.TrimSpace(extraNodes) != "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "nodes.yaml"), extraNodes)
	}
	if strings.TrimSpace(opts.nestedPackageNodeID) != "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "addon", "schema.yaml"), "name: support-addon\nmode: static\n")
		writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "addon", "events.yaml"), "addon.start: {}\n")
		writeSemanticviewFixtureFile(t, filepath.Join(root, "support", "addon", "nodes.yaml"), authoredEmitSiteNodeYAML(opts.nestedPackageNodeID, "addon.start", opts.nestedPackageEmit, ""))
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
market_research.scan_assigned:
  scan_id:
    type: string
  geography:
    type: string
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
scan:
  scan_id:
    type: text
`)
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

func authoredEmitSiteNodeYAML(nodeID, trigger, eventType, guardEventType string) string {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return ""
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
`
}

func authoredEmitSiteNodeYAMLWithGuardObject(nodeID, trigger, eventType, guardEventType string) string {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(eventType) == "" {
		return ""
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
`
}

func authoredEmitSiteRulesSuccessNodeYAML(nodeID, trigger, ruleEventType, successEventType string) string {
	if strings.TrimSpace(nodeID) == "" {
		return ""
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
		return ""
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
		if strings.TrimSpace(site.FlowPathIdentity()) == flowID &&
			strings.TrimSpace(site.NodeID()) == nodeID &&
			strings.TrimSpace(site.Spec.EventType()) == eventType {
			out = append(out, site)
		}
	}
	return out
}

func authoredEmitSiteSummaries(sites []AuthoredEmitSite) []string {
	out := make([]string, 0, len(sites))
	for _, site := range sites {
		out = append(out, strings.Join([]string{site.FlowPathIdentity(), site.SourceScopeKey, site.NodeID(), site.HandlerEvent, site.Site, site.Spec.EventType()}, "|"))
	}
	sort.Strings(out)
	return out
}
