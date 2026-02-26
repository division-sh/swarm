package main

import (
	"go/ast"
	"go/parser"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func parseExpr(t *testing.T, src string) ast.Expr {
	t.Helper()
	expr, err := parser.ParseExpr(src)
	if err != nil {
		t.Fatalf("parse expr %q: %v", src, err)
	}
	return expr
}

func TestParseAndResolveHelpers(t *testing.T) {
	t.Run("unquote", func(t *testing.T) {
		if got, err := unquote(`"x"`); err != nil || got != "x" {
			t.Fatalf("quoted string: got=%q err=%v", got, err)
		}
		if got, err := unquote("`y`"); err != nil || got != "y" {
			t.Fatalf("raw string: got=%q err=%v", got, err)
		}
		if _, err := unquote("z"); err == nil {
			t.Fatal("expected unquote error for bare token")
		}
	})

	t.Run("callName and stringLiteral", func(t *testing.T) {
		if got := callName(parseExpr(t, "mustJSON")); got != "mustJSON" {
			t.Fatalf("unexpected callName ident: %q", got)
		}
		if got := callName(parseExpr(t, "json.Marshal")); got != "json.Marshal" {
			t.Fatalf("unexpected callName selector: %q", got)
		}
		if s, ok := stringLiteral(parseExpr(t, `"hello"`)); !ok || s != "hello" {
			t.Fatalf("stringLiteral mismatch: ok=%v s=%q", ok, s)
		}
		if _, ok := stringLiteral(parseExpr(t, "x")); ok {
			t.Fatal("expected non-literal to fail")
		}
	})

	t.Run("parseEventTypeExpr", func(t *testing.T) {
		if got := parseEventTypeExpr(parseExpr(t, `events.EventType("scan.requested")`)); got != "scan.requested" {
			t.Fatalf("event type call parse failed: %q", got)
		}
		if got := parseEventTypeExpr(parseExpr(t, `"vertical.shortlisted"`)); got != "vertical.shortlisted" {
			t.Fatalf("event type literal parse failed: %q", got)
		}
	})

	t.Run("parseMapCompositeLiteral", func(t *testing.T) {
		fs := parseMapCompositeLiteral(parseExpr(t, `map[string]any{"vertical_id":"v","geography":"us"}`).(*ast.CompositeLit))
		if fs.dynamic {
			t.Fatal("expected map literal to be static")
		}
		if _, ok := fs.guaranteed["vertical_id"]; !ok {
			t.Fatalf("missing expected key: %+v", fs.guaranteed)
		}
	})

	t.Run("resolveFieldSet", func(t *testing.T) {
		vars := map[string]fieldSet{
			"payload": newFieldSet(false, "vertical_id", "geography"),
		}
		fromVar := resolveFieldSet(parseExpr(t, "payload"), vars)
		if fromVar.dynamic || len(fromVar.guaranteed) != 2 {
			t.Fatalf("resolve variable mismatch: %+v", fromVar)
		}
		fromMustJSON := resolveFieldSet(parseExpr(t, `mustJSON(map[string]any{"scan_id":"s"})`), vars)
		if fromMustJSON.dynamic {
			t.Fatalf("mustJSON map should remain static: %+v", fromMustJSON)
		}
		if _, ok := fromMustJSON.guaranteed["scan_id"]; !ok {
			t.Fatalf("missing mustJSON key: %+v", fromMustJSON.guaranteed)
		}
		typed := resolveFieldSet(parseExpr(t, "pc.buildValidationStartedPayload(\"v\", nil, nil)"), vars)
		if _, ok := typed.guaranteed["vertical_name"]; !ok {
			t.Fatalf("expected typed constructor fields: %+v", typed.guaranteed)
		}
		dynamic := resolveFieldSet(parseExpr(t, "unknownPayloadBuilder()"), vars)
		if !dynamic.dynamic {
			t.Fatalf("unknown builder should be dynamic: %+v", dynamic)
		}
	})

	t.Run("resolvePayloadFieldSet", func(t *testing.T) {
		vars := map[string]fieldSet{
			"payload": newFieldSet(false, "campaign_id"),
		}
		got := resolvePayloadFieldSet(parseExpr(t, "mustJSON(payload)"), vars)
		if _, ok := got.guaranteed["campaign_id"]; !ok {
			t.Fatalf("resolve payload field set should deref mustJSON args: %+v", got.guaranteed)
		}
	})
}

func TestContractAndPromptMatchingHelpers(t *testing.T) {
	siteA := emitSite{eventType: "validation.started", file: "a.go", line: 10, fields: newFieldSet(false, "vertical_id", "geography")}
	siteB := emitSite{eventType: "validation.started", file: "b.go", line: 12, fields: newFieldSet(false, "vertical_id", "scoring")}
	siteC := emitSite{eventType: "scan.requested", file: "c.go", line: 20, fields: newFieldSet(true, "scan_id")}

	contracts := buildContracts([]emitSite{siteA, siteB, siteC})
	v := contracts["validation.started"]
	if v == nil {
		t.Fatal("expected validation.started contract")
	}
	// guaranteed intersection across sites => vertical_id only.
	if _, ok := v.guaranteed["vertical_id"]; !ok || len(v.guaranteed) != 1 {
		t.Fatalf("unexpected guaranteed intersection: %+v", v.guaranteed)
	}
	if _, ok := v.union["geography"]; !ok {
		t.Fatalf("expected union fields to include geography: %+v", v.union)
	}
	if !contracts["scan.requested"].dynamicAny {
		t.Fatal("expected dynamicAny true for dynamic site")
	}

	prompt := strings.Join([]string{
		"validation.started:",
		"- payload contains vertical_id and geography",
		"scan.requested:",
		"- read scan_id and mode from payload",
	}, "\n")
	sections := extractEventSections(prompt, "validation.started")
	if len(sections) != 1 {
		t.Fatalf("expected one matching section, got %d", len(sections))
	}
	expected := expectedFieldsForEvent(prompt, "validation.started", []string{"vertical_id", "geography", "scoring"})
	if len(expected) == 0 || expected[0] != "geography" && expected[0] != "vertical_id" {
		t.Fatalf("expected fields extraction failed: %+v", expected)
	}
	if !isEventHeaderLine("validation.started:") || isEventHeaderLine("not an event header") {
		t.Fatal("event header detection mismatch")
	}
	if !isInputHintLine("payload contains vertical_id") || isInputHintLine("just text") {
		t.Fatal("input hint detection mismatch")
	}
	if !mentionsField("read vertical id", "vertical_id") {
		t.Fatal("field alias detection failed")
	}
	if !containsWord("scan_id available", "scan_id") {
		t.Fatal("containsWord should match token boundary")
	}
	if !matchesSubscription("scan.*", "scan.requested") || matchesSubscription("scan.requested", "scan.completed") {
		t.Fatal("subscription matcher mismatch")
	}

	agents := []agentSpec{{
		id:            "business-research-agent",
		role:          "business-research-agent",
		file:          "agent.yaml",
		subscriptions: []string{"validation.started"},
		systemPrompt:  "validation.started: payload contains vertical_id and geography and scoring.",
	}}
	findings := auditContractsAgainstPrompts(contracts, agents)
	if len(findings) == 0 {
		t.Fatal("expected finding when prompt expects fields beyond guaranteed contract")
	}
	if findings[0].eventType != "validation.started" {
		t.Fatalf("unexpected finding event: %+v", findings[0])
	}
}

func TestCollectEmitSitesAndLoadAgentSpecs(t *testing.T) {
	tmp := t.TempDir()
	runtimeDir := filepath.Join(tmp, "runtime")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatalf("mkdir runtime: %v", err)
	}
	runtimeFile := filepath.Join(runtimeDir, "emit.go")
	src := strings.Join([]string{
		"package runtime",
		"func f(pc *FactoryPipelineCoordinator, bus *EventBus) {",
		`  payload := map[string]any{"vertical_id":"v","geography":"us"}`,
		`  pc.publish(nil, "validation.started", "factory", payload)`,
		`  _ = bus.Publish(nil, events.Event{Type: events.EventType("scan.requested"), SourceAgent: "runtime", Payload: mustJSON(map[string]any{"scan_id":"s","mode":"saas_gap"})})`,
		"}",
	}, "\n")
	if err := os.WriteFile(runtimeFile, []byte(src), 0o644); err != nil {
		t.Fatalf("write runtime file: %v", err)
	}
	sites, err := collectEmitSites(runtimeDir)
	if err != nil {
		t.Fatalf("collect emit sites: %v", err)
	}
	if len(sites) < 2 {
		t.Fatalf("expected at least two emit sites, got %d", len(sites))
	}

	agentsDir := filepath.Join(tmp, "agents")
	templatesDir := filepath.Join(tmp, "templates")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		t.Fatalf("mkdir agents: %v", err)
	}
	if err := os.MkdirAll(templatesDir, 0o755); err != nil {
		t.Fatalf("mkdir templates: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentsDir, "roster.yaml"), []byte("agents: {}\n"), 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	if err := os.WriteFile(filepath.Join(templatesDir, "routes.yaml"), []byte("bootstrap: []\nseeded: []\n"), 0o644); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	agentYAML := strings.Join([]string{
		"id: business-research-agent",
		"role: business-research-agent",
		"subscriptions:",
		"  - validation.started",
		"system_prompt: |",
		"  validation.started: read vertical_id and geography from payload",
	}, "\n")
	if err := os.WriteFile(filepath.Join(agentsDir, "business-research-agent.yaml"), []byte(agentYAML), 0o644); err != nil {
		t.Fatalf("write agent yaml: %v", err)
	}

	specs, err := loadAgentSpecs(agentsDir, templatesDir)
	if err != nil {
		t.Fatalf("load agent specs: %v", err)
	}
	if len(specs) != 1 || specs[0].id != "business-research-agent" {
		t.Fatalf("unexpected specs: %+v", specs)
	}
}

func TestReportAndMiscHelpers(t *testing.T) {
	contracts := map[string]*eventContract{
		"validation.started": {
			eventType:  "validation.started",
			guaranteed: map[string]struct{}{"vertical_id": {}},
			union:      map[string]struct{}{"vertical_id": {}, "geography": {}},
			dynamicAny: false,
			sites: []emitSite{
				{eventType: "validation.started", file: "internal/runtime/pipeline_coordinator.go", line: 123},
			},
		},
	}
	findings := []finding{{
		eventType:      "validation.started",
		agentID:        "business-research-agent",
		agentRole:      "business-research-agent",
		subscription:   "validation.started",
		expectedFields: []string{"vertical_id", "geography"},
		guaranteed:     []string{"vertical_id"},
		missing:        []string{"geography"},
	}}
	report := buildReport(contracts, findings, "internal/runtime", []string{"configs/agents", "configs/agents/templates"})
	if !strings.Contains(report, "# Runtime Payload Completeness Audit") {
		t.Fatalf("unexpected report header: %s", report)
	}
	if !strings.Contains(report, "validation.started") {
		t.Fatalf("expected event in report: %s", report)
	}

	outPath := filepath.Join(t.TempDir(), "docs", "reports", "runtime-payload-audit.md")
	if err := writeFile(outPath, []byte(report)); err != nil {
		t.Fatalf("write report: %v", err)
	}
	if b, err := os.ReadFile(outPath); err != nil || len(b) == 0 {
		t.Fatalf("read report: len=%d err=%v", len(b), err)
	}

	if got := keysSorted(map[string]struct{}{"b": {}, "a": {}}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("keysSorted mismatch: %+v", got)
	}
	if got := diff([]string{"a", "b"}, []string{"b"}); len(got) != 1 || got[0] != "a" {
		t.Fatalf("diff mismatch: %+v", got)
	}
	if got := asString(42); got != "42" {
		t.Fatalf("asString mismatch: %q", got)
	}
	if out := toStringSlice([]any{" a ", "", "b"}); len(out) != 2 || out[0] != "a" || out[1] != "b" {
		t.Fatalf("toStringSlice []any mismatch: %+v", out)
	}
	if out := toStringSlice([]string{" x ", "", "y"}); len(out) != 2 || out[0] != "x" || out[1] != "y" {
		t.Fatalf("toStringSlice []string mismatch: %+v", out)
	}
}
