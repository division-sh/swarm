package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPolicySheetLookupValueRowsAcceptsConsumedDefaultedInlineLookup(t *testing.T) {
	handler := bootverifyLookupHandler(true, true)
	findings := bootverifyLookupFindings(handler)
	if len(findings) != 0 {
		t.Fatalf("lookup findings = %#v, want none", findings)
	}
}

func TestPolicySheetLookupValueRowsRequiresDefaultForOpenDomains(t *testing.T) {
	handler := bootverifyLookupHandler(false, true)
	findings := bootverifyLookupFindings(handler)
	if !bootverifyLookupFindingContains(findings, "lookup.default: fail is required") {
		t.Fatalf("lookup findings = %#v, want missing default failure", findings)
	}
}

func TestPolicySheetLookupValueRowsRejectsDeadBinding(t *testing.T) {
	handler := bootverifyLookupHandler(true, false)
	findings := bootverifyLookupFindings(handler)
	if !bootverifyLookupFindingContains(findings, "is not consumed") {
		t.Fatalf("lookup findings = %#v, want dead binding failure", findings)
	}
}

func TestPolicySheetLookupValueRowsTypeChecksPayloadKeys(t *testing.T) {
	handler := bootverifyLookupHandler(true, true)
	handler.Rules[0].Compute.Lookup.Entries[0].Key[1] = runtimecontracts.ComputeLookupLiteral{
		Value:     int64(1),
		Kind:      "int",
		Canonical: "int:1",
		Summary:   "1",
	}
	findings := bootverifyLookupFindings(handler)
	if !bootverifyLookupFindingContains(findings, "payload.language") || !bootverifyLookupFindingContains(findings, "has type int") {
		t.Fatalf("lookup findings = %#v, want key type mismatch", findings)
	}
}

func bootverifyLookupFindings(handler runtimecontracts.SystemNodeEventHandler) []Finding {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"repo_scaffold": {
				ID: "repo_scaffold",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"repo.scaffold_requested": handler,
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo.scaffold_requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scaffold_type": {Type: "string"},
						"language":      {Type: "string"},
					},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	return checkPolicySheetLookupValueRows(newCheckerContext(context.Background(), source, Options{}))
}

func bootverifyLookupHandler(defaultDeclared, includeConsumer bool) runtimecontracts.SystemNodeEventHandler {
	lookup := &runtimecontracts.ComputeLookupSpec{
		RowID: "scaffold_paths",
		On:    []string{"payload.scaffold_type", "payload.language"},
		OnPaths: []paths.Path{
			paths.Parse("payload.scaffold_type"),
			paths.Parse("payload.language"),
		},
		DefaultDeclared: defaultDeclared,
		DefaultFail:     defaultDeclared,
		Entries: []runtimecontracts.ComputeLookupEntry{{
			Key: []runtimecontracts.ComputeLookupLiteral{
				{Value: "service", Kind: "string", Canonical: "string:\"service\"", Summary: `"service"`},
				{Value: "go", Kind: "string", Canonical: "string:\"go\"", Summary: `"go"`},
			},
			Value:        "templates/service/go",
			ValueKind:    "string",
			ValueSummary: `"templates/service/go"`,
		}},
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "scaffold_paths",
			PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindLookup, Lookup: lookup},
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpLookup,
				StoreAs:   "computed.template_path",
				Lookup:    lookup,
			},
		}},
	}
	if includeConsumer {
		handler.Rules = append(handler.Rules, runtimecontracts.HandlerRuleEntry{
			ID:        "service_route",
			Condition: `computed.template_path == "templates/service/go"`,
		})
	}
	return handler
}

func bootverifyLookupFindingContains(findings []Finding, contains string) bool {
	for _, finding := range findings {
		if finding.CheckID == policySheetLookupCheckID && strings.Contains(finding.Message, contains) {
			return true
		}
	}
	return false
}
