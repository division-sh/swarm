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

func TestPolicySheetLookupValueRowsAcceptsEmitFieldConsumer(t *testing.T) {
	handler := bootverifyLookupHandler(true, false)
	handler.Rules = append(handler.Rules, runtimecontracts.HandlerRuleEntry{
		ID:        "emit_template",
		Condition: "true",
		Emit: runtimecontracts.EmitSpec{
			Event: "repo.service_template_selected",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"template_path": runtimecontracts.CELExpression("computed.template_path"),
			},
		},
	})
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

func TestPolicySheetLookupValueRowsRejectsOwnLookupKeyAsConsumer(t *testing.T) {
	handler := bootverifyLookupHandler(true, false)
	handler.Rules[0].Compute.Lookup.On = []string{"computed.template_path"}
	handler.Rules[0].Compute.Lookup.OnPaths = []paths.Path{paths.Parse("computed.template_path")}

	findings := bootverifyLookupFindings(handler)
	if !bootverifyLookupFindingContains(findings, "is not consumed") {
		t.Fatalf("lookup findings = %#v, want own lookup key excluded from downstream consumers", findings)
	}
}

func TestPolicySheetLookupValueRowsRejectsPreBindingReadersAsConsumers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runtimecontracts.SystemNodeEventHandler)
	}{
		{
			name: "query executes before compute",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler) {
				handler.Query = &runtimecontracts.QuerySpec{Source: "computed.template_path"}
			},
		},
		{
			name: "entity selection executes before handler",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler) {
				handler.SelectEntity = &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{
					Field: "id",
					Ref:   "computed.template_path",
				}}}
			},
		},
		{
			name: "earlier policy compute row",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler) {
				earlier := bootverifyLookupConsumerRule("earlier", "computed.earlier", "computed.template_path")
				handler.Rules = append([]runtimecontracts.HandlerRuleEntry{earlier}, handler.Rules...)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := bootverifyLookupHandler(true, false)
			tc.mutate(&handler)
			findings := bootverifyLookupFindings(handler)
			if !findingContainsAll(findings, policySheetLookupCheckID, "lookup row scaffold_paths", "is not consumed") {
				t.Fatalf("lookup findings = %#v, want pre-binding reader excluded from scaffold_paths consumers", findings)
			}
		})
	}
}

func TestPolicySheetLookupValueRowsAcceptsLaterComputeConsumer(t *testing.T) {
	handler := bootverifyLookupHandler(true, false)
	handler.Rules = append(handler.Rules, bootverifyLookupConsumerRule("later", "computed.later", "computed.template_path"))
	findings := bootverifyLookupFindings(handler)
	if findingContainsAll(findings, policySheetLookupCheckID, "lookup row scaffold_paths", "is not consumed") {
		t.Fatalf("lookup findings = %#v, want later compute row accepted as scaffold_paths consumer", findings)
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

func bootverifyLookupConsumerRule(id, storeAs, firstInput string) runtimecontracts.HandlerRuleEntry {
	rule := bootverifyLookupHandler(true, false).Rules[0]
	lookup := *rule.Compute.Lookup
	lookup.On = []string{firstInput, "payload.language"}
	lookup.OnPaths = []paths.Path{paths.Parse(firstInput), paths.Parse("payload.language")}
	compute := *rule.Compute
	compute.StoreAs = storeAs
	compute.Lookup = &lookup
	policyRow := rule.PolicyRow
	policyRow.Lookup = &lookup
	rule.ID = id
	rule.Compute = &compute
	rule.PolicyRow = policyRow
	return rule
}

func bootverifyLookupFindingContains(findings []Finding, contains string) bool {
	for _, finding := range findings {
		if finding.CheckID == policySheetLookupCheckID && strings.Contains(finding.Message, contains) {
			return true
		}
	}
	return false
}
