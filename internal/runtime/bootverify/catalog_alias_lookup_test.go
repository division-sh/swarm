package bootverify

import (
	"context"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func TestPolicySheetLookupAdmittedAliasDomains(t *testing.T) {
	for _, tc := range []struct {
		name, base string
		literal    c.ComputeLookupLiteral
	}{
		{"text", "text", c.ComputeLookupLiteral{Kind: "string", Canonical: "string:ready", Value: "ready"}},
		{"number", "numeric", c.ComputeLookupLiteral{Kind: "number", Canonical: "number:1", Value: float64(1)}},
		{"integer", "integer", c.ComputeLookupLiteral{Kind: "int", Canonical: "int:1", Value: int64(1)}},
		{"boolean", "boolean", c.ComputeLookupLiteral{Kind: "bool", Canonical: "bool:true", Value: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &c.WorkflowContractBundle{
				RootTypes: c.TypeCatalogDocument{Scalars: map[string]c.ScalarTypeDecl{"Alias": {Base: tc.base}}},
				Events:    map[string]c.EventCatalogEntry{"requested": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"key": {Type: "Alias"}}}}},
			}
			source := compileBootverifyRootSource(bundle)
			ref := policySheetLookupRef{EventType: "requested"}
			spec := &c.ComputeLookupSpec{On: []string{"payload.key"}, OnPaths: []paths.Path{paths.Parse("payload.key")}, Entries: []c.ComputeLookupEntry{{Key: []c.ComputeLookupLiteral{tc.literal}}}}
			if got := validatePolicySheetLookupKeyTypes(source, ref, spec); len(got) != 0 {
				t.Fatalf("correct aliased literal refused: %#v", got)
			}
			wrong := c.ComputeLookupLiteral{Kind: "string", Canonical: "string:wrong", Value: "wrong"}
			if tc.literal.Kind == "string" {
				wrong = c.ComputeLookupLiteral{Kind: "bool", Canonical: "bool:true", Value: true}
			}
			spec.Entries[0].Key[0] = wrong
			if got := validatePolicySheetLookupKeyTypes(source, ref, spec); len(got) == 0 {
				t.Fatal("wrong aliased literal accepted")
			}
			if tc.base == "boolean" {
				spec.Entries = []c.ComputeLookupEntry{{Key: []c.ComputeLookupLiteral{tc.literal}}}
				if policySheetLookupDomainsClosedAndExhaustive(source, ref, spec, nil) {
					t.Fatal("incomplete bool table accepted")
				}
				spec.Entries = append(spec.Entries, c.ComputeLookupEntry{Key: []c.ComputeLookupLiteral{{Kind: "bool", Canonical: "bool:false", Value: false}}})
				if !policySheetLookupDomainsClosedAndExhaustive(source, ref, spec, nil) {
					t.Fatal("complete aliased bool table refused")
				}
			}
		})
	}
	for _, refType := range []string{"object", "[text]", "map[text]text"} {
		bundle := &c.WorkflowContractBundle{Events: map[string]c.EventCatalogEntry{"requested": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"key": {Type: refType}}}}}}
		source := compileBootverifyRootSource(bundle)
		spec := &c.ComputeLookupSpec{On: []string{"payload.key"}, OnPaths: []paths.Path{paths.Parse("payload.key")}, DefaultDeclared: true, DefaultFail: true, Entries: []c.ComputeLookupEntry{{Key: []c.ComputeLookupLiteral{{Kind: "string", Canonical: "string:x"}}}}}
		if got := validatePolicySheetLookupKeyTypes(source, policySheetLookupRef{EventType: "requested"}, spec); len(got) != 0 {
			t.Fatalf("admitted open lookup domain %q with default refused: %#v", refType, got)
		}
	}
}

func TestLoopValidationAdmittedTextAlias(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.RootTypes = c.TypeCatalogDocument{Scalars: map[string]c.ScalarTypeDecl{"Revision": {Base: "text"}}}
	for name, entry := range bundle.Events {
		if field, ok := entry.Payload.Properties["revision_id"]; ok {
			field.Type = "Revision"
			entry.Payload.Properties["revision_id"] = field
			bundle.Events[name] = entry
		}
	}
	if findings := checkLoopValidation(newCheckerContext(context.Background(), compileBootverifySchemasPreservingPlans(bundle), Options{})); len(findings) != 0 {
		t.Fatalf("text alias verification: %#v", findings)
	}
}

func TestLoopValidationRevisionDomainsRemainExact(t *testing.T) {
	for _, ref := range []string{"integer", "numeric", "uuid", "timestamp", "Status"} {
		t.Run(ref, func(t *testing.T) {
			bundle := loopValidationBundle()
			bundle.RootTypes = c.TypeCatalogDocument{Enums: map[string]c.EnumTypeDecl{"Status": {Values: []string{"ready"}, Default: "ready"}}}
			field := bundle.Events["draft.ready"].Payload.Properties["revision_id"]
			field.Type = ref
			bundle.Events["draft.ready"].Payload.Properties["revision_id"] = field
			if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "event must require text field revision_id") {
				t.Fatalf("non-text revision %s admitted: %#v", ref, findings)
			}
		})
	}
}

func TestPolicySheetLookupAliasedBooleanNoDefault(t *testing.T) {
	handler := bootverifyLookupHandler(false, true)
	lookup := handler.Rules[0].Compute.Lookup
	lookup.On = []string{"payload.key"}
	lookup.OnPaths = []paths.Path{paths.Parse("payload.key")}
	lookup.Entries = []c.ComputeLookupEntry{
		{Key: []c.ComputeLookupLiteral{{Kind: "bool", Value: true, Canonical: "bool:true"}}, Value: "templates/service/go", ValueKind: "string"},
		{Key: []c.ComputeLookupLiteral{{Kind: "bool", Value: false, Canonical: "bool:false"}}, Value: "templates/service/go", ValueKind: "string"},
	}
	bundle := &c.WorkflowContractBundle{
		RootTypes: c.TypeCatalogDocument{Scalars: map[string]c.ScalarTypeDecl{"Flag": {Base: "boolean"}}},
		Events:    map[string]c.EventCatalogEntry{"requested": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"key": {Type: "Flag"}}}}},
		Nodes:     map[string]c.SystemNodeContract{"lookup": {EventHandlers: map[string]c.SystemNodeEventHandler{"requested": handler}}},
	}
	source := compileBootverifyRootSource(bundle)
	if findings := checkPolicySheetLookupValueRows(newCheckerContext(context.Background(), source, Options{})); len(findings) != 0 {
		t.Fatalf("exhaustive aliased boolean row refused: %#v", findings)
	}
	lookup.Entries = lookup.Entries[:1]
	if findings := checkPolicySheetLookupValueRows(newCheckerContext(context.Background(), source, Options{})); !bootverifyLookupFindingContains(findings, "lookup.default: fail is required") {
		t.Fatalf("incomplete boolean row accepted: %#v", findings)
	}
}

func TestPolicySheetLookupOpenDomainDefaultAndMissingEvidence(t *testing.T) {
	for _, kind := range []string{"json", "jsonb", "object", "[text]", "map[text]text"} {
		t.Run(kind, func(t *testing.T) {
			for _, defaultDeclared := range []bool{true, false} {
				handler := bootverifyLookupHandler(defaultDeclared, true)
				lookup := handler.Rules[0].Compute.Lookup
				lookup.On = []string{"payload.key"}
				lookup.OnPaths = []paths.Path{paths.Parse("payload.key")}
				lookup.Entries[0].Key = lookup.Entries[0].Key[:1]
				bundle := &c.WorkflowContractBundle{
					Events: map[string]c.EventCatalogEntry{"requested": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"key": {Type: kind}}}}},
					Nodes:  map[string]c.SystemNodeContract{"lookup": {EventHandlers: map[string]c.SystemNodeEventHandler{"requested": handler}}},
				}
				source := compileBootverifyRootSource(bundle)
				findings := checkPolicySheetLookupValueRows(newCheckerContext(context.Background(), source, Options{}))
				if defaultDeclared && len(findings) != 0 {
					t.Fatalf("admitted open domain with default refused: %#v", findings)
				}
				if !defaultDeclared && !bootverifyLookupFindingContains(findings, "lookup.default: fail is required") {
					t.Fatalf("open domain without default accepted: %#v", findings)
				}
				lookup.On = []string{"payload.missing"}
				lookup.OnPaths = []paths.Path{paths.Parse("payload.missing")}
				if got := checkPolicySheetLookupValueRows(newCheckerContext(context.Background(), source, Options{})); !bootverifyLookupFindingContains(got, "no admitted") {
					t.Fatalf("missing path accepted: %#v", got)
				}
			}
		})
	}
}

func TestPolicySheetLookupExecutablePathBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, kind, path string
		required, want   bool
	}{
		{"required-json", "json", "payload.key", true, true},
		{"nested-json", "json", "payload.key.code", true, false},
		{"nested-map", "map[text]json", "payload.key.code", true, false},
		{"nested-list", "[text]", "payload.key.code", true, false},
		{"optional-boolean", "boolean", "payload.key", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := bootverifyLookupHandler(true, true)
			lookup := handler.Rules[0].Compute.Lookup
			lookup.On = []string{tc.path}
			lookup.OnPaths = []paths.Path{paths.Parse(tc.path)}
			lookup.Entries[0].Key = lookup.Entries[0].Key[:1]
			bundle := &c.WorkflowContractBundle{
				Events: map[string]c.EventCatalogEntry{"requested": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"key": {Type: tc.kind}}}}},
				Nodes:  map[string]c.SystemNodeContract{"lookup": {EventHandlers: map[string]c.SystemNodeEventHandler{"requested": handler}}},
			}
			if tc.required {
				entry := bundle.Events["requested"]
				entry.Payload.Required = []string{"key"}
				bundle.Events["requested"] = entry
			} else {
				lookup.Entries[0].Key = []c.ComputeLookupLiteral{{Kind: "bool", Value: true, Canonical: "bool:true"}}
			}
			checker := newCheckerContext(context.Background(), compileBootverifyRootSource(bundle), Options{})
			lookupFindings := checkPolicySheetLookupValueRows(checker)
			readerFindings := checkExecutableReaderExpressionValidation(checker)
			if (len(lookupFindings) == 0 && len(readerFindings) == 0) != tc.want {
				t.Fatalf("full path: lookup=%#v reader=%#v, want admitted=%v", lookupFindings, readerFindings, tc.want)
			}
			if strings.Contains(tc.path, ".code") && !bootverifyLookupFindingContains(lookupFindings, "no admitted") {
				t.Fatalf("unsupported descendant gained lookup authority: %#v", lookupFindings)
			}
			if !tc.required && len(readerFindings) == 0 {
				t.Fatal("optional key bypassed executable-reader presence check")
			}
		})
	}
}
