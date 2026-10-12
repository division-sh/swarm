# Issue #2630: Read-Only Correctness Counterexamples

Source base: 770b99f93019c8cf26fea3fcb4b579e37fddff99; effective Go go1.26.8.
Production and maintained tests were unchanged. The following disposable probe
was executed, then removed from the Go package. This document preserves its
exact source; it adds no maintained test root or implementation.

Command:

```text
go test ./internal/runtime/contracts -run '^TestIssue2630Audit' -race -count=3 -timeout=3m -json
```

Receipt: issue2630-audit-constraints-producers-race3.jsonl, retained at
/home/youmew/.local/state/agent-g-2509-postmerge-preaudit-20261012/.
SHA256: 9f16cce4f7977015b40e252638af3f978faab12ab5bce0503233b193aa75d08e.
Exit 1 is the expected baseline result, not a passing qualification.

Six relation leaves (enum/oneof/equality x implicit/explicit_true) each fail
3/3 after source-valid and target-invalid value controls succeed.
Three compiled-result leaves (constructor/with_schemas/with_compiled_result)
each fail 3/3 because the entry is nonzero and the error is nil.
The schema_any relation controls and identical/closed-source/constraint-admission
controls pass 3/3. No skips or race reports. No network/provider/store execution.

The JSON events contain 33 failing test terminals (the nine failing leaves and
two failing parent roots, each repeated three times) and 21 passing terminals.
This is a baseline correctness demonstration, not repair proof.

## Exact Disposable Probe

```go
package contracts

import "testing"

func TestIssue2630AuditAdditionalValueConstraints(t *testing.T) {
	free := MustToolInputSchema(ToolSchemaAny)
	targets := []struct {
		name    string
		raw     map[string]any
		witness map[string]any
	}{
		{"enum", map[string]any{"mode": map[string]any{"type": "any", "enum": []any{"approved"}}}, map[string]any{"mode": "unapproved"}},
		{"oneof", map[string]any{"mode": map[string]any{"type": "any", "oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}}}, map[string]any{"mode": true}},
		{"equality", map[string]any{"mode": map[string]any{"type": "any", "x-swarm-equalTo": "peer"}, "peer": map[string]any{"type": "any"}}, map[string]any{"mode": "unapproved", "peer": "approved"}},
	}
	for _, tc := range targets {
		target, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "properties": tc.raw})
		if err != nil {
			t.Fatal(err)
		}
		for _, sourceKind := range []string{"implicit", "explicit_true", "schema_any"} {
			t.Run(tc.name+"/"+sourceKind, func(t *testing.T) {
				var options []ToolInputSchemaOption
				switch sourceKind {
				case "explicit_true":
					options = []ToolInputSchemaOption{ToolSchemaAdditionalPropertiesAllowed(true)}
				case "schema_any":
					options = []ToolInputSchemaOption{ToolSchemaAdditionalPropertiesSchema(free)}
				}
				source := MustToolInputSchema(ToolSchemaObject, options...)
				if err := source.Validate(tc.witness); err != nil {
					t.Fatalf("source witness: %v", err)
				}
				if err := target.Validate(tc.witness); err == nil {
					t.Fatal("target must reject witness")
				}
				if err := source.ValidateAssignableTo("audit "+tc.name, target); err == nil {
					t.Fatalf("%s unrestricted source accepted constrained target", sourceKind)
				}
			})
		}
	}
}

func TestIssue2630AuditDeclaredAndClosedControls(t *testing.T) {
	for _, kind := range []string{"enum", "oneof", "equality"} {
		t.Run(kind, func(t *testing.T) {
			raw := map[string]any{"type": "object", "properties": map[string]any{"mode": map[string]any{"type": "any"}}, "additionalProperties": false}
			properties := raw["properties"].(map[string]any)
			mode := properties["mode"].(map[string]any)
			switch kind {
			case "enum":
				mode["enum"] = []any{"approved"}
			case "oneof":
				mode["oneOf"] = []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}
			case "equality":
				mode["x-swarm-equalTo"] = "peer"
				properties["peer"] = map[string]any{"type": "any"}
			}
			target, err := AdmitToolInputSchemaMap(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.ValidateAssignableTo("identical", target); err != nil {
				t.Fatal(err)
			}
			closed := MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(false))
			if err := closed.ValidateAssignableTo("closed omits optional", target); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, option := range []ToolInputSchemaOption{ToolSchemaPattern("x"), ToolSchemaMinLength(1), ToolSchemaFormat("uuid"), ToolSchemaMinimum(0), ToolSchemaItems(MustToolInputSchema(ToolSchemaString)), ToolSchemaRequired("mode")} {
		if _, err := NewToolInputSchema(ToolSchemaAny, option); err == nil {
			t.Fatal("any acquired inapplicable constraint")
		}
	}
}

func TestIssue2630AuditCompiledResultProducers(t *testing.T) {
	freeObject := MustToolInputSchema(ToolSchemaObject)
	mode := MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved"))
	goodObject := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"mode": mode}), ToolSchemaAdditionalPropertiesAllowed(false))
	targetObject := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"mode": mode}))
	wrap := func(name string, schema ToolInputSchema) ToolInputSchema {
		return MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{name: schema}), ToolSchemaRequired(name))
	}
	input := MustToolInputSchema(ToolSchemaObject)
	badOutput := wrap("payload", freeObject)
	goodOutput := wrap("payload", goodObject)
	projection := CompiledResultProjection{Fields: map[string]CompiledResultField{"projected": {From: "result.payload"}}, OutputSchema: wrap("projected", targetObject)}
	base := []ToolSchemaEntryOption{WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, goodOutput)}
	good := MustToolSchemaEntry(append(base, WithToolCompiledResult(projection))...)
	if good.IsZero() {
		t.Fatal("valid producer control is zero")
	}
	if err := badOutput.Validate(map[string]any{"payload": map[string]any{"mode": "unapproved"}}); err != nil {
		t.Fatal(err)
	}
	if err := projection.OutputSchema.Validate(map[string]any{"projected": map[string]any{"mode": "unapproved"}}); err == nil {
		t.Fatal("target negative control")
	}
	for _, entrance := range []string{"constructor", "with_schemas", "with_compiled_result"} {
		t.Run(entrance, func(t *testing.T) {
			var got ToolSchemaEntry
			var err error
			switch entrance {
			case "constructor":
				got, err = NewToolSchemaEntry(WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, badOutput), WithToolCompiledResult(projection))
			case "with_schemas":
				got, err = good.WithSchemas(input, badOutput)
			case "with_compiled_result":
				unprojected := MustToolSchemaEntry(WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, badOutput))
				got, err = unprojected.WithCompiledResult(projection)
			}
			if err == nil || !got.IsZero() {
				t.Fatalf("invalid producer granted authority: nonzero=%v err=%v", !got.IsZero(), err)
			}
		})
	}
}
```

