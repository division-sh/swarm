package tools

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestEntitySparseReadSchemaPreservesRootAndRecordPresence(t *testing.T) {
	contract := entityruntime.Contract{
		Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
			"bare": {Type: "text"}, "optional": {Type: "text", IsOptional: true}, "record": {Type: "Record"},
		}},
		Types: contracts.TypeCatalogDocument{Types: map[string]contracts.NamedTypeDecl{
			"Record": {Fields: map[string]contracts.TypeFieldSpec{
				"required": {Type: "text"}, "optional": {Type: "text", IsOptional: true},
			}},
		}},
	}
	schema := roleScopedEntityWholeReadOutputSchema(contract)
	fields := schemaProperties(schema["properties"])["fields"]
	if required := requiredSchemaSet(fields["required"]); len(required) != 0 {
		t.Fatalf("whole read claims unassigned entity roots are present: %#v", required)
	}
	record := schemaProperties(fields["properties"])["record"]
	required := requiredSchemaSet(record["required"])
	if _, ok := required["required"]; !ok || len(required) != 1 {
		t.Fatalf("assigned named record lost exact required members: %#v", record)
	}
	if problems := validateGeneratedJSONSchema("read_work", schema); len(problems) != 0 {
		t.Fatalf("valid sparse read schema rejected: %v", problems)
	}
	fields["additionalProperties"] = true
	if problems := validateGeneratedJSONSchema("read_work", schema); len(problems) == 0 {
		t.Fatal("open sparse schema accepted")
	}
	fields["additionalProperties"] = false
	fields["required"] = []string{"undeclared"}
	if problems := validateGeneratedJSONSchema("read_work", schema); len(problems) == 0 {
		t.Fatal("undeclared required property accepted")
	}
}
