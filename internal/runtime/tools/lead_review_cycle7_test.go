package tools

import (
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"testing"
)

func TestLeadReview7PredicateAliasScope(t *testing.T) {
	schema := testEntityFilterSchema()
	schema.Contract.Types.Types["Metadata"] = rc.NamedTypeDecl{Fields: map[string]rc.TypeFieldSpec{
		"region": {Type: "text", IsOptional: true},
		"notes":  {Type: "list<Note>"},
		"labels": {Type: "map[text]text"},
	}}
	schema.Contract.Types.Types["Note"] = rc.NamedTypeDecl{Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}
	rows := []map[string]any{{"fields": map[string]any{"metadata": map[string]any{"notes": []any{map[string]any{}}, "labels": map[string]any{}}}}}
	for _, expr := range []string{
		`metadata.notes.all(metadata, metadata.note == "")`,
		`metadata.notes.all(x, has(x.note)) && metadata.notes.all(x, x.note == "")`,
		`(false ? [] : metadata.notes)[0].?note.orValue("") == ""`,
		`optional.of(metadata.labels).value()["missing"] == ""`,
		`fields.?metadata.value().notes.all(x, x.note == "")`,
	} {
		for _, data := range [][]map[string]any{nil, rows} {
			if got, err := filterEntityStateRowsCEL(expr, data, schema); err == nil {
				t.Errorf("unsafe %s: %v", expr, got)
			}
		}
	}
	for _, expr := range []string{
		`optional.none().orValue("") == ""`,
		`optional.ofNonZeroValue("").orValue("fallback") == "fallback"`,
		`metadata.notes.all(metadata, metadata.?note.orValue("") == "")`,
		`metadata.notes.all(x, !has(x.note) || x.note == "")`,
		`fields.?metadata.value().notes.all(x, x.?note.orValue("") == "")`,
		`optional.of(metadata.labels).value()[?"missing"].orValue("") == ""`,
	} {
		env, err := newEntityFilterEnv(schema)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := env.CompilePredicate(expr); err != nil {
			t.Fatalf("canonical predicate owner rejected control %s: %v", expr, err)
		}
		got, err := filterEntityStateRowsCEL(expr, rows, schema)
		if err != nil || len(got) != 1 {
			t.Errorf("safe %s: %v %v", expr, got, err)
		}
	}
}
