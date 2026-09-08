package selection

import "testing"

func TestDeclaredModelAliasDoesNotRequireSelectedProvider(t *testing.T) {
	req := ModelResolution{Model: "specialist", Models: ModelAliases{
		"specialist": {BackendAnthropic: "declared-specialist-model"},
	}}
	alias, targets, err := ResolveDeclaredModelAlias(req)
	if err != nil || alias != "specialist" || targets[BackendAnthropic] != "declared-specialist-model" {
		t.Fatalf("declared alias = %q, %v, %v", alias, targets, err)
	}
	profile, err := ResolveActiveBackend(BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveModel(profile, req); err == nil {
		t.Fatal("live selection accepted an alias without a target for its provider")
	}
	for _, model := range []string{"", "undeclared", "claude-3-5-sonnet"} {
		if _, _, err := ResolveDeclaredModelAlias(ModelResolution{Model: model}); err == nil {
			t.Errorf("structural validation accepted undeclared model %q", model)
		}
	}
}
