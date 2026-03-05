package runtime

import "testing"

func TestDirectiveParser(t *testing.T) {
	parser := DirectiveParser{}
	parsed := parser.Parse("Run saas_trend in Paraguay focus on fintech, payroll avoid crypto budget $1200")
	if parsed.Mode != "saas_trend" {
		t.Fatalf("expected mode saas_trend, got %q", parsed.Mode)
	}
	if !parsed.ExplicitMode {
		t.Fatalf("expected explicit mode=true")
	}
	if parsed.Geography != "Paraguay" {
		t.Fatalf("expected geography Paraguay, got %q", parsed.Geography)
	}
	if parsed.BudgetCap != 1200 {
		t.Fatalf("expected budget cap 1200, got %v", parsed.BudgetCap)
	}
	if len(parsed.TaxonomyFocus) == 0 {
		t.Fatalf("expected taxonomy_focus parsed")
	}
	if len(parsed.AvoidSectors) == 0 {
		t.Fatalf("expected avoid_sectors parsed")
	}
	if parsed.Intent == "" {
		t.Fatalf("expected intent to be set")
	}
}

func TestDirectiveParser_ExtractsCorpusPath(t *testing.T) {
	parsed := (DirectiveParser{}).Parse("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if parsed.Mode != "corpus" {
		t.Fatalf("expected corpus mode, got %q", parsed.Mode)
	}
	if parsed.CorpusPath != "/data/test-signals-25.jsonl" {
		t.Fatalf("expected corpus_path extracted, got %q", parsed.CorpusPath)
	}
}
