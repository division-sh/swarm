package runtime

import "testing"

func TestCampaignModesForDirective(t *testing.T) {
	got := campaignModesForDirective("saas_gap", false)
	if len(got) != 3 || got[0] != "saas_gap" || got[1] != "saas_trend" || got[2] != "local_services" {
		t.Fatalf("unexpected full-cycle modes: %+v", got)
	}

	single := campaignModesForDirective("saas_trend", true)
	if len(single) != 1 || single[0] != "saas_trend" {
		t.Fatalf("unexpected explicit single mode: %+v", single)
	}

	corpus := campaignModesForDirective("corpus", true)
	if len(corpus) != 1 || corpus[0] != "corpus" {
		t.Fatalf("unexpected corpus explicit mode: %+v", corpus)
	}
}

func TestIsComplexDirectiveText(t *testing.T) {
	if !isComplexDirectiveText("Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration") {
		t.Fatal("expected complex directive to be detected")
	}
	if isComplexDirectiveText("SaaS in Uruguay") {
		t.Fatal("expected simple directive to stay deterministic-runtime path")
	}
}

func TestParseDirectiveMode_Corpus(t *testing.T) {
	mode, explicit := parseDirectiveMode("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if mode != "corpus" || !explicit {
		t.Fatalf("expected corpus explicit mode, got mode=%q explicit=%v", mode, explicit)
	}
}
