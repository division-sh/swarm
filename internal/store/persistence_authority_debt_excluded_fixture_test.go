package store_test

import "testing"

// #642/6202173993: these declarations delegate to existing owners; this is
// classified excluded-source uncertainty, never executable-proof credit.
func debtReviewedExcludedFixture(finding authorityFinding) string {
	if finding.Kind != "unresolved-excluded-source" || finding.RawSQL {
		return ""
	}
	var declaration, context, reason string
	switch finding.File + "\x00" + finding.Enclosing {
	case "internal/serveapp/issue642_future_provider_test.go\x00TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores":
		declaration = "declaration:931af522b4f0b9379a9842b13e9a28e01e907202547ed55a48a510783d324121"
		context = "bf98ca60306a0fa727206f73aecb6d27e6a9a366183114a0fe7cd8bbfd62c02e"
		reason = "native served construction; real selected executor; canonical source/turn readback; opt-in proof remains unqualified"
	case "internal/serveapp/issue642_future_provider_test.go\x00issue642WaitFutureProviderCompletion":
		declaration = "declaration:69bf4b7d588cc2455f6ce5c20aea5e8968aba3e962c8d66a45d2190e8e18f9bf"
		context = "bf98ca60306a0fa727206f73aecb6d27e6a9a366183114a0fe7cd8bbfd62c02e"
		reason = "existing run-header, delivery-summary and completion-diagnostic readers; no raw authority"
	case "internal/serveapp/lifecycle_template_race_enabled_test.go\x00package":
		declaration = "declaration:15b255986e7151c0f5da8f981d54571c0da322a06f5634a51027a327c67e2cf7"
		context = "8045bd68ac6d6cf60256b51ed70132a98ad0881982ced17c0ed09316e3cc904f"
		reason = "exact race-only constant marker; no authority or generic boolean classification"
	default:
		return ""
	}
	if finding.Member != declaration || finding.Resolved != "unresolved|source-context:"+context {
		return ""
	}
	return reason
}

func debtClassifyReviewedExcludedFixtures(t *testing.T, findings []authorityFinding) []authorityFinding {
	t.Helper()
	counts := map[string]int{}
	for _, finding := range findings {
		counts[finding.File+"\x00"+finding.Enclosing]++
	}
	for i, finding := range findings {
		reason := debtReviewedExcludedFixture(finding)
		if reason == "" || counts[finding.File+"\x00"+finding.Enclosing] != 1 {
			continue
		}
		findings[i].Kind = "reviewed-excluded-source"
		t.Logf("reviewed excluded source %s declaration=%s body=%s context=%s ruling=#642/6202173993 reason=%s", finding.File, finding.Enclosing, finding.Member, finding.Resolved, reason)
	}
	return findings
}
