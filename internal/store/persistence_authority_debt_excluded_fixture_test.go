package store_test

import "testing"

// #642/6202173993 and #2903/6584235489: these declarations use existing owners; this is
// classified excluded-source uncertainty, never executable-proof credit.
func debtReviewedExcludedFixture(finding authorityFinding) string {
	if finding.Kind != "unresolved-excluded-source" || finding.RawSQL {
		return ""
	}
	var declaration, context, reason string
	switch finding.File + "\x00" + finding.Enclosing {
	case "internal/serveapp/issue642_future_provider_test.go\x00TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores":
		declaration = "declaration:931af522b4f0b9379a9842b13e9a28e01e907202547ed55a48a510783d324121"
		if finding.Member == "declaration:a93689c088d0eac109b63733b72f50d2c90bacd48405c1eff84a8b6105cf1d5b" {
			declaration = finding.Member
		}
		context = "bf98ca60306a0fa727206f73aecb6d27e6a9a366183114a0fe7cd8bbfd62c02e"
		if finding.Member == "declaration:a93689c088d0eac109b63733b72f50d2c90bacd48405c1eff84a8b6105cf1d5b" &&
			finding.Resolved == "unresolved|source-context:6632b5256bfc7f4f796dee46748546cad0f83930df4a23794fcd179e971e8f37" {
			context = "6632b5256bfc7f4f796dee46748546cad0f83930df4a23794fcd179e971e8f37"
		}
		reason = "native served construction; real selected executor; canonical source/turn readback; opt-in proof remains unqualified"
	case "internal/serveapp/issue642_future_provider_test.go\x00issue642WaitFutureProviderCompletion":
		declaration = "declaration:69bf4b7d588cc2455f6ce5c20aea5e8968aba3e962c8d66a45d2190e8e18f9bf"
		context = "bf98ca60306a0fa727206f73aecb6d27e6a9a366183114a0fe7cd8bbfd62c02e"
		if finding.Resolved == "unresolved|source-context:6632b5256bfc7f4f796dee46748546cad0f83930df4a23794fcd179e971e8f37" {
			context = "6632b5256bfc7f4f796dee46748546cad0f83930df4a23794fcd179e971e8f37"
		}
		reason = "existing run-header, delivery-summary and completion-diagnostic readers; no raw authority"
	case "internal/serveapp/lifecycle_template_race_enabled_test.go\x00package":
		declaration = "declaration:15b255986e7151c0f5da8f981d54571c0da322a06f5634a51027a327c67e2cf7"
		context = "8045bd68ac6d6cf60256b51ed70132a98ad0881982ced17c0ed09316e3cc904f"
		reason = "exact race-only constant marker; no authority or generic boolean classification"
	case "internal/testutil/testprocess/process_darwin.go\x00processBirth",
		"tests/support/testutil/testprocess/process_darwin.go\x00processBirth":
		declaration = "declaration:90ae93ab0f7934560a0fc4d6c897ea7a3856fa147e0caec9f6972d2af4a62473"
		context = "98149c9df2ccd441aae2bb7ee092f61a28c74305333234ae5782d55b48754693"
		reason = "kernel PID/state/start-time occurrence identity; no persistence or native execution credit"
	case "internal/testutil/testprocess/process_darwin.go\x00startNative",
		"tests/support/testutil/testprocess/process_darwin.go\x00startNative":
		declaration = "declaration:422a6f83c2a2e97f4897583d42601fdf3cc5127cb30d87a80b171f52a9d9afa3"
		context = "98149c9df2ccd441aae2bb7ee092f61a28c74305333234ae5782d55b48754693"
		reason = "current process occurrence delegates to existing watched launch; no persistence"
	case "internal/testutil/testprocess/process_darwin.go\x00validateWatched",
		"tests/support/testutil/testprocess/process_darwin.go\x00validateWatched":
		declaration = "declaration:0828980a6973a0dbf420e39cb162844fd5bbcb06e27922c8019c3337334cdc9a"
		context = "98149c9df2ccd441aae2bb7ee092f61a28c74305333234ae5782d55b48754693"
		reason = "nil-only process attribute validation; no persistence"
	case "internal/serveapp/channel_provider_construction_unsupported.go\x00qualifyWhatsAppPlan",
		"internal/serveapp/channel_provider_construction_unsupported.go\x00openChannelRegistration",
		"internal/serveapp/channel_provider_construction_unsupported.go\x00openTelegramChannelRegistration",
		"internal/serveapp/channel_provider_construction_unsupported.go\x00openWhatsAppChannelRegistration",
		"internal/serveapp/channel_provider_construction_unsupported.go\x00openWhatsAppDeploymentConnection",
		"internal/serveapp/channel_provider_construction_unsupported.go\x00openWhatsAppRuntimeConnection":
		switch finding.Enclosing {
		case "qualifyWhatsAppPlan":
			declaration = "declaration:d4c8f6c11958531c6afb5a4465eb2a92cbad4870135d132fd424d978b809c75f"
		case "openChannelRegistration":
			declaration = "declaration:d0817cda9016b84aeb5ebacecbbf0f482060b2b50c6fa58adc7065a1f55294d6"
		case "openTelegramChannelRegistration":
			declaration = "declaration:aca79be7deab8ce93921f678a8b5e34c8e55a6b84bc0645368d460aab72fb0cc"
		case "openWhatsAppChannelRegistration":
			declaration = "declaration:bfa9b1d5b9817327995bce97ac19b6b7f38ef35a640610ced5d70aa033c31fab"
		case "openWhatsAppDeploymentConnection":
			declaration = "declaration:28a2153cc26ad47e80092a90aa14f2323c30887fba1ff92487d9259f59d242d5"
		case "openWhatsAppRuntimeConnection":
			declaration = "declaration:4e9e57e0a5645aa7124a1c4b58d98125c801bde4696eeb2e290454e9c0b276c3"
		}
		context = "64633fdc113c0fed6bc27dc439e7016bceed87d803fd432b47747cd3afec074d"
		reason = "exact unsupported-platform constructor refuses or delegates to the existing refusal; no native execution credit"
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
		ruling := "#642/6202173993"
		if finding.File == "internal/testutil/testprocess/process_darwin.go" || finding.File == "tests/support/testutil/testprocess/process_darwin.go" {
			ruling = "#2903/6584235489"
		}
		if finding.File == "internal/serveapp/channel_provider_construction_unsupported.go" {
			ruling = "#2338/6840391584"
		}
		t.Logf("reviewed excluded source %s declaration=%s body=%s context=%s ruling=%s reason=%s", finding.File, finding.Enclosing, finding.Member, finding.Resolved, ruling, reason)
	}
	return findings
}
