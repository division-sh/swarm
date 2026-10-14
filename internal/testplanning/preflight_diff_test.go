package testplanning

import "testing"

func TestPreflightDebtDiffRequiresEveryAuthorityInput(t *testing.T) {
	for _, path := range []string{
		"cmd/client/non_store_caller.go", "internal/policy/tagged_test.go", "go.mod", "go.sum", "go.work", "go.work.sum", "nested/go.mod", "nested/go.work.sum",
		"platform-spec.yaml", ".github/test-proof-plan.yaml", "internal/store/testdata/persistence_authority_debt_baseline.tsv",
		"internal/store/testdata/persistence_authority_debt_baseline.collector.json", "internal/store/testdata/persistence_authority_findings.tsv",
		"internal/checkoutsource/fixture_role.go", "internal/store/internal/runtimepersistence/fixtures/authority.yaml", "internal/storetest/fixture.yaml",
	} {
		t.Run(path, func(t *testing.T) {
			for _, status := range []string{"A", "M", "D", "T"} {
				if required, err := PreflightDebtRequired([]byte(status + "\x00" + path + "\x00")); err != nil || !required {
					t.Fatalf("authority path escaped: %t %v", required, err)
				}
			}
			for _, status := range []string{"R100", "C050"} {
				for _, pair := range [][2]string{{path, "docs/neutral.md"}, {"docs/neutral.md", path}} {
					if required, err := PreflightDebtRequired([]byte(status + "\x00" + pair[0] + "\x00" + pair[1] + "\x00")); err != nil || !required {
						t.Fatalf("old/new authority path escaped: %t %v", required, err)
					}
				}
			}
		})
	}
}

func TestPreflightDebtDiffRefusesAmbiguousEvidence(t *testing.T) {
	for _, raw := range []string{"M\x00file.go", "M\x00", "R100\x00file.go\x00", "C101\x00a\x00b\x00", "Rinvalid\x00a\x00b\x00", "R+1\x00a\x00b\x00", "R0000\x00a\x00b\x00", "U\x00file.go\x00", "X\x00file.go\x00", "M\x00../outside\x00", "M\x00/absolute\x00", "M\x00a/../b\x00", "M\x00.\x00"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := PreflightDebtRequired([]byte(raw)); err == nil {
				t.Fatal("ambiguous diff gained docs-only exemption")
			}
		})
	}
	for _, raw := range []string{"", "M\x00docs/notes.md\x00", "R100\x00README.md\x00docs/README.md\x00", "M\x00docs/with\nnewlines.md\x00"} {
		if required, err := PreflightDebtRequired([]byte(raw)); err != nil || required {
			t.Fatalf("readable docs-only/empty diff changed debt membership: %t %v", required, err)
		}
	}
}
