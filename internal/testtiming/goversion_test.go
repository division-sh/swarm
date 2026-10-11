package testtiming

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCommandIdentityRejectsChangedCompilerVersion(t *testing.T) {
	plan, receipt := rootEvidenceFixture(t)
	raw, err := json.Marshal(receipt.BuildContext)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]string
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	fields["goversion"] = "go1.26.8"
	raw, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &receipt.BuildContext); err != nil {
		t.Fatal(err)
	}
	if problems := ValidateCommandIdentity(receipt, plan); len(problems) == 0 {
		t.Fatal("receipt from a different compiler qualified")
	}
}

func TestQualificationRejectsMissingCompilerOnEitherSide(t *testing.T) {
	for _, cut := range []string{"plan_missing", "receipt_missing", "both_missing", "both_unbound", "reference_missing", "reference_foreign", "receipt_foreign"} {
		t.Run(cut, func(t *testing.T) {
			plan, receipt := rootEvidenceFixture(t)
			roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt})
			if err != nil {
				t.Fatal(err)
			}
			reference := TestTimeReference{RunID: TestTimeReferenceRunID, Source: plan.HeadSHA, BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, roots, rootTimingPolicy())}
			switch cut {
			case "plan_missing", "both_missing":
				plan.BuildContext.GOVERSION = ""
			case "both_unbound":
				plan.BuildContext = receipt.BuildContext
				plan.BuildContext.GOOS, plan.BuildContext.GOARCH, plan.BuildContext.CGOEnabled, plan.BuildContext.GOVERSION = "", "", "", ""
			case "reference_missing":
				reference.BuildContext.GOVERSION = ""
			case "reference_foreign":
				reference.BuildContext.GOVERSION = "go1.26.8"
			case "receipt_foreign":
				receipt.BuildContext.GOVERSION = "go1.26.8"
			}
			if cut == "receipt_missing" || cut == "both_missing" {
				receipt.BuildContext.GOVERSION = ""
			}
			if cut == "both_unbound" {
				receipt.BuildContext = plan.BuildContext
			}
			rebindTimingFixture(t, &plan, &receipt)
			if cut != "reference_missing" && cut != "reference_foreign" {
				if len(ValidateCommandIdentity(receipt, plan)) == 0 || len(ValidateCommandEvidence(receipt, plan)) == 0 {
					t.Fatal("unversioned or foreign compiler acquired receipt credit")
				}
			}
			for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
				result := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}, mode)
				if cut == "reference_foreign" {
					if result.Status != BudgetWarn || len(result.Problems) != 0 || len(result.Tiers) == 0 {
						t.Fatalf("valid current proof refused on historical compiler divergence: %+v", result)
					}
					for _, tier := range result.Tiers {
						if tier.Retained != 0 || tier.AddedSeconds != 10 {
							t.Fatalf("historical compiler mismatch granted retained credit: %+v", tier)
						}
					}
					continue
				}
				if result.Status != BudgetIncomplete || len(result.Tiers) != 0 || len(result.Problems) == 0 {
					t.Fatalf("compiler mismatch reached %s cost/advisory comparison: %+v", mode, result)
				}
			}
			if cut != "reference_missing" && cut != "reference_foreign" {
				if _, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}); err == nil {
					t.Fatal("invalid compiler reached root-time capture")
				}
			}
		})
	}
	for _, version := range []string{"go1.25.0", "go1.26.8"} {
		plan, receipt := rootEvidenceFixture(t)
		plan.BuildContext.GOVERSION, receipt.BuildContext.GOVERSION = version, version
		rebindTimingFixture(t, &plan, &receipt)
		if problems := ValidateCommandEvidence(receipt, plan); len(problems) != 0 {
			t.Fatalf("equal version %s refused: %v", version, problems)
		}
	}
}

func TestPinnedReferenceCompilerRefreshPreservesEveryHistoricalByte(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/test-time-reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := LoadTestTimeReference(bytes.NewReader(raw))
	if err != nil || reference.BuildContext.GOVERSION != "go1.25.0" || len(reference.Roots) != 11809 {
		t.Fatalf("verified historical compiler/reference: %+v, %v", reference.BuildContext, err)
	}
	addition := []byte(`,"goversion":"go1.25.0"`)
	if bytes.Count(raw, addition) != 1 {
		t.Fatal("metadata refresh has an ambiguous compiler field")
	}
	old := bytes.Replace(raw, addition, nil, 1)
	digest := sha256.Sum256(old)
	if hex.EncodeToString(digest[:]) != "f195969f9056e3c1e26605d114b50e61c8c2efddec55bdee0745f3de516a9db4" {
		t.Fatal("reference changed beyond the independently approved compiler metadata")
	}
	for _, bad := range [][]byte{old, bytes.Replace(raw, addition, []byte(`,"goversion":"go1.26.8"`), 1)} {
		if _, err := LoadTestTimeReference(bytes.NewReader(bad)); err == nil {
			t.Fatal("unversioned or relabeled historical reference accepted")
		}
	}
}
