package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// #2672/6186986119 approved the source-scope repair and its stronger freeze
// assertion. Preserve both older recipe endpoints and append only that assertion.
const sourceForkFreezeAssertion = `					if name == "explicit" && (!fork.SourceFrozen || fork.SourceRunStatus != "forked") {
						t.Fatalf("explicit target source avoided the source-card freeze: %+v", fork)
					}
`

func validateSourceForkFreezeSuccessor(row recipe, actual string) error {
	if fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "ee11b9a761d33831fcd41b8ff10213b556f2516a109a5b97bb3bb9082d1a05c7" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "8e06cf78db91da5906e68dc75889f58248c8ec166b5a1df48425d97af49f1152" ||
		strings.Count(row.Successor, sourceForkFreezeAssertion) != 1 {
		return fmt.Errorf("source-fork historical endpoints or exact freeze assertion changed")
	}
	restored, err := canonicalFunction(strings.Replace(row.Successor, sourceForkFreezeAssertion, "", 1))
	prior, priorErr := canonicalFunction(row.After)
	current, currentErr := canonicalFunction(row.Successor)
	got, actualErr := canonicalFunction(actual)
	if err != nil || priorErr != nil || currentErr != nil || actualErr != nil || restored != prior || got != current {
		return fmt.Errorf("source-fork successor changed work beyond the reviewed freeze assertion")
	}
	return nil
}

func TestSourceForkFreezeSuccessorPreservesHistoryAndAllOtherAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "Test2376ForkSourceAndPinsFollowSourceRunBothStores" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if err := validateSourceForkFreezeSuccessor(row, actual); err != nil {
			t.Fatal(err)
		}
		for name, source := range map[string]string{
			"missing_assertion":   strings.Replace(actual, sourceForkFreezeAssertion, "", 1),
			"duplicate_assertion": strings.Replace(actual, sourceForkFreezeAssertion, sourceForkFreezeAssertion+sourceForkFreezeAssertion, 1),
			"wrong_status":        strings.Replace(actual, `fork.SourceRunStatus != "forked"`, `fork.SourceRunStatus != "running"`, 1),
			"wrong_branch":        strings.Replace(actual, `name == "explicit" && (!fork.SourceFrozen`, `name == "implicit" && (!fork.SourceFrozen`, 1),
			"lost_source_check":   strings.Replace(actual, "fork.BundleHash != want", "fork.BundleHash == want", 1),
		} {
			t.Run(name, func(t *testing.T) {
				if source == actual {
					t.Fatal("negative control did not change source")
				}
				changed := row
				changed.Successor = source
				if validateSourceForkFreezeSuccessor(changed, source) == nil {
					t.Fatal("matching unreviewed source and successor accepted")
				}
			})
		}
		changed := row
		changed.After = row.Successor
		if validateSourceForkFreezeSuccessor(changed, actual) == nil {
			t.Fatal("historical endpoint replacement accepted")
		}
	}
	if matched != 1 {
		t.Fatalf("source-fork recipe count=%d, want1", matched)
	}
}
