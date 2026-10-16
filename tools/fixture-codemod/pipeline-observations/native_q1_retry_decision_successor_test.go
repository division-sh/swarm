package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// This inverse authenticates Q1's one appended endpoint before an older
// whole-ledger/file oracle runs. It never rewrites a packet or working file.
func q1RetryDecisionReceipt() (fixtureSetupSuccessor2617, error) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != fixtureSetupSuccessorPacketSHA256 {
		return fixtureSetupSuccessor2617{}, fmt.Errorf("Q1 fixture history changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil || len(pins) != 40 {
		return fixtureSetupSuccessor2617{}, fmt.Errorf("Q1 fixture history shape changed: %v", err)
	}
	return pins[39], nil
}

func q1RetryDecisionPriorRows(rows []recipe) ([]recipe, error) {
	pin, err := q1RetryDecisionReceipt()
	if err != nil {
		return nil, err
	}
	prior := append([]recipe(nil), rows...)
	matched := 0
	for index, row := range rows {
		if !sameReviewedRecipeKey(row, pin.Original) {
			continue
		}
		want := pin.Original
		want.Successor = pin.Candidate
		body, reverseErr := reverseReviewedSuccessor2617(row.Successor, pin.Edits)
		if !reflect.DeepEqual(row, want) || reverseErr != nil || body != pin.Original.Successor {
			return nil, fmt.Errorf("Q1 successor changed historical mechanical evidence: %s: %v", row.Function, reverseErr)
		}
		prior[index] = pin.Original
		matched++
	}
	if matched != 1 {
		return nil, fmt.Errorf("exact Q1 mechanical predecessor count=%d, want1", matched)
	}
	return prior, nil
}

func q1RetryDecisionPriorSource(path string, current []byte) ([]byte, error) {
	pin, err := q1RetryDecisionReceipt()
	if err != nil {
		return nil, err
	}
	if path != pin.Original.File {
		return append([]byte(nil), current...), nil
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, current, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	start, end, matched := 0, 0, 0
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != pin.Original.Function {
			continue
		}
		start, end = set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
		matched++
	}
	if matched != 1 || !canonicalComparatorEqual2617(string(current[start:end]), pin.Candidate) {
		return nil, fmt.Errorf("Q1 current source or multiplicity changed: %s", path)
	}
	body := string(current[start:end])
	due := regexp.MustCompile(`(\bRetryBase:\s*)time\.Millisecond\b`)
	if len(due.FindAllStringIndex(body, -1)) != 1 || strings.Count(body, "storetest.WaitManagerRetryDecision(") != 1 {
		return nil, fmt.Errorf("Q1 current retry edits changed: %s", path)
	}
	body = due.ReplaceAllString(body, "${1}time.Hour")
	body = strings.Replace(body, "storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible(", 1)
	if !canonicalComparatorEqual2617(body, pin.Original.Successor) {
		return nil, fmt.Errorf("Q1 exact source inverse changed: %s", path)
	}
	prior := append(append(append([]byte(nil), current[:start]...), body...), current[end:]...)
	return format.Source(prior)
}

// #2592/6192759467 replaces the retry-age write with the committed-due wait.
// The old 39 receipts and every business/temporal assertion remain immutable.
func TestQ1RetryDecisionSuccessorPreservesPriorReceiptsAndOracles(t *testing.T) {
	t.Run("whole_history_composition", testQ1RetryDecisionHistoryComposition)
	var raw []json.RawMessage
	var pins []fixtureSetupSuccessor2617
	var rows []recipe
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &raw); err != nil || len(raw) != 40 {
		t.Fatalf("fixture packet shape: rows=%d err=%v", len(raw), err)
	}
	prior, err := json.Marshal(raw[:39])
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != "daf060dec5ac400e00a35c4a7dea0321a38feeaae8e3bbcb4b20c87cbab0b5b2" {
		t.Fatal("Q1 rewrote earlier fixture evidence")
	}
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	pin := pins[39]
	if pin.Original.Family != "native-shared-coordinator-authority" || pin.Original.File != "internal/runtime/node_delivery_startup_recovery_test.go" || pin.Original.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" || len(pin.Edits) != 2 {
		t.Fatal("Q1 successor changed its exact fixture or finite edit set")
	}
	want := strings.Replace(pin.Original.Successor, "RetryBase: time.Hour", "RetryBase: time.Millisecond", 1)
	want = strings.Replace(want, "storetest.MakeManagerRetryEligible(", "storetest.WaitManagerRetryDecision(", 1)
	if pin.Candidate != want {
		t.Fatal("Q1 successor changed more than its explicit retry setup and canonical due wait")
	}
	matched := 0
	for _, row := range rows {
		if !sameReviewedRecipeKey(row, pin.Original) {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if _, err := fixtureSetupCurrentEndpoint2617(row, actual); err != nil {
			t.Fatal(err)
		}
		for label, cut := range map[string][2]string{
			"prior_age_writer": {"storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible("},
			"prior_due":        {"time.Millisecond", "time.Hour"},
			"changed_bound":    {"time.Second", "time.Minute"},
			"changed_origin":   {"ctx, selected, event, route", "ctx, selected, expiringEvent, route"},
			"extra_setup":      {"{", "{ unreviewedSetup();"},
		} {
			mutant := row
			source := strings.Replace(actual, cut[0], cut[1], 1)
			if source == actual {
				t.Fatalf("control did not change fixture: %s", label)
			}
			mutant.Successor = source
			if _, err := fixtureSetupCurrentEndpoint2617(mutant, source); err == nil {
				t.Fatalf("matching changed source/recipe admitted: %s", label)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("Q1 recipe cardinality=%d, want1", matched)
	}
}

func testQ1RetryDecisionHistoryComposition(t *testing.T) {
	pin, err := q1RetryDecisionReceipt()
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	prior, err := q1RetryDecisionPriorRows(rows)
	if err != nil || len(prior) != len(rows) {
		t.Fatalf("Q1 exact inverse: %v", err)
	}
	target := -1
	for index, row := range rows {
		if sameReviewedRecipeKey(row, pin.Original) {
			target = index
			if !reflect.DeepEqual(prior[index], pin.Original) {
				t.Fatal("Q1 inverse lost its complete original receipt")
			}
		} else if !reflect.DeepEqual(prior[index], row) {
			t.Fatal("Q1 inverse changed an unrelated receipt")
		}
	}
	for _, change := range []string{"missing", "duplicate", "before", "after", "mechanical", "removed", "endpoint", "old_endpoint"} {
		t.Run(change, func(t *testing.T) {
			changed := append([]recipe(nil), rows...)
			switch change {
			case "missing":
				changed = append(changed[:target], changed[target+1:]...)
			case "duplicate":
				changed = append(changed, changed[target])
			case "before":
				changed[target].Before += " changed"
			case "after":
				changed[target].After += " changed"
			case "mechanical":
				changed[target].Mechanical = nil
			case "removed":
				changed[target].Removed = true
			case "endpoint":
				changed[target].Successor += " changed"
			case "old_endpoint":
				changed[target] = pin.Original
			}
			if _, err := q1RetryDecisionPriorRows(changed); err == nil {
				t.Fatal("changed Q1 history acquired an exact predecessor")
			}
		})
	}
	raw, err := os.ReadFile(filepath.Join("../../..", pin.Original.File))
	if err != nil {
		t.Fatal(err)
	}
	source, err := q1RetryDecisionPriorSource(pin.Original.File, raw)
	want := strings.Replace(string(raw), "time.Millisecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()", "time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()", 1)
	want = strings.Replace(want, "storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible(", 1)
	formatted, formatErr := format.Source([]byte(want))
	if err != nil || formatErr != nil || !bytes.Equal(source, formatted) || bytes.Equal(raw, source) {
		t.Fatalf("Q1 exact source inverse changed its finite edit set: %v/%v", err, formatErr)
	}
	for _, changed := range [][]byte{
		[]byte(strings.Replace(string(raw), "storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible(", 1)),
		[]byte(strings.Replace(string(raw), "time.Millisecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()", "time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()", 1)),
		[]byte(strings.Replace(string(raw), "ctx, selected, event, route", "ctx, selected, expiringEvent, route", 1)),
		append(append([]byte(nil), raw...), []byte("\n"+pin.Candidate)...),
	} {
		if bytes.Equal(changed, raw) {
			t.Fatal("source counterexample did not change its intended cut")
		}
		if _, err := q1RetryDecisionPriorSource(pin.Original.File, changed); err == nil {
			t.Fatal("changed Q1 source acquired an exact predecessor")
		}
	}
	foreign, err := q1RetryDecisionPriorSource("unlisted.go", raw)
	if err != nil || !bytes.Equal(foreign, raw) {
		t.Fatal("Q1 inverse changed an unlisted file")
	}
}
