package testplanning

import (
	"regexp"
	"strings"
	"testing"
)

func TestCompiledUnitSelectorPreservesSelection(t *testing.T) {
	for _, unit := range []ProofUnit{
		{}, {Run: "^TestFoo$"}, {Run: "^TestFoo$/^postgres$"}, {Run: "^TestFoo$/["},
		{Skip: "^TestFoo$"}, {Run: "^(Test|Example)", Skip: "^TestFoo$"},
		{Run: "Foo"}, {Run: `^Test\Q.Foo\E$`}, {Skip: "TestFoo/postgres"},
		{Run: "["}, {Skip: "["}, {Run: "^TestFoo$", Skip: "["}, {Run: "[", Skip: "("},
	} {
		selector := compileUnitSelector(unit)
		for _, name := range []string{"", "TestFoo", "TestFooChild", "Test.Foo", "TestBar", "ExampleFoo", "FuzzFoo", "TestFoo/postgres", "TestΩ"} {
			want, wantErr := originalUnitSelectionProof(unit, name)
			got, gotErr := selector.matches(name)
			if got != want || selectorErrorText(gotErr) != selectorErrorText(wantErr) {
				t.Fatalf("run=%q skip=%q name=%q: cached=%t/%v original=%t/%v", unit.Run, unit.Skip, name, got, gotErr, want, wantErr)
			}
		}
	}
}

func TestCompiledUnitSelectorMalformedInputsFailClosed(t *testing.T) {
	for _, unit := range []ProofUnit{{Run: "["}, {Skip: "["}, {Run: "^TestFoo$", Skip: "["}} {
		selector := compileUnitSelector(unit)
		for repeat := 0; repeat < 3; repeat++ {
			if selected, err := selector.matches("TestFoo"); selected || err == nil {
				t.Fatalf("malformed selector admitted run=%q skip=%q: %t %v", unit.Run, unit.Skip, selected, err)
			}
		}
	}
	selector := compileUnitSelector(ProofUnit{Run: "^TestFoo$", Skip: "["})
	if selected, err := selector.matches("TestBar"); selected || err != nil {
		t.Fatalf("skip error escaped the original run short circuit: %t %v", selected, err)
	}
}

// Independent test oracle retains the pre-repair behavior, including error
// precedence and the root-only run prefix. It is not another production reader.
func originalUnitSelectionProof(unit ProofUnit, name string) (bool, error) {
	if run := strings.Split(unit.Run, "/")[0]; run != "" {
		selected, err := regexp.MatchString(run, name)
		if err != nil || !selected {
			return false, err
		}
	}
	if unit.Skip != "" {
		skipped, err := regexp.MatchString(unit.Skip, name)
		if err != nil || skipped {
			return false, err
		}
	}
	return true, nil
}

func selectorErrorText(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
