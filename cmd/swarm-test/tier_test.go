package main

import (
	"testing"
)

func TestCompletionTierSelectionIsExplicitAndNeverInferred(t *testing.T) {
	for _, tc := range []struct {
		args     []string
		selected bool
		tier     string
		explicit bool
		invalid  bool
	}{
		{nil, true, "core", false, false},
		{[]string{"--full"}, true, "full", true, false},
		{[]string{"--tier", "core"}, true, "core", true, false},
		{[]string{"--tier", "lifecycle"}, true, "lifecycle", true, false},
		{[]string{"--tier", "full"}, true, "full", true, false},
		{[]string{"--tier"}, true, "", true, true},
		{[]string{"--tier", "nightly"}, true, "", true, true},
		{[]string{"--full", "--tier", "core"}, true, "", true, true},
		{[]string{"--tier", "full", "--", "-run", "Only"}, true, "", true, true},
		{[]string{"--", "./internal/testplanning"}, false, "", false, false},
		{[]string{"--planned", "fixture"}, false, "", false, false},
	} {
		t.Run(tierCaseName(tc.args), func(t *testing.T) {
			selected, tier, explicit, err := completionSelection(tc.args)
			if selected != tc.selected || tier != tc.tier || explicit != tc.explicit || (err != nil) != tc.invalid {
				t.Fatalf("selection = %t %q %t %v", selected, tier, explicit, err)
			}
		})
	}
}

func tierCaseName(args []string) string {
	if len(args) == 0 {
		return "no_context"
	}
	name := args[0]
	for _, arg := range args[1:] {
		name += "_" + arg
	}
	return name
}
