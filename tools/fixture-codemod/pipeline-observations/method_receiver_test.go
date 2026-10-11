package main

import (
	"strings"
	"testing"
)

func TestFiniteCodemodDistinguishesExactMethodReceivers(t *testing.T) {
	row := recipe{Function: "Observe", Before: "func (a First) Observe() int { return 1 }", After: "func (a First) Observe() int { return 2 }"}
	source := []byte("package witness\nfunc (a Other) Observe() int { return 9 }\nfunc (renamed First) Observe() int { return 1 }\n")
	// Parameter spelling is still protected by the whole-function snapshot.
	if _, _, err := rewriteFunction("test.go", source, row); err == nil {
		t.Fatal("receiver parameter drift bypassed the whole-body oracle")
	}
	source = []byte(strings.Replace(string(source), "renamed First", "a First", 1))
	updated, changed, err := rewriteFunction("test.go", source, row)
	if err != nil || !changed || !strings.Contains(string(updated), "func (a Other) Observe() int { return 9 }") || !strings.Contains(string(updated), row.After) {
		t.Fatalf("exact receiver rewrite changed its sibling: changed=%t err=%v source=%s", changed, err, updated)
	}
	for _, hostile := range []string{
		"package witness\nfunc (a Other) Observe() int { return 1 }\n",
		"package witness\nfunc (a *First) Observe() int { return 1 }\n",
		"package witness\nfunc Observe() int { return 1 }\n",
		"package witness\n" + row.Before + "\n" + row.Before + "\n",
	} {
		if _, _, err := rewriteFunction("test.go", []byte(hostile), row); err == nil {
			t.Fatalf("foreign/pointer/free/ambiguous receiver admitted: %s", hostile)
		}
	}
}
