//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAccountingRetainedPreflightSurvivesProofDisposal(t *testing.T) {
	directory, receipt, _ := preflightStatusFixture(t)
	root := filepath.Dir(filepath.Dir(filepath.Dir(directory)))
	relative, err := filepath.Rel(root, filepath.Join(directory, "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	evidence := t.TempDir()
	retained, err := retainAccountingPreflight(root, relative, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	if err := validateBoundAccountingPreflight(retained, receipt.Plan.HeadSHA, receipt.Plan.BaseSHA); err != nil {
		t.Fatalf("disposed proof tree invalidated retained evidence: %v", err)
	}
	for _, test := range []struct{ head, base string }{
		{"wrong-head", receipt.Plan.BaseSHA}, {receipt.Plan.HeadSHA, "wrong-base"},
	} {
		if validateBoundAccountingPreflight(retained, test.head, test.base) == nil {
			t.Fatal("retained evidence admitted a changed comparison")
		}
	}
	receipt.Complete = false
	writePreflightStatusFixtureReceipt(t, filepath.Dir(retained), receipt)
	if validateBoundAccountingPreflight(retained, receipt.Plan.HeadSHA, receipt.Plan.BaseSHA) == nil {
		t.Fatal("partial retained receipt accepted")
	}
}
