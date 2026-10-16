package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ActivitySnapshotError(source string) error {
	want := `func (f activityForkFixture) capture(t *testing.T, runID string) {
	t.Helper()
	captureSelectedExecutionSourceRevision(t, f.owner.ports.fork, runID)
}`
	actual, err := canonicalFunction(source)
	expected, wantErr := canonicalFunction(want)
	if err != nil || wantErr != nil || actual != expected {
		return fmt.Errorf("activity snapshot bypasses its exact original selected capture owner")
	}
	return nil
}
