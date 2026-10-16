package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ActivityReadChainProofError(name, before, after string) error {
	if name == "runSelectedContractActivityForkCases" {
		if !strings.Contains(after, "selected := fixture.owner.ports.fork") || strings.Contains(after, "QueryRowContext") ||
			!strings.Contains(after, "if sourceBefore.AttemptStatus != tt.sourceAttemptStatus") {
			return fmt.Errorf("activity cases lost original owner or exact positive journal-status witness")
		}
		beforeAssertions, beforeErr := runSetupAssertions(before)
		afterAssertions, afterErr := runSetupAssertions(after)
		var kept []string
		var added int
		for _, call := range afterAssertions {
			if call == `t.Fatalf("source journal observation status=%q want=%q", sourceBefore.AttemptStatus, tt.sourceAttemptStatus)` {
				added++
			} else {
				kept = append(kept, call)
			}
		}
		if beforeErr != nil || afterErr != nil || added != 1 || strings.Join(beforeAssertions, "\n") != strings.Join(kept, "\n") {
			return fmt.Errorf("activity cases changed an original assertion or lost their added exact status control")
		}
		return nil
	}
	for _, raw := range []string{"activityForkFixtureDB", "QueryRowContext", "Scan(", "Database(", "ReadCanonicalEvent"} {
		if strings.Contains(after, raw) {
			return fmt.Errorf("activity read helper retained %s or substituted a decoded event", raw)
		}
	}
	if name == "readActivityForkSourceEvidence" {
		want := `func readActivityForkSourceEvidence(t *testing.T, selected any, runID, entityID, requestID string) activityForkSourceEvidence {
	t.Helper()
	out, err := storetest.ReadActivityForkSourceStorage(context.Background(), selected, runID, entityID, requestID)
	if err != nil { t.Fatal(err) }
	return out
}`
		actual, err := canonicalFunction(after)
		expected, wantErr := canonicalFunction(want)
		if err != nil || wantErr != nil || actual != expected {
			return fmt.Errorf("source storage helper changed original identity, cuts or returned evidence")
		}
		return nil
	}
	wantCalls := map[string]string{
		"loadSelectedContractActivityRequest":       "storetest.ReadActivityForkRequestPayloadStorage(context.Background(), selected, runID, eventID)",
		"assertSelectedContractForkActivityAttempt": "storetest.ReadActivityForkAttemptStorage(context.Background(), selected, requestEventID)",
		"loadSelectedContractActivityResult":        "storetest.ReadActivityForkResultStorage(context.Background(), selected, runID, eventID)",
	}
	if wantCalls[name] == "" || !strings.Contains(after, wantCalls[name]) {
		return fmt.Errorf("activity read helper lost exact selected coordinates: %s", name)
	}
	beforeAssertions, beforeErr := runSetupAssertions(before)
	afterAssertions, afterErr := runSetupAssertions(after)
	if beforeErr != nil || afterErr != nil || strings.Join(beforeAssertions, "\n") != strings.Join(afterAssertions, "\n") {
		return fmt.Errorf("activity read helper lost an original decode, type, identity or failure assertion: %s", name)
	}
	return nil
}

func family145ActivityReadOwner(name string) bool {
	switch name {
	case "runSelectedContractActivityForkCases", "readActivityForkSourceEvidence", "loadSelectedContractActivityRequest", "assertSelectedContractForkActivityAttempt", "loadSelectedContractActivityResult":
		return true
	}
	return false
}
