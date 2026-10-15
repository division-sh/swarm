package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

var family145ForkProducerCallers = map[string]int{
	"runForkReceiverOwnershipJourney":                                                      3,
	"TestSelectedForkIndependentReceiverBusinessMutationBothStores":                        2,
	"TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores": 2,
	"requireLifecycleRootLoopForkRevision":                                                 1,
}

func family145ForkProducerCallerInverse(function, after string) (string, error) {
	const native = "readForkReceiverProducerEvidence(t, rt.Runtime.Bus.Store(), "
	want := family145ForkProducerCallers[function]
	if want == 0 || strings.Count(after, native) != want || strings.Contains(after, "readForkReceiverProducerEvidence(t, rt, ") {
		return "", fmt.Errorf("producer evidence lost the original selected bus owner/call count: %s", function)
	}
	return strings.ReplaceAll(after, native, "readForkReceiverProducerEvidence(t, rt, "), nil
}

func family145ForkProducerReaderProofError(before, after string) error {
	const expected = `func readForkReceiverProducerEvidence(t *testing.T, owner runtimebus.EventStore, runID, eventID string) forkReceiverProducerEvidence {
	t.Helper()
	event := storetest.LoadCanonicalEventRecord(t, context.Background(), owner, eventID)
	if event.RunID() != runID {
		t.Fatalf("producer event %s belongs to run %s, not requested run %s", eventID, event.RunID(), runID)
	}
	return forkReceiverProducerEvidence{Source: event.RoutingSource(), Name: string(event.Type()), Mode: string(event.ExecutionMode())}
}`
	if after != expected || !strings.Contains(before, "WHERE run_id=$1 AND event_id=$2") || !strings.Contains(before, "RestoreRoutingSource(kind, route, authority)") {
		return fmt.Errorf("producer evidence changed exact run/event/source/name/mode or reused a partial/noncanonical reader")
	}
	return nil
}

func family145ForkProducerCallerProofError(function, before, after string) error {
	after = family145WriterStageCallerInverse(function, after)
	var err error
	after, err = family145ForkSourceDomainCallerInverse(function, after)
	if err != nil {
		return err
	}
	normalized, err := family145ForkProducerCallerInverse(function, after)
	if err != nil {
		return err
	}
	if family145ForkReceiverJoinedCallers[function] != 0 {
		normalized, err = family145ForkReceiverJoinedCallerInverse(function, normalized)
		if err != nil {
			return err
		}
	}
	if family145ServedSnapshotCallers[function] != 0 {
		normalized = strings.ReplaceAll(normalized, "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", "snapshotForkReceiverApplication(t, rt)")
	}
	if normalized != before {
		return fmt.Errorf("producer caller changed original source/fork/replay/identity/assertions/cleanup: %s", function)
	}
	return nil
}

func TestFamily145ForkProducerEvidenceUsesCanonicalRecordAndPreservesRequestedRun(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(servedSetupEarlierHistory(t, family145TransitionBytes, false), &rows); err != nil {
		t.Fatal(err)
	}
	seen, helpers := map[string]bool{}, 0
	for _, row := range rows {
		if row.Function == "readForkReceiverProducerEvidence" {
			helpers++
			family145ForkProducerCheckReader(t, row.Before, row.After)
		} else if family145ForkProducerCallers[row.Function] != 0 {
			seen[row.Function] = true
			if err := family145ForkProducerCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if _, err := family145ForkProducerCallerInverse(row.Function, strings.Replace(row.After, "readForkReceiverProducerEvidence(t, rt.Runtime.Bus.Store(), ", "readForkReceiverProducerEvidence(t, foreignProducerOwner(), ", 1)); err == nil {
				t.Fatal("producer caller accepted foreign selected authority")
			}
		} else {
			continue
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(row.File), row.Function)
		if actual != row.After {
			t.Fatalf("producer evidence escaped its finite source: %s", row.Function)
		}
	}
	if helpers != 1 || len(seen) != len(family145ForkProducerCallers) {
		t.Fatalf("producer reader=%d/1 callers=%d/%d", helpers, len(seen), len(family145ForkProducerCallers))
	}
	counts := family145ForkReceiverJoinedOwnedInventory(t)
	for function, want := range family145ForkProducerCallers {
		if counts["producer/"+function] != want {
			t.Fatalf("producer caller count %s=%d/%d", function, counts["producer/"+function], want)
		}
	}
}

func family145ForkProducerCheckReader(t *testing.T, before, after string) {
	t.Helper()
	if err := family145ForkProducerReaderProofError(before, after); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"LoadCanonicalEventRecord(t, context.Background(), owner, eventID)", "event.RunID() != runID", "t.Fatalf", "event.RoutingSource()", "string(event.Type())", "string(event.ExecutionMode())"} {
		if family145ForkProducerReaderProofError(before, strings.Replace(after, cut, "foreignProducerFact()", 1)) == nil {
			t.Fatalf("producer evidence lost identity/source/name/mode/read-error refusal: %s", cut)
		}
	}
}
