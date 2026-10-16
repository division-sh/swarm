package main

import (
	"fmt"
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
