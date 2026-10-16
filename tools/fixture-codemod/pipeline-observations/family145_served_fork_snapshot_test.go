package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func family145ServedSnapshotCaptureInverse(after string, results int) (string, error) {
	const oldCapture = "\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\tdeliveryReader = p.deps.DeliveryStore.SummarizeRun\n\t})"
	const capture = "\tvar snapshotReader func(context.Context) (map[string]storetest.SelectedForkStorageTableSnapshot, error)\n\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\tdeliveryReader = p.deps.DeliveryStore.SummarizeRun\n\t\tsnapshotReader = func(ctx context.Context) (map[string]storetest.SelectedForkStorageTableSnapshot, error) {\n\t\t\treturn storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, p.deps.EventStore)\n\t\t}\n\t})"
	if strings.Count(after, capture) != 1 || strings.Count(after, "ReadApplicationSnapshot: snapshotReader, ") != results {
		return "", fmt.Errorf("every backend result requires the exact selected projection reader")
	}
	normalized := strings.Replace(after, capture, oldCapture, 1)
	normalized = strings.ReplaceAll(normalized, "ReadApplicationSnapshot: snapshotReader, ", "")
	return normalized, nil
}

func family145ServedProjectionProofError(function, before, after string) error {
	results := 2
	if function == "newRetainedMailboxCompletionRuntimeConfigured" {
		after = strings.NewReplacer(
			"\n\tvar receiverReader receiverProofStateReader", "",
			"\n\t\treceiverReader = requireReceiverProofStateReader(t, p.deps.EventStore)", "",
			"ReceiverStateReader: receiverReader, ", "",
		).Replace(after)
		results = 1
	} else if function != "startServedControlProofRuntimeWithFixture" && function != "startServedTestSetupEntitiesProofRuntimeConfigured" {
		return fmt.Errorf("unreviewed served snapshot projection: %s", function)
	}
	normalized, err := family145ServedSnapshotCaptureInverse(after, results)
	if err != nil {
		return err
	}
	if function == "newRetainedMailboxCompletionRuntimeConfigured" {
		normalized, err = family145ServedRestartConstructionInverse(before, normalized)
		if err != nil {
			return err
		}
	}
	if normalized != before {
		return fmt.Errorf("served source/options/handlers/context/workspace/cleanup/readers changed: %s", function)
	}
	return nil
}

func family145ServedForkSnapshotProofError(before, after string) error {
	const expected = "func snapshotForkReceiverApplication(t *testing.T, read func(context.Context) (map[string]storetest.SelectedForkStorageTableSnapshot, error)) map[string]storetest.SelectedForkStorageTableSnapshot {\n\tt.Helper()\n\tif read == nil {\n\t\tt.Fatal(\"application snapshot requires its exact selected serving projection\")\n\t}\n\tsnapshot, err := read(context.Background())\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\treturn snapshot\n}"
	if after != expected || !strings.Contains(before, "sql.LevelRepeatableRead") || !strings.Contains(before, "SELECT * FROM") || !strings.Contains(before, "json.Marshal(columns)") {
		return fmt.Errorf("whole application snapshot did not consume the exact existing native owner")
	}
	return nil
}

var family145ServedSnapshotCallers = map[string]int{
	"runServedRunForkBackendProof":                                                 2,
	"TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores":               2,
	"runForkReceiverOwnershipJourney":                                              2,
	"TestSelectedForkPendingInputMixedCompletionBothStores":                        2,
	"testServedJoinWriterCompletedHistoryRefusal":                                  2,
	"TestExplicitTargetRestrictsConnectedSiblingsBothStores":                       2,
	"TestSelectedForkPendingInputBothStores":                                       4,
	"TestSupportedHumanNoticeAcknowledgmentAfterRetirementBothStores":              3,
	"requireLifecycleFrozenGateControlRefusal":                                     2,
	"TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores": 2,
	"TestRunForkHistoricalIdentityPublicExecutionBothStores":                       5,
	"TestSelectedForkMailboxControlRefusalsBothStores":                             2,
	"TestSelectedForkPendingInputEvidenceBothStores":                               2,
}

func TestFamily145ServedForkSnapshotsUseExactProjectionAndClosedReadOwner(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		var prove func(string, string) error
		switch row.Function {
		case "startServedControlProofRuntimeWithFixture", "startServedTestSetupEntitiesProofRuntimeConfigured", "newRetainedMailboxCompletionRuntimeConfigured":
			prove = func(before, after string) error {
				return family145ServedProjectionProofError(row.Function, before, after)
			}
		case "snapshotForkReceiverApplication":
			prove = family145ServedForkSnapshotProofError
		case "lifecycleRestartHarness":
			prove = func(before, after string) error {
				return family145ServedRestartConstructionProofError(row.Function, before, after)
			}
		default:
			continue
		}
		row.After = family145RequestKeyCallerInverse(row.Function, sourceBlobPreviousDeclaration(t, row.File, row.Function, row.After))
		if seen[row.Function] {
			t.Fatalf("duplicate projection/snapshot: %s", row.Function)
		}
		seen[row.Function] = true
		if err := prove(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(row.File), row.Function)
		actual = family145RequestKeyCallerInverse(row.Function, sourceBlobPreviousDeclaration(t, row.File, row.Function, actual))
		if actual != row.After {
			t.Fatalf("served snapshot projection/read escaped its exact finite recipe: %s", row.Function)
		}
		for _, cut := range []string{"ReadSelectedForkApplicationStorageSnapshot(ctx, p.deps.EventStore)", "ReadApplicationSnapshot: snapshotReader", "snapshot, err := read(context.Background())", "if read == nil", "if err != nil"} {
			if strings.Contains(row.After, cut) && prove(row.Before, strings.Replace(row.After, cut, "foreignSelectedProjection()", 1)) == nil {
				t.Fatalf("foreign/missing projection/snapshot passed: %s/%s", row.Function, cut)
			}
		}
	}
	if len(seen) != 5 {
		t.Fatalf("whole projection/snapshot dependency=%d/5", len(seen))
	}
}
