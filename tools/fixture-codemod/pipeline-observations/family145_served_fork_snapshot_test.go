package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
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

func family145ServedSnapshotProjection(function string) bool {
	return function == "startServedControlProofRuntimeWithFixture" || function == "startServedTestSetupEntitiesProofRuntimeConfigured" || function == "newRetainedMailboxCompletionRuntimeConfigured"
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

func family145ServedSnapshotConsumerProofError(function, before, after string) error {
	after = family145ServedDebugCallerInverse(function, after)
	normalized := after
	normalized = strings.ReplaceAll(normalized, "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", "snapshotForkReceiverApplication(t, rt)")
	switch function {
	case "forkReceiverChangedTables":
		normalized = strings.Replace(normalized, "func forkReceiverChangedTables[T any](before, after map[string]T)", "func forkReceiverChangedTables(before, after map[string][]string)", 1)
	case "TestSelectedForkPendingInputMixedCompletionBothStores":
		normalized = strings.Replace(normalized, "before[\"runs\"].Columns", "before[\"runs/columns\"]", 1)
		normalized = strings.Replace(normalized, "range before[\"runs\"].Rows", "range before[\"runs\"]", 1)
		normalized = strings.Replace(normalized, "range after[\"runs\"].Rows", "range after[\"runs\"]", 1)
	default:
		return fmt.Errorf("unreviewed snapshot consumer: %s", function)
	}
	if normalized != before {
		return fmt.Errorf("snapshot consumer lost a key/row/column/history/identity assertion: %s", function)
	}
	return nil
}

func family145RetainedJoinReaderProofError(before, after string) error {
	const prefix = "func readRetainedRootJoins(t *testing.T, rt servedControlProofRuntime, runID string) (loopruntime.Activation, []joinruntime.Activation) {\n\tt.Helper()\n\tdeadline := time.Now().Add(servedProofPollDeadline)\n\tfor {\n"
	const suffix = "\n\t\tsettled := len(joins) > 0\n\t\tfor _, join := range joins {\n\t\t\tsettled = settled && join.TimerCancelled && !join.OutcomePending && join.OutcomeFired\n\t\t}\n\t\tif settled {\n\t\t\treturn loop, joins\n\t\t}\n\t\tif !time.Now().Before(deadline) {\n\t\t\tfor _, join := range joins {\n\t\t\t\tt.Logf(\"JOIN_COMPLETION_PENDING status=%s close_reason=%s outputs=%d timer_cancelled=%t outcome_pending=%t outcome_fired=%t\", join.Status, join.CloseReason, len(join.Outputs), join.TimerCancelled, join.OutcomePending, join.OutcomeFired)\n\t\t\t}\n\t\t\tt.Fatal(\"retained join outcome/cancellation did not settle before the existing proof deadline\")\n\t\t}\n\t\ttime.Sleep(10 * time.Millisecond)\n\t}\n}"
	if !strings.HasPrefix(after, prefix) || !strings.HasSuffix(after, suffix) {
		return fmt.Errorf("retained completion wait lost cancellation/publication/empty/read-error/deadline refusal")
	}
	body := after[len(prefix) : len(after)-len(suffix)]
	lines := strings.Split(body, "\n")
	for i := range lines {
		lines[i] = strings.TrimPrefix(lines[i], "\t")
	}
	normalized := strings.Replace(prefix[:strings.Index(prefix, "\n\tdeadline")], "\n\tt.Helper()", "", 1) + "\n\tt.Helper()\n" + strings.Join(lines, "\n") + "\n\treturn loop, joins\n}"
	if normalized != before {
		return fmt.Errorf("retained completion read/decode/scope changed")
	}
	return nil
}

func family145RetainedJoinConsumerProofError(before, after string) error {
	after = family145ForkCompletionInverse("testServedJoinWriterCompletedHistoryRefusal", after)
	const diagnostic = "\n\t\t\t\tt.Logf(\"JOIN_COMPLETION_CUT status=%s close_reason=%s outputs=%d timer_cancelled=%t outcome_pending=%t outcome_fired=%t\", join.Status, join.CloseReason, len(join.Outputs), join.TimerCancelled, join.OutcomePending, join.OutcomeFired)"
	normalized := strings.Replace(after, diagnostic, "", 1)
	normalized = strings.ReplaceAll(normalized, "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", "snapshotForkReceiverApplication(t, rt)")
	if normalized != before {
		return fmt.Errorf("retained join consumers lost original generation/repeat/history/fork-refusal/cleanup assertions")
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

func family145ServedSnapshotCallerProofError(function, before, after string) error {
	after = family145WriterStageCallerInverse(function, after)
	var err error
	after, err = family145ForkSourceDomainCallerInverse(function, after)
	if err != nil {
		return err
	}
	if family145ForkProducerCallers[function] != 0 {
		var err error
		after, err = family145ForkProducerCallerInverse(function, after)
		if err != nil {
			return err
		}
	}
	if family145ForkReceiverJoinedCallers[function] != 0 {
		var err error
		after, err = family145ForkReceiverJoinedCallerInverse(function, after)
		if err != nil {
			return err
		}
	}
	const native = "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)"
	want := family145ServedSnapshotCallers[function]
	if want == 0 || strings.Count(after, native) != want {
		return fmt.Errorf("served snapshot caller cardinality changed: %s", function)
	}
	normalized := strings.ReplaceAll(after, native, "snapshotForkReceiverApplication(t, rt)")
	if normalized != before {
		return fmt.Errorf("served snapshot caller lost original assertions/cuts/cleanup: %s", function)
	}
	return nil
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
		if seen[row.Function] {
			t.Fatalf("duplicate projection/snapshot: %s", row.Function)
		}
		seen[row.Function] = true
		if err := prove(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(row.File), row.Function)
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

func family145ServedSnapshotCallerInventory(t *testing.T) map[string]int {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	counts := map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || family145NodeString(call.Fun) != "snapshotForkReceiverApplication" {
					return true
				}
				if len(call.Args) != 2 || family145NodeString(call.Args[1]) != "rt.ReadApplicationSnapshot" {
					t.Fatalf("snapshot transports a raw or foreign owner: %s/%s", path, function.Name.Name)
				}
				counts[function.Name.Name]++
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}
