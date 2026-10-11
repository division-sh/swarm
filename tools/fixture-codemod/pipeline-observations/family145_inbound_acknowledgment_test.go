package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var family145InboundAcknowledgmentCalls = map[string][]string{
	"inboundAcknowledgedSelectedFixture":                                             {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)", "return ctx, selected, target"},
	"TestInboundCommittedSiblingFinalizationRecoversDurablePipelineBothStores":       {"runtimepipeline.NewRecoveryManagerWith(bus).Recover(ctx)", "storetest.CountPipelineEventReceiptStorage(ctx, selected, item.EventID)", "standing.RetireAndWait(context.Background())", "runtimeOwner.RetireAndWait(context.Background())", "process.Join(context.Background())"},
	"TestInboundAcknowledgedPublicationCleanupRespondsAndDoesNotRedeliverBothStores": {"wrapped.hideLoads = 1", "wrapped.corruptRecord = true", "wrapped.missingAck = true", "countInboundMarkers(t, ctx, readback, target,", "countInboundAgentDeliveries(t, ctx, readback, rawEventID, agentID)"},
	"TestInboundAcknowledgedCreatedResultRejectsChangedExecutionFactsBothStores":     {"corruptCreatedFact: fact", "http.StatusServiceUnavailable", "wrapped.commits != 1"},
	"TestInboundAcknowledgedOperatorClaimCleanupRespondsBothStores":                  {"storetest.CountPhysicalEventDeliveries(context.Background(), selected)", "operatorchannel.DispositionConsumedBinding", "wrapped.hideLoads = 1", "wrapped.corruptRecord = true", "wrapped.missingAck = true", "operatorchannel.StateAwaitingConfirmation"},
}

func family145InboundAcknowledgmentProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	index := 0
	for _, assertion := range got {
		if index < len(want) && assertion == want[index] {
			index++
		}
	}
	if index != len(want) {
		return fmt.Errorf("inbound acknowledgment lost original assertion: %s", want[index])
	}
	for _, cut := range family145InboundAcknowledgmentCalls[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("inbound acknowledgment lost owner/lifecycle/fault: %s", cut)
		}
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func TestFamily145InboundAcknowledgmentChainPreservesFaultsAndJoinedRecovery(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		cuts, ok := family145InboundAcknowledgmentCalls[row.Function]
		if !ok {
			continue
		}
		if seen[row.Function] {
			t.Fatalf("duplicate inbound acknowledgment transition: %s", row.Function)
		}
		seen[row.Function] = true
		if err := family145InboundAcknowledgmentProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range cuts {
			if family145InboundAcknowledgmentProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost inbound owner/fault accepted: %s/%s", row.Function, cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145InboundAcknowledgmentProofError(row.Function, row.Before, strings.ReplaceAll(row.After, condition, "false")) == nil {
				t.Fatalf("lost inbound assertion accepted: %s/%s", row.Function, condition)
			}
		}
	}
	if len(seen) != len(family145InboundAcknowledgmentCalls) {
		t.Fatalf("inbound chain incomplete: %d/%d", len(seen), len(family145InboundAcknowledgmentCalls))
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "runtime", "inbound_commit_ack_response_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := family145InboundAcknowledgmentEscape(data); err != nil {
		t.Fatal(err)
	}
	for _, escape := range []string{"storetest.AdmitPostgresRuntimeStore(t, db)", "storetest.DatabaseForTest(selected)", "db.QueryRow(query)", "(*sql.DB)(nil)"} {
		if family145DescriptorReadinessEscape([]byte("package probe; func escape() { _ = "+escape+" }")) == nil {
			t.Fatalf("raw inbound carrier accepted: %s", escape)
		}
	}
	for _, hostile := range []string{
		strings.Replace(string(data), "return o.occurrence.Begin(ctx)", "return db.Begin()", 1),
		strings.Replace(string(data), "*worklifetime.StandingOccurrence", "*sql.DB", 1),
	} {
		if family145InboundAcknowledgmentEscape([]byte(hostile)) == nil {
			t.Fatal("native standing work occurrence replaced by raw transaction authority")
		}
	}
}

func family145InboundAcknowledgmentEscape(data []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "inbound.go", data, parser.AllErrors)
	if err != nil {
		return err
	}
	// This unchanged, source-pinned Begin is live worklifetime admission, not
	// SQL Begin. Classify that exact declaration, never a filename or method-name
	// exemption; every altered body and raw occurrence carrier still fails.
	const original = `func (o inboundExactStandingRecoveryOwner) BeginStandingRunRecovery(ctx context.Context, runID string, origin runtimerunlifecycle.RunOrigin) (*worklifetime.Lease, error) {
	identity := o.occurrence.Identity()
	if identity.RunID != runID || identity.ServiceID != origin.ServiceID() || identity.Generation != uint64(origin.Generation()) {
		return nil, errors.New("standing recovery requested a different generation")
	}
	return o.occurrence.Begin(ctx)
}`
	declarations := file.Decls[:0]
	workOwner := false
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && fn.Name.Name == "BeginStandingRunRecovery" {
			if workOwner {
				return fmt.Errorf("duplicate standing recovery work owner")
			}
			if family145NodeString(fn) != original {
				return fmt.Errorf("standing recovery work owner changed")
			}
			workOwner = true
			continue
		}
		declarations = append(declarations, declaration)
	}
	file.Decls = declarations
	if !workOwner {
		return fmt.Errorf("standing recovery work owner missing")
	}
	return family145DescriptorReadinessEscape([]byte(family145NodeString(file)))
}
