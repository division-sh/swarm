package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
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
