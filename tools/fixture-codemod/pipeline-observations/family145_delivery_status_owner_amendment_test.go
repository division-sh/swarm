package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os/exec"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const repairedDeliveryStatusBridge = `func ReadServedDeliveryStatusCountForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
if err:=validateChannelObservationOwner(selected); err!=nil { return 0,err }
_,postgres:=selected.(*PostgresStore)
var count int
err:=readServedDeliveryObservation(ctx,selected,func(ctx context.Context,tx *sql.Tx)error {
var err error; count,err=delivery.ReadServedDeliveryStatusCount(ctx,tx,postgres,eventID,subscriberType,subscriberID,statuses...); return err
})
if err!=nil { return 0,err }; return count,nil
}`

// Historical whole-file evidence composes only these named, separately approved
// amendments. Every byte outside the exact declarations still matches history.
func deliveryStatusEarlierOwnerSource(t *testing.T, path string, current, original []byte) []byte {
	t.Helper()
	current = servedObservationEarlierToolOwnerSource(t, path, current)
	var err error
	switch path {
	case "internal/store/internal/backend/delivery/read_projections.go":
		current, err = remapDeliveryStatusDeclaration(current, "StandingDeliveryDiagnostic", `type StandingDeliveryDiagnostic struct{ EventID, Subscriber, Status string }`, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, endpoint := range standingDiagnosticManifest.Endpoints {
			if endpoint.File == path && endpoint.Function == "ReadStandingDeliveryDiagnosticsTx" {
				current, err = remapDeliveryStatusDeclaration(current, endpoint.Function, endpoint.After, "")
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		current = deliveryStatusEarlierStopOwner(t, path, current, original)
	case "internal/store/internal/runtimepersistence/test_served_delivery_observation.go":
		for _, endpoint := range [][2]string{
			{"CountTemplatePrefixRoutingRulesForTest", `func CountTemplatePrefixRoutingRulesForTest(ctx context.Context, selected any, template string)(int,error){return readFanOutSemanticStorage(ctx,selected,nil,func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplatePrefixRoutingRulesTx(ctx,tx,template)})}`},
			{"CountTemplateEventReplayScopesForTest", `func CountTemplateEventReplayScopesForTest(ctx context.Context, selected any, event string)(int,error){return readFanOutSemanticStorage(ctx,selected,[]string{event},func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplateEventReplayScopesTx(ctx,tx,event)})}`},
		} {
			current, err = removePublicRollbackDeclaration(current, endpoint[0], endpoint[1])
			if err != nil {
				t.Fatal(err)
			}
		}
		prior, _, _, lookupErr := deliveryStatusSourceDeclaration(original, "ReadServedDeliveryStatusCountForTest")
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		current, err = remapDeliveryStatusDeclaration(current, "ReadServedDeliveryStatusCountForTest", repairedDeliveryStatusBridge, prior)
		if err != nil {
			t.Fatal(err)
		}
	}
	return current
}

// Tool-only correspondence for the reviewed additive reader chain. This does
// not add a collector inverse, occurrence transfer or accounting allowance.
func servedObservationEarlierToolOwnerSource(t *testing.T, path string, current []byte) []byte {
	t.Helper()
	prior, err := servedObservationToolOwnerPrevious(path, current)
	if err != nil {
		t.Fatal(err)
	}
	return prior
}

func servedObservationToolOwnerPrevious(path string, current []byte) ([]byte, error) {
	var err error
	current, err = selectedreadrole.Repair2698HistoricalSource("../../..", path, current)
	if err != nil {
		return nil, err
	}
	if path == agentSnapshotEventOwner && fmt.Sprintf("%x", sha256.Sum256(current)) == "04d9e5826aa632dc59cc9fa0ccd643def30848294387a3de3f50121ca4a6951f" {
		// The accepted native-entry composition follows this exact additive
		// reader cut. Keep both whole-file endpoints, not a new current hash.
		current, err = exec.Command("git", "-C", "../../..", "show", "d99a19dd941cd5c54ac58951d6298adf95adfcff:"+path).Output()
		if err != nil {
			return nil, err
		}
	}
	current, err = deliveryStatusEarlierScenarioBridges(path, current)
	if err != nil {
		return nil, err
	}
	var pin struct {
		current, previous string
		names             []string
	}
	switch path {
	case "internal/store/internal/backend/delivery/read_projections.go":
		current, err = deliveryStatusEarlierInterveningReaders(path, current)
		if err != nil {
			return nil, err
		}
		pin.current, pin.previous = "77456d1d968d550b5d11590bc6d4cddf3ac95aaba42318913c18cbff1da545ee", "f3102e2e31e761dd80917959af16b8e151471fa32c70378677ccf88e51ef4a63"
		pin.names = []string{"CountExactRunNodeEventDeliveryStorageTx", "CountCurrentEventDeliveryOutcomeStorageTx"}
	case "internal/store/internal/runtimepersistence/test_served_delivery_observation.go":
		pin.current, pin.previous = "256de06dd68f2c2fe45eac7c02ae8726e022696e6d977db3ca74f981d3e865a7", "bb5b2b2ec0c32fc6153b712be59cf827c2e858e02b54a7c5cf541b60aac96be3"
		pin.names = []string{"CountExactRunNodeEventDeliveryStorageForTest", "CountCurrentEventDeliveryOutcomeStorageForTest", "CountSubscriberEventReceiptOutcomeStorageForTest", "CountExactRunEventReplayScopeStorageForTest", "CountEventSubscriberReceiptStorageForTest"}
	case "internal/store/storetest/served_delivery_observation.go":
		pin.current, pin.previous = "4b886554c4ab96f56df19e070aece6c3d91fbc7c2fb223838b4d590b95cc6cd5", "cb8fc0e18c68ee00cb4fbb3af36eb965a529acbe91ab0c096e636396381526e2"
		pin.names = []string{"CountExactRunNodeEventDeliveryStorage", "CountCurrentEventDeliveryOutcomeStorage", "CountSubscriberEventReceiptOutcomeStorage", "CountExactRunEventReplayScopeStorage", "CountEventSubscriberReceiptStorage"}
	case "internal/store/internal/runtimepersistence/test_event_support.go":
		pin.current, pin.previous = "b4b7fd3c33610f6758ef73c329c574789ecd18dc881416f5b431007f8f86a437", "b6da96ad49b2f38e1c8422bdc3e14e1173e79f908f7587619a81ba4ea7382134"
		pin.names = []string{"ReadPhysicalPublicationEntityIdentityStorageForTest"}
	default:
		return current, nil
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != pin.current {
		return nil, fmt.Errorf("reviewed additive native reader source changed: %s", path)
	}
	for _, name := range pin.names {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return nil, err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return nil, err
		}
	}
	previous, err := format.Source(current)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != pin.previous {
		return nil, fmt.Errorf("reviewed additive native reader changed retained bytes: %s: %v", path, err)
	}
	return previous, nil
}

func deliveryStatusEarlierScenarioBridges(path string, current []byte) ([]byte, error) {
	var names []string
	switch path {
	case "internal/store/internal/runtimepersistence/test_served_delivery_observation.go":
		names = []string{"CountGlobalDeliveredNodeRowsStorageForTest", "ReadCanonicalRoutingSQLiteDiagnosticForTest"}
	case "internal/store/storetest/served_delivery_observation.go":
		names = []string{"CountGlobalDeliveredNodeRowsStorage", "ReadCanonicalRoutingSQLiteDiagnostic"}
	default:
		return current, nil
	}
	current, err := selectedreadrole.CatchUp2542HistoricalSource("../../..", path, current)
	if err != nil {
		return nil, err
	}
	packet, err := selectedreadrole.CatchUp2542Receipts()
	if err != nil {
		return nil, err
	}
	signed, err := exec.Command("git", "-C", "../../..", "show", packet.Lane+":"+path).Output()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		body, _, _, err := deliveryStatusSourceDeclaration(signed, name)
		if err != nil {
			return nil, err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return nil, err
		}
	}
	return format.Source(current)
}

func deliveryStatusEarlierInterveningReaders(path string, current []byte) ([]byte, error) {
	// The existing whole-file inverses authenticate the live scenario/recovery,
	// selected-fork and standing edges before this earlier additive reader cut.
	current, err := earlierStandingIngressOwnerFile("../../..", path, current)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != standingIngressOwnerAdditions[1].before {
		return nil, fmt.Errorf("delivery polling comparison lost its complete standing predecessor")
	}
	for _, name := range strings.Fields(`
ForkReceiverSettlementStorage ReadForkReceiverSettlementStorageTx ReadForkReceiverBusinessSettlementStorageTx
CountForkReceiverSettledAttemptsStorageTx ReadForkReceiverRuleSelectionStorageTx
PublicationRuleSelectionStorage ReadPublicationRuleSelectionStorageTx ReadDeliveryPayloadProjectionTextStorageTx
CountSuccessfulDirectivePipelineReceiptsStorageTx ReadDirectivePipelineFailureStorageTx
CountUnfinishedMockAgentDeliveriesStorageTx MockAgentDeliveryDiagnosticStorage MockAgentDeliveryDiagnosticsStorage
ReadMockAgentDeliveryDiagnosticsStorageTx ReadMockConnectorDeliveryDiagnosticsStorageTx CountUnfinishedMemoryAgentDeliveriesStorageTx
ServedDeliveryRouteEvidenceStorage ServedTemplateCompletionDiagnosticStorage scanServedPhysicalDiagnosticRows
ReadTemplateCompletionDeliveryDiagnosticsStorageTx ReadServedDeliveryRouteEvidenceStorageTx
DeliveryRecoveryIdentityStorage ReadDeliveryRecoveryIdentityStorageTx DeliveryRecoveryAttemptStorage ReadDeliveryRecoveryAttemptsStorageTx
CountSourceRunDeliveryStorageTx OrderedFanOutOutcomeStorage ReadOrderedRunFanOutOutcomesStorageTx
CommittedFanOutOrdinalStorage ReadCommittedRunFanOutOrdinalsStorageTx
CountRunDeliveredRowsStorageTx DeploymentEventDeliveryCutStorage ReadDeploymentPreReceiptDeliveryCutStorageTx
ReadDeploymentPendingReceiverCutStorageTx DeploymentRunEventDeliveryCutStorage ReadDeploymentRunEventDeliveryCutStorageTx
CountDeploymentRunDeliveryAttemptsStorageTx SelectedDeploymentClaimStorage ReadSelectedDeploymentClaimStorageTx
SelectedDeliveryExecutionStorage ReadSelectedDeliveryExecutionStorageTx CountDeliveredClaimVersionStorageTx
CountDeliveredSuccessorClaimVersionsStorageTx CountPinnedPortfolioSettledDeliveriesStorageTx
PinnedPortfolioDeliveryDiagnosticStorage ReadPinnedPortfolioDeliveryDiagnosticsStorageTx
scanChannelBudgetDiagnosticRows LoopReceiverSettlementStorage ReadLoopReceiverSettlementStorageTx
CountReceiverFirstClaimAgentDeliveriesStorageTx ReadChannelBudgetDeliveriesDiagnosticsStorageTx`) {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return nil, err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return nil, err
		}
	}
	const comment = "// Fixed channel budget failure evidence; no executable-delivery interpretation.\n"
	if bytes.Count(current, []byte(comment)) != 1 {
		return nil, fmt.Errorf("delivery polling comparison lost the exact diagnostic declaration comment")
	}
	return format.Source(bytes.Replace(current, []byte(comment), nil, 1))
}

func removePublicRollbackDeclaration(raw []byte, name, expected string) ([]byte, error) {
	current, start, end, err := deliveryStatusSourceDeclaration(raw, name)
	if err != nil {
		return nil, err
	}
	if canonicalDeliveryStatusDeclaration(current) != canonicalDeliveryStatusDeclaration(expected) || end >= len(raw) || raw[end] != '\n' {
		return nil, fmt.Errorf("unreviewed rollback declaration addition: %s", name)
	}
	// The addition occupied one declaration plus its trailing newline; deleting
	// that exact span preserves the historical spacing of adjacent declarations.
	return append(append([]byte{}, raw[:start]...), raw[end+1:]...), nil
}

func deliveryStatusEarlierStopOwner(t *testing.T, path string, current, original []byte) []byte {
	t.Helper()
	for _, pin := range stopWitnessPins(t) {
		if pin.File != path || pin.Function != "ReadStopPendingDeliveryCountTx" {
			continue
		}
		prior, _, _, err := deliveryStatusSourceDeclaration(original, pin.Function)
		if err != nil {
			t.Fatal(err)
		}
		current, err = remapDeliveryStatusDeclaration(current, pin.Function, pin.After, prior)
		if err != nil {
			t.Fatal(err)
		}
	}
	return current
}

func remapDeliveryStatusDeclaration(raw []byte, name, expected, prior string) ([]byte, error) {
	current, start, end, err := deliveryStatusSourceDeclaration(raw, name)
	if err != nil {
		return nil, err
	}
	if canonicalDeliveryStatusDeclaration(current) != canonicalDeliveryStatusDeclaration(expected) {
		return nil, fmt.Errorf("unreviewed delivery status endpoint amendment: %s", name)
	}
	out := append(append(append([]byte{}, raw[:start]...), prior...), raw[end:]...)
	return format.Source(out)
}

func canonicalDeliveryStatusDeclaration(raw string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+raw, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "invalid declaration: " + raw
	}
	return requestkey.Node(file.Decls[0])
}

func deliveryStatusSourceDeclaration(raw []byte, name string) (string, int, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	if err != nil {
		return "", 0, 0, err
	}
	var matched ast.Decl
	for _, declaration := range file.Decls {
		if !deliveryStatusDeclarationNamed(declaration, name) {
			continue
		}
		if matched != nil {
			return "", 0, 0, fmt.Errorf("ambiguous delivery status amendment: %s", name)
		}
		matched = declaration
	}
	if matched == nil {
		return "", 0, 0, fmt.Errorf("missing delivery status amendment: %s", name)
	}
	start, end := fset.Position(matched.Pos()).Offset, fset.Position(matched.End()).Offset
	return string(raw[start:end]), start, end, nil
}

func deliveryStatusDeclarationNamed(declaration ast.Decl, name string) bool {
	if fn, ok := declaration.(*ast.FuncDecl); ok {
		return fn.Recv == nil && fn.Name.Name == name
	}
	if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 {
		typeSpec, ok := group.Specs[0].(*ast.TypeSpec)
		return ok && typeSpec.Name.Name == name
	}
	return false
}
