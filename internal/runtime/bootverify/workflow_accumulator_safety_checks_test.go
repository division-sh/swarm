package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

func TestRunRejectsAccumulatorHandlerOnCompleteThroughSharedAdmission(t *testing.T) {
	bundle := templatefanin.LoadBundle(t, templatefanin.Options{})
	node := bundle.Nodes[templatefanin.ReceiverNodeID]
	handler := node.EventHandlers[templatefanin.ReceiverEvent]
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{ID: "finite-close", Condition: "accumulated.count >= 2"}}
	writeFlowHandler(t, bundle, templatefanin.ReceiverFlowID, templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.HardInvalidities(), checkIDAccumulatorHandlerIsolation, "cannot be combined with handler.on_complete") {
		t.Fatalf("missing shared accumulator isolation finding: %#v", report.HardInvalidities())
	}
}

func TestRunAcceptsCanonicalStreamAccumulatorProducerPath(t *testing.T) {
	bundle := templatefanin.LoadBundle(t, templatefanin.Options{})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	for _, finding := range report.HardInvalidities() {
		if finding.CheckID == checkIDAccumulatorInputProducer || strings.Contains(finding.Message, "no accepted producer/source path") {
			t.Fatalf("canonical stream producer path was rejected: %#v", report.HardInvalidities())
		}
	}
}
