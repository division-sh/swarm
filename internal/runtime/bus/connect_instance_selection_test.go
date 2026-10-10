package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestConnectSelectionPreservesJoinedLookupFailure(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	plan := mustInstanceKeyConnectRoutePlan(t, source)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	rootObservation := constructionIndexObservation(t, source, busInternalTestRunID, root, "")
	missing := &pipeline.WorkflowInstanceLookupMiss{RequestedKey: "account"}
	independent := errors.New("independent index failure")
	terminal := &pipeline.TerminalReceiverError{FlowID: "account", Stage: "completed"}
	foreign := &pipeline.TerminalReceiverError{FlowID: "foreign", Stage: "completed"}
	corrupt := &pipeline.FlowInstanceConstructionCorruption{RunID: busInternalTestRunID, FlowID: "account", Cause: terminal}
	terminated := &pipeline.TerminatedReceiverError{FlowID: "account"}
	for _, test := range []struct {
		name   string
		err    error
		mapped bool
		kept   []error
	}{
		{"isolated miss", missing, true, nil},
		{"wrapped miss", fmt.Errorf("lookup: %w", missing), true, nil},
		{"joined independent failure", errors.Join(missing, independent), false, []error{missing, independent}},
		{"isolated terminal", terminal, true, nil},
		{"wrapped terminal", fmt.Errorf("lookup: %w", terminal), true, nil},
		{"single terminal join", errors.Join(terminal), true, nil},
		{"joined terminal failure", errors.Join(terminal, independent), false, []error{terminal, independent}},
		{"foreign terminal", foreign, false, []error{foreign}},
		{"corruption wrapping terminal", corrupt, false, []error{corrupt, terminal}},
		{"isolated terminated", terminated, true, nil},
		{"wrapped terminated", fmt.Errorf("lookup: %w", terminated), true, nil},
		{"joined terminated failure", errors.Join(terminated, independent), false, []error{terminated, independent}},
		{"unknown lifecycle failure", independent, false, []error{independent}},
		{"canceled lookup", context.Canceled, false, []error{context.Canceled}},
		{"joined terminal cancellation", errors.Join(terminal, context.Canceled), false, []error{terminal, context.Canceled}},
		{"joined terminated cancellation", errors.Join(terminated, context.Canceled), false, []error{terminated, context.Canceled}},
	} {
		t.Run(test.name, func(t *testing.T) {
			selector := connectInstanceSelector{source: source, index: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation}, selectionErr: test.err}}
			event := eventtest.ExistingRunRootIngress(eventtest.UUID(test.name), "producer/account.ready", "test", "", []byte(`{"account_id":"acct-1"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
			materialized, selection, handled, err := selector.Materialize(constructionIndexContext(t, source), event, plan, map[string]string{"payload.account_id": "acct-1"})
			if !handled || selection.identity != (flowidentity.Instance{}) {
				t.Fatalf("failed selection became a receiver: %+v %t", selection, handled)
			}
			if test.mapped {
				if err != nil || materialized.Failure != pinrouting.ConnectFailureTargetUnresolved {
					t.Fatalf("isolated miss: %+v %v", materialized, err)
				}
			} else {
				if err == nil || !materialized.Failure.Empty() {
					t.Fatalf("independent error was mapped away: %+v %v", materialized, err)
				}
				for _, kept := range test.kept {
					if !errors.Is(err, kept) {
						t.Fatalf("selection lost cause %v: %v", kept, err)
					}
				}
			}
		})
	}
}
