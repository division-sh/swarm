package bus

import (
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
	for _, test := range []struct {
		name   string
		err    error
		mapped bool
	}{
		{"isolated miss", missing, true},
		{"wrapped miss", fmt.Errorf("lookup: %w", missing), true},
		{"joined independent failure", errors.Join(missing, independent), false},
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
			} else if !errors.Is(err, independent) || !errors.Is(err, missing) || !materialized.Failure.Empty() {
				t.Fatalf("independent error was mapped away: %+v %v", materialized, err)
			}
		})
	}
}
