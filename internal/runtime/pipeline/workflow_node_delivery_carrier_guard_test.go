package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type scriptedWorkflowNodeDeliveryStore struct {
	runtimedelivery.Store
	result runtimedelivery.ClaimResult
	err    error
}

func (s scriptedWorkflowNodeDeliveryStore) ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	return s.result, s.err
}

type scriptedWorkflowNodeAuthority struct {
	authority    runtimedelivery.ExecutionAuthority
	continuation *scriptedWorkflowNodeContinuation
	releases     atomic.Int32
}

func (s *scriptedWorkflowNodeAuthority) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	return s.authority, nil
}
func (s *scriptedWorkflowNodeAuthority) AcquireDeliveryContinuation(string) (worklifetime.DeliveryContinuation, error) {
	return s.continuation, nil
}
func (s *scriptedWorkflowNodeAuthority) ReleaseDeliveryContinuation(string) error {
	s.releases.Add(1)
	return nil
}

type scriptedWorkflowNodeContinuation struct {
	deliveryID  string
	returnErrs  atomic.Int32
	consumeErrs atomic.Int32
	returns     atomic.Int32
	consumes    atomic.Int32
	resolution  worklifetime.DeliveryContinuationResolution
}

func (c *scriptedWorkflowNodeContinuation) DeliveryID() string { return c.deliveryID }
func (c *scriptedWorkflowNodeContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if intent == worklifetime.DeliveryContinuationReturn {
		c.returns.Add(1)
		if c.returnErrs.Add(-1) >= 0 {
			return 0, errors.New("injected workflow-node continuation return failure")
		}
		if c.resolution != 0 {
			return c.resolution, nil
		}
		return worklifetime.DeliveryContinuationReturned, nil
	}
	if intent == worklifetime.DeliveryContinuationConsume {
		c.consumes.Add(1)
		if c.consumeErrs.Add(-1) >= 0 {
			return 0, errors.New("injected workflow-node continuation consume failure")
		}
		if c.resolution != 0 {
			return c.resolution, nil
		}
		return worklifetime.DeliveryContinuationConsumed, nil
	}
	return 0, errors.New("invalid workflow-node continuation resolution intent")
}

func workflowNodeCarrierTestEventAndRoute(t *testing.T) (events.Event, events.DeliveryRoute, string) {
	t.Helper()
	eventID := uuid.NewString()
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, "", "node-a")),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "carrier-guard"}),
	}
	deliveryID, err := runtimedelivery.DeliveryID(eventID, route)
	if err != nil {
		t.Fatalf("derive delivery id: %v", err)
	}
	evt := eventtest.PersistedProjection(
		eventID, "workflow.requested", "test", "", []byte(`{}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	return evt, route, deliveryID
}

func workflowNodeCarrierAuthorities(t *testing.T) map[string]runtimedelivery.ExecutionAuthority {
	t.Helper()
	source, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("d", 64))
	if err != nil {
		t.Fatalf("construct delivery source: %v", err)
	}
	normal, err := runtimedelivery.NewNormalExecutionAuthority(source, "runtime-a", 1)
	if err != nil {
		t.Fatalf("construct normal authority: %v", err)
	}
	selected, err := runtimedelivery.NewSelectedExecutionAuthority(source, uuid.NewString(), uuid.NewString(), 1)
	if err != nil {
		t.Fatalf("construct selected authority: %v", err)
	}
	return map[string]runtimedelivery.ExecutionAuthority{"normal": normal, "selected": selected}
}

func TestWorkflowNodeDeliveryCarrierGuardCoversEveryPreAttemptExit(t *testing.T) {
	for authorityName, authority := range workflowNodeCarrierAuthorities(t) {
		for _, test := range []struct {
			name        string
			result      func(string) runtimedelivery.ClaimResult
			claimErr    error
			wantHandled bool
			wantErr     bool
		}{
			{name: "claim_error", claimErr: errors.New("injected claim failure"), wantErr: true},
			{name: "deferred", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimDeferred}
			}, wantHandled: true},
			{name: "busy", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimBusy}
			}, wantHandled: true},
			{name: "terminal", result: func(id string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimTerminal, Snapshot: runtimedelivery.Snapshot{DeliveryID: id}}
			}, wantHandled: true},
			{name: "wrong_authority", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimWrongAuthority, Invariant: errors.New("wrong authority")}
			}, wantErr: true},
			{name: "absent", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimAbsent, Invariant: errors.New("absent")}
			}, wantErr: true},
			{name: "invalid", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimInvariantInvalid, Invariant: errors.New("invalid")}
			}, wantErr: true},
			{name: "unknown", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimDisposition("unknown")}
			}, wantErr: true},
			{name: "acquired_without_claim", result: func(string) runtimedelivery.ClaimResult {
				return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimAcquired}
			}, wantErr: true},
		} {
			t.Run(authorityName+"/"+test.name, func(t *testing.T) {
				evt, route, deliveryID := workflowNodeCarrierTestEventAndRoute(t)
				continuation := &scriptedWorkflowNodeContinuation{deliveryID: deliveryID}
				provider := &scriptedWorkflowNodeAuthority{authority: authority, continuation: continuation}
				result := runtimedelivery.ClaimResult{}
				if test.result != nil {
					result = test.result(deliveryID)
				}
				reports := atomic.Int32{}
				admission, err := admitWorkflowNodeDelivery(
					context.Background(), evt, route, provider,
					scriptedWorkflowNodeDeliveryStore{result: result, err: test.claimErr},
					func(error) { reports.Add(1) },
				)
				if (err != nil) != test.wantErr {
					t.Fatalf("admission error = %v, want error %v", err, test.wantErr)
				}
				if admission.handled != test.wantHandled {
					t.Fatalf("admission handled = %v, want %v", admission.handled, test.wantHandled)
				}
				if strings.HasPrefix(test.name, "terminal") {
					if continuation.consumes.Load() != 0 || continuation.returns.Load() != 1 || provider.releases.Load() != 1 {
						t.Fatalf("terminal consume/return/release = %d/%d/%d, want 0/1/1",
							continuation.consumes.Load(), continuation.returns.Load(), provider.releases.Load())
					}
				} else if continuation.returns.Load() != 1 || continuation.consumes.Load() != 0 {
					t.Fatalf("pre-attempt return/consume = %d/%d, want 1/0", continuation.returns.Load(), continuation.consumes.Load())
				}
			})
		}
	}
}

func TestWorkflowNodeDeliveryCarrierGuardReportsReturnFailureWithoutPolling(t *testing.T) {
	for authorityName, authority := range workflowNodeCarrierAuthorities(t) {
		t.Run(authorityName, func(t *testing.T) {
			evt, route, deliveryID := workflowNodeCarrierTestEventAndRoute(t)
			continuation := &scriptedWorkflowNodeContinuation{deliveryID: deliveryID}
			continuation.returnErrs.Store(1)
			provider := &scriptedWorkflowNodeAuthority{authority: authority, continuation: continuation}
			reports := atomic.Int32{}
			_, err := admitWorkflowNodeDelivery(context.Background(), evt, route, provider,
				scriptedWorkflowNodeDeliveryStore{err: errors.New("injected claim failure")}, func(error) { reports.Add(1) })
			if err == nil || continuation.returns.Load() != 1 || reports.Load() != 1 {
				t.Fatalf("admission error/returns/reports = %v/%d/%d, want error/1/1", err, continuation.returns.Load(), reports.Load())
			}
		})
	}
}
