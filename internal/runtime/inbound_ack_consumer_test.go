package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

type inboundAcknowledgementProbeStore struct {
	*recordingInboundStore
	acknowledged bool
	fault        error
	commits      int
}

func (s *inboundAcknowledgementProbeStore) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	s.commits++
	if !s.acknowledged {
		return runtimeinbound.CommitResult{}, s.fault
	}
	result, err := s.recordingInboundStore.CommitInboundPublication(ctx, command)
	if err != nil {
		return result, err
	}
	result.Acknowledged = true
	return result, s.fault
}

type inboundAcknowledgementDispatchProbe struct{ dispatched atomic.Int32 }

func (p *inboundAcknowledgementDispatchProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.PostCommitDispatchStarted {
		p.dispatched.Add(1)
	}
}

func TestInboundGatewayAcknowledgedCommitErrorDispatchesBeforeReportingFailure(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			probe := &inboundAcknowledgementDispatchProbe{}
			bus, err := newRuntimeTestEventBusWithOptions(t, eventStore, runtimebus.EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			store := &inboundAcknowledgementProbeStore{
				recordingInboundStore: &recordingInboundStore{target: testInboundTarget("entity-1", ""), inserted: true},
				acknowledged:          acknowledged,
				fault:                 errors.New("inbound post-commit fault"),
			}
			gateway := newTestInboundGateway(t, bus, nil, nil, store)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-ack","type":"push"}`))
			response := httptest.NewRecorder()
			gateway.Handler().ServeHTTP(response, req)
			if response.Code != http.StatusServiceUnavailable || store.commits != 1 {
				t.Fatalf("status/commits = %d/%d, want 503/1", response.Code, store.commits)
			}
			if acknowledged {
				if !eventStore.recorded || probe.dispatched.Load() != int32(len(store.record.Events)) {
					t.Fatalf("recorded/dispatches = %t/%d, want true/%d", eventStore.recorded, probe.dispatched.Load(), len(store.record.Events))
				}
				retry := httptest.NewRecorder()
				gateway.Handler().ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-ack","type":"push"}`)))
				if retry.Code != http.StatusOK || store.commits != 1 || probe.dispatched.Load() != int32(len(store.record.Events)) {
					t.Fatalf("retry status/commits/dispatches = %d/%d/%d, want 200/1/%d", retry.Code, store.commits, probe.dispatched.Load(), len(store.record.Events))
				}
			} else if eventStore.recorded || probe.dispatched.Load() != 0 {
				t.Fatalf("unacknowledged recorded/dispatches = %t/%d", eventStore.recorded, probe.dispatched.Load())
			}
		})
	}
}
