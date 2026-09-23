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
	for _, tc := range []struct {
		name         string
		acknowledged bool
		fault        error
		wantStatus   int
	}{
		{name: "unacknowledged", fault: errors.New("inbound commit failed"), wantStatus: http.StatusServiceUnavailable},
		{name: "missing acknowledgement", wantStatus: http.StatusServiceUnavailable},
		{name: "acknowledged cleanup", acknowledged: true, fault: errors.New("inbound post-commit fault"), wantStatus: http.StatusAccepted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			probe := &inboundAcknowledgementDispatchProbe{}
			logs := &runtimeLogPersistenceCapture{}
			bus, err := newRuntimeTestEventBusWithOptions(t, eventStore, runtimebus.EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			store := &inboundAcknowledgementProbeStore{
				recordingInboundStore: &recordingInboundStore{target: testInboundTarget("entity-1", ""), inserted: true},
				acknowledged:          tc.acknowledged,
				fault:                 tc.fault,
			}
			gateway := newTestInboundGateway(t, bus, newTestRuntimeLogger(nil, runtimeLogPersistenceStub{capture: logs}), nil, store)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-ack","type":"push"}`))
			response := httptest.NewRecorder()
			gateway.Handler().ServeHTTP(response, req)
			if response.Code != tc.wantStatus || store.commits != 1 {
				t.Fatalf("status/commits = %d/%d, want %d/1", response.Code, store.commits, tc.wantStatus)
			}
			if strings.Contains(response.Body.String(), "inbound post-commit fault") || strings.Contains(response.Body.String(), "inbound commit failed") {
				t.Fatalf("internal failure leaked in HTTP response: %s", response.Body.String())
			}
			if tc.acknowledged {
				if len(logs.records) != 1 || !strings.Contains(string(logs.records[0].Payload), "publish_post_commit_cleanup_failed") || !strings.Contains(string(logs.records[0].Payload), "evt-ack") || strings.Contains(string(logs.records[0].Payload), tc.fault.Error()) {
					t.Fatalf("unsafe or missing internal cleanup diagnostic: count=%d", len(logs.records))
				}
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
