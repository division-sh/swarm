package serveapp

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type snapshotPublicationLogBarrier struct {
	logger  *runtimepkg.RuntimeLogger
	entered chan string
	release <-chan struct{}
}

func (b *snapshotPublicationLogBarrier) ProjectLifecycleDiagnostic(ctx context.Context, diagnostic diaglog.LifecycleDiagnostic) error {
	return b.logger.ProjectLifecycleDiagnostic(ctx, diagnostic)
}

func (b *snapshotPublicationLogBarrier) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if component == "eventbus" && action == "published" && eventType == "left.work.requested" {
		b.entered <- eventID
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID,
		SessionID: sessionID, Correlation: correlation, Detail: detail,
		Failure: runtimefailures.CloneEnvelope(failure), DurationUS: durationUS,
	})
}

func TestServedCompiledPublicationSnapshotWaitsForExactDiagnosticBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := lifecycletest.New(t)
			configureOwnedMockLifecycleProbe(t, probe)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleNestedCascade(t))
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			entered := make(chan string, 1)
			rt.Runtime.Bus.SetLoggerHook(&snapshotPublicationLogBarrier{logger: rt.Runtime.Logger, entered: entered, release: release})
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "left.work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"seed": true}, "idempotency_key": "tree-seed",
			})
			select {
			case eventID := <-entered:
				if eventID != seed.EventID {
					t.Fatalf("barrier held event %s, want %s", eventID, seed.EventID)
				}
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatal("original publication diagnostic did not enter the barrier")
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			other := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "right.work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"seed": true}, "idempotency_key": "other-tree-seed",
			})
			if other.RunID == seed.RunID || other.EventID == seed.EventID {
				t.Fatal("negative control reused the original publication")
			}
			probe.Expect(other.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
			capturing := make(chan struct{})
			captured := make(chan map[string][][]string, 1)
			go func() {
				close(capturing)
				captured <- settledStaticRunSnapshot(t, rt, probe, seed)
			}()
			<-capturing
			select {
			case <-captured:
				t.Fatal("snapshot returned while the exact publication diagnostic was held")
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			var before map[string][][]string
			select {
			case before = <-captured:
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatal("snapshot fence did not finish after releasing its exact publication")
			}
			waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)
			refusal := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", map[string]any{
				"event_name": "right.work.requested", "run_id": seed.RunID,
				"payload": map[string]any{"seed": true}, "idempotency_key": "redundant-tree-seed",
			})
			details, ok := refusal.Data["details"].(map[string]any)
			if !ok || refusal.Data["code"] != "EVENT_NOT_DECLARED" || details["reason"] != "declared_event_has_no_selected_run_recipient" {
				t.Fatalf("handler-free redundant seed refusal=%#v", refusal)
			}
			if after := repeatedStaticRunSnapshot(t, rt.DB, seed.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected seed changed the settled full snapshot: before=%#v after=%#v", before, after)
			}
		})
	}
}
