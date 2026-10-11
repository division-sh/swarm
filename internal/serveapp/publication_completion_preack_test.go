package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPublicCompletionPreAckRetirementBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			t.Run(string(backend)+"/"+method, func(t *testing.T) {
				probe := installPublicationCompletionAudit(t, false)
				entered := make(chan context.Context, 1)
				probe.beforeCommit = func(ctx context.Context) {
					entered <- ctx
					select {
					case <-ctx.Done():
					case <-probe.release:
					}
				}
				defer probe.releaseOnce.Do(func() { close(probe.release) })
				h := newChannelOnboardingE2EHarness(t, backend, false)
				h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				params := map[string]any{
					"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
					"data": map[string]any{"imports": []any{map[string]any{
						"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
						"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
					}}, "pins": []any{}},
				}
				if method == "event.publish" {
					params["event_name"], params["payload"] = "scan.requested", map[string]any{"topic": "pre-ack-retirement"}
				}
				before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), probe.native)
				if err != nil {
					t.Fatal(err)
				}
				finished := make(chan publicationAuditHTTPResult, 1)
				go func() { finished <- publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params) }()
				var operation context.Context
				select {
				case operation = <-entered:
				case result := <-finished:
					t.Fatalf("native pre-ACK cut not reached: %+v", result)
				case <-time.After(10 * time.Second):
					t.Fatal("native pre-ACK cut not reached")
				}
				owner, ok := worklifetime.OccurrenceFromContext(operation)
				if !ok {
					t.Fatal("native operation lost exact occurrence")
				}
				runtime, ok := owner.(*worklifetime.RuntimeOccurrence)
				if !ok {
					t.Fatalf("served public owner is not its runtime: %T", owner)
				}
				process, _ := worklifetime.ProcessFromContext(operation)
				if operation.Err() != nil {
					t.Fatalf("operation canceled before retirement: %v", operation.Err())
				}
				if process.PublicCompletionCount() != 1 {
					t.Fatal("native cut has no retained capacity")
				}
				runtime.Retire()
				select {
				case <-operation.Done():
				case <-time.After(time.Second):
					t.Fatal("retired occurrence did not cancel the pre-ACK native operation")
				}
				select {
				case result := <-finished:
					if result.err != nil || len(result.rpcError) == 0 {
						t.Fatalf("retired pre-ACK operation did not fail closed: %+v", result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("retired pre-ACK operation did not settle")
				}
				if process.PublicCompletionCount() != 0 {
					t.Fatalf("retirement retained capacity: count=%d", process.PublicCompletionCount())
				}
				after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), probe.native)
				if err != nil || after != before {
					t.Fatalf("canceled native operation changed durable construction/publication facts: before=%+v after=%+v err=%v", before, after, err)
				}
				h.stop(t)
				if _, err := runtime.RetireAndWait(t.Context()); err != nil {
					t.Fatalf("serving shutdown did not join the retired occurrence: %v", err)
				}
			})
		}
	}
}

func TestPublicCompletionAcknowledgedRetirementBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			t.Run(string(backend)+"/"+method, func(t *testing.T) {
				probe := installPublicationCompletionAudit(t, false)
				entered := make(chan context.Context, 1)
				probe.afterCommit = func(ctx context.Context, acknowledged bool) {
					if !acknowledged {
						return
					}
					entered <- ctx
					select {
					case <-ctx.Done():
					case <-probe.release:
					}
				}
				defer probe.releaseOnce.Do(func() { close(probe.release) })
				h := newChannelOnboardingE2EHarness(t, backend, false)
				h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				params := map[string]any{
					"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
					"data": map[string]any{"imports": []any{map[string]any{
						"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
						"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
					}}, "pins": []any{}},
				}
				if method == "event.publish" {
					params["event_name"], params["payload"] = "scan.requested", map[string]any{"topic": "acknowledged-retirement"}
				}
				finished := make(chan publicationAuditHTTPResult, 1)
				go func() { finished <- publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params) }()
				var operation context.Context
				select {
				case operation = <-entered:
				case result := <-finished:
					t.Fatalf("native ACK cut not reached: %+v", result)
				case <-time.After(5 * time.Second):
					t.Fatal("native ACK cut not reached")
				}
				owner, _ := worklifetime.OccurrenceFromContext(operation)
				runtime, ok := owner.(*worklifetime.RuntimeOccurrence)
				if !ok {
					t.Fatalf("native ACK lost exact runtime: %T", owner)
				}
				runtime.Retire()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("ACKed operation did not return")
				}
				probe.mu.Lock()
				request, native, process := probe.request, probe.native, probe.process
				probe.mu.Unlock()
				stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
				if err != nil || !found {
					t.Fatalf("retirement discarded the winning native ACK: found=%t err=%v", found, err)
				}
				h.stop(t)
				if process.PublicCompletionCount() != 0 {
					t.Fatal("retirement released store before exact accepted cleanup joined")
				}
				if _, err := runtime.RetireAndWait(t.Context()); err != nil {
					t.Fatal(err)
				}
				probe.afterCommit = nil
				h.start(t)
				replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params)
				if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, stored.Response) {
					t.Fatalf("winning ACK did not survive recovery byte-exactly: %+v stored=%s", replay, stored.Response)
				}
				probe.mu.Lock()
				calls := probe.commits + probe.deployments
				probe.mu.Unlock()
				if calls != 1 {
					t.Fatalf("recovery repeated native publication/creation: %d", calls)
				}
			})
		}
	}
}

func TestPublicCompletionNativePreAckRetirementBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			t.Run(string(backend)+"/"+method, func(t *testing.T) {
				probe := installPublicationCompletionAudit(t, false)
				defer probe.releaseOnce.Do(func() { close(probe.release) })
				h := newChannelOnboardingE2EHarness(t, backend, false)
				h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
				managers := make(chan *runtimepkg.RuntimeContextManager, 1)
				h.opts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) { managers <- manager }
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				manager := <-managers
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				use, _, err := manager.AcquireBundleHash(t.Context(), identity.SourceArtifacts[0].BundleHash)
				if err != nil || use == nil {
					t.Fatalf("exact runtime selection: %v", err)
				}
				defer use.Done()
				rt := use.Runtime()
				owner, _ := worklifetime.OccurrenceFromContext(use.WorkContext())
				runtime, ok := owner.(*worklifetime.RuntimeOccurrence)
				if !ok {
					t.Fatalf("runtime selection lost its occurrence: %T", owner)
				}
				// Exercise the native public owner with an independent caller, without
				// relying on the serving selector's outer cancellation lease.
				ctx := worklifetime.WithOccurrence(runtimeauthoractivity.WithScope(t.Context(), runtimeauthoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash())), owner)
				entered := make(chan context.Context, 1)
				probe.beforeCommit = func(ctx context.Context) {
					entered <- ctx
					select {
					case <-ctx.Done():
					case <-probe.release:
					}
				}
				before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), probe.native)
				if err != nil {
					t.Fatal(err)
				}
				finished := make(chan error, 1)
				go func() {
					if method == "run.start" {
						ref, err := durabledata.ParseDeclarationRef(".", "scan.requested")
						if err != nil {
							finished <- err
							return
						}
						command := durabledata.RunCreationCommand{RunID: uuid.NewString(), Actor: "operator", BundleHash: rt.Options.SourceArtifactFact.BundleHash(), Data: durabledata.RunCreationDataEnvelope{Imports: []durabledata.FusedImport{{SourceInvocationID: uuid.NewString(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl"}}}}
						_, err = rt.Bus.StartDeploymentRunAcknowledged(ctx, command, apiidempotency.Request{Method: method})
						finished <- err
						return
					}
					endpoint, err := runtimebus.NewRootInputAPIEventPublicationEndpoint(use.Context.Source, "scan.requested")
					if err != nil {
						finished <- err
						return
					}
					evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "scan.requested", "operator", "", json.RawMessage(`{"topic":"pre-ack-retirement"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
					_, _, err = rt.Bus.PublishAPIEventAcknowledged(ctx, evt, &endpoint, apiidempotency.Request{Method: method}, apiidempotency.Completion{ResourceID: evt.ID(), Response: json.RawMessage(`{"ok":true}`)})
					finished <- err
				}()
				var operation context.Context
				select {
				case operation = <-entered:
				case err := <-finished:
					t.Fatalf("native cut not reached: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("native cut not reached")
				}
				if operation.Err() != nil {
					t.Fatalf("operation canceled before retirement: %v", operation.Err())
				}
				process, ok := worklifetime.ProcessFromContext(operation)
				if !ok || process.PublicCompletionCount() != 1 {
					t.Fatal("native pre-ACK operation lost its exact reservation")
				}
				runtime.Retire()
				select {
				case <-operation.Done():
				case <-time.After(time.Second):
					t.Fatal("retirement did not cancel the native pre-ACK operation")
				}
				select {
				case err := <-finished:
					if err == nil {
						t.Fatal("retired native operation succeeded")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("retired native operation did not return")
				}
				after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), probe.native)
				if err != nil || before != after {
					t.Fatalf("unacknowledged operation mutated durable facts: before=%+v after=%+v err=%v", before, after, err)
				}
				if process.PublicCompletionCount() != 0 {
					t.Fatal("unacknowledged operation retained its reservation")
				}
				if err := use.Done(); err != nil {
					t.Fatal(err)
				}
				h.stop(t)
				if _, err := runtime.RetireAndWait(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
