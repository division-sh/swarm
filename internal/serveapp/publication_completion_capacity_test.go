package serveapp

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPublicCompletionCapacityServedBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, false)
			entered := make(chan struct{}, worklifetime.PublicCompletionLimit)
			probe.afterCommit = func(_ context.Context, acknowledged bool) {
				if acknowledged {
					entered <- struct{}{}
					<-probe.release
				}
			}
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			var identity apiv1.RuntimeIdentityResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
			if len(identity.SourceArtifacts) != 1 {
				t.Fatalf("initial source selection: %+v", identity)
			}
			runID := requireServedLiveLandingRunID(t, h.rpcEndpoint(), identity.SourceArtifacts[0].BundleHash, ".")
			declaration := map[string]any{"flow_path": ".", "event": "scan.requested"}
			data := map[string]any{"imports": []any{map[string]any{
				"source_invocation_id": uuid.NewString(), "declaration": declaration,
				"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
			}}, "pins": []any{}}
			var firstParams map[string]any
			var firstResult []byte
			responses := make([]chan publicationAuditHTTPResult, worklifetime.PublicCompletionLimit)
			storedResults := make([][]byte, len(responses))
			for index := range worklifetime.PublicCompletionLimit {
				params := map[string]any{"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": runID, "idempotency_key": uuid.NewString(), "data": data}
				responses[index] = make(chan publicationAuditHTTPResult, 1)
				go func() {
					responses[index] <- publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", params)
				}()
				select {
				case <-entered:
				case response := <-responses[index]:
					t.Fatalf("publication %d missed its native ACK cut: %+v", index, response)
				case <-time.After(10 * time.Second):
					t.Fatalf("publication %d did not reach its native ACK cut", index)
				}
				probe.mu.Lock()
				committed, native, request, process := probe.deployment, probe.native, probe.request, probe.process
				probe.mu.Unlock()
				stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
				if err != nil || !found || committed == nil || !committed.Acknowledged || !bytes.Equal(stored.Response, committed.Completion.Response) || process.PublicCompletionCount() != uint64(index+1) || len(committed.Activations) != 0 {
					t.Fatalf("reply %d lost literal ACK/responsibility: found=%t err=%v", index, found, err)
				}
				storedResults[index] = append([]byte(nil), stored.Response...)
				if index == 0 {
					firstParams, firstResult = params, append([]byte(nil), stored.Response...)
					var version string
					for _, item := range committed.Record.Evidence.RunBinding {
						if item.Pin != nil {
							version = string(item.Pin.VersionID)
						}
					}
					if version == "" {
						t.Fatal("admitted empty feed has no exact version pin")
					}
					data = map[string]any{"imports": []any{}, "pins": []any{map[string]any{"declaration": declaration, "version_id": version}}}
				}
			}
			probe.mu.Lock()
			native, process, calls := probe.native, probe.process, probe.deployments
			probe.mu.Unlock()
			before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), native)
			if err != nil {
				t.Fatal(err)
			}
			refused := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", map[string]any{"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": runID, "idempotency_key": uuid.NewString(), "data": data})
			if refused.err != nil || !bytes.Contains(refused.rpcError, []byte(worklifetime.ErrPublicCompletionCapacity.Error())) {
				t.Fatalf("N+1 did not refuse before mutation: %+v", refused)
			}
			replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", firstParams)
			after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), native)
			probe.mu.Lock()
			afterCalls := probe.deployments
			probe.mu.Unlock()
			if err != nil || before != after || afterCalls != calls || replay.err != nil || len(replay.rpcError) != 0 || !bytes.Equal(replay.result, firstResult) || process.PublicCompletionCount() != worklifetime.PublicCompletionLimit {
				t.Fatalf("saturated refusal/replay changed native facts: before=%+v after=%+v calls=%d/%d err=%v replay=%+v", before, after, calls, afterCalls, err, replay)
			}
			probe.releaseOnce.Do(func() { close(probe.release) })
			for index, response := range responses {
				result := <-response
				if result.err != nil || len(result.rpcError) != 0 || !bytes.Equal(result.result, storedResults[index]) {
					t.Fatalf("exact publication %d changed its acknowledged completion: %+v", index, result)
				}
			}
			deadline := time.Now().Add(20 * time.Second)
			for process.PublicCompletionCount() != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("released publications retained capacity=%d", process.PublicCompletionCount())
				}
				time.Sleep(time.Millisecond)
			}
			h.stop(t)
			if process.PublicCompletionCount() != 0 {
				t.Fatal("joined shutdown retained public responsibility")
			}
		})
	}
}
