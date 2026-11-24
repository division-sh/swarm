package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/division-sh/swarm/tests/support/testutil/llmapi"
)

func requireNestedReceiverProviderResume(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			interrupted, reissued := make(chan llmapi.Request, 1), make(chan llmapi.Request, 1)
			releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
			unblockFirst := sync.OnceFunc(func() { close(releaseFirst) })
			unblockSecond := sync.OnceFunc(func() { close(releaseSecond) })
			provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "resume-key", func(request llmapi.Request) (llmapi.Reply, error) {
				reply := llmapi.Reply{ID: fmt.Sprintf("nested-%d", request.Number), Model: "resume-model", InputTokens: 8, OutputTokens: 2, TotalTokens: 10}
				switch request.Number {
				case 1:
					interrupted <- request
					<-releaseFirst
					reply.StatusCode = http.StatusServiceUnavailable
				case 2:
					reissued <- request
					<-releaseSecond
					reply.Tools = []llmapi.ToolCall{{ID: "create-leaf", Name: "emit_leaf_requested", Arguments: json.RawMessage(`{"worker_id":"alpha-leaf","label":"leaf-parent-A","count":1}`)}}
				case 3:
					reply.Text = "Created the leaf."
				default:
					return llmapi.Reply{}, fmt.Errorf("unexpected nested receiver provider request %d", request.Number)
				}
				return reply, nil
			})
			t.Cleanup(unblockFirst)
			t.Cleanup(unblockSecond)
			root := t.TempDir()
			location := filepath.Join(root, "resume.sqlite")
			var config string
			if backend == "postgres" {
				location = testutil.StartEmptyPostgresDSN(t)
				config = writeChannelOnboardingPostgresRuntimeConfig(t, location)
			} else {
				config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
			}
			setServeRuntimeRecovery(t, config, false, true)
			configureProviderJourneyLLM(t, config, "openai_compatible", provider.URL+"/v1", "resume-model")
			credentialPath := filepath.Join(root, "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			secrets, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := secrets.Set(t.Context(), "OPENAI_COMPATIBLE_API_KEY", "resume-key"); err != nil {
				t.Fatal(err)
			}
			source := canonicalrouting.CopyReceiverInitializationAgentGeometry(t)
			request, err := json.Marshal(map[string]any{"ConfigPath": config, "Source": source, "Live": true,
				"Store": backend, "Token": apiv1.DefaultLoopbackAPIToken, "ShutdownGrace": int64(5 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			env := []string{providerResumeProcessEnv + "=" + provider.URL, "SWARM_TEST_PROVIDER_RESUME_ROOT=" + root,
				"SWARM_TEST_INTERNAL_MOCK_LIFECYCLE_REQUEST=" + string(request)}
			const helper = "TestInterruptedReadOnlyProviderTurnResumesOnlyInterruptedCallBothStores"
			first := startServedCrashProcess(t, helper, env)
			endpoint := first.endpoint(t) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": servedEventPublishFixtureBundleHash(t, source),
				"payload":         map[string]any{"worker_id": "alpha", "label": "parent-A", "count": int64(0)},
				"idempotency_key": "nested-receiver-resume",
			})
			original := awaitNestedReceiverRequest(t, interrupted, first)
			reader, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			})
			requireNestedReceiverInstances(t, reader, seed.RunID, 0)
			attempts := *readNestedReceiverEffects(t, reader, seed.RunID).Attempts
			if len(provider.Requests()) != 1 || len(attempts) != 1 {
				t.Fatalf("cut did not leave exactly one bridge call and attempt: calls=%d attempts=%+v", len(provider.Requests()), attempts)
			}
			predecessor := attempts[0]
			if predecessor.AttemptState != "launched" || predecessor.DeliveryState != "in_progress" ||
				predecessor.OriginDeliveryID == "" || predecessor.OriginEventID == "" || !predecessor.LaunchedAt.Valid ||
				predecessor.ResponseObservedAt.Valid || predecessor.CompletedAt.Valid {
				t.Fatalf("cut did not leave an interrupted recorded provider call: %+v", predecessor)
			}
			var ready operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": predecessor.OriginEventID}, &ready)
			if ready.RunID != seed.RunID || !strings.HasSuffix(ready.EventName, "/worker.ready") ||
				len(ready.Deliveries) != 1 || ready.Deliveries[0].DeliveryID != predecessor.OriginDeliveryID || ready.Deliveries[0].SubscriberType != "agent" {
				t.Fatalf("interrupted call did not consume parent creation: %+v", ready)
			}
			t.Logf("recorded cut: event=%s delivery=%s attempt=%s phase=%s", predecessor.OriginEventID, predecessor.OriginDeliveryID, predecessor.AttemptID, predecessor.AttemptState)
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			unblockFirst()

			second := startServedCrashProcessBeforeReadiness(t, helper, env)
			replacement := awaitNestedReceiverRequest(t, reissued, second)
			if !bytes.Equal(replacement.Body, original.Body) {
				t.Fatal("restart changed the recorded bridge request")
			}
			attempts = *readNestedReceiverEffects(t, reader, seed.RunID).Attempts
			var priorUncertain bool
			var reissuedAttemptID string
			for _, attempt := range attempts {
				if attempt.AttemptID == predecessor.AttemptID {
					priorUncertain = attempt.AttemptState == "outcome_uncertain"
				} else if attempt.AttemptState == "launched" && attempt.OriginEventID == predecessor.OriginEventID &&
					attempt.OriginDeliveryID == predecessor.OriginDeliveryID {
					reissuedAttemptID = attempt.AttemptID
				}
			}
			if len(attempts) != 2 || !priorUncertain || reissuedAttemptID == "" {
				t.Fatalf("restart did not launch the recorded bridge call's reissue: %+v", attempts)
			}
			priorClaims, err := storetest.ReadDeliveryAttemptDiagnosticRows(t.Context(), reader, predecessor.OriginDeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if len(priorClaims) != 2 || priorClaims[0].Version != 1 || priorClaims[0].Closure != "settled" ||
				priorClaims[0].Outcome != "retry_scheduled" || priorClaims[0].Reason != "effect_recovery_outcome_unconfirmed" || priorClaims[1].Version != 2 {
				t.Fatalf("bridge reissue lacks the recovered predecessor claim: %+v", priorClaims)
			}
			t.Logf("reissue: same event and delivery, version 1 retry_scheduled, version 2 attempt=%s, exact request bytes", reissuedAttemptID)

			unblockSecond()
			endpoint = second.endpoint(t) + "/v1/rpc"
			waitServedRunDeliveryQuiescence(t, func(ctx context.Context, runID string) (deliverylifecycle.RunSummary, error) {
				return storetest.ReadServedRunDeliverySummary(ctx, reader, runID)
			}, seed.RunID)
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": predecessor.OriginEventID}, &ready)
			if len(provider.Requests()) != 3 || len(ready.Deliveries) != 1 ||
				ready.Deliveries[0].DeliveryID != predecessor.OriginDeliveryID || ready.Deliveries[0].Status != "delivered" {
				t.Fatalf("original bridge delivery did not finish after its exact reissue: calls=%d deliveries=%+v", len(provider.Requests()), ready.Deliveries)
			}
			requireNestedReceiverInstances(t, reader, seed.RunID, 1)
			var events operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": seed.RunID}, "limit": 100}, &events)
			if events.NextCursor != "" {
				t.Fatal("nested proof did not read the complete event set")
			}
			counts := map[string]int{"/worker.ready": 0, "/leaf.requested": 0, "/leaf.ready": 0}
			for _, event := range events.Events {
				for suffix := range counts {
					if strings.HasSuffix(event.EventName, suffix) {
						counts[suffix]++
					}
				}
			}
			for name, count := range counts {
				if count != 1 {
					t.Fatalf("%s events=%d, want exactly one across restart", name, count)
				}
			}
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func awaitNestedReceiverRequest(t *testing.T, requests <-chan llmapi.Request, process *channelOnboardingCrashServeProcess) llmapi.Request {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-process.exited:
		t.Fatalf("serve exited before the bridge call: %v\n%s", process.waitError(), process.output.String())
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatalf("bridge call did not reach the provider\n%s", process.output.String())
	}
	return llmapi.Request{}
}

func readNestedReceiverEffects(t *testing.T, reader storetest.ReleaseProcessReadOnlyInspection, runID string) storetest.ManagedTurnEffectStorage {
	t.Helper()
	var result storetest.ManagedTurnEffectStorage
	if err := reader.InspectSnapshot(t.Context(), func(ctx context.Context) error {
		var err error
		result, err = storetest.ReadManagedTurnEffectStorage(ctx, reader, runID, "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result.Attempts == nil || len(*result.Attempts) != result.ProviderAttempts {
		t.Fatalf("incomplete provider attempt readback: %+v", result)
	}
	return result
}

func requireNestedReceiverInstances(t *testing.T, reader storetest.ReleaseProcessReadOnlyInspection, runID string, wantLeaves int) {
	t.Helper()
	instances, err := requireReceiverProofStateReader(t, reader).ListWorkflowInstances(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	var parents, leaves int
	for _, instance := range instances {
		switch instance.WorkflowName {
		case "worker":
			parents++
			if instance.Fields["worker_id"] != "alpha" || instance.Fields["label"] != "parent-A" || instance.Fields["count"] != int64(0) {
				t.Fatalf("parent config differs: %#v", instance.Fields)
			}
		case "worker/leaf":
			leaves++
			if instance.Fields["worker_id"] != "alpha-leaf" || instance.Fields["label"] != "leaf-parent-A" || instance.Fields["count"] != int64(1) {
				t.Fatalf("grandchild did not consume exact supplied state: %#v", instance.Fields)
			}
		}
	}
	if parents != 1 || leaves != wantLeaves {
		t.Fatalf("parents=%d leaves=%d, want 1 and %d", parents, leaves, wantLeaves)
	}
}
