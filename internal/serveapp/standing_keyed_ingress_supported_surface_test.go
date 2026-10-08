package serveapp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA9KeyedRootIngressConstructionBothStores(t *testing.T) {
	a9KeyedRootIngressConstruction(t, false)
}

func TestA9KeyedRootIngressCreationPublicationBothStores(t *testing.T) {
	a9KeyedRootIngressConstruction(t, true)
}

func a9KeyedRootIngressConstruction(t *testing.T, autoEmit bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var root string
			if autoEmit {
				root = canonicalrouting.CopyKeyedRootRawIngressCreationEvent(t)
			} else {
				root = canonicalrouting.CopyKeyedRootRawIngress(t)
			}
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "a9-authenticated-keyed-root"
			if err := file.Set(t.Context(), "webhook_signing.partner", secret); err != nil {
				t.Fatal(err)
			}
			_, start := clockDeploymentHarness(t, backend, root)
			var construction pipeline.FlowConstructionPublicationReader
			var inbound inboundpublication.Runner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projection := previous(owner)
				construction, _ = projection.deps.EventStore.(pipeline.FlowConstructionPublicationReader)
				inbound = projection.deps.InboundStore
				return projection
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("joined shutdown=%d", code)
				}
			})
			rt := servedTestProcessRuntime(t, process)
			if construction == nil || inbound == nil {
				t.Fatal("served proof requires the original native construction and inbound receipt readers")
			}
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || !statuses[0].RestartDisposition.Executable() {
				t.Fatalf("zero-instance enabled binding=%+v err=%v", statuses, err)
			}
			status := statuses[0]
			instances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), status.RunID)
			if err != nil || len(instances) != 0 {
				t.Fatalf("binding manufactured receivers before authentication: %+v err=%v", instances, err)
			}
			if events := servedClockEvents(t, served.Endpoint, status.RunID, "root.created"); len(events) != 0 {
				t.Fatalf("declaration alone emitted creation: %+v", events)
			}
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
			const body = `{"delivery_id":"first","account_id":"account-7"}`
			code, refused := a9PostSignedRawIngress(t, endpoint, "wrong-secret", body)
			if code != http.StatusUnauthorized {
				t.Fatalf("invalid authentication status=%d body=%s", code, refused)
			}
			instances, err = rt.Pipeline.ListWorkflowInstances(t.Context(), status.RunID)
			if err != nil || len(instances) != 0 {
				t.Fatalf("authentication failure constructed a receiver: %+v err=%v", instances, err)
			}
			code, accepted := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				var logs operatorread.OperatorRuntimeLogListResult
				requireServedJSONRPCResult(t, served.Endpoint, "runtime.logs", map[string]any{"limit": 100}, &logs)
				t.Fatalf("first authenticated constructor status=%d receipt=%s logs=%+v", code, accepted, logs)
			}
			var receipt struct {
				PublicationID string   `json:"publication_id"`
				ServiceID     string   `json:"service_id"`
				RunID         string   `json:"run_id"`
				Generation    int64    `json:"generation"`
				EventIDs      []string `json:"event_ids"`
			}
			if err := json.Unmarshal(accepted, &receipt); err != nil || len(receipt.EventIDs) != 1 || receipt.ServiceID != status.ServiceID || receipt.RunID != status.RunID || receipt.Generation != status.Generation {
				t.Fatalf("receipt lost binding-generation authority: %+v err=%v", receipt, err)
			}
			a9RequireRootIngressSettlement(t, served.Endpoint, status.RunID, receipt.EventIDs[0])
			var creationID string
			if autoEmit {
				created := servedClockEvents(t, served.Endpoint, status.RunID, "root.created")
				if len(created) != 1 || created[0].EntityID != status.RunID || created[0].Payload["provider_event_id"] != "first" || created[0].Payload["processed_count"] != float64(0) || created[0].Payload["creation_count"] != float64(0) {
					t.Fatalf("creation publication lost original constructor fields: %+v", created)
				}
				creationID = created[0].EventID
				a9RequireRootIngressSettlement(t, served.Endpoint, status.RunID, creationID)
			}
			rootOwner, err := flowidentity.NewRunScopedFlowInstance(status.RunID, flowidentity.StoredRoute(".", status.RunID, status.RunID))
			if err != nil {
				t.Fatal(err)
			}
			initial, err := construction.LoadFlowConstructionPublication(t.Context(), rootOwner, status.RunID)
			if err != nil || initial.CreatingInput != (pipeline.FlowConstructionInput{EventID: receipt.EventIDs[0], Input: "account.opened"}) || initial.Fields["provider_event_id"] != "first" {
				t.Fatalf("creating input or immutable initial fields lost: %+v err=%v", initial, err)
			}
			instances, err = rt.Pipeline.ListWorkflowInstances(t.Context(), status.RunID)
			if err != nil || len(instances) != 2 {
				t.Fatalf("constructor did not create its exact root/keyless child tree: %+v err=%v", instances, err)
			}
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": status.RunID, "entity_id": status.RunID}, &entity)
			if entity.Fields["provider_event_id"] != "first" || entity.Fields["processed_count"] != float64(1) {
				t.Fatalf("creating event was not supplied and delivered once: %+v", entity)
			}
			if autoEmit && entity.Fields["creation_count"] != float64(1) {
				t.Fatalf("declared creation consumer was not executed once: %+v", entity)
			}
			code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusOK || !reflect.DeepEqual(a9AliasReceipt(t, accepted), a9AliasReceipt(t, duplicate)) {
				t.Fatalf("exact retry changed receipt: %s -> %s status=%d", accepted, duplicate, code)
			}
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": status.RunID, "entity_id": status.RunID}, &entity)
			if entity.Fields["processed_count"] != float64(1) {
				t.Fatalf("receipt retry redelivered the constructor event: %+v", entity)
			}
			if autoEmit {
				created := servedClockEvents(t, served.Endpoint, status.RunID, "root.created")
				if len(created) != 1 || created[0].EventID != creationID || entity.Fields["creation_count"] != float64(1) {
					t.Fatalf("retry re-emitted or redelivered creation: events=%+v fields=%+v", created, entity.Fields)
				}
			}
			code, contradiction := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"second","account_id":"account-8"}`)
			if code != http.StatusServiceUnavailable || !strings.Contains(string(contradiction), "immutable constructor key") {
				t.Fatalf("different root key was accepted: status=%d body=%s", code, contradiction)
			}
			instances, err = rt.Pipeline.ListWorkflowInstances(t.Context(), status.RunID)
			if err != nil || len(instances) != 2 {
				t.Fatalf("root-key conflict changed construction: %+v err=%v", instances, err)
			}
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": status.RunID, "entity_id": status.RunID}, &entity)
			if entity.Fields["provider_event_id"] != "first" || entity.Fields["processed_count"] != float64(1) {
				t.Fatalf("root-key conflict mutated initial or business state: %+v", entity)
			}
			again, err := construction.LoadFlowConstructionPublication(t.Context(), rootOwner, status.RunID)
			if err != nil || !reflect.DeepEqual(initial, again) {
				t.Fatalf("retry or contradiction replaced immutable construction: %+v -> %+v err=%v", initial, again, err)
			}
			if _, found, err := inbound.LoadInboundPublicationByIdentity(t.Context(), inboundpublication.Identity{
				ServiceID: status.ServiceID, RunID: status.RunID, Generation: status.Generation, Provider: "partner", ProviderEventID: "second",
			}); err != nil || found {
				t.Fatalf("root-key contradiction left a receipt: found=%t err=%v", found, err)
			}
		})
	}
}

func a9RequireRootIngressSettlement(t *testing.T, endpoint, runID, eventID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		if len(event.Deliveries) == 1 && event.Deliveries[0].Terminal && event.Deliveries[0].Status == "delivered" {
			if event.RunID != runID || event.Deliveries[0].Target.FlowID != "." || event.Deliveries[0].Target.FlowInstance != runID {
				t.Fatalf("delivery escaped the constructed root: %+v", event)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("constructor delivery did not settle: %+v", event)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func a9PostSignedRawIngress(t *testing.T, endpoint, secret, body string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	status, receipt, err := a9SignedRawIngress(ctx, endpoint, secret, body)
	if err != nil {
		t.Fatal(err)
	}
	return status, receipt
}

func a9SignedRawIngress(ctx context.Context, endpoint, secret, body string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(mac, body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	receipt, err := io.ReadAll(response.Body)
	return response.StatusCode, receipt, err
}
