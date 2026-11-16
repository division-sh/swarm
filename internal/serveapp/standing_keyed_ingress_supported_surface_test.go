package serveapp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
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
			if err := file.Set(t.Context(), "partner.webhook_signing_secret", secret); err != nil {
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
			statuses, err := readServedLandingRuns(t.Context(), rt)
			if err != nil || len(statuses) != 1 || statuses[0].State != "live" {
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
				RunID         string   `json:"run_id"`
				EventIDs      []string `json:"event_ids"`
			}
			if err := json.Unmarshal(accepted, &receipt); err != nil || len(receipt.EventIDs) != 1 || receipt.RunID != status.RunID {
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
			if err != nil || initial.CreatingInput != (pipeline.FlowConstructionInput{EventID: receipt.EventIDs[0], Input: "inbound.partner.raw"}) || initial.Fields["provider_event_id"] != "first" {
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
				RunID: status.RunID, Provider: "partner", ProviderEventID: "second",
			}); err != nil || found {
				t.Fatalf("root-key contradiction left a receipt: found=%t err=%v", found, err)
			}
			if !autoEmit {
				t.Run("retained_keyless_audit", func(t *testing.T) {
					proveRetainedKeylessAudit(t, backend, secret)
				})
			}
		})
	}
}

func proveRetainedKeylessAudit(t *testing.T, backend, secret string) {
	t.Helper()
	root := canonicalrouting.CopyConcurrentKeyedRootRawIngress(t)
	path := filepath.Join(root, "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"schema.yaml":       string(raw) + "\noutputs: [audit.observed]\nconnect:\n  - {event: audit.observed, from: ., to: audit}\n",
		"audit/schema.yaml": "stages: []\ninputs: [audit.observed]\n",
		"audit/state.yaml":  "observations: 0\nlast_delivery: \"\"\n",
		"audit/nodes.yaml":  "audit:\n  audit.observed:\n    data_accumulation:\n      writes:\n        - {target_field: observations, value: state.observations + 1}\n        - {target_field: last_delivery, value: payload.delivery}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path = filepath.Join(root, "events.yaml")
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, []byte("\naudit.observed:\n  delivery: text\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(root, "nodes.yaml")
	raw, err = os.ReadFile(path)
	if err != nil || strings.Count(string(raw), "  inbound.partner.raw:\n") != 1 {
		t.Fatalf("retained audit fixture: %s %v", raw, err)
	}
	raw = []byte(strings.Replace(string(raw), "  inbound.partner.raw:\n", "  inbound.partner.raw:\n    emit: {event: audit.observed, fields: {delivery: payload.provider_event_id}}\n", 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	_, start := clockDeploymentHarness(t, backend, root)
	process, served := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("retained audit shutdown=%d", code)
		}
	})
	send := func(id string) string {
		t.Helper()
		endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
		code, raw := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"`+id+`","account_id":"account-7"}`)
		var receipt struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(raw, &receipt); err != nil || code != http.StatusAccepted || receipt.RunID == "" {
			t.Fatalf("retained audit input: status=%d body=%s err=%v", code, raw, err)
		}
		return receipt.RunID
	}
	runID := send("before-restart")
	load := func(want int64) map[string]pipeline.WorkflowInstance {
		t.Helper()
		rt := servedTestProcessRuntime(t, process)
		deadline := time.Now().Add(servedProofPollDeadline)
		for {
			instances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), runID)
			if err != nil {
				t.Fatal(err)
			}
			byFlow := map[string]pipeline.WorkflowInstance{}
			for _, instance := range instances {
				flow := instance.WorkflowName
				if instance.StorageRef == runID {
					flow = "."
				}
				byFlow[flow] = instance
			}
			if audit, found := byFlow["audit"]; found && audit.Fields["observations"] == want {
				if len(byFlow) != 2 || audit.ParentEntityID != byFlow["."].EntityID || audit.Life != 1 || byFlow["."].Life != 1 {
					t.Fatalf("retained parent/audit ownership: %+v", byFlow)
				}
				var diagnosis cliapp.DiagnosticRunDiagnosisResult
				requireServedJSONRPCResult(t, served.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
				if diagnosis.TestQuiescence != nil && cliapp.BoolPointerValue(diagnosis.TestQuiescence.Ready) {
					return byFlow
				}
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("retained audit count=%d: %+v", want, byFlow)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	before := load(1)
	requireServedRunStatus(t, served.Endpoint, runID, "live")
	if code := process.stop(); code != 0 {
		t.Fatalf("retained audit predecessor shutdown=%d", code)
	}
	process, served = start()
	after := load(1)
	requireServedRunStatus(t, served.Endpoint, runID, "live")
	for flow, instance := range before {
		current := after[flow]
		if current.EntityID != instance.EntityID || current.InstanceID != instance.InstanceID || current.StorageRef != instance.StorageRef || current.Life != instance.Life || !reflect.DeepEqual(current.Fields, instance.Fields) {
			t.Fatalf("restart changed retained %s: before=%+v after=%+v", flow, instance, current)
		}
	}
	if got := send("after-restart"); got != runID {
		t.Fatalf("retained parent replaced on next event: %s != %s", got, runID)
	}
	load(2)
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
