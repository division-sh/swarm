package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestQAInboundTypedValidationReturnsPermanentHTTPErrorBothStores(t *testing.T) {
	for _, fixture := range []struct {
		backend, name string
		optionalNull  bool
	}{
		{"sqlite", "sqlite", false},
		{"postgres", "postgres", false},
		{"sqlite", "sqlite_optional_null", true},
		{"postgres", "postgres_optional_null", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			backend := fixture.backend
			root := canonicalrouting.CopyKeyedRootRawIngress(t)
			writeStandingCandidateFile(t, filepath.Join(root, "schema.yaml"), "instance: provider_event_id\nstages: []\npins:\n  inputs: [inbound.partner.raw, inbound.partner.account_opened]\n")
			writeStandingCandidateFile(t, filepath.Join(root, "nodes.yaml"), "receiver:\n  execution_type: system_node\n  subscribes_to: [inbound.partner.account_opened]\n  event_handlers:\n    inbound.partner.account_opened:\n      data_accumulation:\n        source_event: inbound.partner.account_opened\n        writes: [{target_field: processed_count, value: entity.processed_count + 1}]\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [raw, account_opened]\n    verify_secret: webhook_signing_secret\n  cannot: [execute code]\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/trigger.yaml"), `transport: webhook
payload_object_required: true
secret: {required: true}
signature: {type: hmac_sha256, header: X-Signature, prefix: 'sha256=', signed_payload: raw_body}
delivery_id: {json_path: $.delivery_id, required: true}
event_type: {literal: account_opened, required: true}
event_name: {literal: raw}
normalized_events:
  - event: account_opened
    fields:
      provider_event_id: {from: delivery_id, schema: {type: string}}
      data:
        from: input
        schema:
          type: object
          additionalProperties: false
          required: [delivery_id, user_id]
          properties:
            delivery_id: {type: string}
            user_id: {type: string}
            labels:
              type: any
              oneOf:
                - {type: 'null'}
                - {type: array, items: {type: string}}
            attributes:
              type: any
              oneOf:
                - {type: 'null'}
                - {type: object, additionalProperties: {type: string}}
            nested:
              type: any
              oneOf:
                - {type: 'null'}
                - type: object
                  additionalProperties: false
                  required: [user_id]
                  properties:
                    user_id: {type: string}
`)
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "qa-validation-signing-secret"
			if err := file.Set(t.Context(), "partner.webhook_signing_secret", secret); err != nil {
				t.Fatal(err)
			}
			opts, start := clockDeploymentHarness(t, backend, root)
			opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
			var inbound inboundpublication.Runner
			captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) { inbound = persistence.deps.InboundStore })
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("serve cleanup=%d\n%s", code, process.outputString())
				}
			})
			rt := servedTestProcessRuntime(t, process)
			statuses, err := readServedLandingRuns(t.Context(), rt)
			if err != nil || len(statuses) != 1 || statuses[0].State != "live" || inbound == nil {
				t.Fatalf("exact standing ingress fixture: statuses=%+v inbound=%t err=%v", statuses, inbound != nil, err)
			}
			standing := statuses[0]
			before, err := rt.Pipeline.ListWorkflowInstances(t.Context(), standing.RunID)
			if err != nil {
				t.Fatal(err)
			}
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
			for index, invalid := range []struct{ id, body, path string }{
				{"missing-user", `{"delivery_id":"missing-user"}`, "$.data.user_id"},
				{"wrong-user-type", `{"delivery_id":"wrong-user-type","user_id":7}`, "$.data.user_id"},
				{"null-user", `{"delivery_id":"null-user","user_id":null}`, "$.data.user_id"},
				{"null-array", `{"delivery_id":"null-array","user_id":"user-7","labels":[null]}`, "$.data.labels[0]"},
				{"null-map", `{"delivery_id":"null-map","user_id":"user-7","attributes":{"entry":null}}`, "$.data.attributes.entry"},
				{"null-undeclared", `{"delivery_id":"null-undeclared","user_id":"user-7","extra":null}`, "$.data.extra"},
				{"null-nested", `{"delivery_id":"null-nested","user_id":"user-7","nested":{"user_id":null}}`, "$.data.nested.user_id"},
			} {
				body := fmt.Sprintf(`{"delivery_id":%q,"input":%s}`, invalid.id, invalid.body)
				code, response := a9PostSignedRawIngress(t, endpoint, secret, body)
				t.Logf("typed invalid id=%s status=%d body=%s", invalid.id, code, response)
				if code != http.StatusBadRequest && code != http.StatusUnprocessableEntity {
					t.Errorf("typed validation is permanent, status=%d want400/422", code)
				}
				if !strings.Contains(string(response), invalid.path) {
					t.Errorf("typed validation response lost failing path: %s", response)
				}
				if strings.Contains(string(response), "bus:") || strings.Contains(string(response), "publish inbound failed:") {
					t.Errorf("typed validation response leaks internal error: %s", response)
				}
				var logs operatorread.OperatorRuntimeLogListResult
				requireServedJSONRPCResult(t, served.Endpoint, "runtime.logs", map[string]any{
					"bundle_hash": served.BundleHash, "component": "inbound-gateway", "limit": 100,
				}, &logs)
				matched := 0
				for _, row := range logs.Logs {
					if row.CanonicalDetail["provider_event_id"] != invalid.id {
						continue
					}
					matched++
					if row.Level != "warn" || row.Failure == nil || row.Failure.Class != runtimefailures.ClassSchemaInvalid || !strings.Contains(row.Message, invalid.path) {
						t.Errorf("sender rejection misclassified: %+v", row)
					}
				}
				if matched != 1 {
					t.Errorf("gateway rejection logs=%d, want 1", matched)
				}
				for _, component := range []string{"inbound-gateway", "event-bus"} {
					var incidents operatorread.OperatorRuntimeIncidentListResult
					requireServedJSONRPCResult(t, served.Endpoint, "runtime.incidents", map[string]any{
						"bundle_hash": served.BundleHash, "component": component, "limit": 100,
					}, &incidents)
					count := 0
					for _, incident := range incidents.Incidents {
						count += incident.Count
						if incident.Level != "warn" || incident.ErrorCode != "schema_validation_failed" || !strings.Contains(incident.SampleMessage, invalid.path) {
							t.Errorf("sender incident lost schema/path classification: %+v", incident)
						}
					}
					want := 0
					if component == "inbound-gateway" {
						want = index + 1
					}
					if count != want {
						t.Errorf("%s incidents=%+v, want %d sender rejections", component, incidents, want)
					}
				}
				if _, found, err := inbound.LoadInboundPublicationByIdentity(t.Context(), inboundpublication.Identity{
					RunID:    standing.RunID,
					Provider: "partner", ProviderEventID: invalid.id,
				}); err != nil || found {
					t.Fatalf("invalid input published: found=%t err=%v", found, err)
				}
				after, err := rt.Pipeline.ListWorkflowInstances(t.Context(), standing.RunID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("invalid input changed construction: before=%+v after=%+v err=%v", before, after, err)
				}
			}
			body := `{"delivery_id":"valid-user","user_id":"user-7"}`
			if fixture.optionalNull {
				body = `{"delivery_id":"valid-user","user_id":"user-7","labels":null,"attributes":null,"nested":null}`
			}
			body = `{"delivery_id":"valid-user","input":` + body + `}`
			code, accepted := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("valid typed input status=%d receipt=%s", code, accepted)
			}
			var receipt struct {
				RunID    string   `json:"run_id"`
				EventIDs []string `json:"event_ids"`
			}
			if err := json.Unmarshal(accepted, &receipt); err != nil || receipt.RunID != standing.RunID || len(receipt.EventIDs) != 2 {
				t.Fatalf("valid typed receipt=%s err=%v", accepted, err)
			}
			a9RequireRootIngressSettlement(t, served.Endpoint, receipt.RunID, receipt.EventIDs[1])
			t.Logf("valid typed control status=%d receipt=%s", code, accepted)
			config := writeCLIAPIConfigFile(t, map[string]string{"api_server": strings.TrimSuffix(served.Endpoint, "/v1/rpc")})
			var stdout, stderr bytes.Buffer
			code = executeCLIFrom(context.Background(), t.TempDir(), []string{"incidents", "--component", "inbound-gateway", "--level", "warn", "--limit", "100", "--config", config}, &stdout, &stderr, nil)
			if code != 0 {
				t.Fatalf("incidents CLI exit=%d stderr=%s", code, stderr.String())
			}
			incidentOutput := stdout.String()
			if !strings.Contains(incidentOutput, "schema_validation_failed") || !strings.Contains(incidentOutput, "$.data.nested.user_id") || strings.Contains(incidentOutput, "internal failure") {
				t.Fatalf("CLI lost sender rejection classification/path: %s", incidentOutput)
			}
			t.Logf("sender incidents CLI: %s", incidentOutput)
		})
	}
}
