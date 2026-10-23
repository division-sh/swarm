package serveapp

import (
	"encoding/json"
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
			writeStandingCandidateFile(t, filepath.Join(root, "types.yaml"), "types:\n  AccountInput:\n    delivery_id: text\n    user_id: text\n    labels: {type: '[text]?'}\n    attributes: 'map[text]text?'\n    nested: NullProbe?\n  NullProbe:\n    user_id: text\n")
			writeStandingCandidateFile(t, filepath.Join(root, "events.yaml"), "account.opened:\n  provider: text\n  provider_event_id: text\n  provider_event_type: text\n  data: AccountInput\n")
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "qa-validation-signing-secret"
			if err := file.Set(t.Context(), "webhook_signing.partner", secret); err != nil {
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
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || !statuses[0].RestartDisposition.Executable() || inbound == nil {
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
				code, response := a9PostSignedRawIngress(t, endpoint, secret, invalid.body)
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
					ServiceID: standing.ServiceID, RunID: standing.RunID, Generation: standing.Generation,
					Provider: "partner", ProviderEventID: invalid.id,
				}); err != nil || found {
					t.Fatalf("invalid input published: found=%t err=%v", found, err)
				}
				after, err := rt.Pipeline.ListWorkflowInstances(t.Context(), standing.RunID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("invalid input changed construction: before=%+v after=%+v err=%v", before, after, err)
				}
			}
			incidentOutput := issue2394SurfaceCLI(t, servedControlProofRuntime{Endpoint: served.Endpoint}, "incidents", "--component", "inbound-gateway", "--level", "warn", "--limit", "100")
			if !strings.Contains(incidentOutput, "schema_validation_failed") || !strings.Contains(incidentOutput, "$.data.nested.user_id") || strings.Contains(incidentOutput, "internal failure") {
				t.Fatalf("CLI lost sender rejection classification/path: %s", incidentOutput)
			}
			t.Logf("sender incidents CLI: %s", incidentOutput)
			body := `{"delivery_id":"valid-user","user_id":"user-7"}`
			if fixture.optionalNull {
				body = `{"delivery_id":"valid-user","user_id":"user-7","labels":null,"attributes":null,"nested":null}`
			}
			code, accepted := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("valid typed input status=%d receipt=%s", code, accepted)
			}
			var receipt struct {
				RunID    string   `json:"run_id"`
				EventIDs []string `json:"event_ids"`
			}
			if err := json.Unmarshal(accepted, &receipt); err != nil || receipt.RunID != standing.RunID || len(receipt.EventIDs) != 1 {
				t.Fatalf("valid typed receipt=%s err=%v", accepted, err)
			}
			a9RequireRootIngressSettlement(t, served.Endpoint, receipt.RunID, receipt.EventIDs[0])
			t.Logf("valid typed control status=%d receipt=%s", code, accepted)
		})
	}
}
