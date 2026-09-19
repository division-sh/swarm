package serveapp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

// Generate an actual serving failure, not a seeded log or synthetic reader.
// This proves positive incident transport, not a missed-opportunity D3 episode.
func TestIssue2394ServedFanOutFailureIncidentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2394ServedReporterSource(t)
			path := filepath.Join(root, "portfolio", "nodes.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			const expression = "gem_score: account.gem_score"
			if strings.Count(string(raw), expression) != 1 {
				t.Fatal("incident fixture lost exact authored evaluation site")
			}
			writeWorkflowValidationFixtureFile(t, path, strings.Replace(string(raw), expression, "gem_score: int(account.gem_score)", 1))
			opts, start := lifecycleRestartHarness(t, backend, root)
			opts.TestLLMRuntime = servedNoopLLMRuntime{}
			_, rt := start()
			opened := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "portfolio.opened", "idempotency_key": uuid.NewString(),
				"payload": map[string]any{"portfolio_id": "incident", "threshold": 75},
			})
			waitPublicationSiteCompletion(t, rt, opened.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"run_id": opened.RunID, "event_name": "portfolio/portfolio.accounts.register.requested", "idempotency_key": uuid.NewString(),
				"payload": map[string]any{"portfolio_id": "incident", "account_ids": []map[string]any{
					{"account_id": "valid-first", "eng_roles": 7, "gem_score": 7, "external_id": uuid.NewString()},
					{"account_id": "evaluation-fails", "eng_roles": 8, "gem_score": "not-a-number", "external_id": uuid.NewString()},
				}},
			})
			const code = "fan_out_evaluate_ordinal_failed"
			var page fanoutobligation.ListPage
			var logs operatorread.OperatorRuntimeLogListResult
			var incidents operatorread.OperatorRuntimeIncidentListResult
			var sampleID string
			var incident operatorread.OperatorRuntimeIncident
			for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fan_out.list", map[string]any{"run_id": opened.RunID}, &page)
				if len(page.Intents) == 1 && page.Intents[0].Cursor != 0 {
					t.Fatalf("evaluation failure became committed/rejected ordinal progress: %+v", page)
				}
				requireServedJSONRPCResult(t, rt.Endpoint, "runtime.logs", map[string]any{
					"run_id": opened.RunID, "component": "workflow-runtime", "level": "warn", "limit": 100,
				}, &logs)
				for _, log := range logs.Logs {
					if log.ErrorCode == code {
						if log.RunID != opened.RunID || log.Action != "serve_fan_out_obligation" || log.Failure == nil || log.Failure.Detail.Code != code {
							t.Fatalf("actual served failure lost canonical identity: %+v", log)
						}
						sampleID = log.LogID
					}
				}
				requireServedJSONRPCResult(t, rt.Endpoint, "runtime.incidents", map[string]any{
					"bundle_hash": rt.BundleHash, "component": "workflow-runtime", "level": "warn", "limit": 100,
				}, &incidents)
				for _, row := range incidents.Incidents {
					if row.ErrorCode == code {
						incident = row
					}
				}
				if len(page.Intents) == 1 && page.Intents[0].Status == fanoutobligation.StatusBlocked && sampleID != "" && incident.IncidentID != "" {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if len(page.Intents) != 1 || page.Intents[0].Status != fanoutobligation.StatusBlocked || sampleID == "" || incident.IncidentID == "" {
				t.Fatalf("served positive failure evidence missing: page=%+v logs=%+v incidents=%+v", page, logs, incidents)
			}
			row := page.Intents[0]
			if row.Cardinality != 2 || row.Cursor != 0 || row.Owed != 2 || row.Failure == nil || row.Failure.Detail.Code != code ||
				row.Runtime.Availability != "available" || row.Runtime.Eligible == nil || *row.Runtime.Eligible {
				t.Fatalf("blocked runtime intent lost exact failure/eligibility: %+v", row)
			}
			if incident.Count != 1 || len(incident.SampleLogIDs) != 1 || incident.SampleLogIDs[0] != sampleID || incident.Component != "workflow-runtime" {
				t.Fatalf("served incident did not reference exact real runtime log: %+v sample=%s", incident, sampleID)
			}
			before := issue2394SurfaceSnapshot(t, rt.DB, opened.RunID)
			var diagnosis cliapp.DiagnosticRunDiagnosisResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": opened.RunID}, &diagnosis)
			if diagnosis.FanOut.Blocked != 1 || diagnosis.FanOut.Owed != 2 || diagnosis.FanOut.Committed != 0 || diagnosis.FanOut.SemanticRejected != 0 || len(diagnosis.FanOut.BlockedIntents) != 1 {
				t.Fatalf("diagnosis lost blocked versus semantic-rejection distinction: %+v", diagnosis.FanOut)
			}
			var cliPage fanoutobligation.ListPage
			if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", opened.RunID, "--status", "blocked", "--json")), &cliPage); err != nil {
				t.Fatal(err)
			}
			if len(cliPage.Intents) != 1 || cliPage.Intents[0].Key != row.Key || !reflect.DeepEqual(cliPage.Intents[0].Failure, row.Failure) {
				t.Fatalf("CLI lost exact blocked intent/failure: %+v", cliPage)
			}
			t.Run("status_json", func(t *testing.T) {
				var result cliapp.DiagnosticRunDiagnosisResult
				if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "status", opened.RunID, "--json")), &result); err != nil {
					t.Fatal(err)
				}
				if result.FanOut.Blocked != 1 || result.FanOut.Owed != 2 || !reflect.DeepEqual(result.FanOut.BlockedIntents, diagnosis.FanOut.BlockedIntents) {
					t.Fatalf("CLI JSON diagnosis lost exact blocked evidence: %+v", result.FanOut)
				}
			})
			for _, args := range [][]string{
				{"run", "status", opened.RunID},
				{"logs", "--run-id", opened.RunID, "--component", "workflow-runtime", "--level", "warn", "--limit", "100"},
				{"incidents", "--component", "workflow-runtime", "--level", "warn", "--limit", "100"},
			} {
				t.Run(args[0], func(t *testing.T) {
					output := issue2394SurfaceCLI(t, rt, args...)
					if !strings.Contains(output, code) {
						t.Fatalf("CLI %v lost actual typed failure %s: %s", args, code, output)
					}
					if args[0] == "incidents" && !strings.Contains(output, incident.IncidentID) {
						t.Fatalf("CLI incidents lost exact incident identity: %s", output)
					}
				})
			}
			if after := issue2394SurfaceSnapshot(t, rt.DB, opened.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("served diagnostics changed blocked issuance: before=%v after=%v", before, after)
			}
		})
	}
}
