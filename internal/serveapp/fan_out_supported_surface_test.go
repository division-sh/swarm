package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// This is a small supported-surface integration proof, not the separate S01
// twenty-by-twenty-five timing workload or a served positive D3 detector proof.
func TestIssue2394ServedFanOutSupportedSurfacesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2394ServedReporterSource(t)
			opts, start := lifecycleRestartHarness(t, backend, root)
			opts.TestLLMRuntime = servedNoopLLMRuntime{}
			process, rt := start()
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("served output:\n%s", process.outputString())
				}
			})
			for _, method := range []string{"run.start", "event.publish"} {
				t.Run(method, func(t *testing.T) {
					portfolio := "surface-" + uuid.NewString()
					params := map[string]any{
						"bundle_hash": rt.BundleHash, "event_name": "portfolio.opened",
						"payload": map[string]any{"portfolio_id": portfolio, "threshold": 75}, "idempotency_key": uuid.NewString(),
					}
					if method == "run.start" {
						params["run_id"] = uuid.NewString()
					}
					var opened struct {
						RunID   string `json:"run_id"`
						EventID string `json:"event_id"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, method, params, &opened)
					if opened.RunID == "" {
						t.Fatalf("%s did not create a run: %+v", method, opened)
					}
					waitPublicationSiteCompletion(t, rt, opened.RunID)
					wanted := make(map[string]map[string]any)
					for batch := 0; batch < 2; batch++ {
						var rows []map[string]any
						for ordinal := 0; ordinal < 3; ordinal++ {
							row := map[string]any{
								"account_id": fmt.Sprintf("account-%d-%d", batch, ordinal), "eng_roles": batch*3 + ordinal,
								"gem_score": float64(batch*3+ordinal) + 0.25, "external_id": uuid.NewString(),
							}
							if ordinal == 1 {
								// Input admits text; authored emission requires UUID.
								row["external_id"] = "not-a-uuid"
							} else {
								wanted[row["account_id"].(string)] = row
							}
							rows = append(rows, row)
						}
						requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
							"run_id": opened.RunID, "event_name": "portfolio/portfolio.accounts.register.requested",
							"payload": map[string]any{"portfolio_id": portfolio, "account_ids": rows}, "idempotency_key": uuid.NewString(),
						})
					}
					diagnosis := awaitIssue2394SurfaceDiagnosis(t, rt, opened.RunID)
					before := issue2394SurfaceSnapshot(t, rt.DB, opened.RunID)
					t.Run("configured_clients", func(t *testing.T) {
						proveIssue2394SurfacePages(t, rt, opened.RunID, backend)
						proveIssue2394SurfaceEvents(t, rt, opened.RunID, portfolio, wanted)
						proveIssue2394SurfaceDiagnostics(t, rt, opened.RunID, diagnosis)
						proveIssue2394SurfaceRefusals(t, rt, opened.RunID)
					})
					t.Run("cli_connection_flags", func(t *testing.T) {
						stdout, stderr, code := runServedCLICommand(t, rt.Endpoint, []string{"run", "fan-out", "list", opened.RunID, "--json"})
						if code != 0 {
							t.Fatalf("supported --api-server flag failed: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
						}
						var page fanoutobligation.ListPage
						if err := json.Unmarshal([]byte(stdout), &page); err != nil || page.RunID != opened.RunID || len(page.Intents) != 2 {
							t.Fatalf("flag-connected CLI lost exact served page: %+v err=%v", page, err)
						}
					})
					if after := issue2394SurfaceSnapshot(t, rt.DB, opened.RunID); !reflect.DeepEqual(before, after) {
						t.Fatalf("supported reads/refusals mutated durable fan-out evidence: before=%v after=%v", before, after)
					}
				})
			}
		})
	}
}

func issue2394ServedReporterSource(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyServedFanOutReporter(t)
}

func issue2394SurfaceSnapshot(t *testing.T, db *sql.DB, runID string) [][]sql.NullString {
	t.Helper()
	var snapshot [][]sql.NullString
	for _, query := range []string{
		`SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,cardinality,cursor,status,claim_owner,claim_generation,CAST(lease_expires_at AS TEXT),blocked_reason,CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(updated_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path`,
		`SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,CAST(event_id AS TEXT),CAST(failure AS TEXT),CAST(created_at AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal`,
		`SELECT event_id,event_name,CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name <> 'platform.runtime_log' ORDER BY event_id`,
	} {
		rows, err := db.Query(query, runID)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			args := make([]any, len(columns))
			for i := range values {
				args[i] = &values[i]
			}
			if err := rows.Scan(args...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			snapshot = append(snapshot, values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func awaitIssue2394SurfaceDiagnosis(t *testing.T, rt servedControlProofRuntime, runID string) cliapp.DiagnosticRunDiagnosisResult {
	t.Helper()
	var result cliapp.DiagnosticRunDiagnosisResult
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &result)
		if result.FanOut.Cursor == 6 && result.TestQuiescence != nil && cliapp.BoolPointerValue(result.TestQuiescence.Ready) {
			if err := result.FanOut.Validate(); err != nil {
				t.Fatal(err)
			}
			f := result.FanOut
			if f.RunID != runID || f.Intents != 2 || f.Cardinality != 6 || f.Committed != 4 || f.SemanticRejected != 2 ||
				f.Owed != 0 || f.Open != 0 || f.Blocked != 0 || f.Unsettled != 0 || f.SemanticRejectionSample == nil {
				t.Fatalf("served diagnosis lost exact mixed outcomes: %+v", f)
			}
			return result
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("served fan-out did not settle: %+v\n%s", result, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return result
}

func issue2394SurfaceCLI(t *testing.T, rt servedControlProofRuntime, args ...string) string {
	t.Helper()
	config := writeCLIAPIConfigFile(t, map[string]string{"api_server": strings.TrimSuffix(rt.Endpoint, "/v1/rpc")})
	var stdout, stderr bytes.Buffer
	code := executeCLIFrom(context.Background(), t.TempDir(), append(args, "--config", config), &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("CLI %v exit=%d stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
	}
	return stdout.String()
}

func proveIssue2394SurfacePages(t *testing.T, rt servedControlProofRuntime, runID, backend string) {
	t.Helper()
	query := fanoutobligation.ListQuery{RunID: runID, Limit: 1, Filter: fanoutobligation.ListFilter{Status: fanoutobligation.StatusClosed, FlowPath: "portfolio"}}
	var first fanoutobligation.ListPage
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fan_out.list", map[string]any{
		"run_id": runID, "limit": 1, "filter": map[string]any{"status": "closed", "flow_path": "portfolio"},
	}, &first)
	if err := first.Validate(query); err != nil {
		t.Fatal(err)
	}
	if len(first.Intents) != 1 || first.NextCursor == "" {
		t.Fatalf("first served page must be bounded with continuation: %+v", first)
	}
	stdout := issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", runID,
		"--status", "closed", "--flow-path", "portfolio", "--limit", "1", "--cursor", first.NextCursor, "--json")
	var second fanoutobligation.ListPage
	if err := json.Unmarshal([]byte(stdout), &second); err != nil {
		t.Fatal(err)
	}
	query.Cursor = first.NextCursor
	if err := second.Validate(query); err != nil {
		t.Fatal(err)
	}
	if len(second.Intents) != 1 || second.NextCursor != "" || first.Intents[0].Key == second.Intents[0].Key {
		t.Fatalf("CLI continuation lost exact page identity: %+v / %+v", first, second)
	}
	for _, row := range append(first.Intents, second.Intents...) {
		workers := 1
		if backend == "postgres" {
			workers = 4
		}
		if row.Key.RunID != runID || row.BundleHash != rt.BundleHash || row.Cardinality != 3 || row.Cursor != 3 || row.Owed != 0 || row.DurableState != "closed" {
			t.Fatalf("served row lost authored durable state: %+v", row)
		}
		if row.Runtime.Availability != "available" || row.Runtime.ObservedAt == nil || row.Runtime.Eligible == nil || *row.Runtime.Eligible ||
			row.Runtime.Workers == nil || *row.Runtime.Workers != workers || row.Runtime.ActiveWorkers == nil || *row.Runtime.ActiveWorkers < 0 || *row.Runtime.ActiveWorkers > workers {
			t.Fatalf("served process observer unavailable or invented execution eligibility: %+v", row.Runtime)
		}
	}
	human := issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", runID, "--limit", "1")
	for _, want := range []string{"cursor=3/3", "owed=0", "runtime=available", "eligible=false", "next_cursor="} {
		if !strings.Contains(human, want) {
			t.Fatalf("human fan-out output missing %q: %s", want, human)
		}
	}
	quiet := issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", runID, "--quiet")
	keys := strings.Fields(quiet)
	if len(keys) != 2 || keys[0] != first.Intents[0].Key.String() || keys[1] != second.Intents[0].Key.String() {
		t.Fatalf("quiet CLI changed exact keyset order: %q", quiet)
	}
	wrongScope := requestServedJSONRPC(t, rt.Endpoint, "run.fan_out.list", map[string]any{
		"run_id": runID, "cursor": first.NextCursor, "filter": map[string]any{"status": "open", "flow_path": "portfolio"},
	})
	if wrongScope.Error == nil || wrongScope.Error.Code != -32602 {
		t.Fatalf("served cursor filter mismatch was accepted: %+v", wrongScope)
	}
}

func proveIssue2394SurfaceEvents(t *testing.T, rt servedControlProofRuntime, runID, portfolio string, wanted map[string]map[string]any) {
	t.Helper()
	var result operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 1000}, &result)
	if result.NextCursor != "" {
		t.Fatal("small fixture unexpectedly exceeded event read page")
	}
	seen := make(map[string]bool)
	for _, event := range result.Events {
		if event.EventName != "portfolio/account.registered" {
			continue
		}
		id, _ := event.Payload["account_id"].(string)
		row, exists := wanted[id]
		if !exists || seen[id] || event.RunID != runID || event.Payload["portfolio_id"] != portfolio || event.Payload["eligible"] != true ||
			event.Payload["eng_roles"] != float64(row["eng_roles"].(int)) || event.Payload["gem_score"] != row["gem_score"] || event.Payload["external_id"] != row["external_id"] {
			t.Fatalf("served emitted payload lost identity/numeric value or published rejected ordinal: %+v", event)
		}
		seen[id] = true
	}
	if len(seen) != len(wanted) {
		t.Fatalf("served emitted results=%v want=%v", seen, wanted)
	}
}

func proveIssue2394SurfaceDiagnostics(t *testing.T, rt servedControlProofRuntime, runID string, diagnosis cliapp.DiagnosticRunDiagnosisResult) {
	t.Helper()
	var cliDiagnosis cliapp.DiagnosticRunDiagnosisResult
	if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "status", runID, "--json")), &cliDiagnosis); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cliDiagnosis.FanOut, diagnosis.FanOut) {
		t.Fatalf("CLI diagnosis changed fan-out evidence: %+v / %+v", cliDiagnosis.FanOut, diagnosis.FanOut)
	}
	human := issue2394SurfaceCLI(t, rt, "run", "status", runID)
	for _, want := range []string{"4 committed items", "2 semantic rejections", "0 unsettled items"} {
		if !strings.Contains(human, want) {
			t.Fatalf("CLI status missing %q: %s", want, human)
		}
	}
	type runHeaderResult struct {
		Run struct {
			RunID  string `json:"run_id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	var header runHeaderResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": runID}, &header)
	var cliHeader runHeaderResult
	if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "status", runID, "--no-diagnose", "--json")), &cliHeader); err != nil {
		t.Fatal(err)
	}
	if header.Run.RunID != runID || cliHeader.Run.RunID != runID || cliHeader.Run.Status != header.Run.Status {
		t.Fatalf("CLI header differs from served run.get: %+v / %+v", header, cliHeader)
	}
	var logs operatorread.OperatorRuntimeLogListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "runtime.logs", map[string]any{"run_id": runID, "limit": 1000}, &logs)
	if len(logs.Logs) == 0 {
		t.Fatal("real served execution produced no runtime log evidence")
	}
	for _, log := range logs.Logs {
		if log.RunID != runID || log.LogID == "" {
			t.Fatalf("served logs lost run isolation: %+v", log)
		}
	}
	logOutput := issue2394SurfaceCLI(t, rt, "logs", "--run-id", runID, "--limit", "1000")
	if !strings.Contains(logOutput, logs.Logs[0].Component) || strings.Contains(logOutput, "No runtime logs") {
		t.Fatalf("CLI logs did not render real served execution: %s", logOutput)
	}
	// A healthy serving journey must not manufacture D3. Do not confuse the
	// failure envelope's component with the persisted logger's component.
	var incidents operatorread.OperatorRuntimeIncidentListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "runtime.incidents", map[string]any{
		"bundle_hash": rt.BundleHash, "component": "workflow-runtime", "level": "warn", "limit": 500,
	}, &incidents)
	if incidents.NextCursor != "" {
		t.Fatalf("small served journey unexpectedly exceeded incident page: %+v", incidents)
	}
	output := issue2394SurfaceCLI(t, rt, "incidents", "--component", "workflow-runtime", "--level", "warn", "--limit", "500")
	for _, incident := range incidents.Incidents {
		if incident.ErrorCode == "fan_out_claim_opportunity_missed" || incident.Component != "workflow-runtime" || incident.Level != "warn" ||
			!strings.Contains(output, incident.IncidentID) {
			t.Fatalf("served/CLI incident evidence differs or healthy issuance manufactured D3: %+v output=%s", incident, output)
		}
	}
	if len(incidents.Incidents) == 0 && !strings.Contains(output, "No runtime incidents match") {
		t.Fatalf("CLI incidents changed empty served evidence: %s", output)
	}
}

func proveIssue2394SurfaceRefusals(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"surface-refusal","method":"run.fan_out.list","params":{"run_id":%q}}`, runID)
	request, err := http.NewRequest(http.MethodPost, rt.Endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated served list status=%d", response.StatusCode)
	}
	for _, method := range []string{"run.start", "event.publish"} {
		unsupported := semanticNumericRPC(t, rt.Endpoint, "ws", fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"transport-refusal","method":%q,"params":{"run_id":%q,"bundle_hash":%q,"event_name":"portfolio/portfolio.accounts.register.requested","payload":{"portfolio_id":"refused","account_ids":[]},"idempotency_key":%q}}`,
			method, runID, rt.BundleHash, uuid.NewString()))
		if unsupported.Error == nil || unsupported.Error.Code != -32601 {
			t.Fatalf("unsupported served command transport did not refuse at method admission: %+v", unsupported)
		}
		details, _ := unsupported.Error.Data["details"].(map[string]any)
		if details["method"] != method || details["transport"] != "websocket" {
			t.Fatalf("transport refusal lost its exact method/transport: %+v", unsupported.Error)
		}
	}
}
