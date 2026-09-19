package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// Compose the real held runtime's persisted D3 episode with authenticated
// production read handlers and CLI clients. This is not a serve-boot test.
func assertServingD3SupportedReadback(t *testing.T, f *servingMatrixFixture, ctx context.Context, key fanoutobligation.IntentKey, grantID string) {
	t.Helper()
	reader, ok := f.selected.(apiv1.ObservabilityReadStore)
	if !ok {
		t.Fatalf("selected store %T lacks the canonical public observability reader", f.selected)
	}
	const token = "fan-out-d3-supported-read-token"
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"),
		AuthTokens:       []string{token},
		Handlers:         apiv1.OperatorObservabilityHandlers(apiv1.ObservabilityHandlerOptions{Observability: reader}),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	read := func(t *testing.T, method string, params map[string]any, result any) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || (len(envelope.Error) > 0 && string(envelope.Error) != "null") {
			t.Fatalf("%s failed: status=%d error=%s", method, response.StatusCode, envelope.Error)
		}
		if err := json.Unmarshal(envelope.Result, result); err != nil {
			t.Fatal(err)
		}
	}
	before := readServingLifetimeState(t, f.db, key.RunID)
	bundle := f.runtimes[0].sourceArtifactFact.BundleHash()
	var logs operatorread.OperatorRuntimeLogListResult
	read(t, "runtime.logs", map[string]any{
		"run_id": key.RunID, "bundle_hash": bundle, "component": "workflow-runtime", "level": "warn", "limit": 10,
	}, &logs)
	if len(logs.Logs) != 1 || logs.NextCursor != "" {
		t.Fatalf("HTTP lost exact D3 episode: %+v", logs)
	}
	log := logs.Logs[0]
	if log.RunID != key.RunID || log.ErrorCode != servingD3Code || log.Action != "serve_fan_out_obligation" {
		t.Fatalf("HTTP changed the real D3 log identity: %+v", log)
	}
	assertServingD3Failure(t, log.Failure, key, grantID)
	var incidents operatorread.OperatorRuntimeIncidentListResult
	read(t, "runtime.incidents", map[string]any{
		"bundle_hash": bundle, "component": "workflow-runtime", "level": "warn", "since_hours": 1, "limit": 10,
	}, &incidents)
	if len(incidents.Incidents) != 1 || incidents.NextCursor != "" {
		t.Fatalf("HTTP lost exact D3 incident: %+v", incidents)
	}
	incident := incidents.Incidents[0]
	if incident.ErrorCode != servingD3Code || incident.Count != 1 || len(incident.SampleLogIDs) != 1 || incident.SampleLogIDs[0] != log.LogID {
		t.Fatalf("HTTP incident is not the actual runtime log: %+v log=%+v", incident, log)
	}
	// #2454 already owns this filter contract. Keep its assertion without
	// preventing the remaining positive CLI/settlement boundaries from running.
	t.Run("foreign_bundle_filter_issue2454", func(t *testing.T) {
		var foreign operatorread.OperatorRuntimeLogListResult
		read(t, "runtime.logs", map[string]any{"run_id": key.RunID, "bundle_hash": f.runtimes[1].sourceArtifactFact.BundleHash(), "limit": 10}, &foreign)
		if len(foreign.Logs) != 0 {
			t.Fatalf("HTTP leaked D3 across the exact source filter: %+v", foreign)
		}
	})
	tokenFile := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(configFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"logs", "--run-id", key.RunID, "--component", "workflow-runtime", "--level", "warn", "--limit", "10"},
		{"incidents", "--component", "workflow-runtime", "--level", "warn", "--since-hours", "1", "--limit", "10"},
	} {
		var stdout, stderr bytes.Buffer
		command := append(append([]string(nil), args...), "--config", configFile, "--api-server", server.URL, "--api-token-file", tokenFile)
		if code := cliapp.Execute(ctx, command, &stdout, &stderr, nil, nil); code != 0 {
			t.Fatalf("CLI %v failed: code=%d stderr=%s stdout=%s", args, code, stderr.String(), stdout.String())
		}
		if !strings.Contains(stdout.String(), servingD3Code) || (args[0] == "incidents" && !strings.Contains(stdout.String(), incident.IncidentID)) {
			t.Fatalf("CLI %v lost exact D3 evidence: %s", args, stdout.String())
		}
	}
	assertServingLifetimeUnchanged(t, before, readServingLifetimeState(t, f.db, key.RunID))
	t.Log("real D3 detector/log -> authenticated HTTP -> CLI reads: exact source, grant, run, sample log and incident; no issuance mutation")
}
