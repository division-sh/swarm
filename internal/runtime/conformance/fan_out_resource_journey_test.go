package conformance

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

// The payload/entity semantic fixtures still use run-creating ingress. Keep
// their helper separate from the deployment-feed journeys below.
func publishSemanticProofRunWithPins(t *testing.T, f *semanticProofFixture, pins []durabledata.ExplicitPin) {
	t.Helper()
	eventType := events.EventType(f.source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, "portfolio.opened"))
	apiEndpoint, err := bus.NewOrdinaryFlowAPIEventPublicationEndpoint(f.source, notifyallchildren.OwnerFlowID, string(eventType))
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	payload, err := json.Marshal(map[string]any{"portfolio_id": f.runID, "threshold": 75})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithMode(eventID, eventType, notifyallchildren.OwnerFlowID, "", payload, 0, f.runID, "", events.EventEnvelope{}, time.Now().UTC(), executionmode.Live)
	initialEvent, err := json.Marshal(map[string]any{"event_name": event.Type(), "payload": json.RawMessage(event.Payload()), "emitter": "operator-api", "entity_id": "", "flow_instance": "", "source_event_id": ""})
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("operator"), IdempotencyKey: eventID, RequestHash: "semantic-proof-" + eventID}
	completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
	creation := durabledata.RunCreationCommand{RunID: f.runID, Actor: "operator", BundleHash: f.runtime.sourceArtifactFact.BundleHash(), EventID: eventID, InitialEvent: initialEvent, Data: durabledata.RunCreationDataEnvelope{Pins: pins}}
	result, replay, err := f.runtime.bus.PublishAPIEventWithRunCreationAcknowledged(f.ctx, event, &apiEndpoint, request, completion, &creation)
	if err != nil || replay || result.ResourceID != eventID {
		t.Fatalf("pinned public run creation: result=%+v replay=%v err=%v", result, replay, err)
	}
	waitNotifyAllChildrenRuntime(t, f.runtime, f.runID)
}

func readJobflowCorpus(t *testing.T) []byte {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "durabledata", "testdata", "jobflow-gems.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if len(raw) != 666737 || hex.EncodeToString(digest[:]) != "7f91b3f892fd32e8605c9155bd4f7b4d90f4ff8dda1b556b1a68f0bb2d865a67" {
		t.Fatalf("jobflow source corpus changed: bytes=%d digest=%x", len(raw), digest)
	}
	return raw
}

func jobflowDeploymentRows(t *testing.T) []byte {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(readJobflowCorpus(t)))
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	var result bytes.Buffer
	count := 0
	for scanner.Scan() {
		var document map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &document); err != nil {
			t.Fatalf("decode source document %d: %v", count, err)
		}
		slug, ok := document["slug"].(string)
		if !ok || slug == "" {
			t.Fatalf("source document %d lacks slug: %#v", count, document)
		}
		row, err := json.Marshal(map[string]any{"account_id": slug, "document": document})
		if err != nil {
			t.Fatal(err)
		}
		result.Write(row)
		result.WriteByte('\n')
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1362 || result.Len() > durabledata.MaxDecodedImportBytes {
		t.Fatalf("document corpus is not a valid single import: rows=%d bytes=%d limit=%d", count, result.Len(), durabledata.MaxDecodedImportBytes)
	}
	return result.Bytes()
}

func deploymentResourceVersion(t *testing.T, f *deploymentResourceFixture, input []byte) (durabledata.DeclarationRef, durabledata.CompiledVersion) {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/account.registered")
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("deployment proof requires the admitted bundle")
	}
	declaration, ok := bundle.DurableDataDeclarationByRef(ref)
	if !ok || declaration.BusinessKey != "account_id" {
		t.Fatalf("deployment event declaration = %+v", declaration)
	}
	compiled, defects := durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, input)
	if len(defects) != 0 {
		t.Fatalf("deployment document corpus rejected: %+v", defects)
	}
	return ref, compiled
}

func (f *deploymentResourceFixture) operatorServer(t *testing.T) *httptest.Server {
	t.Helper()
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("deployment operator fixture lacks bundle")
	}
	identity, err := contracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, ok := f.selected.(apiv1.APIIdempotencyStore)
	if !ok {
		t.Fatalf("selected store %T lacks API idempotency", f.selected)
	}
	contextOwner, ok := f.selected.(apiv1.RunBundleContextStore)
	if !ok {
		t.Fatalf("selected store %T lacks run bundle context", f.selected)
	}
	dataOwner, ok := f.selected.(apiv1.DurableDataStore)
	if !ok {
		t.Fatalf("selected store %T lacks public durable-data owner", f.selected)
	}
	observability, ok := f.selected.(apiv1.ObservabilityReadStore)
	if !ok {
		t.Fatalf("selected store %T lacks public event readback", f.selected)
	}
	publication := apiv1.EventPublicationOptions{
		Idempotency: idempotency, Events: f.runtime.bus, Acknowledged: f.runtime.bus,
		SourceArtifact: f.runtime.bus, RunBundleContext: contextOwner,
		Source: f.source, Bundle: identity,
	}
	methods := apiv1.MergeOperatorHandlers(
		apiv1.OperatorRunStartHandlers(apiv1.RunStartHandlerOptions{Publication: publication}),
		apiv1.OperatorDataHandlers(apiv1.DataHandlerOptions{Store: dataOwner}),
		apiv1.OperatorObservabilityHandlers(apiv1.ObservabilityHandlerOptions{Observability: observability}),
		map[string]apiv1.MethodHandler{"health.check": func(context.Context, apiv1.Request) (any, error) {
			return map[string]any{"alive": true, "ready": true, "db_ok": true, "runtime_ok": true, "bundle": identity}, nil
		}},
	)
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"),
		AuthTokens:       []string{apiv1.DefaultLoopbackAPIToken}, Handlers: methods,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func startDeploymentResourceRun(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, flag string, value string) string {
	t.Helper()
	runID := uuid.NewString()
	var stdout, stderr bytes.Buffer
	args := []string{"run", "start", "--connect", server.URL,
		"--bundle-hash", f.runtime.sourceArtifactFact.BundleHash(), "--run-id", runID,
		flag, value, "--no-follow"}
	if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 {
		t.Fatalf("operator %v: code=%d stderr=%s stdout=%s", args, code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "run_id="+runID) {
		t.Fatalf("run.start returned wrong identity: %s", stdout.String())
	}
	return runID
}

func deploymentResourcePublicEvents(t *testing.T, ctx context.Context, server *httptest.Server, runID, cursor string) operatorread.OperatorEventListResult {
	return deploymentResourcePublicEventsByName(t, ctx, server, runID, "portfolio/account.registered", cursor)
}

func deploymentResourcePublicEventsByName(t *testing.T, ctx context.Context, server *httptest.Server, runID, eventName, cursor string) operatorread.OperatorEventListResult {
	t.Helper()
	params := map[string]any{"filter": map[string]any{"run_id": runID, "event_name": eventName}, "limit": 200}
	if cursor != "" {
		params["cursor"] = cursor
	}
	requestBody, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "event.list", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Result operatorread.OperatorEventListResult `json:"result"`
		Error  json.RawMessage                      `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(envelope.Error) != 0 {
		t.Fatalf("public event.list status=%d error=%s", response.StatusCode, envelope.Error)
	}
	return envelope.Result
}

func assertDeploymentResourceRows(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID string, compiled durabledata.CompiledVersion) {
	t.Helper()
	defer func() {
		if t.Failed() {
			logDeploymentResourceFailure(t, f.db, runID)
		}
	}()
	ctx := f.ctx
	if len(compiled.Rows) > 100 {
		waitDeploymentResourceLargeRun(t, f, runID, len(compiled.Rows))
	} else {
		waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)
	}
	var intents, committed, children, delivered int
	for _, check := range []struct {
		query string
		value *int
	}{
		{`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, &intents},
		{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, &committed},
		{`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`, &children},
		{`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered' AND d.status='delivered'`, &delivered},
	} {
		if err := f.db.QueryRowContext(ctx, check.query, runID).Scan(check.value); err != nil {
			t.Fatal(err)
		}
	}
	if intents != 1 || committed != len(compiled.Rows) || children != len(compiled.Rows) || delivered != len(compiled.Rows) {
		var events, deliveries, receipts, routes int
		for _, check := range []struct {
			query string
			value *int
		}{
			{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='portfolio/account.registered'`, &events},
			{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, &deliveries},
			{`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'`, &receipts},
			{`SELECT COUNT(*) FROM decision_card_route_obligations r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1`, &routes},
		} {
			if err := f.db.QueryRowContext(ctx, check.query, runID).Scan(check.value); err != nil {
				t.Fatal(err)
			}
		}
		statusRows, err := f.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM event_deliveries WHERE run_id=$1 GROUP BY status ORDER BY status`, runID)
		if err != nil {
			t.Fatal(err)
		}
		var deliveryStatuses []string
		for statusRows.Next() {
			var status string
			var count int
			if err := statusRows.Scan(&status, &count); err != nil {
				statusRows.Close()
				t.Fatal(err)
			}
			deliveryStatuses = append(deliveryStatuses, fmt.Sprintf("%s=%d", status, count))
		}
		if err := statusRows.Err(); err != nil {
			statusRows.Close()
			t.Fatal(err)
		}
		statusRows.Close()
		t.Fatalf("deployment source did not settle through real receivers: intents=%d committed=%d events=%d deliveries=%d delivery_statuses=%v pipeline_receipts=%d routes=%d children=%d delivered=%d want=%d", intents, committed, events, deliveries, deliveryStatuses, receipts, routes, children, delivered, len(compiled.Rows))
	}
	var pinnedVersion, intentVersion string
	if err := f.db.QueryRowContext(ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='portfolio' AND event_name='portfolio/account.registered'`, runID).Scan(&pinnedVersion); err != nil {
		t.Fatalf("load exact run pin: %v", err)
	}
	if err := f.db.QueryRowContext(ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intentVersion); err != nil {
		t.Fatalf("load exact deployment intent source: %v", err)
	}
	if pinnedVersion != string(compiled.VersionID) || intentVersion != pinnedVersion {
		t.Fatalf("deployment intent did not consume its exact pin: pin=%s intent=%s want=%s", pinnedVersion, intentVersion, compiled.VersionID)
	}
	want := make(map[string]any, len(compiled.Rows))
	for _, row := range compiled.Rows {
		var payload map[string]any
		if err := json.Unmarshal(row.Canonical, &payload); err != nil {
			t.Fatal(err)
		}
		want[payload["account_id"].(string)] = payload
	}
	seen := make(map[string]bool, len(want))
	cursor := ""
	for {
		page := deploymentResourcePublicEvents(t, ctx, server, runID, cursor)
		for _, event := range page.Events {
			payload := event.Payload
			key, _ := payload["account_id"].(string)
			if seen[key] || !reflect.DeepEqual(payload, want[key]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
				t.Fatalf("public event %s changed document or settlement: %+v", event.EventID, event)
			}
			seen[key] = true
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatal("public event pagination did not advance")
		}
		cursor = page.NextCursor
	}
	if len(seen) != len(want) {
		t.Fatalf("public event readback=%d want=%d", len(seen), len(want))
	}
}

func waitDeploymentResourceLargeRun(t *testing.T, f *deploymentResourceFixture, runID string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var cardinality, cursor int
		var status string
		if err := f.db.QueryRowContext(ctx, `SELECT status,cardinality,cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&status, &cardinality, &cursor); err != nil {
			t.Fatalf("load deployment progress: %v", err)
		}
		if cardinality != expected || cursor > cardinality {
			t.Fatalf("deployment progress contradicts exact source: status=%s cursor=%d cardinality=%d want=%d", status, cursor, cardinality, expected)
		}
		if status == "blocked" {
			t.Fatalf("deployment source blocked before settlement: cursor=%d cardinality=%d", cursor, cardinality)
		}
		if cursor == expected {
			var outcomes, events, delivered int
			for _, check := range []struct {
				query string
				value *int
			}{
				{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, &outcomes},
				{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='portfolio/account.registered'`, &events},
				{`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered' AND d.status='delivered'`, &delivered},
			} {
				if err := f.db.QueryRowContext(ctx, check.query, runID).Scan(check.value); err != nil {
					t.Fatalf("load deployment settlement progress: %v", err)
				}
			}
			if outcomes == expected && events == expected && delivered == expected {
				if err := f.runtime.bus.WaitForQuiescence(ctx); err != nil {
					t.Fatalf("wait for deployment EventBus settlement: %v", err)
				}
				summary, err := f.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
				if err != nil {
					t.Fatalf("final deployment FanOutRunSummary: %v", err)
				}
				if summary.Owed != 0 || summary.Open != 0 || summary.Blocked != 0 || summary.Unsettled != 0 || summary.BarrierArmed != 0 || summary.BarrierPending != 0 {
					t.Fatalf("deployment remains semantically unsettled after exact row counts: %+v", summary)
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for deployment settlement: %v; status=%s cursor=%d cardinality=%d want=%d", ctx.Err(), status, cursor, cardinality, expected)
		case <-ticker.C:
		}
	}
}

func logDeploymentResourceFailure(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT status,cardinality,cursor FROM fan_out_intents WHERE run_id=$1 ORDER BY deployment_feed_id`, runID)
	if err != nil {
		t.Logf("deployment diagnostic intents: %v", err)
		return
	}
	var intents []string
	for rows.Next() {
		var status string
		var cardinality, cursor int
		if err := rows.Scan(&status, &cardinality, &cursor); err != nil {
			rows.Close()
			t.Logf("deployment diagnostic intent scan: %v", err)
			return
		}
		intents = append(intents, fmt.Sprintf("%s:%d/%d", status, cursor, cardinality))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Logf("deployment diagnostic intent rows: %v", err)
		return
	}
	rows.Close()
	var outcomes, events, deliveries, receivers int
	for _, check := range []struct {
		query string
		value *int
	}{
		{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, &outcomes},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1`, &events},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, &deliveries},
		{`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, &receivers},
	} {
		if err := db.QueryRowContext(ctx, check.query, runID).Scan(check.value); err != nil {
			t.Logf("deployment diagnostic count %q: %v", check.query, err)
			return
		}
	}
	statusRows, err := db.QueryContext(ctx, `SELECT status,COUNT(*) FROM event_deliveries WHERE run_id=$1 GROUP BY status ORDER BY status`, runID)
	if err != nil {
		t.Logf("deployment diagnostic delivery statuses: %v", err)
		return
	}
	var statuses []string
	for statusRows.Next() {
		var status string
		var count int
		if err := statusRows.Scan(&status, &count); err != nil {
			statusRows.Close()
			t.Logf("deployment diagnostic delivery status scan: %v", err)
			return
		}
		statuses = append(statuses, fmt.Sprintf("%s=%d", status, count))
	}
	if err := statusRows.Err(); err != nil {
		statusRows.Close()
		t.Logf("deployment diagnostic delivery status rows: %v", err)
		return
	}
	statusRows.Close()
	t.Logf("deployment failure snapshot run=%s intents=%v outcomes=%d events=%d deliveries=%d delivery_statuses=%v receiver_instances=%d", runID, intents, outcomes, events, deliveries, statuses, receivers)
	logRows, err := db.QueryContext(ctx, `SELECT event_name, payload FROM events WHERE run_id=$1 ORDER BY created_at LIMIT 12`, runID)
	if err != nil {
		t.Logf("deployment diagnostic events: %v", err)
		return
	}
	defer logRows.Close()
	for logRows.Next() {
		var name string
		var payload []byte
		if err := logRows.Scan(&name, &payload); err != nil {
			t.Logf("deployment diagnostic event scan: %v", err)
			return
		}
		if name == "platform.runtime_log" {
			t.Logf("deployment runtime diagnostic: %s", payload)
		}
	}
}

func TestVolumeFanOutExactJobflow1362ImportRouteAndSettleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeploymentResourceFixtureWithAgent(t, backend, false)
			input := jobflowDeploymentRows(t)
			_, compiled := deploymentResourceVersion(t, f, input)
			if len(compiled.Rows) != 1362 {
				t.Fatalf("compiled corpus has %d rows", len(compiled.Rows))
			}
			file := filepath.Join(t.TempDir(), "leads.jsonl")
			if err := os.WriteFile(file, input, 0o600); err != nil {
				t.Fatal(err)
			}
			server := f.operatorServer(t)
			runID := startDeploymentResourceRun(t, f, server, "--data", "portfolio/account.registered="+file)
			assertDeploymentResourceRows(t, f, server, runID, compiled)
			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			assertServingJoinComplete(t, join, old, nil)
			f.boot(t)
			f.runtime.fanOutServing.Wake()
			assertDeploymentResourceRows(t, f, server, runID, compiled)
			t.Logf("%s: %d document events, receiver instances, settlements and public rows survived restart", backend, len(compiled.Rows))
		})
	}
}

func TestDeploymentResourceRunStartPinDocumentRowsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeploymentResourceFixture(t, backend)
			input := []byte("{\"account_id\":\"first\",\"document\":{\"slug\":\"first\",\"funds\":[\"A\",\"B\"],\"ats\":{\"provider\":\"greenhouse\"}}}\n")
			_, compiled := deploymentResourceVersion(t, f, input)
			file := filepath.Join(t.TempDir(), "document.jsonl")
			if err := os.WriteFile(file, input, 0o600); err != nil {
				t.Fatal(err)
			}
			server := f.operatorServer(t)
			importRun := startDeploymentResourceRun(t, f, server, "--data", "portfolio/account.registered="+file)
			assertDeploymentResourceRows(t, f, server, importRun, compiled)
			pinRun := startDeploymentResourceRun(t, f, server, "--pin", "portfolio/account.registered@head")
			if pinRun == importRun {
				t.Fatal("pin selected the import run instead of creating a new processing attempt")
			}
			assertDeploymentResourceRows(t, f, server, pinRun, compiled)
		})
	}
}

func TestDeploymentResourceRunStartEmptyVersionDoesNotInventReceiverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeploymentResourceFixture(t, backend)
			file := filepath.Join(t.TempDir(), "empty.jsonl")
			if err := os.WriteFile(file, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			server := f.operatorServer(t)
			runID := startDeploymentResourceRun(t, f, server, "--data", "portfolio/account.registered="+file)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)
			for _, check := range []struct {
				name, query string
			}{
				{"row events", `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='portfolio/account.registered'`},
				{"row deliveries", `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered'`},
				{"receivers", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`},
				{"outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`},
			} {
				var count int
				if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("empty deployment feed invented %s: count=%d err=%v", check.name, count, err)
				}
			}
		})
	}
}
