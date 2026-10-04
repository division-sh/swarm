package conformance

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/packadmission"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The root output pin, connect-free root route, and root-collector are authored
// by CopySelectedDeploymentResource. Only the event declaration varies here.
func textFileDeploymentFixture2456(t *testing.T, backend string, keyed bool) (*deploymentResourceFixture, string) {
	t.Helper()
	root := canonicalrouting.CopySelectedDeploymentResource(t, "root", keyed)
	declaration := "root.ready:\n  body: text\n"
	if keyed {
		declaration = "root.ready:\n  key: account_id\n  account_id: text\n  body: text\n  cover: text?\n"
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte(declaration), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(conformanceRepoRoot(t), root, contracts.DefaultPlatformSpecFile(conformanceRepoRoot(t)))
	if err != nil {
		t.Fatalf("load authored text-file bundle: %v", err)
	}
	return newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle)), root
}

func textFileVersion2456(t *testing.T, f *deploymentResourceFixture, rows []map[string]any) durabledata.CompiledVersion {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", "root.ready")
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("text-file source is not bundle-backed")
	}
	declaration, ok := bundle.DurableDataDeclarationByRef(ref)
	if !ok {
		t.Fatal("authored root.ready is not a durable-data declaration")
	}
	var input bytes.Buffer
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(encoded)
		input.WriteByte('\n')
	}
	if input.Len() > durabledata.MaxDecodedImportBytes {
		t.Fatalf("fixture exceeds decoded import limit: %d", input.Len())
	}
	version, defects := durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, input.Bytes())
	if len(defects) != 0 || len(version.Rows) != len(rows) {
		t.Fatalf("authored text rows did not compile: defects=%+v rows=%d want=%d", defects, len(version.Rows), len(rows))
	}
	return version
}

func startTextFileRun2456(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID string, operands ...string) string {
	t.Helper()
	args := []string{"run", "start", "--connect", server.URL, "--run-id", runID}
	args = append(args, operands...)
	args = append(args, "--no-follow")
	var stdout, stderr bytes.Buffer
	if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 || !strings.Contains(stdout.String(), "run_id="+runID) {
		t.Fatalf("real operator run start %v: code=%d stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
	}
	return runID
}

func assertTextFileRun2456(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID string, version durabledata.CompiledVersion, rows []map[string]any) {
	t.Helper()
	defer func() {
		if t.Failed() {
			logDeploymentResourceFailure(t, f.db, runID)
		}
	}()
	waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)
	settleCtx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	if err := f.runtime.bus.WaitForQuiescence(settleCtx); err != nil {
		t.Fatalf("EventBus did not settle: %v", err)
	}
	summary, err := f.selected.FanOutRunSummary(f.ctx, runID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Owed != 0 || summary.Open != 0 || summary.Blocked != 0 || summary.Unsettled != 0 || summary.BarrierArmed != 0 || summary.BarrierPending != 0 {
		t.Fatalf("deployment feed remains unsettled: %+v", summary)
	}
	var pin, intentVersion string
	if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&pin); err != nil {
		t.Fatalf("read exact run pin: %v", err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&intentVersion); err != nil {
		t.Fatalf("read deployment feed version: %v", err)
	}
	if pin != string(version.VersionID) || intentVersion != pin {
		t.Fatalf("feed did not consume exact pinned version: pin=%s feed=%s want=%s", pin, intentVersion, version.VersionID)
	}
	for _, check := range []struct {
		name, query string
		want        int
	}{
		{"deployment intents", `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, 1},
		{"committed outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, len(rows)},
		{"row events", `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, len(rows)},
		{"delivered routes", `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.status='delivered'`, len(rows)},
		{"pipeline receipts", `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name='root.ready' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, len(rows)},
	} {
		var got int
		if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", check.name, err)
		}
		if check.name == "pipeline receipts" && got != check.want {
			deadline := time.Now().Add(5 * time.Second)
			for got != check.want && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(&got); err != nil {
					t.Fatalf("read %s after quiescence: %v", check.name, err)
				}
			}
		}
		if got != check.want {
			t.Fatalf("%s=%d want=%d", check.name, got, check.want)
		}
	}
	want := make(map[string]map[string]any, len(rows))
	for i, row := range rows {
		key := fmt.Sprint(i)
		if value, ok := row["account_id"].(string); ok {
			key = value
		}
		want[key] = row
	}
	seen := make(map[string]bool, len(rows))
	cursor := ""
	for {
		page := deploymentResourcePublicEventsByName(t, f.ctx, server, runID, "root.ready", cursor)
		for _, event := range page.Events {
			key := fmt.Sprint(len(seen))
			if value, ok := event.Payload["account_id"].(string); ok {
				key = value
			}
			if seen[key] || !reflect.DeepEqual(event.Payload, want[key]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].SubscriberID != conformanceNode(t, "", "root-collector").Key() {
				t.Fatalf("public event lost row, route, or settlement: %+v want=%+v", event, want[key])
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
	if len(seen) != len(rows) {
		t.Fatalf("public event readback has %d rows, want %d", len(seen), len(rows))
	}
}

func TestDataTextFile2456KeylessOperatorRouteRestartReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, root := textFileDeploymentFixture2456(t, backend, false)
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "resume.md")
			body := "First line\nA quoted \"line\" and a backslash \\ remain text.\n"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			rows := []map[string]any{{"body": body}}
			version := textFileVersion2456(t, f, rows)
			runID := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+path)
			assertTextFileRun2456(t, f, server, runID, version, rows)
			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			assertServingJoinComplete(t, join, old, nil)
			f.boot(t)
			f.runtime.fanOutServing.Wake()
			server = f.operatorServer(t)
			startTextFileRun2456(t, f, server, runID, "--data", "root.ready.body="+path)
			assertTextFileRun2456(t, f, server, runID, version, rows)
			var runs, versions int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&runs); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM resource_versions WHERE version_id=$1`, string(version.VersionID)).Scan(&versions); err != nil {
				t.Fatal(err)
			}
			if runs != 1 || versions != 1 {
				t.Fatalf("replay duplicated durable run/version: runs=%d versions=%d", runs, versions)
			}
			before := f.runtime.sourceArtifactFact.BundleHash()
			if err := os.WriteFile(path, []byte("changed external deployment data\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			reloaded, err := contracts.LoadWorkflowContractBundleWithOverrides(conformanceRepoRoot(t), root, contracts.DefaultPlatformSpecFile(conformanceRepoRoot(t)))
			if err != nil {
				t.Fatal(err)
			}
			after := conformanceSourceArtifactFact(t, semanticview.Wrap(reloaded)).BundleHash()
			if before != after {
				t.Fatalf("external deployment data mutation changed bundle hash: %s -> %s", before, after)
			}
			var replayOut, replayErr bytes.Buffer
			replayArgs := []string{"run", "start", "--connect", server.URL, "--run-id", runID, "--data", "root.ready.body=" + path, "--no-follow"}
			if code := cliapp.Execute(f.ctx, replayArgs, &replayOut, &replayErr, nil, nil); code == 0 {
				t.Fatalf("changed file silently replayed old operation: stdout=%s stderr=%s", replayOut.String(), replayErr.String())
			}
			changedBody := "changed external deployment data\n"
			changedRows := []map[string]any{{"body": changedBody}}
			changedVersion := textFileVersion2456(t, f, changedRows)
			if changedVersion.VersionID == version.VersionID {
				t.Fatal("changed external file retained original version identity")
			}
			changedRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+path)
			assertTextFileRun2456(t, f, server, changedRun, changedVersion, changedRows)
			owner, recovery := deploymentForkOwner(t, f)
			if len(recovery) != 0 {
				t.Fatalf("fresh selected fork owner recovered unexpected work: %+v", recovery)
			}
			forkServer := deploymentForkServer(t, f, owner)
			forked, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, map[string]any{
				"source_run_id": runID, "bundle_hash": before, "allow_source_freeze": true, "idempotency_key": uuid.NewString(),
			})
			if len(rpcErr) != 0 || forked.ForkRunID == "" || forked.ForkRunID == runID || len(forked.DataPins) != 1 || forked.DataPins[0].VersionID != version.VersionID {
				t.Fatalf("old-version selected fork changed source pin: fork=%+v error=%s want=%s", forked, rpcErr, version.VersionID)
			}
			var forkPin string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, forked.ForkRunID).Scan(&forkPin); err != nil {
				t.Fatalf("read selected fork pin: %v", err)
			}
			if forkPin != string(version.VersionID) {
				t.Fatalf("selected fork pin=%s want=%s", forkPin, version.VersionID)
			}
			var forkOutcomes int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, forked.ForkRunID).Scan(&forkOutcomes); err != nil {
				t.Fatal(err)
			}
			if forkOutcomes != 1 {
				t.Fatalf("selected fork lost or duplicated inherited completion: outcomes=%d", forkOutcomes)
			}
			if err := owner.RetireSelectedContexts(f.ctx); err != nil {
				t.Fatalf("retire selected fork contexts: %v", err)
			}
		})
	}
}

func TestFileRow2456Keyed37DeltaOldPinBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := textFileDeploymentFixture2456(t, backend, true)
			server := f.operatorServer(t)
			dir := t.TempDir()
			rows := make([]map[string]any, 37)
			for i := range rows {
				key := fmt.Sprintf("candidate-%02d", i)
				body := fmt.Sprintf("Resume %02d\n", i)
				if err := os.WriteFile(filepath.Join(dir, key+".md"), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				rows[i] = map[string]any{"account_id": key, "body": body}
			}
			original := textFileVersion2456(t, f, rows)
			originalRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+dir)
			assertTextFileRun2456(t, f, server, originalRun, original, rows)
			for _, i := range []int{4, 19} {
				key := fmt.Sprintf("candidate-%02d", i)
				body := fmt.Sprintf("Revised resume %02d\n", i)
				if err := os.WriteFile(filepath.Join(dir, key+".md"), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				rows[i] = map[string]any{"account_id": key, "body": body}
			}
			changed := textFileVersion2456(t, f, rows)
			if changed.VersionID == original.VersionID {
				t.Fatal("changed-two directory reused the old version identity")
			}
			changedRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+dir)
			assertTextFileRun2456(t, f, server, changedRun, changed, rows)
			dataStore, ok := f.selected.(apiv1.DurableDataStore)
			if !ok {
				t.Fatalf("selected store %T lacks durable operation readback", f.selected)
			}
			record, err := dataStore.LoadDataRunCreationOperation(f.ctx, changedRun)
			if err != nil {
				t.Fatal(err)
			}
			if record.RequestBinding == nil || len(record.RequestBinding.Imports) != 1 {
				t.Fatalf("changed-two run has no exact child request binding: %+v", record.RequestBinding)
			}
			child, err := dataStore.LoadDataSourceOperation(f.ctx, record.RequestBinding.Imports[0].SourceInvocationID)
			if err != nil {
				t.Fatalf("read changed-two child evaluation: %v", err)
			}
			delta := child.Result.Delta
			if delta.State != "computed" || delta.RowIdentity != durabledata.DeltaRowIdentityBusinessKey || delta.Summary == nil || *delta.Summary != (durabledata.DeltaSummary{Changed: 2}) {
				t.Fatalf("changed-two import delta = %+v, want +0 -0 ~2", delta)
			}
			originalRows := make([]map[string]any, 37)
			for i := range originalRows {
				originalRows[i] = map[string]any{"account_id": fmt.Sprintf("candidate-%02d", i), "body": fmt.Sprintf("Resume %02d\n", i)}
			}
			oldPinRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--pin", "root.ready@"+string(original.VersionID))
			assertTextFileRun2456(t, f, server, oldPinRun, original, originalRows)
			assertTextFileRun2456(t, f, server, originalRun, original, originalRows)
		})
	}
}

func TestDataTextFile2456Keyed37DynamicReceiversBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopySelectedDeploymentResource(t, "dynamic", true)
			if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("root.ready:\n  key: account_id\n  account_id: text\n  body: text\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(conformanceRepoRoot(t), root, contracts.DefaultPlatformSpecFile(conformanceRepoRoot(t)))
			if err != nil {
				t.Fatal(err)
			}
			f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
			server := f.operatorServer(t)
			directory := filepath.Join(t.TempDir(), "resumes")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			rows := make([]map[string]any, 0, 37)
			wantBody := make(map[string]string, 37)
			for ordinal := 36; ordinal >= 0; ordinal-- {
				key := fmt.Sprintf("candidate-%02d", ordinal)
				body := fmt.Sprintf("Resume %02d\n", ordinal)
				if err := os.WriteFile(filepath.Join(directory, key+".md"), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, map[string]any{"account_id": key, "body": body})
				wantBody[key] = body
			}
			version := textFileVersion2456(t, f, rows)
			runID := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+directory)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)
			for _, check := range []struct {
				name, query string
			}{
				{"dynamic receiver instances", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='consumer'`},
				{"committed row outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`},
				{"delivered row events", `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.status='delivered'`},
			} {
				var count int
				if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(&count); err != nil || count != 37 {
					t.Fatalf("%s=%d, %v; want 37", check.name, count, err)
				}
			}
			var pinned string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&pinned); err != nil || pinned != string(version.VersionID) {
				t.Fatalf("dynamic receiver pin=%s, %v; want %s", pinned, err, version.VersionID)
			}
			seen := map[string]bool{}
			cursor := ""
			for {
				page := deploymentResourcePublicEventsByName(t, f.ctx, server, runID, "root.ready", cursor)
				for _, event := range page.Events {
					key, _ := event.Payload["account_id"].(string)
					if seen[key] || event.Payload["body"] != wantBody[key] || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
						t.Fatalf("dynamic receiver public readback lost unique settled key: %+v", event)
					}
					seen[key] = true
				}
				if page.NextCursor == "" {
					break
				}
				if page.NextCursor == cursor {
					t.Fatal("dynamic receiver public cursor did not advance")
				}
				cursor = page.NextCursor
			}
			if len(seen) != 37 {
				t.Fatalf("public dynamic receiver keys=%d, want 37", len(seen))
			}
		})
	}
}

func TestDataTextFile2456MultiFieldAndLimitsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := textFileDeploymentFixture2456(t, backend, true)
			server := f.operatorServer(t)
			root := t.TempDir()
			bodies := filepath.Join(root, "bodies")
			covers := filepath.Join(root, "covers")
			for _, dir := range []string{bodies, covers} {
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for name, content := range map[string]string{"ada.md": "Ada resume\n", "bea.md": "Bea resume\n"} {
				if err := os.WriteFile(filepath.Join(bodies, name), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(covers, "ada.md"), []byte("Ada cover\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			rows := []map[string]any{{"account_id": "ada", "body": "Ada resume\n", "cover": "Ada cover\n"}, {"account_id": "bea", "body": "Bea resume\n"}}
			version := textFileVersion2456(t, f, rows)
			runID := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+bodies, "--data", "root.ready.cover="+covers)
			assertTextFileRun2456(t, f, server, runID, version, rows)
			if durabledata.MaxDecodedImportBytes != 720<<10 || durabledata.MaxCanonicalRowBytes != 128<<10 {
				t.Fatalf("file lowering changed existing import/row limits: import=%d row=%d", durabledata.MaxDecodedImportBytes, durabledata.MaxCanonicalRowBytes)
			}
			oversizeDir := filepath.Join(root, "oversize")
			if err := os.Mkdir(oversizeDir, 0o700); err != nil {
				t.Fatal(err)
			}
			tooLarge := filepath.Join(oversizeDir, "candidate.md")
			if err := os.WriteFile(tooLarge, bytes.Repeat([]byte("x"), durabledata.MaxDecodedImportBytes+1), 0o600); err != nil {
				t.Fatal(err)
			}
			args := []string{"run", "start", "--connect", server.URL, "--run-id", uuid.NewString(), "--data", "root.ready.body=" + oversizeDir, "--no-follow"}
			var stdout, stderr bytes.Buffer
			if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code == 0 || !strings.Contains(stderr.String(), fmt.Sprint(durabledata.MaxDecodedImportBytes)) {
				t.Fatalf("oversize file escaped decoded import limit: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			largeRowDir := filepath.Join(root, "large-row")
			if err := os.Mkdir(largeRowDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(largeRowDir, "candidate.md"), bytes.Repeat([]byte("r"), durabledata.MaxCanonicalRowBytes), 0o600); err != nil {
				t.Fatal(err)
			}
			largeRowRunID := uuid.NewString()
			args = []string{"run", "start", "--connect", server.URL, "--run-id", largeRowRunID, "--data", "root.ready.body=" + largeRowDir, "--no-follow"}
			stdout.Reset()
			stderr.Reset()
			if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code == 0 {
				t.Fatalf("oversize canonical row escaped 128KiB limit: stdout=%s stderr=%s", stdout.String(), stderr.String())
			}
			dataStore, ok := f.selected.(apiv1.DurableDataStore)
			if !ok {
				t.Fatalf("selected store %T lacks rejected operation readback", f.selected)
			}
			rejected, err := dataStore.LoadDataRunCreationOperation(f.ctx, largeRowRunID)
			if err != nil {
				t.Fatalf("canonical-row failure lacks durable rejection: %v", err)
			}
			if len(rejected.Evidence.ChildDefects) != 1 || rejected.Evidence.ChildDefects[0].Defect.Code != "row_too_large" {
				t.Fatalf("canonical-row failure has wrong durable defect: %+v", rejected.Evidence.ChildDefects)
			}
		})
	}
}

type textFileRPCResult2456 struct {
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func textFileRPCWire2456(t *testing.T, server *httptest.Server, wire []byte) textFileRPCResult2456 {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(wire))
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
	var result textFileRPCResult2456
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode served RPC status=%d: %v", response.StatusCode, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("served RPC status=%d result=%s error=%s", response.StatusCode, result.Result, result.Error)
	}
	return result
}

func textFileRPCParams2456(t *testing.T, server *httptest.Server, method string, params map[string]any) textFileRPCResult2456 {
	t.Helper()
	wire, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": uuid.NewString(), "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	return textFileRPCWire2456(t, server, wire)
}

func textFileRequestBinding2456(t *testing.T, server *httptest.Server, runID string) durabledata.RunCreationRequestBinding {
	t.Helper()
	result := textFileRPCParams2456(t, server, "data.show", map[string]any{
		"view": "operation", "operation_ref": map[string]any{"kind": "run_creation", "run_id": runID}, "detail": "request_binding",
	})
	if len(result.Error) != 0 {
		t.Fatalf("public request binding for %s: %s", runID, result.Error)
	}
	var binding durabledata.RunCreationRequestBinding
	if err := json.Unmarshal(result.Result, &binding); err != nil {
		t.Fatal(err)
	}
	if err := binding.Validate(); err != nil || binding.RunID != runID {
		t.Fatalf("invalid public request binding: %+v err=%v", binding, err)
	}
	return binding
}

func textFileSwarmBinary2456(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "swarm-2456")
	command := exec.Command("go", "build", "-o", binary, "./cmd/swarm")
	command.Dir = conformanceRepoRoot(t)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fresh-process operator CLI: %v\n%s", err, output)
	}
	return binary
}

type textFileProcessResult2456 struct {
	stdout string
	stderr string
	err    error
}

func textFileSwarmProcess2456(ctx context.Context, binary, repo string, args ...string) textFileProcessResult2456 {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = repo
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "SWARM_TEST_") {
			command.Env = append(command.Env, variable)
		}
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return textFileProcessResult2456{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

func assertTextFileProcessRun2456(t *testing.T, result textFileProcessResult2456, runID string) {
	t.Helper()
	if result.err != nil || !strings.Contains(result.stdout, "run_id="+runID) {
		t.Fatalf("fresh CLI run %s: err=%v stdout=%s stderr=%s", runID, result.err, result.stdout, result.stderr)
	}
}

type textFileBindingSnapshot2456 struct {
	runs, operations, versions, pins, feeds, events int
	head                                            string
}

func textFileSnapshot2456(t *testing.T, f *deploymentResourceFixture, runID string) textFileBindingSnapshot2456 {
	t.Helper()
	var snapshot textFileBindingSnapshot2456
	for _, check := range []struct {
		query string
		value *int
	}{
		{`SELECT COUNT(*) FROM runs WHERE run_id=$1`, &snapshot.runs},
		{`SELECT COUNT(*) FROM resource_run_creation_operations WHERE run_id=$1`, &snapshot.operations},
		{`SELECT COUNT(*) FROM resource_versions WHERE flow_path='.' AND event_name='root.ready'`, &snapshot.versions},
		{`SELECT COUNT(*) FROM resource_version_pins WHERE run_id=$1`, &snapshot.pins},
		{`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, &snapshot.feeds},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1`, &snapshot.events},
	} {
		args := []any{}
		if strings.Contains(check.query, "$1") {
			args = append(args, runID)
		}
		if err := f.db.QueryRowContext(f.ctx, check.query, args...).Scan(check.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_heads WHERE flow_path='.' AND event_name='root.ready'`).Scan(&snapshot.head); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// The proxy forwards every request to the real operator. Capturing the first
// run.start bytes lets a test contrast immutable wire replay with a new CLI
// process that must reconstruct the same request from durable metadata.
func textFileWireCaptureProxy2456(t *testing.T, target *httptest.Server) (*httptest.Server, func() []byte) {
	t.Helper()
	var mu sync.Mutex
	var first []byte
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request struct {
			Method string `json:"method"`
		}
		dropCommittedResponse := false
		if json.Unmarshal(wire, &request) == nil && request.Method == "run.start" {
			mu.Lock()
			if first == nil {
				first = bytes.Clone(wire)
				dropCommittedResponse = true
			}
			mu.Unlock()
		}
		if dropCommittedResponse {
			forward, err := http.NewRequestWithContext(r.Context(), r.Method, target.URL+r.URL.RequestURI(), bytes.NewReader(wire))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			forward.Header = r.Header.Clone()
			response, err := http.DefaultClient.Do(forward)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			_, copyErr := io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if copyErr != nil {
				http.Error(w, copyErr.Error(), http.StatusBadGateway)
				return
			}
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("drop committed response: %v", err)
				return
			}
			connection.Close()
			return
		}
		textFileForwardRPC2456(w, r, target, wire)
	}))
	t.Cleanup(proxy.Close)
	return proxy, func() []byte {
		mu.Lock()
		defer mu.Unlock()
		return bytes.Clone(first)
	}
}

func textFileForwardRPC2456(w http.ResponseWriter, inbound *http.Request, target *httptest.Server, wire []byte) {
	forward, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, target.URL+inbound.URL.RequestURI(), bytes.NewReader(wire))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	forward.Header = inbound.Header.Clone()
	response, err := http.DefaultClient.Do(forward)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}

func textFileTwoBundleOperator2456(t *testing.T, f *deploymentResourceFixture, oldSource semanticview.Source, oldRuntime notifyAllChildrenRuntime) *httptest.Server {
	t.Helper()
	oldBundle, ok := semanticview.Bundle(oldSource)
	if !ok {
		t.Fatal("original runtime has no admitted bundle")
	}
	newBundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("new default runtime has no admitted bundle")
	}
	oldIdentity, err := contracts.BootBundleIdentity(oldBundle)
	if err != nil {
		t.Fatal(err)
	}
	newIdentity, err := contracts.BootBundleIdentity(newBundle)
	if err != nil {
		t.Fatal(err)
	}
	contextFor := func(source semanticview.Source, runtime notifyAllChildrenRuntime, identity contracts.BundleIdentity) swruntime.BundleContext {
		bundle, ok := semanticview.Bundle(source)
		if !ok || bundle.PackInventory == nil {
			t.Fatal("runtime context requires compiled bundle pack inventory")
		}
		projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		bundle.PackAdmission = projection
		subjects, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
		if err != nil {
			t.Fatal(err)
		}
		selected := &swruntime.Runtime{Bus: runtime.bus, ExecutionPosture: executionposture.Live}
		selected.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
		selected.Options.SourceArtifactFact = runtime.sourceArtifactFact
		return swruntime.BundleContext{
			SourceArtifactFact: runtime.sourceArtifactFact, BundleIdentity: identity,
			Source: source, Runtime: selected, WorkOwner: runtime.workOwner,
			PackInventoryDigest:       bundle.PackInventory.Digest(),
			ProviderTriggerGeneration: projection.ProviderTriggers.Generation(),
			InstalledTriggerSubjects:  subjects,
		}
	}
	availability, ok := f.selected.(swruntime.RunBundleAvailabilityReader)
	if !ok {
		t.Fatalf("selected store %T lacks run bundle availability", f.selected)
	}
	contexts, err := swruntime.NewRuntimeContextManager(availability,
		contextFor(f.source, f.runtime, newIdentity),
		contextFor(oldSource, oldRuntime, oldIdentity),
	)
	if err != nil {
		t.Fatalf("admit both real bundle runtimes: %v", err)
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
		t.Fatalf("selected store %T lacks durable data", f.selected)
	}
	observability, ok := f.selected.(apiv1.ObservabilityReadStore)
	if !ok {
		t.Fatalf("selected store %T lacks observability", f.selected)
	}
	publication := apiv1.EventPublicationOptions{
		Idempotency: idempotency, Events: f.runtime.bus, Acknowledged: f.runtime.bus,
		SourceArtifact: f.runtime.bus, RunBundleContext: contextOwner,
		RuntimeContexts: contexts, Source: f.source, Bundle: newIdentity,
	}
	methods := apiv1.MergeOperatorHandlers(
		apiv1.OperatorRunStartHandlers(apiv1.RunStartHandlerOptions{Publication: publication}),
		apiv1.OperatorDataHandlers(apiv1.DataHandlerOptions{Store: dataOwner}),
		apiv1.OperatorObservabilityHandlers(apiv1.ObservabilityHandlerOptions{Observability: observability}),
		map[string]apiv1.MethodHandler{"health.check": func(context.Context, apiv1.Request) (any, error) {
			return map[string]any{"alive": true, "ready": true, "db_ok": true, "runtime_ok": true, "bundle": newIdentity}, nil
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

func TestFileRow2456FreshProcessBindingAndExactWireBothStores(t *testing.T) {
	binary := textFileSwarmBinary2456(t)
	repo := conformanceRepoRoot(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := textFileDeploymentFixture2456(t, backend, false)
			server := f.operatorServer(t)
			proxy, capturedWire := textFileWireCaptureProxy2456(t, server)
			path := filepath.Join(t.TempDir(), "original.md")
			body := "Original binding survives a moved head.\n"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			rows := []map[string]any{{"body": body}}
			original := textFileVersion2456(t, f, rows)
			runID := uuid.NewString()
			args := []string{"run", "start", "--connect", proxy.URL, "--run-id", runID, "--data", "root.ready.body=" + path, "--no-follow"}
			lost := textFileSwarmProcess2456(f.ctx, binary, repo, args...)
			if lost.err == nil {
				t.Fatalf("first CLI received a response that the proxy was required to drop: stdout=%s stderr=%s", lost.stdout, lost.stderr)
			}
			wire := capturedWire()
			if len(wire) == 0 {
				t.Fatal("real CLI run.start wire was not captured")
			}
			assertTextFileRun2456(t, f, server, runID, original, rows)

			changedPath := filepath.Join(t.TempDir(), "new-head.md")
			changedBody := "A different deployment moves the head.\n"
			if err := os.WriteFile(changedPath, []byte(changedBody), 0o600); err != nil {
				t.Fatal(err)
			}
			changedRows := []map[string]any{{"body": changedBody}}
			changed := textFileVersion2456(t, f, changedRows)
			changedRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+changedPath)
			assertTextFileRun2456(t, f, server, changedRun, changed, changedRows)
			if original.VersionID == changed.VersionID {
				t.Fatal("second import did not move the head")
			}
			oldRuntime := f.runtime
			join := beginServingLifetimeJoin(oldRuntime, nil)
			assertServingJoinComplete(t, join, oldRuntime, nil)
			f.boot(t)
			server = f.operatorServer(t)
			before := textFileSnapshot2456(t, f, runID)
			if before.head != string(changed.VersionID) || before.runs != 1 || before.operations != 1 || before.feeds != 1 {
				t.Fatalf("head movement/restart lost original operation: %+v", before)
			}

			exact := textFileRPCWire2456(t, server, wire)
			if len(exact.Error) != 0 || !bytes.Contains(exact.Result, []byte(runID)) {
				t.Fatalf("exact immutable wire did not replay: result=%s error=%s", exact.Result, exact.Error)
			}
			args[3] = server.URL
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo, args...), runID)
			if after := textFileSnapshot2456(t, f, runID); after != before {
				t.Fatalf("fresh-process replay mutated durable state: before=%+v after=%+v", before, after)
			}
			assertTextFileRun2456(t, f, server, runID, original, rows)
			binding := textFileRequestBinding2456(t, server, runID)
			if binding.BundleHash != f.runtime.sourceArtifactFact.BundleHash() || len(binding.Imports) != 1 || binding.Imports[0].ExpectedHead.State != "absent" {
				t.Fatalf("replay did not retain original import CAS: %+v", binding)
			}

			if err := os.WriteFile(path, []byte("The caller changed the original file.\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			conflict := textFileSwarmProcess2456(f.ctx, binary, repo, args...)
			if conflict.err == nil {
				t.Fatalf("changed-file fresh CLI replay succeeded: stdout=%s stderr=%s", conflict.stdout, conflict.stderr)
			}
			if after := textFileSnapshot2456(t, f, runID); after != before {
				t.Fatalf("changed-file conflict mutated durable state: before=%+v after=%+v stderr=%s", before, after, conflict.stderr)
			}
			exactAgain := textFileRPCWire2456(t, server, wire)
			if len(exactAgain.Error) != 0 || !bytes.Equal(exactAgain.Result, exact.Result) {
				t.Fatalf("changed host file altered immutable exact-wire replay: before=%+v after=%+v", exact, exactAgain)
			}
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			newRunID := uuid.NewString()
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo,
				"run", "start", "--connect", server.URL,
				"--run-id", newRunID, "--data", "root.ready.body="+path, "--no-follow"), newRunID)
			assertTextFileRun2456(t, f, server, newRunID, original, rows)
			if newRunID == runID || textFileRequestBinding2456(t, server, newRunID).RequestHash == binding.RequestHash {
				t.Fatal("new run ID reused the old permanent operation")
			}
			var versionCount int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM resource_versions WHERE version_id=$1`, string(original.VersionID)).Scan(&versionCount); err != nil {
				t.Fatal(err)
			}
			if versionCount != 1 {
				t.Fatalf("equal content reminted the same semantic version %d times", versionCount)
			}
		})
	}
}

func TestFileRow2456DefaultBundleAndHeadPinReplayBothStores(t *testing.T) {
	binary := textFileSwarmBinary2456(t)
	repo := conformanceRepoRoot(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, root := textFileDeploymentFixture2456(t, backend, false)
			server := f.operatorServer(t)
			originalBundle := f.runtime.sourceArtifactFact.BundleHash()
			path := filepath.Join(t.TempDir(), "original.md")
			body := "Pinned before the default bundle changes.\n"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			rows := []map[string]any{{"body": body}}
			version := textFileVersion2456(t, f, rows)
			importRunID := uuid.NewString()
			importArgs := []string{"run", "start", "--connect", server.URL, "--run-id", importRunID, "--data", "root.ready.body=" + path, "--no-follow"}
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo, importArgs...), importRunID)
			assertTextFileRun2456(t, f, server, importRunID, version, rows)
			pinRunID := uuid.NewString()
			pinArgs := []string{"run", "start", "--connect", server.URL, "--run-id", pinRunID, "--pin", "root.ready@head", "--no-follow"}
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo, pinArgs...), pinRunID)
			assertTextFileRun2456(t, f, server, pinRunID, version, rows)
			pinBinding := textFileRequestBinding2456(t, server, pinRunID)
			if pinBinding.BundleHash != originalBundle || len(pinBinding.Pins) != 1 || pinBinding.Pins[0].VersionID != version.VersionID {
				t.Fatalf("mutable @head was not permanently resolved: %+v", pinBinding)
			}

			changedPath := filepath.Join(t.TempDir(), "new-head.md")
			changedBody := "New head, same event declaration.\n"
			if err := os.WriteFile(changedPath, []byte(changedBody), 0o600); err != nil {
				t.Fatal(err)
			}
			changedRows := []map[string]any{{"body": changedBody}}
			changed := textFileVersion2456(t, f, changedRows)
			changedRun := startTextFileRun2456(t, f, server, uuid.NewString(), "--data", "root.ready.body="+changedPath)
			assertTextFileRun2456(t, f, server, changedRun, changed, changedRows)
			if changed.VersionID == version.VersionID {
				t.Fatal("new import did not move @head")
			}
			beforeExplicitPin := textFileSnapshot2456(t, f, pinRunID)
			conflictingPinArgs := append([]string(nil), pinArgs...)
			for index := range conflictingPinArgs {
				if conflictingPinArgs[index] == "root.ready@head" {
					conflictingPinArgs[index] = "root.ready@" + string(changed.VersionID)
				}
			}
			if conflict := textFileSwarmProcess2456(f.ctx, binary, repo, conflictingPinArgs...); conflict.err == nil {
				t.Fatalf("explicit conflicting pin replay succeeded: stdout=%s stderr=%s", conflict.stdout, conflict.stderr)
			}
			if after := textFileSnapshot2456(t, f, pinRunID); after != beforeExplicitPin {
				t.Fatalf("explicit conflicting pin mutated durable state: before=%+v after=%+v", beforeExplicitPin, after)
			}
			oldRuntime, oldSource := f.runtime, f.source
			manifestPath := filepath.Join(root, "manifest.yaml")
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			const oldVersion = `version: "1.0.0"`
			if strings.Count(string(manifest), oldVersion) != 1 {
				t.Fatalf("authored bundle manifest changed: %s", manifest)
			}
			if err := os.WriteFile(manifestPath, []byte(strings.Replace(string(manifest), oldVersion, `version: "1.0.1"`, 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("load moved default bundle: %v", err)
			}
			f.source = semanticview.Wrap(bundle)
			fact := conformanceSourceArtifactFact(t, f.source)
			if fact.BundleHash() == originalBundle {
				t.Fatal("second admitted bundle did not move the default")
			}
			f.ctx = testAuthorActivityContextForBundle(context.Background(), fact)
			f.topology.completeSources = []semanticview.Source{oldSource, f.source}
			f.boot(t)
			server = textFileTwoBundleOperator2456(t, f, oldSource, oldRuntime)
			beforeImport := textFileSnapshot2456(t, f, importRunID)
			beforePin := textFileSnapshot2456(t, f, pinRunID)
			if beforeImport.head != string(changed.VersionID) || beforePin.head != string(changed.VersionID) {
				t.Fatalf("default switch unexpectedly moved resource head: import=%+v pin=%+v", beforeImport, beforePin)
			}
			importArgs[3], pinArgs[3] = server.URL, server.URL
			importBinding := textFileRequestBinding2456(t, server, importRunID)
			input, err := json.Marshal(rows[0])
			if err != nil {
				t.Fatal(err)
			}
			originalImport := importBinding.Imports[0]
			conflict := textFileRPCParams2456(t, server, "run.start", map[string]any{
				"run_id": importRunID, "bundle_hash": fact.BundleHash(),
				"data": map[string]any{"imports": []any{map[string]any{
					"source_invocation_id": originalImport.SourceInvocationID,
					"declaration":          originalImport.Declaration, "expected_head": originalImport.ExpectedHead,
					"input": map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString(append(input, '\n'))},
				}}, "pins": []any{}},
			})
			var mismatch struct {
				Data struct {
					Code    string `json:"code"`
					Details struct {
						RequestedHash string `json:"requested_hash"`
						RunBundleHash string `json:"run_bundle_hash"`
					} `json:"details"`
				} `json:"data"`
			}
			if err := json.Unmarshal(conflict.Error, &mismatch); err != nil || mismatch.Data.Code != "BUNDLE_MISMATCH" || mismatch.Data.Details.RequestedHash != fact.BundleHash() || mismatch.Data.Details.RunBundleHash != originalBundle {
				t.Fatalf("explicit conflicting bundle replay did not reach exact-source admission: result=%s error=%s", conflict.Result, conflict.Error)
			}
			if after := textFileSnapshot2456(t, f, importRunID); after != beforeImport {
				t.Fatalf("explicit conflicting bundle mutated durable state: before=%+v after=%+v", beforeImport, after)
			}
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo, importArgs...), importRunID)
			assertTextFileProcessRun2456(t, textFileSwarmProcess2456(f.ctx, binary, repo, pinArgs...), pinRunID)
			if after := textFileSnapshot2456(t, f, importRunID); after != beforeImport {
				t.Fatalf("moved-default import replay mutated durable state: before=%+v after=%+v", beforeImport, after)
			}
			if after := textFileSnapshot2456(t, f, pinRunID); after != beforePin {
				t.Fatalf("moved-default @head replay mutated durable state: before=%+v after=%+v", beforePin, after)
			}
			for _, runID := range []string{importRunID, pinRunID} {
				binding := textFileRequestBinding2456(t, server, runID)
				if binding.BundleHash != originalBundle {
					t.Fatalf("run %s rebound to current default bundle: %+v", runID, binding)
				}
				assertTextFileRun2456(t, f, server, runID, version, rows)
			}
		})
	}
}

func TestDataTextFile2456RejectedMultiPinServedReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeploymentResourceFixtureWithSource(t, backend, twoDeploymentFeedSource(t))
			server := f.operatorServer(t)
			bundleHash := f.runtime.sourceArtifactFact.BundleHash()
			seed := []struct {
				name, row string
			}{
				{"portfolio/account.registered", `{"account_id":"registered"}` + "\n"},
				{"portfolio/account.notify.requested", `{"account_id":"notified","command":"ping"}` + "\n"},
			}
			versions := make(map[string]string, len(seed))
			for _, item := range seed {
				path := filepath.Join(t.TempDir(), "rows.jsonl")
				if err := os.WriteFile(path, []byte(item.row), 0o600); err != nil {
					t.Fatal(err)
				}
				seedRun := startDeploymentResourceRun(t, f, server, "--data", item.name+"="+path)
				waitNotifyAllChildrenRuntimeWithin(t, f.runtime, seedRun, 30*time.Second)
				var version string
				if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='portfolio' AND event_name=$2`, seedRun, item.name).Scan(&version); err != nil {
					t.Fatal(err)
				}
				versions[item.name] = version
			}
			const missingVersion = "resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			runID := uuid.NewString()
			params := map[string]any{
				"run_id": runID, "bundle_hash": bundleHash, "idempotency_key": uuid.NewString(),
				"data": map[string]any{
					"imports": []any{}, "pins": []any{
						map[string]any{"declaration": map[string]any{"flow_path": "portfolio", "event": "portfolio/account.notify.requested"}, "version_id": missingVersion},
						map[string]any{"declaration": map[string]any{"flow_path": "portfolio", "event": "portfolio/account.registered"}, "version_id": versions["portfolio/account.registered"]},
					},
				},
			}
			wire, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": uuid.NewString(), "method": "run.start", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			first := textFileRPCWire2456(t, server, wire)
			if len(first.Error) == 0 {
				t.Fatalf("missing first pin admitted: %s", first.Result)
			}
			binding := textFileRequestBinding2456(t, server, runID)
			if binding.BundleHash != bundleHash || len(binding.Pins) != 2 || len(binding.Imports) != 0 {
				t.Fatalf("rejected request lost its complete two-pin binding: %+v", binding)
			}
			boundPins := make(map[string]durabledata.VersionID, 2)
			for _, pin := range binding.Pins {
				boundPins[pin.Declaration.EventName] = pin.VersionID
			}
			if boundPins["portfolio/account.notify.requested"] != missingVersion || boundPins["portfolio/account.registered"] != durabledata.VersionID(versions["portfolio/account.registered"]) {
				t.Fatalf("rejected two-pin request was partially reconstructed: %+v", binding.Pins)
			}
			count := func(table string) int {
				t.Helper()
				var got int
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&got); err != nil {
					t.Fatal(err)
				}
				return got
			}
			if count("resource_run_creation_operations") != 1 || count("runs") != 0 || count("resource_version_pins") != 0 || count("fan_out_intents") != 0 || count("events") != 0 {
				t.Fatal("rejected multi-pin operation created a run, pin, feed, or event")
			}
			changedPath := filepath.Join(t.TempDir(), "changed.jsonl")
			if err := os.WriteFile(changedPath, []byte(`{"account_id":"registered-again"}`+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			changedRun := startDeploymentResourceRun(t, f, server, "--data", "portfolio/account.registered="+changedPath)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, changedRun, 30*time.Second)
			var newHead string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_heads WHERE flow_path='portfolio' AND event_name='portfolio/account.registered'`).Scan(&newHead); err != nil {
				t.Fatal(err)
			}
			if newHead == versions["portfolio/account.registered"] {
				t.Fatal("multi-pin replay proof did not move a head")
			}
			oldRuntime := f.runtime
			join := beginServingLifetimeJoin(oldRuntime, nil)
			assertServingJoinComplete(t, join, oldRuntime, nil)
			f.boot(t)
			server = f.operatorServer(t)
			params["idempotency_key"] = uuid.NewString()
			var replayWire map[string]any
			if err := json.Unmarshal(wire, &replayWire); err != nil {
				t.Fatal(err)
			}
			replayWire["params"] = params
			independentWire, err := json.Marshal(replayWire)
			if err != nil {
				t.Fatal(err)
			}
			replay := textFileRPCWire2456(t, server, independentWire)
			if len(replay.Error) == 0 || !bytes.Equal(replay.Error, first.Error) || count("resource_run_creation_operations") != 1 || count("runs") != 0 || count("resource_version_pins") != 0 || count("fan_out_intents") != 0 || count("events") != 0 {
				t.Fatalf("rejected multi-pin replay changed permanent outcome: first=%s replay=%s", first.Error, replay.Error)
			}
			if after := textFileRequestBinding2456(t, server, runID); !reflect.DeepEqual(after, binding) {
				t.Fatalf("rejected two-pin binding changed after replay: before=%+v after=%+v", binding, after)
			}
		})
	}
}

// Both first request-binding reads reach the selected store before either
// client can begin run.start. The proxy delays only the real absence responses.
func textFileAbsentBindingBarrier2456(t *testing.T, target *httptest.Server, runID string) (*httptest.Server, func() (int, []string)) {
	t.Helper()
	var mu sync.Mutex
	arrived := 0
	var defects []string
	release := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, inbound *http.Request) {
		wire, err := io.ReadAll(inbound.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request struct {
			Method string `json:"method"`
			Params struct {
				Detail       string `json:"detail"`
				OperationRef struct {
					RunID string `json:"run_id"`
				} `json:"operation_ref"`
			} `json:"params"`
		}
		_ = json.Unmarshal(wire, &request)
		forward, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, target.URL+inbound.URL.RequestURI(), bytes.NewReader(wire))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward.Header = inbound.Header.Clone()
		response, err := http.DefaultClient.Do(forward)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		responseWire, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if request.Method == "data.show" && request.Params.Detail == "request_binding" && request.Params.OperationRef.RunID == runID {
			mu.Lock()
			if arrived < 2 {
				arrived++
				if !bytes.Contains(responseWire, []byte(string(durabledata.CodeOperationMissing))) {
					defects = append(defects, fmt.Sprintf("first binding read %d was not typed absence: %s", arrived, responseWire))
				}
				if arrived == 2 {
					close(release)
				}
			}
			mu.Unlock()
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				mu.Lock()
				defects = append(defects, "two first-attempt reads did not reach the operator before timeout")
				mu.Unlock()
			}
		}
		for name, values := range response.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(responseWire)
	}))
	t.Cleanup(proxy.Close)
	return proxy, func() (int, []string) {
		mu.Lock()
		defer mu.Unlock()
		return arrived, append([]string(nil), defects...)
	}
}

func TestDataTextFile2456ConcurrentFirstAttemptBothStores(t *testing.T) {
	binary := textFileSwarmBinary2456(t)
	repo := conformanceRepoRoot(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := textFileDeploymentFixture2456(t, backend, false)
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "simultaneous.md")
			body := "Both processes must bind the same immutable import.\n"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			rows := []map[string]any{{"body": body}}
			version := textFileVersion2456(t, f, rows)
			runID := uuid.NewString()
			proxy, barrier := textFileAbsentBindingBarrier2456(t, server, runID)
			args := []string{"run", "start", "--connect", proxy.URL, "--run-id", runID, "--data", "root.ready.body=" + path, "--no-follow"}
			results := make(chan textFileProcessResult2456, 2)
			start := make(chan struct{})
			for i := 0; i < 2; i++ {
				go func() {
					<-start
					results <- textFileSwarmProcess2456(f.ctx, binary, repo, args...)
				}()
			}
			close(start)
			for i := 0; i < 2; i++ {
				assertTextFileProcessRun2456(t, <-results, runID)
			}
			if arrived, defects := barrier(); arrived != 2 || len(defects) != 0 {
				t.Fatalf("first-attempt barrier did not establish two genuine absent reads: arrived=%d defects=%v", arrived, defects)
			}
			assertTextFileRun2456(t, f, server, runID, version, rows)
			snapshot := textFileSnapshot2456(t, f, runID)
			if snapshot.runs != 1 || snapshot.operations != 1 || snapshot.versions != 1 || snapshot.pins != 1 || snapshot.feeds != 1 || snapshot.events != 1 || snapshot.head != string(version.VersionID) {
				t.Fatalf("concurrent first attempt duplicated or lost durable state: %+v", snapshot)
			}
		})
	}
}

type textFileGrammarCounts2456 struct {
	sources, operations, runs, versions, pins, feeds int
}

func textFileGrammarState2456(t *testing.T, f *deploymentResourceFixture) textFileGrammarCounts2456 {
	t.Helper()
	var state textFileGrammarCounts2456
	for _, item := range []struct {
		query string
		out   *int
	}{
		{`SELECT COUNT(*) FROM resource_source_invocations`, &state.sources},
		{`SELECT COUNT(*) FROM resource_run_creation_operations`, &state.operations},
		{`SELECT COUNT(*) FROM runs`, &state.runs},
		{`SELECT COUNT(*) FROM resource_versions`, &state.versions},
		{`SELECT COUNT(*) FROM resource_version_pins`, &state.pins},
		{`SELECT COUNT(*) FROM fan_out_intents`, &state.feeds},
	} {
		if err := f.db.QueryRowContext(f.ctx, item.query).Scan(item.out); err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func rejectTextFileGrammar2456(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, want string, operands ...string) {
	t.Helper()
	before := textFileGrammarState2456(t, f)
	if before != (textFileGrammarCounts2456{}) {
		t.Fatalf("hostile grammar fixture is not fresh: %+v", before)
	}
	runID := uuid.NewString()
	args := []string{"run", "start", "--connect", server.URL, "--run-id", runID}
	args = append(args, operands...)
	args = append(args, "--no-follow")
	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil)
	if code == 0 || !strings.Contains(strings.ToLower(stderr.String()), strings.ToLower(want)) {
		t.Fatalf("hostile grammar %v: code=%d stdout=%s stderr=%s want=%s", args, code, stdout.String(), stderr.String(), want)
	}
	if after := textFileGrammarState2456(t, f); after != before {
		t.Fatalf("hostile grammar created source/run/version/pin/feed rows: before=%+v after=%+v", before, after)
	}
}

func TestDataTextFile2456HostileGrammarNoMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := textFileDeploymentFixture2456(t, backend, true)
			server := f.operatorServer(t)
			root := t.TempDir()
			jsonl := filepath.Join(root, "rows.jsonl")
			if err := os.WriteFile(jsonl, []byte(`{"account_id":"candidate","body":"hello"}`+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			bodyDir := filepath.Join(root, "bodies")
			if err := os.Mkdir(bodyDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bodyDir, "candidate.md"), []byte("hello\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			full := "root.ready=" + jsonl
			field := "root.ready.body=" + bodyDir
			for _, tc := range []struct {
				name     string
				want     string
				operands []string
			}{
				{"jsonl_then_field", "conflicting JSONL and field", []string{"--data", full, "--data", field}},
				{"field_then_jsonl", "conflicting JSONL and field", []string{"--data", field, "--data", full}},
				{"pin_then_field", "selected by both", []string{"--pin", "root.ready@head", "--data", field}},
				{"field_then_pin", "selected by both", []string{"--data", field, "--pin", "root.ready@head"}},
				{"duplicate_field", "repeats field", []string{"--data", field, "--data", field}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rejectTextFileGrammar2456(t, f, server, tc.want, tc.operands...)
				})
			}
		})
		t.Run(backend+"/dotted_collision", func(t *testing.T) {
			root := canonicalrouting.CopySelectedDeploymentResource(t, "root", false)
			if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("root.ready:\n  body: text\nroot.ready.body:\n  note: text\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			schemaPath := filepath.Join(root, "schema.yaml")
			schema, err := os.ReadFile(schemaPath)
			if err != nil {
				t.Fatal(err)
			}
			const oldPins = "    - root.ready\n"
			if strings.Count(string(schema), oldPins) != 1 {
				t.Fatalf("dotted fixture has changed output pin: %s", schema)
			}
			if err := os.WriteFile(schemaPath, []byte(strings.Replace(string(schema), oldPins, "    - root.ready\n    - root.ready.body\n", 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(conformanceRepoRoot(t), root, contracts.DefaultPlatformSpecFile(conformanceRepoRoot(t)))
			if err != nil {
				t.Fatalf("load dotted full-event/field collision bundle: %v", err)
			}
			f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "note.md")
			if err := os.WriteFile(path, []byte("ambiguous selector\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			rejectTextFileGrammar2456(t, f, server, "ambiguous", "--data", "root.ready.body="+path)
		})
	}
}
