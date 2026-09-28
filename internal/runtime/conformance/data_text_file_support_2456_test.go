package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func standaloneTextDataFixture2456(t *testing.T, backend string, keyed bool) *deploymentResourceFixture {
	t.Helper()
	root := canonicalrouting.CopySelectedDeploymentResource(t, "root", keyed)
	declaration := "root.ready:\n  body: text\n"
	if keyed {
		declaration = "root.ready:\n  key: account_id\n  account_id: text\n  body: text\n"
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte(declaration), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(conformanceRepoRoot(t), root, contracts.DefaultPlatformSpecFile(conformanceRepoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	return newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
}

func standaloneTextDataConnect2456(t *testing.T, serverURL string) {
	t.Helper()
	encoded, err := json.Marshal(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(configPath, []byte("connection:\n  api_server: "+string(encoded)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SWARM_CONFIG", configPath)
}

func standaloneTextDataCommand2456(t *testing.T, f *deploymentResourceFixture, sourceID, expectedHead string, check bool, selector, assignment string) (durabledata.SourceOperationResult, int, string) {
	t.Helper()
	args := []string{"data", "import", selector, assignment,
		"--bundle-hash", f.runtime.sourceArtifactFact.BundleHash(),
		"--source-invocation-id", sourceID, "--expected-head", expectedHead, "--json"}
	if check {
		args = append(args, "--check")
	}
	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil)
	if code != 0 {
		return durabledata.SourceOperationResult{}, code, stderr.String()
	}
	var result durabledata.SourceOperationResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode standalone command %v output %q: %v", args, stdout.String(), err)
	}
	return result, code, stderr.String()
}

func standaloneTextDataCounts2456(t *testing.T, f *deploymentResourceFixture) (versions, history, receipts, revision int, head string) {
	t.Helper()
	for _, item := range []struct {
		query string
		out   *int
	}{
		{`SELECT COUNT(*) FROM resource_versions WHERE flow_path='.' AND event_name='root.ready'`, &versions},
		{`SELECT COUNT(*) FROM resource_head_history WHERE flow_path='.' AND event_name='root.ready'`, &history},
		{`SELECT COUNT(*) FROM resource_source_invocations`, &receipts},
	} {
		if err := f.db.QueryRowContext(f.ctx, item.query).Scan(item.out); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT COALESCE(version_id, ''), revision FROM resource_heads WHERE flow_path='.' AND event_name='root.ready'`).Scan(&head, &revision); err != nil {
		t.Fatal(err)
	}
	return
}

func standaloneTextDataReceiptCount2456(t *testing.T, f *deploymentResourceFixture, sourceID string) int {
	t.Helper()
	var count int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM resource_source_invocations WHERE source_invocation_id=$1`, sourceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func standaloneTextDataShowShapeCode2456(t *testing.T, ctx context.Context, serverURL, bundleHash string, ref durabledata.DeclarationRef, schemaDigest durabledata.SchemaDigest) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "data.show",
		"params": map[string]any{"view": "import_shape", "bundle_hash": bundleHash,
			"declaration": ref, "schema_digest": schemaDigest},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/v1/rpc", bytes.NewReader(body))
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
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		return envelope.Error.Data.Code
	}
	if len(envelope.Result) == 0 {
		t.Fatal("data.show import_shape returned neither result nor error")
	}
	return ""
}

func TestStandaloneTextFileImportSupport2456BothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := standaloneTextDataFixture2456(t, backend, false)
			server := f.operatorServer(t)
			standaloneTextDataConnect2456(t, server.URL)
			bundleHash := f.runtime.sourceArtifactFact.BundleHash()
			ref, err := durabledata.ParseDeclarationRef(".", "root.ready")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "body.md")
			if err := os.WriteFile(path, []byte("Standalone \"text\" and \\backslash\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			assignment := "body=" + path
			versions, history, receipts, revision, head := standaloneTextDataCounts2456(t, f)
			if versions != 0 || history != 0 || receipts != 0 || revision != 0 || head != "" {
				t.Fatalf("unexpected initial data state: versions=%d history=%d receipts=%d revision=%d head=%q", versions, history, receipts, revision, head)
			}
			checkID := uuid.NewString()
			check, code, stderr := standaloneTextDataCommand2456(t, f, checkID, "absent", true, "./root.ready", assignment)
			if code != 0 || check.Operation != "check" || check.Outcome != "accepted" || check.Candidate.State != "candidate" {
				t.Fatalf("standalone --check: code=%d result=%#v stderr=%s", code, check, stderr)
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 0 || history != 0 || receipts != 1 || revision != 0 || head != "" {
				t.Fatalf("--check mutated data state: versions=%d history=%d receipts=%d revision=%d head=%q", versions, history, receipts, revision, head)
			}
			checkReplay, code, stderr := standaloneTextDataCommand2456(t, f, checkID, "absent", true, "./root.ready", assignment)
			if code != 0 || !reflect.DeepEqual(checkReplay, check) {
				t.Fatalf("exact --check replay: code=%d result=%#v stderr=%s", code, checkReplay, stderr)
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 0 || history != 0 || receipts != 1 || revision != 0 || head != "" {
				t.Fatal("--check replay mutated data state")
			}
			importID := uuid.NewString()
			imported, code, stderr := standaloneTextDataCommand2456(t, f, importID, "absent", false, "./root.ready", assignment)
			if code != 0 || imported.Operation != "import" || imported.Outcome != "accepted" || imported.Candidate.State != "version" {
				t.Fatalf("standalone import: code=%d result=%#v stderr=%s", code, imported, stderr)
			}
			if imported.Candidate.VersionID != check.Candidate.VersionID {
				t.Fatalf("check/import version mismatch: check=%s import=%s", check.Candidate.VersionID, imported.Candidate.VersionID)
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 1 || history != 1 || receipts != 2 || revision != 1 || head != string(imported.Candidate.VersionID) {
				t.Fatalf("import state: versions=%d history=%d receipts=%d revision=%d head=%q", versions, history, receipts, revision, head)
			}
			importReplay, code, stderr := standaloneTextDataCommand2456(t, f, importID, "absent", false, "./root.ready", assignment)
			if code != 0 || !reflect.DeepEqual(importReplay, imported) {
				t.Fatalf("exact import replay: code=%d result=%#v stderr=%s", code, importReplay, stderr)
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 1 || history != 1 || receipts != 2 || revision != 1 || head != string(imported.Candidate.VersionID) {
				t.Fatal("import replay mutated durable state")
			}

			wrongID := uuid.NewString()
			wrongArgs := []string{"data", "import", "./root.ready", assignment,
				"--bundle-hash", "bundle-v2:sha256:" + strings.Repeat("f", 64),
				"--source-invocation-id", wrongID, "--expected-head", "absent"}
			var stdout, errorOut bytes.Buffer
			if code := cliapp.Execute(f.ctx, wrongArgs, &stdout, &errorOut, nil, nil); code == 0 {
				t.Fatalf("wrong-bundle import succeeded: %s", stdout.String())
			}
			if standaloneTextDataReceiptCount2456(t, f, wrongID) != 0 {
				t.Fatal("wrong-bundle request created a source receipt")
			}
			shapeID := uuid.NewString()
			_, code, stderr = standaloneTextDataCommand2456(t, f, shapeID, string(imported.Candidate.VersionID), false, "./root.ready", "unknown="+path)
			if code == 0 || !strings.Contains(stderr, "unknown") {
				t.Fatalf("wrong-field shape accepted: code=%d stderr=%s", code, stderr)
			}
			if standaloneTextDataReceiptCount2456(t, f, shapeID) != 0 {
				t.Fatal("wrong-field request created a source receipt")
			}

			shapeOwner, ok := f.selected.(interface {
				GetDeclarationImportShape(context.Context, string, durabledata.DeclarationRef) (durabledata.ImportShape, error)
			})
			if !ok {
				t.Fatalf("selected store %T lacks import-shape readback", f.selected)
			}
			shape, err := shapeOwner.GetDeclarationImportShape(f.ctx, bundleHash, ref)
			if err != nil {
				t.Fatal(err)
			}
			if code := standaloneTextDataShowShapeCode2456(t, f.ctx, server.URL, bundleHash, ref, shape.SchemaDigest); code != "" {
				t.Fatalf("valid shape API readback error = %s", code)
			}
			wrongSchema := durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("f", 64))
			if code := standaloneTextDataShowShapeCode2456(t, f.ctx, server.URL, bundleHash, ref, wrongSchema); code != string(durabledata.CodeIntegrity) {
				t.Fatalf("wrong-schema API error = %s, want %s", code, durabledata.CodeIntegrity)
			}
			if _, err := f.db.ExecContext(f.ctx, `UPDATE resource_bundle_import_shapes SET shape_json=$1 WHERE bundle_hash=$2 AND flow_path='.' AND event_name='root.ready'`, []byte(`{"bad":true}`), bundleHash); err != nil {
				t.Fatalf("tamper selected import shape: %v", err)
			}
			var domain *durabledata.DomainError
			if _, err := shapeOwner.GetDeclarationImportShape(f.ctx, bundleHash, ref); !errors.As(err, &domain) || domain.Code != durabledata.CodeIntegrity {
				t.Fatalf("tampered selected shape store error = %v", err)
			}
			if code := standaloneTextDataShowShapeCode2456(t, f.ctx, server.URL, bundleHash, ref, shape.SchemaDigest); code != string(durabledata.CodeIntegrity) {
				t.Fatalf("tampered selected shape API error = %s", code)
			}
			corruptID := uuid.NewString()
			_, code, _ = standaloneTextDataCommand2456(t, f, corruptID, string(imported.Candidate.VersionID), false, "./root.ready", assignment)
			if code == 0 {
				t.Fatal("CLI accepted corrupt selected import shape")
			}
			if standaloneTextDataReceiptCount2456(t, f, corruptID) != 0 {
				t.Fatal("corrupt-shape request created a source receipt")
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 1 || history != 1 || receipts != 2 || revision != 1 || head != string(imported.Candidate.VersionID) {
				t.Fatal("refused requests mutated durable data state")
			}
		})
	}
}

func TestStandaloneTextFileEmptyKeyedRequiredDirectory2456BothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := standaloneTextDataFixture2456(t, backend, true)
			server := f.operatorServer(t)
			standaloneTextDataConnect2456(t, server.URL)
			empty := filepath.Join(t.TempDir(), "empty-bodies")
			if err := os.Mkdir(empty, 0o700); err != nil {
				t.Fatal(err)
			}
			assignment := "body=" + empty
			check, code, stderr := standaloneTextDataCommand2456(t, f, uuid.NewString(), "absent", true, "./root.ready", assignment)
			if code != 0 || check.Operation != "check" || check.Outcome != "accepted" ||
				check.Candidate.State != "candidate" || check.Candidate.Manifest == nil ||
				check.Candidate.Manifest.RowCount != 0 || check.Candidate.VersionID.Validate() != nil {
				t.Fatalf("empty keyed --check: code=%d result=%#v stderr=%s", code, check, stderr)
			}
			versions, history, receipts, revision, head := standaloneTextDataCounts2456(t, f)
			if versions != 0 || history != 0 || receipts != 1 || revision != 0 || head != "" {
				t.Fatalf("empty keyed --check mutated data: versions=%d history=%d receipts=%d revision=%d head=%q", versions, history, receipts, revision, head)
			}
			imported, code, stderr := standaloneTextDataCommand2456(t, f, uuid.NewString(), "absent", false, "./root.ready", assignment)
			if code != 0 || imported.Operation != "import" || imported.Outcome != "accepted" ||
				imported.Candidate.State != "version" || imported.Candidate.Manifest == nil ||
				imported.Candidate.Manifest.RowCount != 0 || imported.Candidate.VersionID != check.Candidate.VersionID {
				t.Fatalf("empty keyed import: code=%d result=%#v stderr=%s", code, imported, stderr)
			}
			var jsonl []byte
			if err := f.db.QueryRowContext(f.ctx, `SELECT canonical_jsonl FROM resource_versions WHERE version_id=$1`, string(imported.Candidate.VersionID)).Scan(&jsonl); err != nil || len(jsonl) != 0 {
				t.Fatalf("stored zero-row JSONL = %q, %v", jsonl, err)
			}
			versions, history, receipts, revision, head = standaloneTextDataCounts2456(t, f)
			if versions != 1 || history != 1 || receipts != 2 || revision != 1 || head != string(imported.Candidate.VersionID) {
				t.Fatalf("empty keyed import state: versions=%d history=%d receipts=%d revision=%d head=%q", versions, history, receipts, revision, head)
			}
		})
	}
}
