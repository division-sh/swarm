package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type multiBundleOptionAMatrix struct {
	Version         int                    `yaml:"version"`
	Kind            string                 `yaml:"kind"`
	Issue           int                    `yaml:"issue"`
	ParentIssue     int                    `yaml:"parent_issue"`
	Source          multiBundleSource      `yaml:"source"`
	Classifications []string               `yaml:"classifications"`
	Rows            []multiBundleMatrixRow `yaml:"rows"`
}

type multiBundleSource struct {
	PlatformSpec        string `yaml:"platform_spec"`
	OpenRPCArtifact     string `yaml:"openrpc_artifact"`
	APIComplianceMatrix string `yaml:"api_compliance_matrix"`
}

type multiBundleMatrixRow struct {
	ID             string                 `yaml:"id"`
	Category       string                 `yaml:"category"`
	Concept        string                 `yaml:"concept"`
	Classification string                 `yaml:"classification"`
	OwnerRefs      []multiBundleProofRef  `yaml:"owner_refs"`
	ProofRefs      []multiBundleProofRef  `yaml:"proof_refs"`
	Extra          map[string]interface{} `yaml:",inline"`
}

type multiBundleProofRef struct {
	Kind   string `yaml:"kind"`
	Path   string `yaml:"path,omitempty"`
	Ref    string `yaml:"ref,omitempty"`
	Method string `yaml:"method,omitempty"`
	Name   string `yaml:"name,omitempty"`
	Issue  int    `yaml:"issue,omitempty"`
	PR     int    `yaml:"pr,omitempty"`
}

func TestMultiBundleOptionAMatrixCoversApprovedRows(t *testing.T) {
	root := conformanceRepoRoot(t)
	matrix := loadMultiBundleOptionAMatrix(t, root)

	if matrix.Version != 1 {
		t.Fatalf("matrix version = %d, want 1", matrix.Version)
	}
	if matrix.Kind != "multi_bundle_option_a_lifecycle_scoped_api_conformance_matrix" {
		t.Fatalf("matrix kind = %q", matrix.Kind)
	}
	if matrix.Issue != 1025 || matrix.ParentIssue != 1011 {
		t.Fatalf("matrix issue/parent = #%d/#%d, want #1025/#1011", matrix.Issue, matrix.ParentIssue)
	}
	if matrix.Source.PlatformSpec != "platform-spec.yaml" || matrix.Source.OpenRPCArtifact != "openrpc.json" || matrix.Source.APIComplianceMatrix != "internal/apiv1/testdata/openrpc_compliance_matrix.yaml" {
		t.Fatalf("unexpected matrix source block: %#v", matrix.Source)
	}

	assertStringSetEquals(t, "classifications", matrix.Classifications, []string{
		"implemented_proof",
		"explicit_split",
		"not_current_option_a",
	})

	ids := make([]string, 0, len(matrix.Rows))
	for _, row := range matrix.Rows {
		ids = append(ids, row.ID)
	}
	assertStringSetEquals(t, "row ids", ids, requiredMultiBundleOptionARows())
}

func TestMultiBundleOptionAMatrixProofReferencesResolve(t *testing.T) {
	root := conformanceRepoRoot(t)
	matrix := loadMultiBundleOptionAMatrix(t, root)
	openRPCMethods := loadMatrixOpenRPCMethods(t, filepath.Join(root, matrix.Source.OpenRPCArtifact))
	apiMatrixMethods := loadMatrixAPIMethods(t, filepath.Join(root, matrix.Source.APIComplianceMatrix))
	goTests := conformanceGoTestNames(t, root)

	allowedClassifications := map[string]bool{}
	for _, classification := range matrix.Classifications {
		allowedClassifications[classification] = true
	}

	seenRows := map[string]bool{}
	for _, row := range matrix.Rows {
		if strings.TrimSpace(row.ID) == "" {
			t.Fatal("matrix row missing id")
		}
		if seenRows[row.ID] {
			t.Fatalf("duplicate matrix row id %q", row.ID)
		}
		seenRows[row.ID] = true
		if strings.TrimSpace(row.Category) == "" {
			t.Fatalf("%s missing category", row.ID)
		}
		if strings.TrimSpace(row.Concept) == "" {
			t.Fatalf("%s missing concept", row.ID)
		}
		if !allowedClassifications[row.Classification] {
			t.Fatalf("%s classification = %q, want one of %#v", row.ID, row.Classification, matrix.Classifications)
		}
		if len(row.OwnerRefs) == 0 {
			t.Fatalf("%s missing owner_refs", row.ID)
		}
		if len(row.ProofRefs) == 0 {
			t.Fatalf("%s missing proof_refs", row.ID)
		}
		for _, ref := range append(row.OwnerRefs, row.ProofRefs...) {
			validateMultiBundleRef(t, root, ref, openRPCMethods, apiMatrixMethods, goTests)
		}

		switch row.Classification {
		case "implemented_proof":
			if !hasExecutableMultiBundleProof(row.ProofRefs) {
				t.Fatalf("%s implemented row lacks executable proof ref: %#v", row.ID, row.ProofRefs)
			}
		case "explicit_split", "not_current_option_a":
			if !hasDispositionMultiBundleProof(row.ProofRefs) {
				t.Fatalf("%s disposition row lacks issue/pr/spec disposition ref: %#v", row.ID, row.ProofRefs)
			}
		}
	}
}

func loadMultiBundleOptionAMatrix(t *testing.T, root string) multiBundleOptionAMatrix {
	t.Helper()
	path := filepath.Join(root, "internal", "runtime", "conformance", "testdata", "multi_bundle_option_a_matrix.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read matrix: %v", err)
	}
	var matrix multiBundleOptionAMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse matrix yaml: %v", err)
	}
	return matrix
}

func requiredMultiBundleOptionARows() []string {
	return []string{
		"source_authority_openrpc",
		"canonical_bundle_hash_identity",
		"schema_foundation",
		"run_source_availability",
		"serve_contracts_persisted_ingest",
		"serve_dev_ephemeral_source",
		"db_loaded_serve_boot",
		"dynamic_post_boot_load",
		"bundle_catalog_api",
		"bundle_delete_non_force",
		"bundle_delete_force",
		"phase5_delete_source_owner",
		"runtime_nuke_include_bundles",
		"startup_recovery",
		"sqlite_first_event_run_lock",
		"scoped_read_filters",
		"create_new_work_bundle_admission",
		"runtime_context_boot_pinned",
		"runtime_context_unload_deactivation",
		"fork_same_bundle_disk",
		"fork_same_bundle_db_loaded",
		"fork_cross_bundle_loaded_target",
		"cli_bundle_fork_consumers",
		"cli_bundle_register_archive",
		"cli_agents_list_run_id",
		"ambient_cli_bundle_context",
		"option_b_artifact_capture",
		"duplicate_agent_slug_posture",
		"provider_native_llm_exclusion",
		"static_root_pin_verifier",
		"sqlite_exact_once_handler_effects",
		"preservation_cleanup_watchlist",
		"empire_template_delivery",
		"empire_parent_route_metadata",
		"empire_auto_emit_run_id",
		"empire_post_commit_delivery",
		"empire_terminal_route_teardown",
		"empire_route_wildcards",
		"empire_accumulator_bucket_identity",
		"empire_output_event_namespace",
		"empire_event_publish_flow_scope",
		"empire_query_entities_guard_context",
	}
}

func validateMultiBundleRef(t *testing.T, root string, ref multiBundleProofRef, openRPCMethods, apiMatrixMethods, goTests map[string]bool) {
	t.Helper()
	switch ref.Kind {
	case "artifact":
		if strings.TrimSpace(ref.Path) == "" {
			t.Fatalf("artifact ref missing path: %#v", ref)
		}
		if _, err := os.Stat(filepath.Join(root, ref.Path)); err != nil {
			t.Fatalf("artifact ref %s does not resolve: %v", ref.Path, err)
		}
	case "spec_ref":
		if strings.TrimSpace(ref.Path) == "" || strings.TrimSpace(ref.Ref) == "" {
			t.Fatalf("spec_ref missing path/ref: %#v", ref)
		}
		raw, err := os.ReadFile(filepath.Join(root, ref.Path))
		if err != nil {
			t.Fatalf("read spec ref %s: %v", ref.Path, err)
		}
		if !strings.Contains(string(raw), ref.Ref) {
			t.Fatalf("spec_ref %s#%s does not resolve by text search", ref.Path, ref.Ref)
		}
	case "openrpc_method":
		if !openRPCMethods[ref.Method] {
			t.Fatalf("openrpc_method %q does not resolve", ref.Method)
		}
	case "api_matrix_method":
		if !apiMatrixMethods[ref.Method] {
			t.Fatalf("api_matrix_method %q does not resolve", ref.Method)
		}
	case "go_test":
		if !goTests[ref.Name] {
			t.Fatalf("go_test %q does not resolve", ref.Name)
		}
	case "command":
		if strings.TrimSpace(ref.Name) == "" {
			t.Fatalf("command ref missing name: %#v", ref)
		}
	case "issue":
		if ref.Issue <= 0 {
			t.Fatalf("issue ref missing issue number: %#v", ref)
		}
	case "pr":
		if ref.PR <= 0 {
			t.Fatalf("pr ref missing pr number: %#v", ref)
		}
	default:
		t.Fatalf("unknown proof ref kind %q in %#v", ref.Kind, ref)
	}
}

func hasExecutableMultiBundleProof(refs []multiBundleProofRef) bool {
	for _, ref := range refs {
		switch ref.Kind {
		case "go_test", "api_matrix_method", "openrpc_method", "command":
			return true
		}
	}
	return false
}

func hasDispositionMultiBundleProof(refs []multiBundleProofRef) bool {
	for _, ref := range refs {
		switch ref.Kind {
		case "issue", "pr", "spec_ref":
			return true
		}
	}
	return false
}

func loadMatrixOpenRPCMethods(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read openrpc artifact: %v", err)
	}
	var doc struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse openrpc artifact: %v", err)
	}
	out := map[string]bool{}
	for _, method := range doc.Methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			t.Fatal("openrpc method missing name")
		}
		out[name] = true
	}
	return out
}

func loadMatrixAPIMethods(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read api compliance matrix: %v", err)
	}
	var matrix struct {
		Methods []struct {
			Method string `yaml:"method"`
		} `yaml:"methods"`
	}
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse api compliance matrix: %v", err)
	}
	out := map[string]bool{}
	for _, method := range matrix.Methods {
		name := strings.TrimSpace(method.Method)
		if name == "" {
			t.Fatal("api compliance matrix method missing name")
		}
		out[name] = true
	}
	return out
}

func conformanceRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
}

func assertStringSetEquals(t *testing.T, label string, got, want []string) {
	t.Helper()
	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)
	if strings.Join(gotSorted, "\n") != strings.Join(wantSorted, "\n") {
		t.Fatalf("%s mismatch:\ngot:\n%s\nwant:\n%s", label, strings.Join(gotSorted, "\n"), strings.Join(wantSorted, "\n"))
	}
}
