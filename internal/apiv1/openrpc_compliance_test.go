package apiv1

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/division-sh/swarm/internal/apispec"
	"github.com/division-sh/swarm/internal/platform"
)

type openRPCComplianceMatrix struct {
	Version        int                       `yaml:"version"`
	Kind           string                    `yaml:"kind"`
	Issue          int                       `yaml:"issue"`
	IssueRole      string                    `yaml:"issue_role"`
	ActiveTrackers []complianceActiveTracker `yaml:"active_trackers"`
	Methods        []openRPCMethodMatrix     `yaml:"methods"`
}

func complianceOpenRPCPath(repoRoot string) string {
	return platform.DefaultOpenRPCFile(repoRoot)
}

func compliancePlatformSpecPath(repoRoot string) string {
	return platform.DefaultPlatformSpecFile(repoRoot)
}

type complianceActiveTracker struct {
	Kind      string `yaml:"kind"`
	Issue     int    `yaml:"issue"`
	Watchlist string `yaml:"watchlist"`
}

type openRPCMethodMatrix struct {
	Method                         string             `yaml:"method"`
	Transport                      string             `yaml:"transport"`
	RequiredParams                 []string           `yaml:"required_params"`
	DeclaredErrors                 []string           `yaml:"declared_errors"`
	HappyPath                      complianceEvidence `yaml:"happy_path"`
	RequiredParamValidation        complianceEvidence `yaml:"required_param_validation"`
	UnknownTopLevelParamValidation complianceEvidence `yaml:"unknown_top_level_param_validation"`
	Auth                           complianceEvidence `yaml:"auth"`
	DeclaredErrorTests             complianceEvidence `yaml:"declared_error_tests"`
	Idempotency                    complianceEvidence `yaml:"idempotency"`
	ResultSchema                   complianceEvidence `yaml:"result_schema"`
	NotificationSchema             complianceEvidence `yaml:"notification_schema"`
	Examples                       complianceEvidence `yaml:"examples"`
	ServiceDiscoveryPublication    complianceEvidence `yaml:"service_discovery_publication"`
	GapClassification              []string           `yaml:"gap_classification"`
}

type complianceEvidence struct {
	Status             string               `yaml:"status"`
	ProofRefs          []complianceProofRef `yaml:"proof_refs"`
	LegacyProof        []string             `yaml:"proof"`
	LegacyProofPresent bool                 `yaml:"-"`
}

func (e *complianceEvidence) UnmarshalYAML(value *yaml.Node) error {
	type plain complianceEvidence
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*e = complianceEvidence(decoded)
	if value.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(value.Content); i += 2 {
		if value.Content[i].Value == "proof" {
			e.LegacyProofPresent = true
			return nil
		}
	}
	return nil
}

type complianceProofRef struct {
	Kind      string `yaml:"kind"`
	Name      string `yaml:"name,omitempty"`
	Path      string `yaml:"path,omitempty"`
	Issue     int    `yaml:"issue,omitempty"`
	Watchlist string `yaml:"watchlist,omitempty"`
}

type rawOpenRPCDocument struct {
	Methods []map[string]json.RawMessage `json:"methods"`
}

const openRPCComplianceMatrixTestName = "TestOpenRPCComplianceMatrixCoversEveryGeneratedMethod"
const serviceDiscoveryPolicyRuntimeTestName = "TestServiceDiscoveryPolicyDoesNotServeRPCDiscover"

func TestOpenRPCComplianceMatrixCoversEveryGeneratedMethod(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	doc, rawMethods := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	if matrix.Version != 1 {
		t.Fatalf("matrix version = %d, want 1", matrix.Version)
	}
	if matrix.Kind != "openrpc_compliance_matrix" {
		t.Fatalf("matrix kind = %q, want openrpc_compliance_matrix", matrix.Kind)
	}
	if matrix.Issue != 835 {
		t.Fatalf("matrix issue = %d, want 835", matrix.Issue)
	}
	if matrix.IssueRole != "provenance" {
		t.Fatalf("matrix issue_role = %q, want provenance", matrix.IssueRole)
	}
	if len(doc.Methods) != 59 {
		t.Fatalf("generated OpenRPC methods = %d, want 59", len(doc.Methods))
	}
	if len(matrix.Methods) != len(doc.Methods) {
		t.Fatalf("matrix rows = %d, want generated method count %d", len(matrix.Methods), len(doc.Methods))
	}
	assertExamplesPolicyDeferred(t, api.ExamplesPolicy)
	assertServiceDiscoveryPolicyNotPublished(t, api.ServiceDiscoveryPolicy)
	if problems := validateProofReferenceIntegrity(root, matrix); len(problems) > 0 {
		t.Fatalf("compliance matrix proof-reference integrity failed:\n- %s", strings.Join(problems, "\n- "))
	}

	openRPCByName := map[string]apispec.OpenRPCMethod{}
	for _, method := range doc.Methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			t.Fatal("generated OpenRPC method missing name")
		}
		if _, exists := openRPCByName[name]; exists {
			t.Fatalf("generated OpenRPC method %s appears more than once", name)
		}
		openRPCByName[name] = method
	}
	assertRPCDiscoverNotPublishedUnderPolicy(t, api.ServiceDiscoveryPolicy, openRPCByName)

	rowsByName := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		name := strings.TrimSpace(row.Method)
		if name == "" {
			t.Fatal("matrix row missing method")
		}
		if _, exists := rowsByName[name]; exists {
			t.Fatalf("matrix method %s appears more than once", name)
		}
		rowsByName[name] = row
	}

	mutatingMethods := complianceStringSet(api.Conventions.Idempotency.MutatingMethods)
	for methodName, openRPCMethod := range openRPCByName {
		row, ok := rowsByName[methodName]
		if !ok {
			t.Fatalf("matrix missing generated OpenRPC method %s", methodName)
		}
		specMethod, ok := api.MethodCatalog[methodName]
		if !ok {
			t.Fatalf("generated OpenRPC method %s missing from platform spec method_catalog", methodName)
		}

		if row.Transport != expectedComplianceTransport(methodName, specMethod) {
			t.Fatalf("%s transport = %q, want %q", methodName, row.Transport, expectedComplianceTransport(methodName, specMethod))
		}
		assertStringList(t, methodName+" required_params", row.RequiredParams, requiredParamNames(specMethod))
		assertStringList(t, methodName+" declared_errors", row.DeclaredErrors, specMethod.Errors)
		assertStringList(t, methodName+" openrpc declared_errors", row.DeclaredErrors, openRPCErrorCodes(t, methodName, openRPCMethod))

		assertEvidence(t, methodName, "happy_path", row.HappyPath, true)
		assertEvidence(t, methodName, "unknown_top_level_param_validation", row.UnknownTopLevelParamValidation, false)
		assertEvidence(t, methodName, "auth", row.Auth, false)
		assertEvidence(t, methodName, "result_schema", row.ResultSchema, true)
		assertEvidence(t, methodName, "examples", row.Examples, false)
		assertEvidence(t, methodName, "service_discovery_publication", row.ServiceDiscoveryPublication, false)

		if len(row.RequiredParams) == 0 {
			assertNotApplicable(t, methodName, "required_param_validation", row.RequiredParamValidation)
		} else {
			assertEvidence(t, methodName, "required_param_validation", row.RequiredParamValidation, false)
		}
		if len(row.DeclaredErrors) == 0 {
			assertNotApplicable(t, methodName, "declared_error_tests", row.DeclaredErrorTests)
		} else {
			assertEvidence(t, methodName, "declared_error_tests", row.DeclaredErrorTests, true)
		}
		if _, mutating := mutatingMethods[methodName]; mutating {
			assertEvidence(t, methodName, "idempotency", row.Idempotency, true)
		} else {
			assertNotApplicable(t, methodName, "idempotency", row.Idempotency)
		}
		if specMethod.NotificationSchema == nil {
			assertNotApplicable(t, methodName, "notification_schema", row.NotificationSchema)
		} else {
			assertEvidence(t, methodName, "notification_schema", row.NotificationSchema, true)
		}
		assertExamplesPolicyMatrixRow(t, methodName, rawMethods[methodName], row.Examples)
		assertServiceDiscoveryPolicyMatrixRow(t, methodName, api.ServiceDiscoveryPolicy, row.ServiceDiscoveryPublication)
	}

	for methodName := range rowsByName {
		if _, ok := openRPCByName[methodName]; !ok {
			t.Fatalf("matrix has method %s absent from generated OpenRPC", methodName)
		}
	}
}

func TestOpenRPCComplianceMatrixRejectsInvalidProofReferences(t *testing.T) {
	root := repoRoot(t)
	matrixPath := filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml")
	trackerRef := complianceProofRef{Kind: "tracker", Issue: 999999, Watchlist: "operator_surfaces.v1_openrpc_api_conformance"}
	tests := []struct {
		name   string
		mutate func(*openRPCComplianceMatrix)
		want   string
	}{
		{
			name: "stale go test",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.HappyPath.ProofRefs = []complianceProofRef{{Kind: "go_test", Name: "TestDoesNotExist"}}
			},
			want: "go_test proof_ref TestDoesNotExist does not resolve",
		},
		{
			name: "stale go helper",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.UnknownTopLevelParamValidation.ProofRefs = []complianceProofRef{{Kind: "go_helper", Name: "missingHelper"}}
			},
			want: "go_helper proof_ref missingHelper does not resolve",
		},
		{
			name: "stale tracker",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "mailbox.reject")
				row.HappyPath.ProofRefs = []complianceProofRef{{Kind: "tracker", Issue: 835, Watchlist: "operator_surfaces.v1_openrpc_api_conformance"}}
			},
			want: "tracker proof_ref issue #835 is the provenance issue",
		},
		{
			name: "unknown tracker",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "mailbox.reject")
				row.HappyPath.ProofRefs = []complianceProofRef{{Kind: "tracker", Issue: 999999, Watchlist: "operator_surfaces.v1_openrpc_api_conformance"}}
			},
			want: "tracker proof_ref issue #999999 watchlist \"operator_surfaces.v1_openrpc_api_conformance\" is not in active_trackers",
		},
		{
			name: "orphan active tracker",
			mutate: func(matrix *openRPCComplianceMatrix) {
				matrix.ActiveTrackers = append(matrix.ActiveTrackers, complianceActiveTracker{
					Kind:      "github_issue",
					Issue:     129,
					Watchlist: "operator_surfaces.canonical_operator_projection_truth",
				})
			},
			want: "active tracker issue #129 watchlist \"operator_surfaces.canonical_operator_projection_truth\" has no tracker proof_ref consumer",
		},
		{
			name: "unknown gap class",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.GapClassification = append(row.GapClassification, "unknown_gap_class")
			},
			want: "unknown gap_classification",
		},
		{
			name: "tracked gap missing classification",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.HappyPath.Status = "tracked_gap"
				row.HappyPath.ProofRefs = nil
				row.GapClassification = nil
			},
			want: "agent.get tracked_gap evidence requires gap_classification",
		},
		{
			name: "unsupported status proof kind",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.HappyPath.ProofRefs = []complianceProofRef{{Kind: "go_test", Name: "TestOperatorAgentConversationHandlersExposeReadOwner"}, trackerRef}
			},
			want: "status covered does not allow tracker proof_ref",
		},
		{
			name: "unknown proof kind",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.HappyPath.ProofRefs = []complianceProofRef{{Kind: "spreadsheet", Name: "manual-audit"}}
			},
			want: "proof_ref kind \"spreadsheet\" is not allowed",
		},
		{
			name: "invalid artifact path",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.ServiceDiscoveryPublication.ProofRefs = []complianceProofRef{{Kind: "artifact", Path: "artifacts/missing-openrpc.json"}}
			},
			want: "artifact proof_ref path artifacts/missing-openrpc.json does not exist",
		},
		{
			name: "policy deferred examples missing artifact proof",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.Examples.ProofRefs = []complianceProofRef{{Kind: "go_test", Name: openRPCComplianceMatrixTestName}}
			},
			want: "examples status policy_deferred requires an artifact proof_ref",
		},
		{
			name: "policy not published service discovery missing artifact proof",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.ServiceDiscoveryPublication.ProofRefs = []complianceProofRef{{Kind: "go_test", Name: openRPCComplianceMatrixTestName}}
			},
			want: "service_discovery_publication status policy_not_published requires an artifact proof_ref",
		},
		{
			name: "stale service discovery status",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.ServiceDiscoveryPublication.Status = "published_without_discovery"
			},
			want: `service_discovery_publication status "published_without_discovery" is not allowed`,
		},
		{
			name: "legacy proof strings",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.HappyPath.LegacyProof = []string{"TestOperatorAgentConversationHandlersExposeReadOwner"}
			},
			want: "uses legacy proof strings",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matrix := loadComplianceMatrix(t, matrixPath)
			tc.mutate(&matrix)
			problems := validateProofReferenceIntegrity(root, matrix)
			if !problemContains(problems, tc.want) {
				t.Fatalf("validateProofReferenceIntegrity() problems = %v, want substring %q", problems, tc.want)
			}
		})
	}
}

func TestOpenRPCComplianceMatrixRejectsEmptyLegacyProofKey(t *testing.T) {
	root := repoRoot(t)
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))
	row := complianceMatrixRow(t, &matrix, "agent.get")
	raw := []byte(`status: covered
proof: []
proof_refs:
  - kind: go_test
    name: TestOperatorAgentConversationHandlersExposeReadOwner
`)
	if err := yaml.Unmarshal(raw, &row.HappyPath); err != nil {
		t.Fatalf("parse evidence fixture: %v", err)
	}
	if !row.HappyPath.LegacyProofPresent {
		t.Fatal("legacy proof key presence was not recorded")
	}
	problems := validateProofReferenceIntegrity(root, matrix)
	if !problemContains(problems, "agent.get happy_path uses legacy proof strings") {
		t.Fatalf("validateProofReferenceIntegrity() problems = %v, want empty legacy proof key rejection", problems)
	}
}

func TestServiceDiscoveryPolicyDoesNotServeRPCDiscover(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	assertServiceDiscoveryPolicyNotPublished(t, api.ServiceDiscoveryPolicy)
	if _, ok := api.MethodCatalog["rpc.discover"]; ok {
		t.Fatal("api_specification.method_catalog includes rpc.discover while service_discovery_policy is not_published")
	}
	registry := testRegistry(t)
	if _, ok := registry.Method("rpc.discover"); ok {
		t.Fatal("runtime registry serves rpc.discover while service_discovery_policy is not_published")
	}

	handler := testHandler(t, Options{AuthTokens: []string{testToken}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"discover","method":"rpc.discover","params":{}}`))
	req.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/rpc status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc response: %v body=%s", err, rec.Body.String())
	}
	if resp.Error == nil {
		t.Fatalf("rpc.discover response error = nil, want method not found: %#v", resp.Result)
	}
	if resp.Error.Code != codeMethodNotFound {
		t.Fatalf("rpc.discover error code = %d, want %d body=%s", resp.Error.Code, codeMethodNotFound, rec.Body.String())
	}
}

func loadComplianceAPISpec(t *testing.T, root string) *apispec.APISpecification {
	t.Helper()
	api, err := apispec.LoadPlatformSpec(compliancePlatformSpecPath(root))
	if err != nil {
		t.Fatalf("LoadPlatformSpec() error = %v", err)
	}
	return api
}

type rawOpenRPCMethodProof struct {
	hasExamples bool
}

func loadComplianceOpenRPC(t *testing.T, path string) (apispec.OpenRPCDocument, map[string]rawOpenRPCMethodProof) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read openrpc artifact: %v", err)
	}
	var doc apispec.OpenRPCDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse openrpc artifact: %v", err)
	}
	var rawDoc rawOpenRPCDocument
	if err := json.Unmarshal(raw, &rawDoc); err != nil {
		t.Fatalf("parse raw openrpc artifact: %v", err)
	}
	rawMethods := map[string]rawOpenRPCMethodProof{}
	for _, method := range rawDoc.Methods {
		var name string
		if err := json.Unmarshal(method["name"], &name); err != nil {
			t.Fatalf("parse raw openrpc method name: %v", err)
		}
		rawMethods[name] = rawOpenRPCMethodProof{hasExamples: rawJSONHasContent(method["examples"]) || rawJSONHasContent(method["example"])}
	}
	return doc, rawMethods
}

func loadComplianceMatrix(t *testing.T, path string) openRPCComplianceMatrix {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read compliance matrix: %v", err)
	}
	var matrix openRPCComplianceMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse compliance matrix: %v", err)
	}
	return matrix
}

func validateProofReferenceIntegrity(root string, matrix openRPCComplianceMatrix) []string {
	index, problems := newComplianceProofIndex(root, matrix)
	trackerConsumers := trackerProofRefConsumers(matrix)
	if matrix.IssueRole != "provenance" {
		problems = append(problems, fmt.Sprintf("matrix issue_role = %q, want provenance", matrix.IssueRole))
	}
	for _, tracker := range matrix.ActiveTrackers {
		if strings.TrimSpace(tracker.Kind) != "github_issue" {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d kind = %q, want github_issue", tracker.Issue, tracker.Kind))
		}
		if tracker.Issue == 0 {
			problems = append(problems, "active tracker missing issue")
		}
		if matrix.IssueRole == "provenance" && tracker.Issue == matrix.Issue {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d must not reuse provenance issue", tracker.Issue))
		}
		if strings.TrimSpace(tracker.Watchlist) == "" {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d missing watchlist", tracker.Issue))
		}
		key := trackerKey(tracker.Issue, tracker.Watchlist)
		if tracker.Issue != 0 && strings.TrimSpace(tracker.Watchlist) != "" && trackerConsumers[key] == 0 {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d watchlist %q has no tracker proof_ref consumer", tracker.Issue, tracker.Watchlist))
		}
	}
	for _, row := range matrix.Methods {
		for _, gapClass := range row.GapClassification {
			if _, ok := allowedComplianceGapClasses()[gapClass]; !ok {
				problems = append(problems, fmt.Sprintf("%s unknown gap_classification %q", row.Method, gapClass))
			}
		}
		if rowHasTrackedGap(row) && len(row.GapClassification) == 0 {
			problems = append(problems, fmt.Sprintf("%s tracked_gap evidence requires gap_classification", row.Method))
		}
		for _, evidence := range complianceEvidenceFields(row) {
			problems = append(problems, validateEvidenceProofRefs(root, index, matrix, row.Method, evidence.field, evidence.evidence)...)
		}
	}
	sort.Strings(problems)
	return problems
}

func trackerProofRefConsumers(matrix openRPCComplianceMatrix) map[string]int {
	consumers := map[string]int{}
	for _, row := range matrix.Methods {
		for _, evidence := range complianceEvidenceFields(row) {
			for _, ref := range evidence.evidence.ProofRefs {
				if ref.Kind == "tracker" {
					consumers[trackerKey(ref.Issue, ref.Watchlist)]++
				}
			}
		}
	}
	return consumers
}

type complianceProofIndex struct {
	goTests        map[string]string
	goHelpers      map[string]string
	activeTrackers map[string]struct{}
}

func newComplianceProofIndex(root string, matrix openRPCComplianceMatrix) (complianceProofIndex, []string) {
	index := complianceProofIndex{
		goTests:        map[string]string{},
		goHelpers:      map[string]string{},
		activeTrackers: map[string]struct{}{},
	}
	var problems []string
	symbols, err := loadGoFunctionSymbols(root)
	if err != nil {
		problems = append(problems, err.Error())
	} else {
		index.goTests = symbols.tests
		index.goHelpers = symbols.helpers
	}
	for _, tracker := range matrix.ActiveTrackers {
		key := trackerKey(tracker.Issue, tracker.Watchlist)
		index.activeTrackers[key] = struct{}{}
	}
	return index, problems
}

type namedComplianceEvidence struct {
	field    string
	evidence complianceEvidence
}

func complianceEvidenceFields(row openRPCMethodMatrix) []namedComplianceEvidence {
	return []namedComplianceEvidence{
		{field: "happy_path", evidence: row.HappyPath},
		{field: "required_param_validation", evidence: row.RequiredParamValidation},
		{field: "unknown_top_level_param_validation", evidence: row.UnknownTopLevelParamValidation},
		{field: "auth", evidence: row.Auth},
		{field: "declared_error_tests", evidence: row.DeclaredErrorTests},
		{field: "idempotency", evidence: row.Idempotency},
		{field: "result_schema", evidence: row.ResultSchema},
		{field: "notification_schema", evidence: row.NotificationSchema},
		{field: "examples", evidence: row.Examples},
		{field: "service_discovery_publication", evidence: row.ServiceDiscoveryPublication},
	}
}

func rowHasTrackedGap(row openRPCMethodMatrix) bool {
	for _, evidence := range complianceEvidenceFields(row) {
		if strings.TrimSpace(evidence.evidence.Status) == "tracked_gap" {
			return true
		}
	}
	return false
}

func validateEvidenceProofRefs(root string, index complianceProofIndex, matrix openRPCComplianceMatrix, methodName, field string, evidence complianceEvidence) []string {
	var problems []string
	label := methodName + " " + field
	status := strings.TrimSpace(evidence.Status)
	if evidence.LegacyProofPresent || len(evidence.LegacyProof) > 0 {
		problems = append(problems, fmt.Sprintf("%s uses legacy proof strings; use proof_refs", label))
	}
	if status == "not_applicable" {
		if len(evidence.ProofRefs) > 0 {
			problems = append(problems, fmt.Sprintf("%s status not_applicable must not carry proof_refs", label))
		}
		return problems
	}
	if len(evidence.ProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s status %s requires proof_refs", label, status))
		return problems
	}
	if allowedStatuses := allowedEvidenceStatuses(field); len(allowedStatuses) > 0 {
		if _, ok := allowedStatuses[status]; !ok && status != "tracked_gap" {
			problems = append(problems, fmt.Sprintf("%s status %q is not allowed for %s", label, status, field))
		}
	}
	allowedKinds := allowedProofRefKinds(status, field)
	kinds := map[string]struct{}{}
	for _, ref := range evidence.ProofRefs {
		kind := strings.TrimSpace(ref.Kind)
		kinds[kind] = struct{}{}
		if len(allowedKinds) > 0 {
			if _, ok := allowedKinds[kind]; !ok {
				problems = append(problems, fmt.Sprintf("%s status %s does not allow %s proof_ref", label, status, kind))
			}
		}
		switch kind {
		case "go_test":
			if strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", label))
			} else if _, ok := index.goTests[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", label, ref.Name))
			}
		case "go_helper":
			if strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s go_helper proof_ref missing name", label))
			} else if _, ok := index.goHelpers[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s go_helper proof_ref %s does not resolve", label, ref.Name))
			}
		case "artifact":
			if strings.TrimSpace(ref.Path) == "" {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref missing path", label))
				continue
			}
			if filepath.IsAbs(ref.Path) || strings.HasPrefix(filepath.Clean(ref.Path), "..") {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s must be repo-relative", label, ref.Path))
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Clean(ref.Path))); err != nil {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s does not exist", label, ref.Path))
			}
		case "tracker":
			if ref.Issue == 0 {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref missing issue", label))
			}
			if matrix.IssueRole == "provenance" && ref.Issue == matrix.Issue {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref issue #%d is the provenance issue, not an active tracker", label, ref.Issue))
			}
			key := trackerKey(ref.Issue, ref.Watchlist)
			if _, ok := index.activeTrackers[key]; !ok {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref issue #%d watchlist %q is not in active_trackers", label, ref.Issue, ref.Watchlist))
			}
		case "policy_gap":
			if _, ok := allowedCompliancePolicyGaps()[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s policy_gap proof_ref %q is not allowed", label, ref.Name))
			}
		default:
			problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", label, kind))
		}
	}
	switch status {
	case "covered", "spot_checked":
		if _, ok := kinds["go_test"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status %s requires at least one go_test proof_ref", label, status))
		}
	case "shared":
		if _, hasTest := kinds["go_test"]; !hasTest {
			if _, hasHelper := kinds["go_helper"]; !hasHelper {
				problems = append(problems, fmt.Sprintf("%s status shared requires at least one go_test or go_helper proof_ref", label))
			}
		}
	case "tracked_gap":
		if _, ok := kinds["tracker"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status tracked_gap requires a tracker proof_ref", label))
		}
	case "policy_not_published":
		if _, ok := kinds["artifact"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status policy_not_published requires an artifact proof_ref", label))
		}
		if _, ok := kinds["go_test"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status policy_not_published requires a go_test proof_ref", label))
		}
	case "policy_deferred":
		if _, ok := kinds["artifact"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status policy_deferred requires an artifact proof_ref", label))
		}
		if _, ok := kinds["go_test"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status policy_deferred requires a go_test proof_ref", label))
		}
	}
	return problems
}

func allowedComplianceGapClasses() map[string]struct{} {
	return complianceStringSet([]string{
		"missing_generated_declared_error_cases",
		"missing_generated_result_schema_validation",
		"missing_happy_path_test",
		"missing_idempotency_test",
		"missing_result_schema_proof",
	})
}

func allowedCompliancePolicyGaps() map[string]struct{} {
	return complianceStringSet(nil)
}

func allowedProofRefKinds(status, field string) map[string]struct{} {
	switch status {
	case "covered", "spot_checked":
		return complianceStringSet([]string{"go_test"})
	case "shared":
		return complianceStringSet([]string{"go_test", "go_helper"})
	case "tracked_gap":
		if field == "examples" {
			return complianceStringSet([]string{"tracker", "policy_gap"})
		}
		return complianceStringSet([]string{"tracker"})
	case "policy_not_published":
		return complianceStringSet([]string{"artifact", "go_test"})
	case "policy_deferred":
		return complianceStringSet([]string{"artifact", "go_test"})
	default:
		return map[string]struct{}{}
	}
}

type goFunctionSymbols struct {
	tests   map[string]string
	helpers map[string]string
}

func loadGoFunctionSymbols(root string) (goFunctionSymbols, error) {
	symbols := goFunctionSymbols{
		tests:   map[string]string{},
		helpers: map[string]string{},
	}
	for _, dir := range []string{"internal/apispec", "internal/apiv1"} {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			isTestFile := strings.HasSuffix(path, "_test.go")
			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv != nil {
					continue
				}
				name := fn.Name.Name
				if isTestFile && strings.HasPrefix(name, "Test") {
					symbols.tests[name] = rel
					continue
				}
				symbols.helpers[name] = rel
			}
			return nil
		})
		if err != nil {
			return symbols, fmt.Errorf("load go function symbols from %s: %w", dir, err)
		}
	}
	return symbols, nil
}

func trackerKey(issue int, watchlist string) string {
	return fmt.Sprintf("%d:%s", issue, strings.TrimSpace(watchlist))
}

func complianceMatrixRow(t *testing.T, matrix *openRPCComplianceMatrix, method string) *openRPCMethodMatrix {
	t.Helper()
	for i := range matrix.Methods {
		if matrix.Methods[i].Method == method {
			return &matrix.Methods[i]
		}
	}
	t.Fatalf("matrix method %s not found", method)
	return nil
}

func problemContains(problems []string, want string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}

func expectedComplianceTransport(methodName string, method apispec.Method) string {
	if methodName == "rpc.unsubscribe" || method.NotificationSchema != nil {
		return "ws"
	}
	return "http"
}

func requiredParamNames(method apispec.Method) []string {
	var names []string
	for _, param := range method.Params {
		if param.Required {
			names = append(names, param.Name)
		}
	}
	return names
}

func openRPCErrorCodes(t *testing.T, methodName string, method apispec.OpenRPCMethod) []string {
	t.Helper()
	out := make([]string, 0, len(method.Errors))
	for _, errDef := range method.Errors {
		data, ok := errDef.Data.(map[string]any)
		if !ok {
			t.Fatalf("%s OpenRPC error %d data = %#v, want object", methodName, errDef.Code, errDef.Data)
		}
		properties, ok := data["properties"].(map[string]any)
		if !ok {
			t.Fatalf("%s OpenRPC error %d data.properties = %#v, want object", methodName, errDef.Code, data["properties"])
		}
		code, ok := properties["code"].(map[string]any)
		if !ok {
			t.Fatalf("%s OpenRPC error %d data.properties.code = %#v, want object", methodName, errDef.Code, properties["code"])
		}
		name, ok := code["const"].(string)
		if !ok || strings.TrimSpace(name) == "" {
			t.Fatalf("%s OpenRPC error %d data.properties.code.const = %#v, want error code", methodName, errDef.Code, code["const"])
		}
		out = append(out, name)
	}
	return out
}

func assertStringList(t *testing.T, label string, got, want []string) {
	t.Helper()
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	if got == nil {
		got = []string{}
	}
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func assertEvidence(t *testing.T, methodName, field string, evidence complianceEvidence, allowTrackedGap bool) {
	t.Helper()
	status := strings.TrimSpace(evidence.Status)
	if status == "" {
		t.Fatalf("%s %s status is empty", methodName, field)
	}
	allowed := allowedEvidenceStatuses(field)
	if len(allowed) == 0 {
		t.Fatalf("%s uses unknown evidence field %q", methodName, field)
	}
	if allowTrackedGap {
		allowed["tracked_gap"] = struct{}{}
	}
	if _, ok := allowed[status]; !ok {
		t.Fatalf("%s %s status = %q, want one of %v", methodName, field, status, sortedKeys(allowed))
	}
	if evidence.LegacyProofPresent || len(evidence.LegacyProof) > 0 {
		t.Fatalf("%s %s proof uses legacy string refs; use proof_refs", methodName, field)
	}
	if len(evidence.ProofRefs) == 0 {
		t.Fatalf("%s %s proof_refs must name the test, helper, issue, or artifact backing status %q", methodName, field, status)
	}
}

func allowedEvidenceStatuses(field string) map[string]struct{} {
	switch field {
	case "happy_path":
		return complianceStringSet([]string{"covered"})
	case "required_param_validation", "unknown_top_level_param_validation":
		return complianceStringSet([]string{"shared", "covered"})
	case "auth":
		return complianceStringSet([]string{"shared", "covered"})
	case "declared_error_tests":
		return complianceStringSet([]string{"covered", "spot_checked"})
	case "idempotency":
		return complianceStringSet([]string{"covered"})
	case "result_schema", "notification_schema":
		return complianceStringSet([]string{"covered", "spot_checked"})
	case "examples":
		return complianceStringSet([]string{"covered", "policy_deferred"})
	case "service_discovery_publication":
		return complianceStringSet([]string{"policy_not_published"})
	default:
		return map[string]struct{}{}
	}
}

func assertExamplesPolicyDeferred(t *testing.T, policy apispec.ExamplesPolicy) {
	t.Helper()
	if policy.Status != apispec.ExamplesPolicyStatusDeferred {
		t.Fatalf("examples_policy.status = %q, want %q", policy.Status, apispec.ExamplesPolicyStatusDeferred)
	}
	if policy.Owner != apispec.ExamplesPolicyOwner {
		t.Fatalf("examples_policy.owner = %q, want %q", policy.Owner, apispec.ExamplesPolicyOwner)
	}
	if policy.AppliesTo != apispec.ExamplesPolicyAppliesToAllGenerated {
		t.Fatalf("examples_policy.applies_to = %q, want %q", policy.AppliesTo, apispec.ExamplesPolicyAppliesToAllGenerated)
	}
	if policy.OpenRPCMethodExamples != apispec.ExamplesPolicyOpenRPCExamplesOmitted {
		t.Fatalf("examples_policy.openrpc_method_examples = %q, want %q", policy.OpenRPCMethodExamples, apispec.ExamplesPolicyOpenRPCExamplesOmitted)
	}
	if policy.RuntimeProbeFixtures != apispec.ExamplesPolicyRuntimeFixturesNotExamples {
		t.Fatalf("examples_policy.runtime_probe_fixtures = %q, want %q", policy.RuntimeProbeFixtures, apispec.ExamplesPolicyRuntimeFixturesNotExamples)
	}
}

func assertServiceDiscoveryPolicyNotPublished(t *testing.T, policy apispec.ServiceDiscoveryPolicy) {
	t.Helper()
	if policy.Status != apispec.ServiceDiscoveryPolicyStatusNotPublished {
		t.Fatalf("service_discovery_policy.status = %q, want %q", policy.Status, apispec.ServiceDiscoveryPolicyStatusNotPublished)
	}
	if policy.Owner != apispec.ServiceDiscoveryPolicyOwner {
		t.Fatalf("service_discovery_policy.owner = %q, want %q", policy.Owner, apispec.ServiceDiscoveryPolicyOwner)
	}
	if policy.AppliesTo != apispec.ServiceDiscoveryPolicyAppliesToGeneratedCatalog {
		t.Fatalf("service_discovery_policy.applies_to = %q, want %q", policy.AppliesTo, apispec.ServiceDiscoveryPolicyAppliesToGeneratedCatalog)
	}
	if policy.RPCDiscover != apispec.ServiceDiscoveryPolicyRPCDiscoverOmitted {
		t.Fatalf("service_discovery_policy.rpc_discover = %q, want %q", policy.RPCDiscover, apispec.ServiceDiscoveryPolicyRPCDiscoverOmitted)
	}
	if policy.PublicationArtifact != apispec.ServiceDiscoveryPolicyPublicationArtifactOpenRPC {
		t.Fatalf("service_discovery_policy.publication_artifact = %q, want %q", policy.PublicationArtifact, apispec.ServiceDiscoveryPolicyPublicationArtifactOpenRPC)
	}
	if policy.RuntimeBehavior != apispec.ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound {
		t.Fatalf("service_discovery_policy.runtime_behavior = %q, want %q", policy.RuntimeBehavior, apispec.ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound)
	}
}

func assertRPCDiscoverNotPublishedUnderPolicy(t *testing.T, policy apispec.ServiceDiscoveryPolicy, methods map[string]apispec.OpenRPCMethod) {
	t.Helper()
	assertServiceDiscoveryPolicyNotPublished(t, policy)
	if _, ok := methods["rpc.discover"]; ok {
		t.Fatal("generated OpenRPC publishes rpc.discover while service_discovery_policy is not_published")
	}
}

func assertExamplesPolicyMatrixRow(t *testing.T, methodName string, rawProof rawOpenRPCMethodProof, evidence complianceEvidence) {
	t.Helper()
	if rawProof.hasExamples {
		t.Fatalf("%s publishes OpenRPC examples while examples_policy is deferred", methodName)
	}
	if evidence.Status != "policy_deferred" {
		t.Fatalf("%s examples status = %q, want policy_deferred while examples_policy is deferred", methodName, evidence.Status)
	}
	if !evidenceHasArtifact(evidence, "platform-spec.yaml") {
		t.Fatalf("%s examples missing platform-spec.yaml artifact proof_ref", methodName)
	}
	if !evidenceHasArtifact(evidence, "openrpc.json") {
		t.Fatalf("%s examples missing openrpc.json artifact proof_ref", methodName)
	}
	if !evidenceHasGoTest(evidence, openRPCComplianceMatrixTestName) {
		t.Fatalf("%s examples missing go_test proof_ref %s", methodName, openRPCComplianceMatrixTestName)
	}
}

func assertServiceDiscoveryPolicyMatrixRow(t *testing.T, methodName string, policy apispec.ServiceDiscoveryPolicy, evidence complianceEvidence) {
	t.Helper()
	assertServiceDiscoveryPolicyNotPublished(t, policy)
	if evidence.Status != "policy_not_published" {
		t.Fatalf("%s service_discovery_publication status = %q, want policy_not_published while service_discovery_policy is not_published", methodName, evidence.Status)
	}
	if !evidenceHasArtifact(evidence, "platform-spec.yaml") {
		t.Fatalf("%s service_discovery_publication missing platform-spec.yaml artifact proof_ref", methodName)
	}
	if !evidenceHasArtifact(evidence, "openrpc.json") {
		t.Fatalf("%s service_discovery_publication missing openrpc.json artifact proof_ref", methodName)
	}
	if !evidenceHasGoTest(evidence, openRPCComplianceMatrixTestName) {
		t.Fatalf("%s service_discovery_publication missing go_test proof_ref %s", methodName, openRPCComplianceMatrixTestName)
	}
	if !evidenceHasGoTest(evidence, serviceDiscoveryPolicyRuntimeTestName) {
		t.Fatalf("%s service_discovery_publication missing go_test proof_ref %s", methodName, serviceDiscoveryPolicyRuntimeTestName)
	}
}

func evidenceHasArtifact(evidence complianceEvidence, path string) bool {
	for _, ref := range evidence.ProofRefs {
		if ref.Kind == "artifact" && ref.Path == path {
			return true
		}
	}
	return false
}

func assertNotApplicable(t *testing.T, methodName, field string, evidence complianceEvidence) {
	t.Helper()
	if evidence.Status != "not_applicable" {
		t.Fatalf("%s %s status = %q, want not_applicable", methodName, field, evidence.Status)
	}
	if evidence.LegacyProofPresent || len(evidence.LegacyProof) > 0 || len(evidence.ProofRefs) > 0 {
		t.Fatalf("%s %s not_applicable must not carry proof refs", methodName, field)
	}
}

func rawJSONHasContent(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null" && trimmed != "[]"
}

func sortedKeys(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func complianceStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		clean := strings.TrimSpace(value)
		if clean != "" {
			out[clean] = struct{}{}
		}
	}
	return out
}
