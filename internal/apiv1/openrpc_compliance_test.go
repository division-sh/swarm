package apiv1

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"swarm/internal/apispec"
)

type openRPCComplianceMatrix struct {
	Version        int                       `yaml:"version"`
	Kind           string                    `yaml:"kind"`
	Issue          int                       `yaml:"issue"`
	IssueRole      string                    `yaml:"issue_role"`
	ActiveTrackers []complianceActiveTracker `yaml:"active_trackers"`
	Methods        []openRPCMethodMatrix     `yaml:"methods"`
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

func TestOpenRPCComplianceMatrixCoversEveryGeneratedMethod(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	doc, rawMethods := loadComplianceOpenRPC(t, filepath.Join(root, "docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json"))
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
	if len(doc.Methods) != 42 {
		t.Fatalf("generated OpenRPC methods = %d, want 42", len(doc.Methods))
	}
	if len(matrix.Methods) != len(doc.Methods) {
		t.Fatalf("matrix rows = %d, want generated method count %d", len(matrix.Methods), len(doc.Methods))
	}
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
	if _, ok := openRPCByName["rpc.discover"]; ok {
		t.Fatal("rpc.discover is not part of the approved #835 v1 publication boundary")
	}

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
		assertEvidence(t, methodName, "examples", row.Examples, true)
		assertEvidence(t, methodName, "service_discovery_publication", row.ServiceDiscoveryPublication, false)

		if len(row.GapClassification) == 0 {
			t.Fatalf("%s gap_classification must classify remaining proof gaps", methodName)
		}
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
		if !rawMethods[methodName].hasExamples && row.Examples.Status != "tracked_gap" {
			t.Fatalf("%s examples status = %q, want tracked_gap while openrpc.json has no method examples", methodName, row.Examples.Status)
		}
		if row.ServiceDiscoveryPublication.Status != "published_without_discovery" {
			t.Fatalf("%s service_discovery_publication status = %q, want published_without_discovery", methodName, row.ServiceDiscoveryPublication.Status)
		}
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
	trackerRef := complianceProofRef{Kind: "tracker", Issue: 857, Watchlist: "operator_surfaces.v1_openrpc_api_conformance"}
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
			name: "unknown gap class",
			mutate: func(matrix *openRPCComplianceMatrix) {
				row := complianceMatrixRow(t, matrix, "agent.get")
				row.GapClassification = append(row.GapClassification, "unknown_gap_class")
			},
			want: "unknown gap_classification",
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
				row.ServiceDiscoveryPublication.ProofRefs = []complianceProofRef{{Kind: "artifact", Path: "docs/specs/missing-openrpc.json"}}
			},
			want: "artifact proof_ref path docs/specs/missing-openrpc.json does not exist",
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

func loadComplianceAPISpec(t *testing.T, root string) *apispec.APISpecification {
	t.Helper()
	api, err := apispec.LoadPlatformSpec(filepath.Join(root, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
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
		rawMethods[name] = rawOpenRPCMethodProof{hasExamples: rawJSONHasContent(method["examples"])}
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
	if matrix.IssueRole != "provenance" {
		problems = append(problems, fmt.Sprintf("matrix issue_role = %q, want provenance", matrix.IssueRole))
	}
	if len(matrix.ActiveTrackers) == 0 {
		problems = append(problems, "matrix active_trackers must list at least one active tracker")
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
	}
	for _, row := range matrix.Methods {
		for _, gapClass := range row.GapClassification {
			if _, ok := allowedComplianceGapClasses()[gapClass]; !ok {
				problems = append(problems, fmt.Sprintf("%s unknown gap_classification %q", row.Method, gapClass))
			}
		}
		for _, evidence := range complianceEvidenceFields(row) {
			problems = append(problems, validateEvidenceProofRefs(root, index, matrix, row.Method, evidence.field, evidence.evidence)...)
		}
	}
	sort.Strings(problems)
	return problems
}

type complianceProofIndex struct {
	goTests             map[string]string
	goHelpers           map[string]string
	activeTrackers      map[string]struct{}
	watchlistNodes      map[string]struct{}
	watchlistIssueLinks map[string]struct{}
}

func newComplianceProofIndex(root string, matrix openRPCComplianceMatrix) (complianceProofIndex, []string) {
	index := complianceProofIndex{
		goTests:             map[string]string{},
		goHelpers:           map[string]string{},
		activeTrackers:      map[string]struct{}{},
		watchlistNodes:      map[string]struct{}{},
		watchlistIssueLinks: map[string]struct{}{},
	}
	var problems []string
	symbols, err := loadGoFunctionSymbols(root)
	if err != nil {
		problems = append(problems, err.Error())
	} else {
		index.goTests = symbols.tests
		index.goHelpers = symbols.helpers
	}
	watchlists, err := loadWatchlistProofIndex(root, complianceWatchlistIDs(matrix))
	if err != nil {
		problems = append(problems, err.Error())
	} else {
		index.watchlistNodes = watchlists.nodes
		index.watchlistIssueLinks = watchlists.issueLinks
	}
	for _, tracker := range matrix.ActiveTrackers {
		key := trackerKey(tracker.Issue, tracker.Watchlist)
		index.activeTrackers[key] = struct{}{}
		if _, ok := index.watchlistNodes[tracker.Watchlist]; !ok {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d watchlist %q does not resolve", tracker.Issue, tracker.Watchlist))
		}
		if _, ok := index.watchlistIssueLinks[key]; !ok {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d is not mapped to watchlist %q", tracker.Issue, tracker.Watchlist))
		}
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
	case "published_without_discovery":
		if _, ok := kinds["artifact"]; !ok {
			problems = append(problems, fmt.Sprintf("%s status published_without_discovery requires an artifact proof_ref", label))
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
		"missing_openrpc_examples",
		"missing_result_schema_proof",
		"notification_schema_not_emitted_in_openrpc_method",
	})
}

func allowedCompliancePolicyGaps() map[string]struct{} {
	return complianceStringSet([]string{
		"openrpc_examples_absent",
	})
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
	case "published_without_discovery":
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

type watchlistProofIndex struct {
	nodes      map[string]struct{}
	issueLinks map[string]struct{}
}

func complianceWatchlistIDs(matrix openRPCComplianceMatrix) map[string]struct{} {
	ids := map[string]struct{}{}
	add := func(watchlist string) {
		id, _, ok := strings.Cut(strings.TrimSpace(watchlist), ".")
		if ok && id != "" {
			ids[id] = struct{}{}
		}
	}
	for _, tracker := range matrix.ActiveTrackers {
		add(tracker.Watchlist)
	}
	for _, row := range matrix.Methods {
		for _, evidence := range complianceEvidenceFields(row) {
			for _, ref := range evidence.evidence.ProofRefs {
				if ref.Kind == "tracker" {
					add(ref.Watchlist)
				}
			}
		}
	}
	return ids
}

func loadWatchlistProofIndex(root string, ids map[string]struct{}) (watchlistProofIndex, error) {
	index := watchlistProofIndex{
		nodes:      map[string]struct{}{},
		issueLinks: map[string]struct{}{},
	}
	paths := watchlistPaths(root, ids)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return index, fmt.Errorf("read watchlist %s: %w", path, err)
		}
		var doc struct {
			ID           string `yaml:"id"`
			ActiveIssues []struct {
				ID     int      `yaml:"id"`
				MapsTo []string `yaml:"maps_to"`
			} `yaml:"active_issues"`
			Nodes []struct {
				ID string `yaml:"id"`
			} `yaml:"nodes"`
		}
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return index, fmt.Errorf("parse watchlist %s: %w", path, err)
		}
		watchlistID := strings.TrimSpace(doc.ID)
		for _, node := range doc.Nodes {
			nodeID := strings.TrimSpace(node.ID)
			if watchlistID != "" && nodeID != "" {
				index.nodes[watchlistID+"."+nodeID] = struct{}{}
			}
		}
		for _, issue := range doc.ActiveIssues {
			for _, nodeID := range issue.MapsTo {
				key := trackerKey(issue.ID, watchlistID+"."+strings.TrimSpace(nodeID))
				index.issueLinks[key] = struct{}{}
			}
		}
	}
	return index, nil
}

func watchlistPaths(root string, ids map[string]struct{}) []string {
	seen := map[string]struct{}{}
	var paths []string
	for id := range ids {
		for _, candidate := range []string{
			filepath.Join(root, "docs", "watchlists", id+".yaml"),
			filepath.Join(root, "docs", "watchlists", strings.ReplaceAll(id, "_", "-")+".yaml"),
		} {
			if _, ok := seen[candidate]; ok {
				continue
			}
			if _, err := os.Stat(candidate); err == nil {
				seen[candidate] = struct{}{}
				paths = append(paths, candidate)
			}
		}
	}
	sort.Strings(paths)
	return paths
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
		return complianceStringSet([]string{"covered"})
	case "service_discovery_publication":
		return complianceStringSet([]string{"published_without_discovery"})
	default:
		return map[string]struct{}{}
	}
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
