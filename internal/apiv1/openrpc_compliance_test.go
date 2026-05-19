package apiv1

import (
	"encoding/json"
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
	Version int                   `yaml:"version"`
	Kind    string                `yaml:"kind"`
	Issue   int                   `yaml:"issue"`
	Methods []openRPCMethodMatrix `yaml:"methods"`
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
	Status string   `yaml:"status"`
	Proof  []string `yaml:"proof"`
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
	if len(doc.Methods) != 42 {
		t.Fatalf("generated OpenRPC methods = %d, want 42", len(doc.Methods))
	}
	if len(matrix.Methods) != len(doc.Methods) {
		t.Fatalf("matrix rows = %d, want generated method count %d", len(matrix.Methods), len(doc.Methods))
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
	if len(evidence.Proof) == 0 {
		t.Fatalf("%s %s proof must name the test, helper, issue, or artifact backing status %q", methodName, field, status)
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
