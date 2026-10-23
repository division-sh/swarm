package apiv1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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
	if len(doc.Methods) != 73 {
		t.Fatalf("generated OpenRPC methods = %d, want 73", len(doc.Methods))
	}
	if len(matrix.Methods) != len(doc.Methods) {
		t.Fatalf("matrix rows = %d, want generated method count %d", len(matrix.Methods), len(doc.Methods))
	}
	assertExamplesPolicyDeferred(t, api.ExamplesPolicy)
	assertServiceDiscoveryPolicyNotPublished(t, api.ServiceDiscoveryPolicy)

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

		assertExamplesPolicyMatrixRow(t, methodName, rawMethods[methodName], row.Examples)
		assertServiceDiscoveryPolicyMatrixRow(t, methodName, api.ServiceDiscoveryPolicy, row.ServiceDiscoveryPublication)
	}

	for methodName := range rowsByName {
		if _, ok := openRPCByName[methodName]; !ok {
			t.Fatalf("matrix has method %s absent from generated OpenRPC", methodName)
		}
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
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))

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

func trackerKey(issue int, watchlist string) string {
	return fmt.Sprintf("%d:%s", issue, strings.TrimSpace(watchlist))
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

func rawJSONHasContent(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null" && trimmed != "[]"
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
