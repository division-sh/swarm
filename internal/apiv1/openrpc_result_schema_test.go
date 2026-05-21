package apiv1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/apispec"
)

const resultSchemaProbeTestName = "TestOpenRPCSuccessfulResultSchemas"
const notificationSchemaProbeTestName = "TestOpenRPCSubscriptionNotificationSchemas"

func TestOpenRPCSuccessfulResultSchemas(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	openRPC, _ := loadComplianceOpenRPC(t, filepath.Join(root, "docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json"))
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	methods := resultSchemaRuntimeMethods(t, api, openRPC, matrix)
	assertStringList(t, "successful result-schema method set", methods, approvedResultSchemaRuntimeMethods())
	assertResultSchemaMatrixProofRefs(t, matrix, methods)

	validator := newOpenRPCResultSchemaValidator(t, openRPC)
	validator.preflightMethodResultSchemas(t, methods)
	for _, methodName := range methods {
		methodName := methodName
		t.Run(methodName, func(t *testing.T) {
			result := successfulRuntimeResult(t, methodName)
			validator.validateMethodResult(t, methodName, result)
		})
	}
}

func TestOpenRPCResultSchemaPreflightRejectsUnreachableBranches(t *testing.T) {
	validator := newOpenRPCResultSchemaValidator(t, apispec.OpenRPCDocument{
		Methods: []apispec.OpenRPCMethod{{
			Name: "probe.unreachable",
			Result: &apispec.ContentDescriptor{
				Name: "result",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"empty_array": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type":                "object",
								"x-unsupported-probe": true,
							},
						},
					},
				},
			},
		}},
	})
	err := validator.preflightMethodResultSchema("probe.unreachable")
	if err == nil {
		t.Fatal("preflight accepted unsupported keyword inside an unpopulated array item schema")
	}
	if !strings.Contains(err.Error(), "x-unsupported-probe") {
		t.Fatalf("preflight error = %v, want unsupported keyword path", err)
	}
}

func TestOpenRPCSubscriptionNotificationSchemas(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	openRPC, _ := loadComplianceOpenRPC(t, filepath.Join(root, "docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json"))
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	methods := notificationSchemaRuntimeMethods(t, api, openRPC, matrix)
	assertStringList(t, "subscription notification-schema method set", methods, approvedNotificationSchemaRuntimeMethods())
	assertNotificationSchemaMatrixProofRefs(t, matrix, methods)

	validator := newOpenRPCResultSchemaValidator(t, openRPC)
	validator.preflightMethodNotificationSchemas(t, methods)
	for _, methodName := range methods {
		methodName := methodName
		t.Run(methodName, func(t *testing.T) {
			result := successfulWebSocketRuntimeNotificationResult(t, methodName)
			validator.validateMethodNotificationResult(t, methodName, result)
		})
	}
}

func resultSchemaRuntimeMethods(t *testing.T, api *apispec.APISpecification, openRPC apispec.OpenRPCDocument, matrix openRPCComplianceMatrix) []string {
	t.Helper()
	openRPCMethods := map[string]apispec.OpenRPCMethod{}
	for _, method := range openRPC.Methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			t.Fatal("generated OpenRPC method missing name")
		}
		if method.Result == nil {
			t.Fatalf("%s generated OpenRPC method missing result descriptor", name)
		}
		openRPCMethods[name] = method
	}
	rows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		rows[row.Method] = row
	}

	fixtures := complianceStringSet(approvedResultSchemaRuntimeMethods())
	out := make([]string, 0, len(openRPCMethods))
	for methodName, method := range api.MethodCatalog {
		if _, ok := openRPCMethods[methodName]; !ok {
			t.Fatalf("%s missing from generated OpenRPC artifact", methodName)
		}
		row, ok := rows[methodName]
		if !ok {
			t.Fatalf("%s missing from OpenRPC compliance matrix", methodName)
		}
		if row.Transport != expectedComplianceTransport(methodName, method) {
			t.Fatalf("%s matrix transport = %q, want %q", methodName, row.Transport, expectedComplianceTransport(methodName, method))
		}
		if _, ok := fixtures[methodName]; !ok {
			t.Fatalf("%s missing successful runtime result fixture", methodName)
		}
		out = append(out, methodName)
	}
	for methodName := range fixtures {
		if _, ok := api.MethodCatalog[methodName]; !ok {
			t.Fatalf("%s fixture absent from platform spec method_catalog", methodName)
		}
	}
	sort.Strings(out)
	return out
}

func approvedResultSchemaRuntimeMethods() []string {
	out := append([]string{}, approvedReadOnlyHTTPRuntimeMethods()...)
	out = append(out, approvedMutatingHTTPRuntimeMethods()...)
	out = append(out, approvedWebSocketRuntimeMethods()...)
	sort.Strings(out)
	return out
}

func notificationSchemaRuntimeMethods(t *testing.T, api *apispec.APISpecification, openRPC apispec.OpenRPCDocument, matrix openRPCComplianceMatrix) []string {
	t.Helper()
	openRPCMethods := map[string]apispec.OpenRPCMethod{}
	for _, method := range openRPC.Methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			t.Fatal("generated OpenRPC method missing name")
		}
		if _, exists := openRPCMethods[name]; exists {
			t.Fatalf("generated OpenRPC method %s appears more than once", name)
		}
		openRPCMethods[name] = method
	}
	rows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		rows[row.Method] = row
	}

	fixtures := complianceStringSet(approvedNotificationSchemaRuntimeMethods())
	out := make([]string, 0, len(fixtures))
	for methodName, method := range api.MethodCatalog {
		openRPCMethod, ok := openRPCMethods[methodName]
		if !ok {
			t.Fatalf("%s missing from generated OpenRPC artifact", methodName)
		}
		if _, ok := rows[methodName]; !ok {
			t.Fatalf("%s missing from OpenRPC compliance matrix", methodName)
		}
		if method.NotificationSchema == nil {
			if openRPCMethod.NotificationSchema != nil {
				t.Fatalf("%s unexpectedly publishes notification_schema: %s", methodName, compactJSON(openRPCMethod.NotificationSchema))
			}
			continue
		}
		if _, ok := fixtures[methodName]; !ok {
			t.Fatalf("%s declares notification_schema but is outside the approved notification-schema runtime set", methodName)
		}
		if openRPCMethod.NotificationSchema == nil {
			t.Fatalf("%s generated OpenRPC method missing notification_schema", methodName)
		}
		if !jsonValueEqual(method.NotificationSchema, openRPCMethod.NotificationSchema) {
			t.Fatalf("%s generated notification_schema = %s, want platform spec schema %s", methodName, compactJSON(openRPCMethod.NotificationSchema), compactJSON(method.NotificationSchema))
		}
		out = append(out, methodName)
	}
	for methodName := range fixtures {
		method, ok := api.MethodCatalog[methodName]
		if !ok {
			t.Fatalf("%s fixture absent from platform spec method_catalog", methodName)
		}
		if method.NotificationSchema == nil {
			t.Fatalf("%s fixture has no platform notification_schema", methodName)
		}
	}
	sort.Strings(out)
	return out
}

func approvedNotificationSchemaRuntimeMethods() []string {
	return []string{
		"event.subscribe",
		"health.subscribe",
		"run.subscribe_trace",
		"runtime.subscribe_logs",
	}
}

func assertResultSchemaMatrixProofRefs(t *testing.T, matrix openRPCComplianceMatrix, methods []string) {
	t.Helper()
	methodSet := complianceStringSet(methods)
	for _, row := range matrix.Methods {
		_, inScope := methodSet[row.Method]
		if !inScope && evidenceHasGoTest(row.ResultSchema, resultSchemaProbeTestName) {
			t.Fatalf("%s has %s result_schema proof_ref but is outside the approved result-schema set", row.Method, resultSchemaProbeTestName)
		}
		if !inScope {
			continue
		}
		if row.ResultSchema.Status != "covered" {
			t.Fatalf("%s result_schema status = %q, want covered", row.Method, row.ResultSchema.Status)
		}
		if !evidenceHasGoTest(row.ResultSchema, resultSchemaProbeTestName) {
			t.Fatalf("%s result_schema missing go_test proof_ref %s", row.Method, resultSchemaProbeTestName)
		}
		for _, gap := range row.GapClassification {
			if gap == "missing_generated_result_schema_validation" {
				t.Fatalf("%s still carries missing_generated_result_schema_validation after generated result-schema proof", row.Method)
			}
		}
	}
}

func assertNotificationSchemaMatrixProofRefs(t *testing.T, matrix openRPCComplianceMatrix, methods []string) {
	t.Helper()
	methodSet := complianceStringSet(methods)
	for _, row := range matrix.Methods {
		_, inScope := methodSet[row.Method]
		if !inScope && evidenceHasGoTest(row.NotificationSchema, notificationSchemaProbeTestName) {
			t.Fatalf("%s has %s notification_schema proof_ref but is outside the approved notification-schema set", row.Method, notificationSchemaProbeTestName)
		}
		if !inScope {
			continue
		}
		if row.NotificationSchema.Status != "covered" {
			t.Fatalf("%s notification_schema status = %q, want covered", row.Method, row.NotificationSchema.Status)
		}
		if !evidenceHasGoTest(row.NotificationSchema, notificationSchemaProbeTestName) {
			t.Fatalf("%s notification_schema missing go_test proof_ref %s", row.Method, notificationSchemaProbeTestName)
		}
		for _, gap := range row.GapClassification {
			if gap == "notification_schema_not_emitted_in_openrpc_method" {
				t.Fatalf("%s still carries notification_schema_not_emitted_in_openrpc_method after generated notification-schema proof", row.Method)
			}
		}
	}
}

func successfulRuntimeResult(t *testing.T, methodName string) any {
	t.Helper()
	switch {
	case containsString(approvedReadOnlyHTTPRuntimeMethods(), methodName):
		fixture := readOnlyHTTPRuntimeFixtures()[methodName]
		handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
		status, resp, body := callReadOnlyProbeRPC(t, handler, methodName, fixture.Params, "Bearer "+testToken)
		assertSuccessfulResultProbeResponse(t, methodName, status, resp, body)
		if calls[methodName] != 1 {
			t.Fatalf("%s handler calls = %d, want 1", methodName, calls[methodName])
		}
		return resp.Result
	case containsString(approvedMutatingHTTPRuntimeMethods(), methodName):
		fixture := mutatingHTTPRuntimeFixtures()[methodName]
		handler, calls, _ := newMutatingRuntimeProbeHandler(t, methodName)
		key := "schema-" + strings.ReplaceAll(methodName, ".", "-")
		params := mutatingProbeParamsWithIdempotency(fixture.Params, key)
		status, resp, body := callMutatingProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
		assertSuccessfulResultProbeResponse(t, methodName, status, resp, body)
		if calls[methodName] != 1 {
			t.Fatalf("%s handler calls = %d, want 1", methodName, calls[methodName])
		}
		return resp.Result
	case containsString(approvedWebSocketRuntimeMethods(), methodName):
		return successfulWebSocketRuntimeResult(t, methodName)
	default:
		t.Fatalf("%s has no successful runtime result producer", methodName)
		return nil
	}
}

func successfulWebSocketRuntimeResult(t *testing.T, methodName string) any {
	t.Helper()
	base := time.Unix(1700001400, 0).UTC()
	handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	if methodName == "rpc.unsubscribe" {
		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      "schema-sub-for-unsubscribe",
			"method":  "health.subscribe",
			"params":  map[string]any{},
		})
		subscribe := readWSResponse(t, conn)
		subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, "health.subscribe", subscribe)
		notification := readWSNotification(t, conn)
		assertWebSocketRuntimeNotification(t, "health.subscribe", subscriptionID, notification)
		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      webSocketRuntimeProbeID("rpc.unsubscribe"),
			"method":  "rpc.unsubscribe",
			"params":  map[string]any{"subscription_id": subscriptionID},
		})
		resp := readWSResponse(t, conn)
		assertSuccessfulResultProbeResponse(t, methodName, http.StatusOK, resp, "")
		return resp.Result
	}

	fixture := webSocketRuntimeProbeFixtures(base)[methodName]
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      webSocketRuntimeProbeID(methodName),
		"method":  methodName,
		"params":  fixture.Params,
	})
	resp := readWSResponse(t, conn)
	assertSuccessfulResultProbeResponse(t, methodName, http.StatusOK, resp, "")
	return resp.Result
}

func successfulWebSocketRuntimeNotificationResult(t *testing.T, methodName string) any {
	t.Helper()
	base := time.Unix(1700001400, 0).UTC()
	handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	fixture, ok := webSocketRuntimeProbeFixtures(base)[methodName]
	if !ok {
		t.Fatalf("%s missing websocket notification runtime fixture", methodName)
	}
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      webSocketRuntimeProbeID(methodName),
		"method":  methodName,
		"params":  fixture.Params,
	})
	resp := readWSResponse(t, conn)
	subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, methodName, resp)
	notification := readWSNotification(t, conn)
	assertWebSocketRuntimeNotification(t, methodName, subscriptionID, notification)
	return notification.Params.Result
}

func assertSuccessfulResultProbeResponse(t *testing.T, methodName string, status int, resp rpcResponse, body string) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("%s status = %d, want 200 body=%s", methodName, status, body)
	}
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.Error != nil {
		t.Fatalf("%s error = %#v, want success", methodName, resp.Error)
	}
}

type openRPCResultSchemaValidator struct {
	methods    map[string]apispec.OpenRPCMethod
	components map[string]any
}

func newOpenRPCResultSchemaValidator(t *testing.T, doc apispec.OpenRPCDocument) openRPCResultSchemaValidator {
	t.Helper()
	methods := map[string]apispec.OpenRPCMethod{}
	for _, method := range doc.Methods {
		methods[strings.TrimSpace(method.Name)] = method
	}
	return openRPCResultSchemaValidator{
		methods:    methods,
		components: doc.Components.Schemas,
	}
}

func (v openRPCResultSchemaValidator) preflightMethodResultSchemas(t *testing.T, methods []string) {
	t.Helper()
	for _, methodName := range methods {
		if err := v.preflightMethodResultSchema(methodName); err != nil {
			t.Fatalf("%s generated OpenRPC result schema preflight failed: %v", methodName, err)
		}
	}
}

func (v openRPCResultSchemaValidator) preflightMethodResultSchema(methodName string) error {
	method, ok := v.methods[methodName]
	if !ok {
		return fmt.Errorf("%s missing from generated OpenRPC methods", methodName)
	}
	if method.Result == nil {
		return fmt.Errorf("%s missing generated OpenRPC result descriptor", methodName)
	}
	return v.preflightSchema("$."+methodName+".result", method.Result.Schema, map[string]bool{})
}

func (v openRPCResultSchemaValidator) preflightMethodNotificationSchemas(t *testing.T, methods []string) {
	t.Helper()
	for _, methodName := range methods {
		if err := v.preflightMethodNotificationSchema(methodName); err != nil {
			t.Fatalf("%s generated OpenRPC notification schema preflight failed: %v", methodName, err)
		}
	}
}

func (v openRPCResultSchemaValidator) preflightMethodNotificationSchema(methodName string) error {
	method, ok := v.methods[methodName]
	if !ok {
		return fmt.Errorf("%s missing from generated OpenRPC methods", methodName)
	}
	if method.NotificationSchema == nil {
		return fmt.Errorf("%s missing generated OpenRPC notification_schema", methodName)
	}
	return v.preflightSchema("$."+methodName+".notification_schema", method.NotificationSchema, map[string]bool{})
}

func (v openRPCResultSchemaValidator) preflightSchema(path string, schema any, refStack map[string]bool) error {
	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return fmt.Errorf("%s schema is %T, want object", path, schema)
	}
	if err := validateOpenRPCSchemaKeys(path, schemaMap); err != nil {
		return err
	}
	if rawRef, ok := schemaMap["$ref"]; ok {
		ref, ok := rawRef.(string)
		if !ok || strings.TrimSpace(ref) == "" {
			return fmt.Errorf("%s $ref must be a non-empty string", path)
		}
		if hasUnsupportedRefSiblings(schemaMap) {
			return fmt.Errorf("%s $ref schema has unsupported validation siblings", path)
		}
		if refStack[ref] {
			return fmt.Errorf("%s schema ref %q is recursive; recursive result schemas are unsupported by this proof", path, ref)
		}
		resolved, err := v.resolveRef(path, ref)
		if err != nil {
			return err
		}
		refStack[ref] = true
		defer delete(refStack, ref)
		return v.preflightSchema(path+"."+strings.TrimPrefix(ref, "#/components/schemas/"), resolved, refStack)
	}
	if rawOneOf, ok := schemaMap["oneOf"]; ok {
		return v.preflightOneOf(path, rawOneOf, refStack)
	}
	if rawEnum, ok := schemaMap["enum"]; ok {
		if _, ok := rawEnum.([]any); !ok {
			return fmt.Errorf("%s.enum is %T, want array", path, rawEnum)
		}
	}

	schemaType, err := schemaTypeForValidation(path, schemaMap)
	if err != nil {
		return err
	}
	if schemaType == "" {
		return nil
	}
	if err := validateOpenRPCSchemaKeywordPlacement(path, schemaMap, schemaType); err != nil {
		return err
	}

	switch schemaType {
	case "object":
		return v.preflightObjectSchema(path, schemaMap, refStack)
	case "array":
		return v.preflightArraySchema(path, schemaMap, refStack)
	case "string":
		return validateOpenRPCStringSchema(path, schemaMap)
	case "integer", "number":
		return validateOpenRPCNumberSchema(path, schemaMap)
	case "boolean", "null":
		return nil
	default:
		return fmt.Errorf("%s has unsupported schema type %q", path, schemaType)
	}
}

func (v openRPCResultSchemaValidator) preflightObjectSchema(path string, schema map[string]any, refStack map[string]bool) error {
	props, err := schemaPropertySchemas(path, schema["properties"])
	if err != nil {
		return err
	}
	if _, err := schemaRequiredList(path, schema["required"]); err != nil {
		return err
	}
	_, additionalSchema, err := schemaAdditionalProperties(path, schema["additionalProperties"])
	if err != nil {
		return err
	}
	if additionalSchema != nil {
		if err := v.preflightSchema(path+".additionalProperties", additionalSchema, refStack); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := v.preflightSchema(path+".properties."+name, props[name], refStack); err != nil {
			return err
		}
	}
	return nil
}

func (v openRPCResultSchemaValidator) preflightArraySchema(path string, schema map[string]any, refStack map[string]bool) error {
	itemSchema, hasItems := schema["items"]
	if !hasItems {
		return nil
	}
	if _, ok := itemSchema.(map[string]any); !ok {
		return fmt.Errorf("%s.items schema is %T, want object", path, itemSchema)
	}
	return v.preflightSchema(path+".items", itemSchema, refStack)
}

func (v openRPCResultSchemaValidator) preflightOneOf(path string, raw any, refStack map[string]bool) error {
	options, ok := raw.([]any)
	if !ok || len(options) == 0 {
		return fmt.Errorf("%s oneOf must be a non-empty array", path)
	}
	for i, option := range options {
		if _, ok := option.(map[string]any); !ok {
			return fmt.Errorf("%s oneOf[%d] schema is %T, want object", path, i, option)
		}
		if err := v.preflightSchema(fmt.Sprintf("%s.oneOf[%d]", path, i), option, refStack); err != nil {
			return err
		}
	}
	return nil
}

func (v openRPCResultSchemaValidator) validateMethodResult(t *testing.T, methodName string, result any) {
	t.Helper()
	method, ok := v.methods[methodName]
	if !ok {
		t.Fatalf("%s missing from generated OpenRPC methods", methodName)
	}
	if method.Result == nil {
		t.Fatalf("%s missing generated OpenRPC result descriptor", methodName)
	}
	if err := v.validateValue("$."+methodName+".result", method.Result.Schema, result); err != nil {
		t.Fatalf("%s result does not match generated OpenRPC schema: %v\nresult=%s", methodName, err, compactJSON(result))
	}
}

func (v openRPCResultSchemaValidator) validateMethodNotificationResult(t *testing.T, methodName string, result any) {
	t.Helper()
	method, ok := v.methods[methodName]
	if !ok {
		t.Fatalf("%s missing from generated OpenRPC methods", methodName)
	}
	if method.NotificationSchema == nil {
		t.Fatalf("%s missing generated OpenRPC notification_schema", methodName)
	}
	if err := v.validateValue("$."+methodName+".notification_schema", method.NotificationSchema, result); err != nil {
		t.Fatalf("%s notification result does not match generated OpenRPC notification_schema: %v\nresult=%s", methodName, err, compactJSON(result))
	}
}

func (v openRPCResultSchemaValidator) validateValue(path string, schema any, value any) error {
	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return fmt.Errorf("%s schema is %T, want object", path, schema)
	}
	if err := validateOpenRPCSchemaKeys(path, schemaMap); err != nil {
		return err
	}
	if rawRef, ok := schemaMap["$ref"]; ok {
		ref, ok := rawRef.(string)
		if !ok || strings.TrimSpace(ref) == "" {
			return fmt.Errorf("%s $ref must be a non-empty string", path)
		}
		if hasUnsupportedRefSiblings(schemaMap) {
			return fmt.Errorf("%s $ref schema has unsupported validation siblings", path)
		}
		resolved, err := v.resolveRef(path, ref)
		if err != nil {
			return err
		}
		return v.validateValue(path, resolved, value)
	}
	if rawOneOf, ok := schemaMap["oneOf"]; ok {
		return v.validateOneOf(path, rawOneOf, value)
	}
	if rawConst, ok := schemaMap["const"]; ok && !jsonValueEqual(value, rawConst) {
		return fmt.Errorf("%s must equal const %s", path, compactJSON(rawConst))
	}
	if rawEnum, ok := schemaMap["enum"]; ok {
		if !jsonValueInEnum(value, rawEnum) {
			return fmt.Errorf("%s value %s is not in enum %s", path, compactJSON(value), compactJSON(rawEnum))
		}
	}

	schemaType, err := schemaTypeForValidation(path, schemaMap)
	if err != nil {
		return err
	}
	if schemaType == "" {
		return nil
	}
	if err := validateOpenRPCSchemaKeywordPlacement(path, schemaMap, schemaType); err != nil {
		return err
	}

	switch schemaType {
	case "object":
		return v.validateObject(path, schemaMap, value)
	case "array":
		return v.validateArray(path, schemaMap, value)
	case "string":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s must be string, got %T", path, value)
		}
		return validateOpenRPCString(path, schemaMap, text)
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be boolean, got %T", path, value)
		}
	case "integer":
		if !isSchemaInteger(value) {
			return fmt.Errorf("%s must be integer, got %s", path, compactJSON(value))
		}
		return validateOpenRPCNumber(path, schemaMap, value)
	case "number":
		if !isSchemaNumber(value) {
			return fmt.Errorf("%s must be number, got %T", path, value)
		}
		return validateOpenRPCNumber(path, schemaMap, value)
	case "null":
		if value != nil {
			return fmt.Errorf("%s must be null, got %T", path, value)
		}
	default:
		return fmt.Errorf("%s has unsupported schema type %q", path, schemaType)
	}
	return nil
}

func (v openRPCResultSchemaValidator) validateObject(path string, schema map[string]any, value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be object, got %T", path, value)
	}
	props, err := schemaPropertySchemas(path, schema["properties"])
	if err != nil {
		return err
	}
	required, err := schemaRequiredList(path, schema["required"])
	if err != nil {
		return err
	}
	for _, name := range required {
		if _, ok := obj[name]; !ok {
			return fmt.Errorf("%s.%s is required", path, name)
		}
	}
	additional, additionalSchema, err := schemaAdditionalProperties(path, schema["additionalProperties"])
	if err != nil {
		return err
	}
	for name, propValue := range obj {
		propSchema, ok := props[name]
		if !ok {
			if !additional {
				return fmt.Errorf("%s.%s is not allowed", path, name)
			}
			if additionalSchema != nil {
				if err := v.validateValue(path+"."+name, additionalSchema, propValue); err != nil {
					return err
				}
			}
			continue
		}
		if err := v.validateValue(path+"."+name, propSchema, propValue); err != nil {
			return err
		}
	}
	return nil
}

func (v openRPCResultSchemaValidator) validateArray(path string, schema map[string]any, value any) error {
	items, ok := value.([]any)
	if !ok {
		return fmt.Errorf("%s must be array, got %T", path, value)
	}
	itemSchema, hasItems := schema["items"]
	if !hasItems {
		return nil
	}
	if _, ok := itemSchema.(map[string]any); !ok {
		return fmt.Errorf("%s.items schema is %T, want object", path, itemSchema)
	}
	for i, item := range items {
		if err := v.validateValue(fmt.Sprintf("%s[%d]", path, i), itemSchema, item); err != nil {
			return err
		}
	}
	return nil
}

func (v openRPCResultSchemaValidator) validateOneOf(path string, raw any, value any) error {
	options, ok := raw.([]any)
	if !ok || len(options) == 0 {
		return fmt.Errorf("%s oneOf must be a non-empty array", path)
	}
	matches := 0
	var failures []string
	for i, option := range options {
		if _, ok := option.(map[string]any); !ok {
			return fmt.Errorf("%s oneOf[%d] schema is %T, want object", path, i, option)
		}
		if err := v.validateValue(fmt.Sprintf("%s.oneOf[%d]", path, i), option, value); err != nil {
			failures = append(failures, err.Error())
			continue
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("%s matched %d oneOf schemas, want exactly 1; failures=%s", path, matches, strings.Join(failures, " | "))
	}
	return nil
}

func (v openRPCResultSchemaValidator) resolveRef(path, ref string) (any, error) {
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(ref, prefix) {
		return nil, fmt.Errorf("%s has unsupported schema ref %q", path, ref)
	}
	name := strings.TrimPrefix(ref, prefix)
	schema, ok := v.components[name]
	if !ok {
		return nil, fmt.Errorf("%s schema ref %q targets missing component", path, ref)
	}
	if _, ok := schema.(map[string]any); !ok {
		return nil, fmt.Errorf("%s schema ref %q resolved to %T, want object", path, ref, schema)
	}
	return schema, nil
}

func validateOpenRPCSchemaKeys(path string, schema map[string]any) error {
	allowed := complianceStringSet([]string{
		"$ref",
		"additionalProperties",
		"const",
		"default",
		"description",
		"enum",
		"format",
		"items",
		"maxLength",
		"maximum",
		"minLength",
		"minimum",
		"oneOf",
		"pattern",
		"properties",
		"required",
		"type",
	})
	for key := range schema {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s schema uses unsupported keyword %q", path, key)
		}
	}
	return nil
}

func hasUnsupportedRefSiblings(schema map[string]any) bool {
	for key := range schema {
		switch key {
		case "$ref", "description":
			continue
		default:
			return true
		}
	}
	return false
}

func schemaTypeForValidation(path string, schema map[string]any) (string, error) {
	if rawType, ok := schema["type"]; ok {
		schemaType, ok := rawType.(string)
		if !ok || strings.TrimSpace(schemaType) == "" {
			return "", fmt.Errorf("%s.type is %T, want non-empty string", path, rawType)
		}
		return strings.TrimSpace(schemaType), nil
	}
	switch {
	case schema["properties"] != nil || schema["required"] != nil || schema["additionalProperties"] != nil:
		return "object", nil
	case schema["items"] != nil:
		return "array", nil
	case schema["const"] != nil || schema["enum"] != nil:
		return "", nil
	default:
		return "", fmt.Errorf("%s schema missing supported type/$ref/oneOf", path)
	}
}

func validateOpenRPCSchemaKeywordPlacement(path string, schema map[string]any, schemaType string) error {
	if schema["items"] != nil && schemaType != "array" {
		return fmt.Errorf("%s.items is only supported on array schemas", path)
	}
	if schemaType != "object" {
		for _, key := range []string{"properties", "required", "additionalProperties"} {
			if schema[key] != nil {
				return fmt.Errorf("%s.%s is only supported on object schemas", path, key)
			}
		}
	}
	if schemaType != "string" {
		for _, key := range []string{"minLength", "maxLength", "pattern", "format"} {
			if schema[key] != nil {
				return fmt.Errorf("%s.%s is only supported on string schemas", path, key)
			}
		}
	}
	if schemaType != "integer" && schemaType != "number" {
		for _, key := range []string{"minimum", "maximum"} {
			if schema[key] != nil {
				return fmt.Errorf("%s.%s is only supported on numeric schemas", path, key)
			}
		}
	}
	return nil
}

func schemaPropertySchemas(path string, raw any) (map[string]any, error) {
	if raw == nil {
		return map[string]any{}, nil
	}
	props, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.properties is %T, want object", path, raw)
	}
	out := make(map[string]any, len(props))
	for name, schema := range props {
		if _, ok := schema.(map[string]any); !ok {
			return nil, fmt.Errorf("%s.properties.%s is %T, want object", path, name, schema)
		}
		out[name] = schema
	}
	return out, nil
}

func schemaRequiredList(path string, raw any) ([]string, error) {
	switch typed := raw.(type) {
	case nil:
		return nil, nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s.required contains %T, want string", path, item)
			}
			out = append(out, text)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s.required is %T, want array", path, raw)
	}
}

func schemaAdditionalProperties(path string, raw any) (bool, any, error) {
	switch typed := raw.(type) {
	case nil:
		return true, nil, nil
	case bool:
		return typed, nil, nil
	case map[string]any:
		return true, typed, nil
	default:
		return false, nil, fmt.Errorf("%s.additionalProperties is %T, want bool or schema object", path, raw)
	}
}

func validateOpenRPCStringSchema(path string, schema map[string]any) error {
	if minRaw, ok := schema["minLength"]; ok {
		if _, ok := schemaNumber(minRaw); !ok {
			return fmt.Errorf("%s minLength is %T, want number", path, minRaw)
		}
	}
	if maxRaw, ok := schema["maxLength"]; ok {
		if _, ok := schemaNumber(maxRaw); !ok {
			return fmt.Errorf("%s maxLength is %T, want number", path, maxRaw)
		}
	}
	if pattern, ok := schema["pattern"].(string); ok && strings.TrimSpace(pattern) != "" {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("%s pattern %q does not compile: %w", path, pattern, err)
		}
	} else if rawPattern, ok := schema["pattern"]; ok {
		return fmt.Errorf("%s pattern is %T, want string", path, rawPattern)
	}
	if format, ok := schema["format"].(string); ok {
		switch strings.TrimSpace(format) {
		case "", "date-time", "uuid":
		default:
			return fmt.Errorf("%s uses unsupported string format %q", path, format)
		}
	} else if rawFormat, ok := schema["format"]; ok {
		return fmt.Errorf("%s format is %T, want string", path, rawFormat)
	}
	return nil
}

func validateOpenRPCString(path string, schema map[string]any, value string) error {
	if err := validateOpenRPCStringSchema(path, schema); err != nil {
		return err
	}
	if minRaw, ok := schema["minLength"]; ok {
		min, ok := schemaNumber(minRaw)
		if !ok {
			return fmt.Errorf("%s minLength is %T, want number", path, minRaw)
		}
		if len([]rune(value)) < int(min) {
			return fmt.Errorf("%s length must be >= %d", path, int(min))
		}
	}
	if maxRaw, ok := schema["maxLength"]; ok {
		max, ok := schemaNumber(maxRaw)
		if !ok {
			return fmt.Errorf("%s maxLength is %T, want number", path, maxRaw)
		}
		if len([]rune(value)) > int(max) {
			return fmt.Errorf("%s length must be <= %d", path, int(max))
		}
	}
	if pattern, ok := schema["pattern"].(string); ok && strings.TrimSpace(pattern) != "" {
		matched, err := regexp.MatchString(pattern, value)
		if err != nil {
			return fmt.Errorf("%s pattern %q does not compile: %w", path, pattern, err)
		}
		if !matched {
			return fmt.Errorf("%s must match pattern %q", path, pattern)
		}
	}
	if format, ok := schema["format"].(string); ok {
		switch strings.TrimSpace(format) {
		case "":
		case "date-time":
			if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err != nil {
				return fmt.Errorf("%s must be RFC3339 date-time: %w", path, err)
			}
		case "uuid":
			if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
				return fmt.Errorf("%s must be uuid: %w", path, err)
			}
		default:
			return fmt.Errorf("%s uses unsupported string format %q", path, format)
		}
	}
	return nil
}

func validateOpenRPCNumberSchema(path string, schema map[string]any) error {
	if minRaw, ok := schema["minimum"]; ok {
		if _, ok := schemaNumber(minRaw); !ok {
			return fmt.Errorf("%s minimum is %T, want number", path, minRaw)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		if _, ok := schemaNumber(maxRaw); !ok {
			return fmt.Errorf("%s maximum is %T, want number", path, maxRaw)
		}
	}
	return nil
}

func validateOpenRPCNumber(path string, schema map[string]any, value any) error {
	if err := validateOpenRPCNumberSchema(path, schema); err != nil {
		return err
	}
	number, ok := schemaNumber(value)
	if !ok {
		return fmt.Errorf("%s value is %T, want number", path, value)
	}
	if minRaw, ok := schema["minimum"]; ok {
		min, ok := schemaNumber(minRaw)
		if !ok {
			return fmt.Errorf("%s minimum is %T, want number", path, minRaw)
		}
		if number < min {
			return fmt.Errorf("%s must be >= %v", path, min)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		max, ok := schemaNumber(maxRaw)
		if !ok {
			return fmt.Errorf("%s maximum is %T, want number", path, maxRaw)
		}
		if number > max {
			return fmt.Errorf("%s must be <= %v", path, max)
		}
	}
	return nil
}

func isSchemaNumber(value any) bool {
	_, ok := schemaNumber(value)
	return ok
}

func isSchemaInteger(value any) bool {
	number, ok := schemaNumber(value)
	return ok && number == float64(int64(number))
}

func schemaNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case json.Number:
		out, err := typed.Float64()
		return out, err == nil
	default:
		return 0, false
	}
}

func jsonValueInEnum(value any, raw any) bool {
	options, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, option := range options {
		if jsonValueEqual(value, option) {
			return true
		}
	}
	return false
}

func jsonValueEqual(a, b any) bool {
	var normalizedA any
	var normalizedB any
	if err := json.Unmarshal([]byte(compactJSON(a)), &normalizedA); err != nil {
		return reflect.DeepEqual(a, b)
	}
	if err := json.Unmarshal([]byte(compactJSON(b)), &normalizedB); err != nil {
		return reflect.DeepEqual(a, b)
	}
	return reflect.DeepEqual(normalizedA, normalizedB)
}

func compactJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(raw)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
