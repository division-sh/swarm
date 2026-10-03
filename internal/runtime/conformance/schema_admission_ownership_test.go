package conformance

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSchemaAdmissionOwnershipHasNoRetiredInterpreter(t *testing.T) {
	root := conformanceRepoRoot(t)
	assertExactYAMLFields(t, reflect.TypeOf(runtimecontracts.FlowSchemaDocument{}), []string{"activation", "auto_emit_on_create", "connect", "imports", "ingress", "instance", "instance_variables", "loops", "name", "pins", "required_agents", "stages"})
	retired := strings.Fields(`decodeReceiverInitialize decodeReceiverVariable validateFlowSchemaDocumentFields resolveHandlerRuleYAMLNode validateHandlerRuleYAMLAliasGraph validateFlowPinsNode validateFlowPinDirectionNode decodeFlowInputPinEventsNode decodeFlowOutputPinEventsNode decodeFlowInputPinEventNode decodeFlowOutputPinEventNode decodeExactFlowPinEvent validateExactW2MappingKeys decodeExactNonEmptyFlowPinScalar decodeFlowPinFieldNamesNode decodeExactFlowPinFieldSequence decodeStageGateOutcomes decodeStageGateInputFields validateUniqueNormalizedMappingKeys decodeEmitFieldsNode decodeExpressionValueMapNode decodeExpressionValueNode decodeExpressionContainer decodeLiteralExpressionNode validateKnownMappingFields decodeSchemaRefinementPattern decodeSchemaLengthRefinement decodeSchemaRangeRefinement decodeIntNode decodeFloatNode admitEventLengthRefinement admitEventRangeRefinement`)
	forbidden := map[string]bool{}
	for _, name := range retired {
		forbidden[name] = true
	}
	for _, name := range strings.Fields(`CompileFlowEntityPermissions FlowReadPins FlowWritePins WritePinOwners projectSchemaPinFieldsValue checkWritePinOwnershipValidation writePinOwnership wave1PinFieldName wave1RootFieldContract wave1FlowReadsRootField wave1FlowWritesRootField`) {
		forbidden[name] = true
	}
	var files []string
	for _, zone := range []string{"contracts", "semanticview", "bootverify"} {
		matches, err := filepath.Glob(filepath.Join(root, "internal/runtime", zone, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		assertNoRetiredSchemaInterpreter(t, path, forbidden)
	}
	loading, err := os.ReadFile(filepath.Join(root, "internal/runtime/contracts/workflow_contract_loading.go"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(loading, []byte("DecodeYAML(source.Schema")) || !bytes.Contains(loading, []byte("projectFlowSchemaValue(document.Root())")) {
		t.Fatal("schema loader bypasses authoritative Value root")
	}
	events, err := os.ReadFile(filepath.Join(root, "internal/runtime/contracts/event_catalog_admission.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"decodeSchemaLengthRefinementValue(", "decodeSchemaRangeRefinementValue("} {
		if !bytes.Contains(events, []byte(owner)) {
			t.Errorf("event bounds bypass shared owner %s", owner)
		}
	}
}

func assertNoRetiredSchemaInterpreter(t *testing.T, path string, forbidden map[string]bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && forbidden[fn.Name.Name] {
			t.Errorf("retired interpreter restored: %s in %s", fn.Name.Name, path)
		}
	}
	if strings.HasPrefix(filepath.Base(path), "workflow_contract_schema_") {
		for _, bypass := range []string{"yaml.Node", "yaml.Unmarshal", "ValueFromNode", "DecodeYAML"} {
			if bytes.Contains(data, []byte(bypass)) {
				t.Errorf("schema source bypass %s in %s", bypass, path)
			}
		}
	}
}
