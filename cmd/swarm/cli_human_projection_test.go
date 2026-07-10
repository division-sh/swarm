package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/userfacing"
	"gopkg.in/yaml.v3"
)

func TestCLIHumanCodePhrasesMatchCurrentCanonicalValues(t *testing.T) {
	want := cliHumanCodePhrasesFromSpec(t, loadCLIHumanCodeProjectionSpec(t))
	if !reflect.DeepEqual(cliHumanCodePhrases, want) {
		t.Fatalf("human code phrase registry differs from authoritative platform spec:\ngot:  %#v\nwant: %#v", cliHumanCodePhrases, want)
	}
}

func TestCLIHumanCodePhraseParityRejectsSpecOnlyDrift(t *testing.T) {
	projection := loadCLIHumanCodeProjectionSpec(t)
	families := driftMappingValue(projection, "families")
	conversationMode := driftMappingValue(families, "conversation_mode")
	phrases := driftMappingValue(conversationMode, "phrases")
	driftMappingValue(phrases, "session_per_entity").Value = "one session per entity"

	if got := cliHumanCodePhrasesFromSpec(t, projection); reflect.DeepEqual(cliHumanCodePhrases, got) {
		t.Fatal("spec-only phrase drift did not break implementation parity")
	}
}

func loadCLIHumanCodeProjectionSpec(t *testing.T) *yaml.Node {
	t.Helper()
	cli := loadCLISpecification(t)
	foundations := driftMappingValue(cli, "foundations")
	outputContract := driftMappingValue(foundations, "output_contract")
	sharedRenderer := driftMappingValue(outputContract, "shared_renderer_contract")
	projection := driftMappingValue(sharedRenderer, "human_code_projection")
	if projection == nil {
		t.Fatal("platform spec is missing human_code_projection")
	}
	return projection
}

func cliHumanCodePhrasesFromSpec(t *testing.T, projection *yaml.Node) map[cliHumanCodeFamily]map[string]string {
	t.Helper()
	out := map[cliHumanCodeFamily]map[string]string{}
	families := driftMappingValue(projection, "families")
	if families == nil {
		t.Fatal("human_code_projection is missing families")
	}
	knownSpecFamilies := map[string]bool{
		"run_status": true, "operational_state": true, "agent_status": true,
		"conversation_mode": true, "session_scope": true, "delivery_status": true,
		"run_blocking_tuples": true, "agent_lifecycle_tuples": true, "watchdog_tuples": true,
	}
	forEachYAMLMappingNode(t, families, func(name string, _ *yaml.Node) {
		if !knownSpecFamilies[name] {
			t.Fatalf("human_code_projection has unclassified family %q", name)
		}
	})

	for specName, family := range map[string]cliHumanCodeFamily{
		"run_status":        cliHumanCodeRunStatus,
		"operational_state": cliHumanCodeOperationalState,
		"agent_status":      cliHumanCodeAgentStatus,
		"conversation_mode": cliHumanCodeConversationMode,
		"session_scope":     cliHumanCodeSessionScope,
		"delivery_status":   cliHumanCodeDeliveryStatus,
	} {
		phrases := driftMappingValue(driftMappingValue(families, specName), "phrases")
		forEachYAMLMapping(t, phrases, func(code, phrase string) {
			addCLIHumanCodeSpecPhrase(t, out, family, code, phrase)
		})
	}

	forEachYAMLSequence(t, driftMappingValue(driftMappingValue(families, "run_blocking_tuples"), "current_non_empty"), func(row *yaml.Node) {
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeRunBlockingLayer, yamlScalar(t, row, "blocking_layer"), yamlScalar(t, row, "layer_phrase"))
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeRunBlockingReason, yamlScalar(t, row, "blocking_reason"), yamlScalar(t, row, "reason_phrase"))
	})
	forEachYAMLSequence(t, driftMappingValue(driftMappingValue(families, "agent_lifecycle_tuples"), "current"), func(row *yaml.Node) {
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeAgentLifecycleState, yamlScalar(t, row, "state"), yamlScalar(t, row, "state_phrase"))
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeAgentLifecycleBlockingLayer, yamlScalar(t, row, "blocking_layer"), yamlScalar(t, row, "layer_phrase"))
	})
	forEachYAMLSequence(t, driftMappingValue(driftMappingValue(families, "watchdog_tuples"), "current"), func(row *yaml.Node) {
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeWatchdogState, yamlScalar(t, row, "state"), yamlScalar(t, row, "state_phrase"))
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeWatchdogBlockingLayer, yamlScalar(t, row, "blocking_layer"), yamlScalar(t, row, "layer_phrase"))
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeWatchdogAction, yamlScalar(t, row, "action"), yamlScalar(t, row, "action_phrase"))
		addCLIHumanCodeSpecPhrase(t, out, cliHumanCodeWatchdogOutcome, yamlScalar(t, row, "outcome"), yamlScalar(t, row, "outcome_phrase"))
	})
	return out
}

func addCLIHumanCodeSpecPhrase(t *testing.T, target map[cliHumanCodeFamily]map[string]string, family cliHumanCodeFamily, code, phrase string) {
	t.Helper()
	if target[family] == nil {
		target[family] = map[string]string{}
	}
	if prior, exists := target[family][code]; exists && prior != phrase {
		t.Fatalf("spec family %s gives code %q conflicting phrases %q and %q", family, code, prior, phrase)
	}
	target[family][code] = phrase
}

func forEachYAMLMapping(t *testing.T, node *yaml.Node, visit func(key, value string)) {
	t.Helper()
	if node == nil || node.Kind != yaml.MappingNode {
		t.Fatalf("YAML node kind = %v, want mapping", yamlNodeKind(node))
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		visit(node.Content[i].Value, node.Content[i+1].Value)
	}
}

func forEachYAMLMappingNode(t *testing.T, node *yaml.Node, visit func(key string, value *yaml.Node)) {
	t.Helper()
	if node == nil || node.Kind != yaml.MappingNode {
		t.Fatalf("YAML node kind = %v, want mapping", yamlNodeKind(node))
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		visit(node.Content[i].Value, node.Content[i+1])
	}
}

func forEachYAMLSequence(t *testing.T, node *yaml.Node, visit func(*yaml.Node)) {
	t.Helper()
	if node == nil || node.Kind != yaml.SequenceNode {
		t.Fatalf("YAML node kind = %v, want sequence", yamlNodeKind(node))
	}
	for _, value := range node.Content {
		visit(value)
	}
}

func yamlScalar(t *testing.T, node *yaml.Node, key string) string {
	t.Helper()
	value := driftMappingValue(node, key)
	if value == nil || value.Kind != yaml.ScalarNode || strings.TrimSpace(value.Value) == "" {
		t.Fatalf("YAML mapping is missing non-empty scalar %q", key)
	}
	return value.Value
}

func yamlNodeKind(node *yaml.Node) yaml.Kind {
	if node == nil {
		return 0
	}
	return node.Kind
}

func TestCLIHumanCodePhrasesUseAuthorFacingVocabulary(t *testing.T) {
	for family, phrases := range cliHumanCodePhrases {
		for code, phrase := range phrases {
			if found := userfacing.FindForbidden(userfacing.ProfileStatusDetail, phrase); len(found) > 0 {
				t.Errorf("family %s code %q phrase %q contains forbidden terms %v", family, code, phrase, found)
			}
		}
	}
}

func TestCLIHumanCodeUnknownValuesRemainVerbatim(t *testing.T) {
	const raw = "  future_status_code  "
	if got := formatCLIHumanCode(cliHumanCodeRunStatus, raw); got != raw {
		t.Fatalf("unknown projection = %q, want exact raw value %q", got, raw)
	}
}

func TestCLIHumanCodePublicConsumersUseSharedProjector(t *testing.T) {
	required := map[string][]string{
		"agents.go\x00writeAgentListResult":                            {string(cliHumanCodeAgentStatus), string(cliHumanCodeConversationMode), string(cliHumanCodeSessionScope)},
		"agents.go\x00writeAgentDeliveryLifecycleListResult":           {string(cliHumanCodeDeliveryStatus)},
		"agents.go\x00writeAgentDiagnosisResult":                       {string(cliHumanCodeAgentLifecycleBlockingLayer), string(cliHumanCodeAgentLifecycleState), string(cliHumanCodeAgentStatus), string(cliHumanCodeWatchdogAction), string(cliHumanCodeWatchdogBlockingLayer), string(cliHumanCodeWatchdogOutcome), string(cliHumanCodeWatchdogState)},
		"agents.go\x00writeAgentDetailResult":                          {string(cliHumanCodeAgentStatus), string(cliHumanCodeConversationMode), string(cliHumanCodeSessionScope)},
		"bundle.go\x00writeBundleAgentsHuman":                          {string(cliHumanCodeConversationMode), string(cliHumanCodeSessionScope)},
		"cli_identifier_resolver.go\x00newCLIIdentifierAmbiguousError": {"<dynamic>"},
		"diagnostics.go\x00writeDiagnosticRunList":                     {string(cliHumanCodeRunStatus)},
		"diagnostics.go\x00writeDiagnosticRunHeader":                   {string(cliHumanCodeRunStatus)},
		"diagnostics.go\x00writeDiagnosticRunDiagnosis":                {string(cliHumanCodeDeliveryStatus), string(cliHumanCodeOperationalState), string(cliHumanCodeRunBlockingLayer), string(cliHumanCodeRunBlockingReason), string(cliHumanCodeRunStatus)},
		"diagnostics.go\x00writeDiagnosticRunTrace":                    {string(cliHumanCodeDeliveryStatus)},
		"diagnostics.go\x00writeDiagnosticRunTraceDeliverySummary":     {string(cliHumanCodeDeliveryStatus)},
		"diagnostics.go\x00writeDiagnosticRunTraceDeliveryDetail":      {string(cliHumanCodeDeliveryStatus)},
		"event_publish.go\x00writeEventPublishResult":                  {string(cliHumanCodeDeliveryStatus)},
		"events.go\x00writeEventDetailResult":                          {string(cliHumanCodeDeliveryStatus)},
		"events.go\x00writeEventReplayResult":                          {string(cliHumanCodeDeliveryStatus)},
		"fork.go\x00writeRunForkHuman":                                 {string(cliHumanCodeRunStatus)},
		"run_command.go\x00writeRunCommandStarted":                     {string(cliHumanCodeRunStatus)},
		"run_command.go\x00writeRunCommandReattached":                  {string(cliHumanCodeRunStatus)},
		"run_command.go\x00Write":                                      {string(cliHumanCodeDeliveryStatus)},
		"run_command.go\x00writeRunCommandTerminalSummary":             {string(cliHumanCodeRunStatus)},
	}

	familyValues := map[string]string{
		"cliHumanCodeRunStatus":                   string(cliHumanCodeRunStatus),
		"cliHumanCodeOperationalState":            string(cliHumanCodeOperationalState),
		"cliHumanCodeRunBlockingLayer":            string(cliHumanCodeRunBlockingLayer),
		"cliHumanCodeRunBlockingReason":           string(cliHumanCodeRunBlockingReason),
		"cliHumanCodeAgentStatus":                 string(cliHumanCodeAgentStatus),
		"cliHumanCodeConversationMode":            string(cliHumanCodeConversationMode),
		"cliHumanCodeSessionScope":                string(cliHumanCodeSessionScope),
		"cliHumanCodeDeliveryStatus":              string(cliHumanCodeDeliveryStatus),
		"cliHumanCodeAgentLifecycleState":         string(cliHumanCodeAgentLifecycleState),
		"cliHumanCodeAgentLifecycleBlockingLayer": string(cliHumanCodeAgentLifecycleBlockingLayer),
		"cliHumanCodeWatchdogState":               string(cliHumanCodeWatchdogState),
		"cliHumanCodeWatchdogBlockingLayer":       string(cliHumanCodeWatchdogBlockingLayer),
		"cliHumanCodeWatchdogAction":              string(cliHumanCodeWatchdogAction),
		"cliHumanCodeWatchdogOutcome":             string(cliHumanCodeWatchdogOutcome),
	}
	actual := map[string][]string{}
	rawOutput := map[string][]string{}
	_, files := parseProductionCLIHumanCodeFiles(t)
	projected, raw := inspectCLIHumanCodeConsumers(t, files, familyValues)
	for key, values := range projected {
		actual[key] = append(actual[key], values...)
	}
	for key, values := range raw {
		rawOutput[key] = append(rawOutput[key], values...)
	}

	for key, want := range required {
		got := append([]string(nil), actual[key]...)
		got = uniqueSortedStrings(got)
		want = uniqueSortedStrings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s shared human-code families = %v, want %v", key, got, want)
		}
	}
	for key := range actual {
		if _, ok := required[key]; !ok {
			t.Errorf("%s uses the shared human-code projector but is missing from the exhaustive public-consumer registry", key)
		}
	}
	for key, want := range cliHumanCodeRawOutputAllowances {
		got := append([]string(nil), rawOutput[key]...)
		sort.Strings(got)
		wantNames := append([]string(nil), want.Names...)
		sort.Strings(wantNames)
		if !reflect.DeepEqual(got, wantNames) {
			t.Errorf("%s raw code-like human output reads = %v, want classified allowance %v (%s)", key, got, wantNames, want.Reason)
		}
		delete(rawOutput, key)
	}
	for key, names := range rawOutput {
		t.Errorf("%s renders registered-code-like values %v without formatCLIHumanCode or an exact different-concept allowance", key, names)
	}
}

func TestCLIHumanCodeConsumerGuardRejectsRawRegisteredCode(t *testing.T) {
	const source = `package main
import (
	"fmt"
	"io"
)
type reviewProbeRun struct { Status string }
func reviewProbeWriteCode(out io.Writer, value string) {
	fmt.Fprintln(out, value)
}
func reviewProbeRawRunStatus(out io.Writer, run reviewProbeRun) {
	fmt.Fprintln(out, run.Status)
}
func reviewProbeRawRunStatusViaRows(out io.Writer, run reviewProbeRun) {
	rows := [][]string{{run.Status}}
	writeCLITable(out, cliTable{Rows: rows})
}
func reviewProbeRawRunStatusViaHelper(out io.Writer, run reviewProbeRun) {
	reviewProbeWriteCode(out, run.Status)
}
func reviewProbeRawRunStatusViaWriter(out io.Writer, run reviewProbeRun) {
	_, _ = out.Write([]byte(run.Status))
}
func reviewProbeRawRunStatusViaEmptyState(out io.Writer, run reviewProbeRun) {
	writeCLIEmptyState(out, run.Status)
}
func reviewProbeRawRunStatusViaFooter(out io.Writer, run reviewProbeRun) {
	writeCLIFooterLines(out, []string{run.Status})
}`
	fileSet, files := parseProductionCLIHumanCodeFiles(t)
	file, err := parser.ParseFile(fileSet, "review_probe.go", source, 0)
	if err != nil {
		t.Fatalf("parse review probe: %v", err)
	}
	files = append(files, cliHumanCodeSourceFile{base: "review_probe.go", file: file})
	_, raw := inspectCLIHumanCodeConsumers(t, files, map[string]string{})
	if got := raw["review_probe.go\x00reviewProbeRawRunStatus"]; !reflect.DeepEqual(got, []string{"status"}) {
		t.Fatalf("raw registered-code review probe findings = %v, want [status]", got)
	}
	if got := raw["review_probe.go\x00reviewProbeRawRunStatusViaRows"]; !reflect.DeepEqual(got, []string{"status"}) {
		t.Fatalf("raw registered-code row-indirection probe findings = %v, want [status]", got)
	}
	for _, name := range []string{
		"reviewProbeRawRunStatusViaHelper",
		"reviewProbeRawRunStatusViaWriter",
		"reviewProbeRawRunStatusViaEmptyState",
		"reviewProbeRawRunStatusViaFooter",
	} {
		if got := raw["review_probe.go\x00"+name]; !reflect.DeepEqual(got, []string{"status"}) {
			t.Fatalf("%s raw registered-code findings = %v, want [status]", name, got)
		}
	}
}

type cliHumanCodeRawOutputAllowance struct {
	Names  []string
	Reason string
}

var cliHumanCodeRawOutputAllowances = map[string]cliHumanCodeRawOutputAllowance{
	"bundle.go\x00writeBundleDeleteHuman": {
		Names:  []string{"status"},
		Reason: "bundle.delete mutation outcome is not a registered run/agent/delivery status family",
	},
	"connections.go\x00newConnectionsCallbackCommand": {
		Names:  []string{"status"},
		Reason: "managed-credential connection status is a separate command taxonomy",
	},
	"connections.go\x00newConnectionsConnectCommand": {
		Names:  []string{"status", "status", "status", "status"},
		Reason: "managed-credential connection status is a separate command taxonomy",
	},
	"connections.go\x00writeConnectionsTable": {
		Names:  []string{"status"},
		Reason: "managed-credential connection status is a separate command taxonomy",
	},
	"context_command.go\x00writeContextCurrentText": {
		Names:  []string{"status"},
		Reason: "local context registry status is a separate local-targeting taxonomy",
	},
	"context_command.go\x00writeContextEntryText": {
		Names:  []string{"status"},
		Reason: "local context registry status is a separate local-targeting taxonomy",
	},
	"context_command.go\x00writeContextListText": {
		Names:  []string{"status", "status"},
		Reason: "local context registry status is a separate local-targeting taxonomy",
	},
	"context_command.go\x00writeContextPruneText": {
		Names:  []string{"status"},
		Reason: "local context registry status is a separate local-targeting taxonomy",
	},
	"control_mailbox.go\x00writeMailboxDecisionResult": {
		Names:  []string{"action", "status"},
		Reason: "mailbox decision action/status are separate control taxonomies",
	},
	"control_mailbox.go\x00writeMailboxDetailResult": {
		Names:  []string{"action", "status"},
		Reason: "mailbox item/history action/status are separate control taxonomies",
	},
	"control_mailbox.go\x00writeMailboxListResult": {
		Names:  []string{"status"},
		Reason: "mailbox item status is a separate control taxonomy",
	},
	"control_mailbox.go\x00runMailboxDecisionCommand": {
		Names:  []string{"action", "action"},
		Reason: "mailbox decision action is a separate control taxonomy propagated through validation and result rendering",
	},
	"control_nuke.go\x00writeRuntimeNukeResult": {
		Names:  []string{"status"},
		Reason: "runtime.nuke mutation outcome is a separate destructive-control taxonomy",
	},
	"control_run.go\x00runControlRunCommand": {
		Names:  []string{"action", "action", "action"},
		Reason: "control command action is command input and mutation-result prose, not a registered human-code family",
	},
	"control_run.go\x00writeControlOK": {
		Names:  []string{"action"},
		Reason: "control command action is command input, not a registered human-code family",
	},
	"conversations.go\x00writeConversationDetailResult": {
		Names:  []string{"status"},
		Reason: "conversation status is a separate conversation-detail taxonomy owned by #1820",
	},
	"conversations.go\x00writeConversationListResult": {
		Names:  []string{"status"},
		Reason: "conversation status is a separate conversation-detail taxonomy owned by #1820",
	},
	"conversations.go\x00writeConversationTurnDetailResult": {
		Names:  []string{"outcome", "status"},
		Reason: "conversation session status and turn outcome are separate detail taxonomies owned by #1820",
	},
	"forkchat.go\x00writeForkChatSessionHeader": {
		Names:  []string{"state"},
		Reason: "fork-chat state is a separate conversation-detail taxonomy owned by #1820",
	},
	"forkchat.go\x00writeForkChatListResult": {
		Names:  []string{"state"},
		Reason: "fork-chat state is a separate conversation-detail taxonomy owned by #1820",
	},
	"describe.go\x00writeDescribeText": {
		Names:  []string{"mode"},
		Reason: "flow composition mode is an authoring-view concept, not ConversationMode",
	},
	"local_preflight.go\x00writeLocalPreflightText": {
		Names:  []string{"mode", "status"},
		Reason: "preflight mode and aggregate result are typed diagnostic rendering inputs",
	},
	"logs.go\x00writeRuntimeLogFollowEntry": {
		Names:  []string{"action"},
		Reason: "runtime-log action is exact log evidence owned by #1819, not a registered watchdog action",
	},
	"main.go\x00emit": {
		Names:  []string{"status"},
		Reason: "serve boot progress status is process-progress output, not a registered API code family",
	},
	"main.go\x00runtimeSink": {
		Names:  []string{"status"},
		Reason: "serve boot runtime event status feeds process-progress output, not a registered API code family",
	},
	"main.go\x00printRunForkActivation": {
		Names:  []string{"forkrunstatus", "sourcerunstatus"},
		Reason: "private legacy run-fork runtime-owner harness explicitly excluded by the approved gate",
	},
	"main.go\x00printRunForkMaterialization": {
		Names:  []string{"forkrunstatus"},
		Reason: "private legacy run-fork runtime-owner harness explicitly excluded by the approved gate",
	},
	"main.go\x00printRunForkPlan": {
		Names:  []string{"sourcerunstatus", "status"},
		Reason: "private legacy run-fork runtime-owner harness explicitly excluded by the approved gate",
	},
	"main.go\x00printSelectedContractExecution": {
		Names:  []string{"forkrunstatus", "sourcerunstatus"},
		Reason: "private legacy run-fork runtime-owner harness explicitly excluded by the approved gate",
	},
	"target_resolution.go\x00writeDoctorTargetText": {
		Names:  []string{"mode", "mode", "status", "status", "status", "status", "status", "status", "status", "status", "status", "status", "status"},
		Reason: "doctor target/context/config statuses are separate targeting and typed-diagnostic taxonomies",
	},
}

type cliHumanCodeSourceFile struct {
	base string
	file *ast.File
}

type cliHumanCodeFunction struct {
	base          string
	declaration   *ast.FuncDecl
	assignments   map[string][]ast.Expr
	parameters    map[string]int
	variadicIndex int
}

type cliHumanCodeCallFlow struct {
	parameters map[int]bool
	variadic   map[int]bool
}

func parseProductionCLIHumanCodeFiles(t *testing.T) (*token.FileSet, []cliHumanCodeSourceFile) {
	t.Helper()
	fileSet := token.NewFileSet()
	files := make([]cliHumanCodeSourceFile, 0)
	for _, path := range productionCLIGoFiles(t) {
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files = append(files, cliHumanCodeSourceFile{base: filepath.Base(path), file: file})
	}
	return fileSet, files
}

func inspectCLIHumanCodeConsumers(
	t *testing.T,
	files []cliHumanCodeSourceFile,
	familyValues map[string]string,
) (map[string][]string, map[string][]string) {
	t.Helper()
	functions := cliHumanCodeFunctions(files)
	callFlow := cliHumanCodeOutputCallFlow(functions)
	projected := map[string][]string{}
	rawPositions := map[string]map[token.Pos]string{}
	for _, function := range functions {
		key := function.base + "\x00" + function.declaration.Name.Name
		ast.Inspect(function.declaration.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CallExpr:
				if family, ok := cliHumanCodeProjectionFamily(value, familyValues); ok {
					projected[key] = append(projected[key], family)
					return true
				}
				for _, expression := range cliHumanCodeOutputCallExpressions(value, callFlow) {
					collectRawCLIHumanCodeExpressions(expression, function.assignments, map[string]bool{}, rawPositionsForKey(rawPositions, key))
				}
			case *ast.CompositeLit:
				if isCLIHumanOutputComposite(value) {
					collectRawCLIHumanCodeExpressions(value, function.assignments, map[string]bool{}, rawPositionsForKey(rawPositions, key))
				}
			}
			return true
		})
	}
	raw := map[string][]string{}
	for key, positions := range rawPositions {
		for _, name := range positions {
			raw[key] = append(raw[key], name)
		}
		sort.Strings(raw[key])
	}
	return projected, raw
}

func cliHumanCodeFunctions(files []cliHumanCodeSourceFile) []cliHumanCodeFunction {
	functions := make([]cliHumanCodeFunction, 0)
	for _, source := range files {
		for _, declaration := range source.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			parameters, variadicIndex := cliHumanCodeFunctionParameters(function)
			functions = append(functions, cliHumanCodeFunction{
				base:          source.base,
				declaration:   function,
				assignments:   cliHumanCodeLocalAssignments(function.Body),
				parameters:    parameters,
				variadicIndex: variadicIndex,
			})
		}
	}
	return functions
}

func cliHumanCodeFunctionParameters(function *ast.FuncDecl) (map[string]int, int) {
	parameters := map[string]int{}
	if function.Recv != nil {
		for _, field := range function.Recv.List {
			for _, name := range field.Names {
				parameters[name.Name] = -1
			}
		}
	}
	variadicIndex := -1
	index := 0
	if function.Type.Params == nil {
		return parameters, variadicIndex
	}
	for _, field := range function.Type.Params.List {
		if _, ok := field.Type.(*ast.Ellipsis); ok {
			variadicIndex = index
		}
		for _, name := range field.Names {
			parameters[name.Name] = index
			index++
		}
	}
	return parameters, variadicIndex
}

func cliHumanCodeOutputCallFlow(functions []cliHumanCodeFunction) map[string]cliHumanCodeCallFlow {
	flow := map[string]cliHumanCodeCallFlow{}
	for _, function := range functions {
		name := function.declaration.Name.Name
		entry := flow[name]
		if entry.parameters == nil {
			entry.parameters = map[int]bool{}
		}
		if entry.variadic == nil {
			entry.variadic = map[int]bool{}
		}
		if function.variadicIndex >= 0 {
			entry.variadic[function.variadicIndex] = true
		}
		flow[name] = entry
	}

	for changed := true; changed; {
		changed = false
		for _, function := range functions {
			name := function.declaration.Name.Name
			entry := flow[name]
			for _, expression := range cliHumanCodeFunctionOutputExpressions(function.declaration.Body, flow) {
				sources := map[string]bool{}
				collectCLIHumanDataflowIdentifiers(expression, function.assignments, map[string]bool{}, sources)
				for source := range sources {
					index, ok := function.parameters[source]
					if ok && !entry.parameters[index] {
						entry.parameters[index] = true
						changed = true
					}
				}
			}
			flow[name] = entry
		}
	}
	return flow
}

func cliHumanCodeFunctionOutputExpressions(body *ast.BlockStmt, flow map[string]cliHumanCodeCallFlow) []ast.Expr {
	expressions := make([]ast.Expr, 0)
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			expressions = append(expressions, cliHumanCodeOutputCallExpressions(value, flow)...)
		case *ast.CompositeLit:
			if isCLIHumanOutputComposite(value) {
				expressions = append(expressions, value)
			}
		}
		return true
	})
	return expressions
}

func cliHumanCodeOutputCallExpressions(call *ast.CallExpr, flow map[string]cliHumanCodeCallFlow) []ast.Expr {
	if callee, ok := call.Fun.(*ast.Ident); ok {
		if callee.Name == "formatCLIHumanCode" {
			return nil
		}
		if summary, ok := flow[callee.Name]; ok {
			return cliHumanCodeFlowingCallArguments(call, summary, nil)
		}
		return nil
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	qualifier, _ := callee.X.(*ast.Ident)
	if qualifier != nil && qualifier.Name == "fmt" {
		switch callee.Sel.Name {
		case "Fprint", "Fprintf", "Fprintln":
			return call.Args[minCLIHumanCodeInt(1, len(call.Args)):]
		}
	}
	if qualifier != nil && qualifier.Name == "io" && callee.Sel.Name == "WriteString" {
		return call.Args[minCLIHumanCodeInt(1, len(call.Args)):]
	}
	switch callee.Sel.Name {
	case "Write", "Print", "Printf", "Println":
		return call.Args
	}
	if summary, ok := flow[callee.Sel.Name]; ok {
		return cliHumanCodeFlowingCallArguments(call, summary, callee.X)
	}
	return nil
}

func cliHumanCodeFlowingCallArguments(call *ast.CallExpr, summary cliHumanCodeCallFlow, receiver ast.Expr) []ast.Expr {
	expressions := make([]ast.Expr, 0, len(summary.parameters))
	indexes := make([]int, 0, len(summary.parameters))
	for index := range summary.parameters {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		if index == -1 {
			if receiver != nil {
				expressions = append(expressions, receiver)
			}
			continue
		}
		if index >= len(call.Args) {
			continue
		}
		if summary.variadic[index] {
			expressions = append(expressions, call.Args[index:]...)
			continue
		}
		expressions = append(expressions, call.Args[index])
	}
	return expressions
}

func minCLIHumanCodeInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func cliHumanCodeProjectionFamily(call *ast.CallExpr, familyValues map[string]string) (string, bool) {
	callee, ok := call.Fun.(*ast.Ident)
	if !ok || callee.Name != "formatCLIHumanCode" || len(call.Args) == 0 {
		return "", false
	}
	switch family := call.Args[0].(type) {
	case *ast.Ident:
		value, ok := familyValues[family.Name]
		if !ok {
			return "<unknown:" + family.Name + ">", true
		}
		return value, true
	case *ast.SelectorExpr:
		return "<dynamic>", true
	default:
		return "<non-auditable>", true
	}
}

func rawPositionsForKey(target map[string]map[token.Pos]string, key string) map[token.Pos]string {
	if target[key] == nil {
		target[key] = map[token.Pos]string{}
	}
	return target[key]
}

func cliHumanCodeLocalAssignments(body *ast.BlockStmt) map[string][]ast.Expr {
	assignments := map[string][]ast.Expr{}
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.AssignStmt:
			if len(value.Lhs) != len(value.Rhs) {
				return true
			}
			for i, lhs := range value.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && ident.Name != "_" {
					assignments[ident.Name] = append(assignments[ident.Name], value.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(value.Names) != len(value.Values) {
				return true
			}
			for i, name := range value.Names {
				if name.Name != "_" {
					assignments[name.Name] = append(assignments[name.Name], value.Values[i])
				}
			}
		case *ast.RangeStmt:
			for _, expression := range []ast.Expr{value.Key, value.Value} {
				if ident, ok := expression.(*ast.Ident); ok && ident.Name != "_" {
					assignments[ident.Name] = append(assignments[ident.Name], value.X)
				}
			}
		}
		return true
	})
	return assignments
}

func collectCLIHumanDataflowIdentifiers(node ast.Node, assignments map[string][]ast.Expr, visiting map[string]bool, found map[string]bool) {
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
		case *ast.KeyValueExpr:
			collectCLIHumanDataflowIdentifiers(value.Value, assignments, visiting, found)
			return false
		case *ast.CallExpr:
			if callee, ok := value.Fun.(*ast.Ident); ok && callee.Name == "formatCLIHumanCode" {
				return false
			}
			for _, argument := range value.Args {
				collectCLIHumanDataflowIdentifiers(argument, assignments, visiting, found)
			}
			return false
		case *ast.SelectorExpr:
			collectCLIHumanDataflowIdentifiers(value.X, assignments, visiting, found)
			return false
		case *ast.Ident:
			if !visiting[value.Name] {
				if assigned := assignments[value.Name]; len(assigned) > 0 {
					visiting[value.Name] = true
					for _, expression := range assigned {
						collectCLIHumanDataflowIdentifiers(expression, assignments, visiting, found)
					}
					delete(visiting, value.Name)
					return false
				}
			}
			found[value.Name] = true
			return false
		}
		return true
	})
}

func collectRawCLIHumanCodeExpressions(node ast.Node, assignments map[string][]ast.Expr, visiting map[string]bool, found map[token.Pos]string) {
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
		case *ast.KeyValueExpr:
			collectRawCLIHumanCodeExpressions(value.Value, assignments, visiting, found)
			return false
		case *ast.CallExpr:
			if callee, ok := value.Fun.(*ast.Ident); ok && callee.Name == "formatCLIHumanCode" {
				return false
			}
		case *ast.SelectorExpr:
			if name, ok := cliHumanCodeLikeName(value.Sel.Name); ok {
				found[value.Sel.Pos()] = name
			}
			// The selected field is the value expression. Its receiver is an
			// object/container identity, not another rendered value.
			return false
		case *ast.Ident:
			if name, ok := cliHumanCodeLikeName(value.Name); ok {
				found[value.Pos()] = name
				return false
			}
			if !visiting[value.Name] {
				if assigned := assignments[value.Name]; len(assigned) > 0 {
					visiting[value.Name] = true
					for _, expression := range assigned {
						collectRawCLIHumanCodeExpressions(expression, assignments, visiting, found)
					}
					delete(visiting, value.Name)
				}
			}
		}
		return true
	})
}

func cliHumanCodeLikeName(raw string) (string, bool) {
	name := strings.ToLower(strings.ReplaceAll(raw, "_", ""))
	switch name {
	case "status", "runstatus", "forkrunstatus", "sourcerunstatus", "operationalstate", "blockinglayer", "blockingreason", "mode", "sessionscope", "deliverystatus", "state", "action", "outcome":
		return name, true
	default:
		return "", false
	}
}

func isCLIHumanOutputComposite(literal *ast.CompositeLit) bool {
	typeName, ok := literal.Type.(*ast.Ident)
	if !ok {
		return false
	}
	switch typeName.Name {
	case "cliTable", "cliDetailField", "cliLabeledDetail", "cliLabeledDetailRow", "cliLabeledDetailSection":
		return true
	default:
		return false
	}
}

func uniqueSortedStrings(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
