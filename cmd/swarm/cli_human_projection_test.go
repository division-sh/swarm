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
	for _, path := range productionCLIGoFiles(t) {
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		projected, raw := inspectCLIHumanCodeConsumers(t, fileSet, file, filepath.Base(path), familyValues)
		for key, values := range projected {
			actual[key] = append(actual[key], values...)
		}
		for key, values := range raw {
			rawOutput[key] = append(rawOutput[key], values...)
		}
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
func reviewProbeRawRunStatus(out io.Writer, status string) {
	fmt.Fprintln(out, status)
}
func reviewProbeRawRunStatusViaRows(out io.Writer, status string) {
	rows := [][]string{{status}}
	writeCLITable(out, cliTable{Rows: rows})
}`
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "review_probe.go", source, 0)
	if err != nil {
		t.Fatalf("parse review probe: %v", err)
	}
	_, raw := inspectCLIHumanCodeConsumers(t, fileSet, file, "review_probe.go", map[string]string{})
	if got := raw["review_probe.go\x00reviewProbeRawRunStatus"]; !reflect.DeepEqual(got, []string{"status"}) {
		t.Fatalf("raw registered-code review probe findings = %v, want [status]", got)
	}
	if got := raw["review_probe.go\x00reviewProbeRawRunStatusViaRows"]; !reflect.DeepEqual(got, []string{"status"}) {
		t.Fatalf("raw registered-code row-indirection probe findings = %v, want [status]", got)
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
	"control_nuke.go\x00writeRuntimeNukeResult": {
		Names:  []string{"status"},
		Reason: "runtime.nuke mutation outcome is a separate destructive-control taxonomy",
	},
	"control_run.go\x00runControlRunCommand": {
		Names:  []string{"action"},
		Reason: "control command action is command input, not a registered human-code family",
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

func inspectCLIHumanCodeConsumers(
	t *testing.T,
	fileSet *token.FileSet,
	file *ast.File,
	base string,
	familyValues map[string]string,
) (map[string][]string, map[string][]string) {
	t.Helper()
	projected := map[string][]string{}
	rawPositions := map[string]map[token.Pos]string{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		key := base + "\x00" + function.Name.Name
		assignments := cliHumanCodeLocalAssignments(function.Body)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CallExpr:
				if family, ok := cliHumanCodeProjectionFamily(value, familyValues); ok {
					projected[key] = append(projected[key], family)
					return true
				}
				if isCLIHumanOutputSink(value) {
					collectRawCLIHumanCodeExpressions(value, assignments, map[string]bool{}, rawPositionsForKey(rawPositions, key))
				}
			case *ast.CompositeLit:
				if isCLIHumanOutputComposite(value) {
					collectRawCLIHumanCodeExpressions(value, assignments, map[string]bool{}, rawPositionsForKey(rawPositions, key))
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
		}
		return true
	})
	return assignments
}

func collectRawCLIHumanCodeExpressions(node ast.Node, assignments map[string][]ast.Expr, visiting map[string]bool, found map[token.Pos]string) {
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
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

func isCLIHumanOutputSink(call *ast.CallExpr) bool {
	switch callee := call.Fun.(type) {
	case *ast.Ident:
		switch callee.Name {
		case "writeCLITable", "writeCLILabeledDetail", "writeCLIFieldLine", "writeCLITitle":
			return true
		}
	case *ast.SelectorExpr:
		qualifier, _ := callee.X.(*ast.Ident)
		if qualifier != nil && qualifier.Name == "fmt" {
			switch callee.Sel.Name {
			case "Fprint", "Fprintf", "Fprintln":
				return true
			}
		}
		if qualifier != nil && qualifier.Name == "io" && callee.Sel.Name == "WriteString" {
			return true
		}
	}
	return false
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
