package main

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
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
		"provider_subject_kind": true, "provider_subject_status": true, "provider_capability": true,
		"provider_guarantee": true, "provider_requirement_status": true,
		"run_blocking_tuples": true, "agent_lifecycle_tuples": true, "watchdog_tuples": true,
	}
	forEachYAMLMappingNode(t, families, func(name string, _ *yaml.Node) {
		if !knownSpecFamilies[name] {
			t.Fatalf("human_code_projection has unclassified family %q", name)
		}
	})

	for specName, family := range map[string]cliHumanCodeFamily{
		"run_status":                  cliHumanCodeRunStatus,
		"operational_state":           cliHumanCodeOperationalState,
		"agent_status":                cliHumanCodeAgentStatus,
		"conversation_mode":           cliHumanCodeConversationMode,
		"session_scope":               cliHumanCodeSessionScope,
		"delivery_status":             cliHumanCodeDeliveryStatus,
		"provider_subject_kind":       cliHumanCodeProviderSubjectKind,
		"provider_subject_status":     cliHumanCodeProviderSubjectStatus,
		"provider_capability":         cliHumanCodeProviderCapability,
		"provider_guarantee":          cliHumanCodeProviderGuarantee,
		"provider_requirement_status": cliHumanCodeProviderRequirementStatus,
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
		"cliHumanCodeProviderSubjectKind":         string(cliHumanCodeProviderSubjectKind),
		"cliHumanCodeProviderSubjectStatus":       string(cliHumanCodeProviderSubjectStatus),
		"cliHumanCodeProviderCapability":          string(cliHumanCodeProviderCapability),
		"cliHumanCodeProviderGuarantee":           string(cliHumanCodeProviderGuarantee),
		"cliHumanCodeProviderRequirementStatus":   string(cliHumanCodeProviderRequirementStatus),
	}
	actual := map[string][]string{}
	rawOutput := map[string][]string{}
	fileSet, files := parseProductionCLIHumanCodeFiles(t)
	projected, raw := inspectCLIHumanCodeConsumers(t, fileSet, files, familyValues)
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
	"strings"

	"github.com/spf13/cobra"
)
type reviewProbeRun struct { Status string }
type reviewProbeBufferHolder struct { Buffer strings.Builder }
func reviewProbeWriteCode(out io.Writer, value string) {
	fmt.Fprintln(out, value)
}
func reviewProbeRawRunStatusValue(run reviewProbeRun) string {
	return run.Status
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
}
func reviewProbeRawRunStatusViaCommand(cmd *cobra.Command, run reviewProbeRun) {
	cmd.Print(run.Status)
	cmd.Printf("%s", run.Status)
	cmd.Println(run.Status)
	cmd.PrintErr(run.Status)
	cmd.PrintErrf("%s", run.Status)
	cmd.PrintErrln(run.Status)
}
func reviewProbeRawRunStatusViaBuffer(out io.Writer, run reviewProbeRun) {
	var buffer strings.Builder
	buffer.WriteString(run.Status)
	fmt.Fprint(out, buffer.String())
}
func reviewProbeRawRunStatusViaReturn(out io.Writer, run reviewProbeRun) {
	fmt.Fprint(out, reviewProbeRawRunStatusValue(run))
}
func reviewProbeRawRunStatusViaAddressedBuffer(out io.Writer, run reviewProbeRun) {
	var buffer strings.Builder
	(&buffer).WriteString(run.Status)
	fmt.Fprint(out, buffer.String())
}
func reviewProbeRawRunStatusViaSelectedBuffer(out io.Writer, run reviewProbeRun) {
	var holder reviewProbeBufferHolder
	(&holder.Buffer).WriteString(run.Status)
	fmt.Fprint(out, holder.Buffer.String())
}
func reviewProbeRawRunStatusViaIndexedBuffer(out io.Writer, run reviewProbeRun) {
	var buffers [1]strings.Builder
	(&buffers[0]).WriteString(run.Status)
	fmt.Fprint(out, buffers[0].String())
}
func reviewProbeRawRunStatusViaBufferAlias(out io.Writer, run reviewProbeRun) {
	var buffer strings.Builder
	alias := &buffer
	alias.WriteString(run.Status)
	fmt.Fprint(out, buffer.String())
}`
	fileSet, files := parseProductionCLIHumanCodeFiles(t)
	file, err := parser.ParseFile(fileSet, "review_probe.go", source, 0)
	if err != nil {
		t.Fatalf("parse review probe: %v", err)
	}
	files = append(files, cliHumanCodeSourceFile{base: "review_probe.go", file: file})
	_, raw := inspectCLIHumanCodeConsumers(t, fileSet, files, map[string]string{})
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
		"reviewProbeRawRunStatusViaBuffer",
		"reviewProbeRawRunStatusViaReturn",
		"reviewProbeRawRunStatusViaAddressedBuffer",
		"reviewProbeRawRunStatusViaSelectedBuffer",
		"reviewProbeRawRunStatusViaIndexedBuffer",
		"reviewProbeRawRunStatusViaBufferAlias",
	} {
		if got := raw["review_probe.go\x00"+name]; !reflect.DeepEqual(got, []string{"status"}) {
			t.Fatalf("%s raw registered-code findings = %v, want [status]", name, got)
		}
	}
	if got := raw["review_probe.go\x00reviewProbeRawRunStatusViaCommand"]; !reflect.DeepEqual(got, []string{"status", "status", "status", "status", "status", "status"}) {
		t.Fatalf("Cobra command output family raw registered-code findings = %v, want six status findings", got)
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
		Names:  []string{"mode"},
		Reason: "preflight mode is a typed diagnostic rendering input, not a registered conversation mode",
	},
	"logs.go\x00writeRuntimeLogListResult": {
		Names:  []string{"action"},
		Reason: "runtime-log action is exact log evidence owned by #1819, not a registered watchdog action",
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
		Names:  []string{"mode", "mode", "status", "status", "status", "status", "status", "status", "status", "status", "status", "status"},
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
	callable      *types.Func
	assignments   map[string][]ast.Expr
	receiverData  map[string][]ast.Expr
	parameters    map[string]int
	variadicIndex int
}

type cliHumanCodeCallFlow struct {
	parameters map[int]bool
	variadic   map[int]bool
}

type cliHumanCodeReturnFlow struct {
	cliHumanCodeCallFlow
	names map[string]bool
}

type cliHumanCodeRawFinding struct {
	position token.Pos
	name     string
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
	fileSet *token.FileSet,
	files []cliHumanCodeSourceFile,
	familyValues map[string]string,
) (map[string][]string, map[string][]string) {
	t.Helper()
	typeInfo := cliHumanCodeTypeCheck(t, fileSet, files)
	functions := cliHumanCodeFunctions(files, typeInfo)
	returnFlow := cliHumanCodeReturnCallFlow(functions, typeInfo)
	callFlow := cliHumanCodeOutputCallFlow(functions, returnFlow, typeInfo)
	projected := map[string][]string{}
	rawPositions := map[string]map[cliHumanCodeRawFinding]struct{}{}
	for _, function := range functions {
		key := function.base + "\x00" + function.declaration.Name.Name
		ast.Inspect(function.declaration.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CallExpr:
				if family, ok := cliHumanCodeProjectionFamily(value, familyValues); ok {
					projected[key] = append(projected[key], family)
					return true
				}
				expressions := cliHumanCodeOutputCallExpressions(value, callFlow, typeInfo)
				for _, expression := range expressions {
					collectRawCLIHumanCodeExpressions(expression, function.assignments, function.receiverData, returnFlow, typeInfo, map[string]bool{}, rawPositionsForKey(rawPositions, key))
				}
			case *ast.CompositeLit:
				if isCLIHumanOutputComposite(value) {
					collectRawCLIHumanCodeExpressions(value, function.assignments, function.receiverData, returnFlow, typeInfo, map[string]bool{}, rawPositionsForKey(rawPositions, key))
				}
			}
			return true
		})
	}
	raw := map[string][]string{}
	for key, findings := range rawPositions {
		for finding := range findings {
			raw[key] = append(raw[key], finding.name)
		}
		sort.Strings(raw[key])
	}
	return projected, raw
}

func cliHumanCodeTypeCheck(t *testing.T, fileSet *token.FileSet, files []cliHumanCodeSourceFile) *types.Info {
	t.Helper()
	astFiles := make([]*ast.File, 0, len(files))
	for _, file := range files {
		astFiles = append(astFiles, file.file)
	}
	info := &types.Info{
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	config := types.Config{Importer: importer.ForCompiler(fileSet, "gc", cliHumanCodeExportLookup())}
	if _, err := config.Check("github.com/division-sh/swarm/cmd/swarm", fileSet, astFiles, info); err != nil {
		t.Fatalf("type-check production CLI consumer audit: %v", err)
	}
	return info
}

func cliHumanCodeExportLookup() func(string) (io.ReadCloser, error) {
	exports := map[string]string{}
	return func(path string) (io.ReadCloser, error) {
		exportPath, ok := exports[path]
		if !ok {
			command := exec.Command("go", "list", "-export", "-f={{.Export}}", path)
			output, err := command.CombinedOutput()
			if err != nil {
				return nil, fmt.Errorf("go list export for %s: %w: %s", path, err, strings.TrimSpace(string(output)))
			}
			exportPath = strings.TrimSpace(string(output))
			if exportPath == "" {
				return nil, fmt.Errorf("go list export for %s returned an empty path", path)
			}
			exports[path] = exportPath
		}
		return os.Open(exportPath)
	}
}

func cliHumanCodeFunctions(files []cliHumanCodeSourceFile, typeInfo *types.Info) []cliHumanCodeFunction {
	functions := make([]cliHumanCodeFunction, 0)
	for _, source := range files {
		for _, declaration := range source.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			parameters, variadicIndex := cliHumanCodeFunctionParameters(function)
			assignments, receiverData := cliHumanCodeLocalAssignments(function.Body)
			callable, _ := typeInfo.Defs[function.Name].(*types.Func)
			if callable == nil {
				continue
			}
			functions = append(functions, cliHumanCodeFunction{
				base:          source.base,
				declaration:   function,
				callable:      callable,
				assignments:   assignments,
				receiverData:  receiverData,
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

func cliHumanCodeReturnCallFlow(functions []cliHumanCodeFunction, typeInfo *types.Info) map[*types.Func]cliHumanCodeReturnFlow {
	flow := map[*types.Func]cliHumanCodeReturnFlow{}
	for _, function := range functions {
		entry := flow[function.callable]
		if entry.parameters == nil {
			entry.parameters = map[int]bool{}
		}
		if entry.variadic == nil {
			entry.variadic = map[int]bool{}
		}
		if entry.names == nil {
			entry.names = map[string]bool{}
		}
		if function.variadicIndex >= 0 {
			entry.variadic[function.variadicIndex] = true
		}
		flow[function.callable] = entry
	}

	for changed := true; changed; {
		changed = false
		for _, function := range functions {
			if !cliHumanCodeCallableReturnsText(function.callable) {
				continue
			}
			entry := flow[function.callable]
			parameterSemanticNames := map[string]bool{}
			for parameter := range function.parameters {
				if semanticName, ok := cliHumanCodeLikeName(parameter); ok {
					parameterSemanticNames[semanticName] = true
				}
			}
			for _, expression := range cliHumanCodeFunctionReturnExpressions(function.declaration.Body) {
				sources := map[string]bool{}
				collectCLIHumanDataflowIdentifiers(expression, function.assignments, function.receiverData, flow, typeInfo, map[string]bool{}, sources)
				for source := range sources {
					index, ok := function.parameters[source]
					if ok && !entry.parameters[index] {
						entry.parameters[index] = true
						changed = true
					}
				}
				findings := map[cliHumanCodeRawFinding]struct{}{}
				collectRawCLIHumanCodeExpressions(expression, function.assignments, function.receiverData, flow, typeInfo, map[string]bool{}, findings)
				for finding := range findings {
					if parameterSemanticNames[finding.name] {
						continue
					}
					if !entry.names[finding.name] {
						entry.names[finding.name] = true
						changed = true
					}
				}
			}
			flow[function.callable] = entry
		}
	}
	return flow
}

func cliHumanCodeCallableReturnsText(function *types.Func) bool {
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Results() == nil {
		return false
	}
	for index := 0; index < signature.Results().Len(); index++ {
		if cliHumanCodeTypeCarriesText(signature.Results().At(index).Type()) {
			return true
		}
	}
	return false
}

func cliHumanCodeTypeCarriesText(value types.Type) bool {
	switch typed := value.Underlying().(type) {
	case *types.Basic:
		return typed.Kind() == types.String
	case *types.Slice:
		if basic, ok := typed.Elem().Underlying().(*types.Basic); ok {
			return basic.Kind() == types.String || basic.Kind() == types.Uint8
		}
	case *types.Array:
		if basic, ok := typed.Elem().Underlying().(*types.Basic); ok {
			return basic.Kind() == types.String || basic.Kind() == types.Uint8
		}
	}
	return false
}

func cliHumanCodeFunctionReturnExpressions(body *ast.BlockStmt) []ast.Expr {
	expressions := make([]ast.Expr, 0)
	ast.Inspect(body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		statement, ok := node.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		expressions = append(expressions, statement.Results...)
		return false
	})
	return expressions
}

func cliHumanCodeOutputCallFlow(functions []cliHumanCodeFunction, returnFlow map[*types.Func]cliHumanCodeReturnFlow, typeInfo *types.Info) map[*types.Func]cliHumanCodeCallFlow {
	flow := map[*types.Func]cliHumanCodeCallFlow{}
	for _, function := range functions {
		entry := flow[function.callable]
		if entry.parameters == nil {
			entry.parameters = map[int]bool{}
		}
		if entry.variadic == nil {
			entry.variadic = map[int]bool{}
		}
		if function.variadicIndex >= 0 {
			entry.variadic[function.variadicIndex] = true
		}
		flow[function.callable] = entry
	}

	for changed := true; changed; {
		changed = false
		for _, function := range functions {
			entry := flow[function.callable]
			for _, expression := range cliHumanCodeFunctionOutputExpressions(function.declaration.Body, flow, typeInfo) {
				sources := map[string]bool{}
				collectCLIHumanDataflowIdentifiers(expression, function.assignments, function.receiverData, returnFlow, typeInfo, map[string]bool{}, sources)
				for source := range sources {
					index, ok := function.parameters[source]
					if ok && !entry.parameters[index] {
						entry.parameters[index] = true
						changed = true
					}
				}
			}
			flow[function.callable] = entry
		}
	}
	return flow
}

func cliHumanCodeFunctionOutputExpressions(body *ast.BlockStmt, flow map[*types.Func]cliHumanCodeCallFlow, typeInfo *types.Info) []ast.Expr {
	expressions := make([]ast.Expr, 0)
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			expressions = append(expressions, cliHumanCodeOutputCallExpressions(value, flow, typeInfo)...)
		case *ast.CompositeLit:
			if isCLIHumanOutputComposite(value) {
				expressions = append(expressions, value)
			}
		}
		return true
	})
	return expressions
}

func cliHumanCodeOutputCallExpressions(call *ast.CallExpr, flow map[*types.Func]cliHumanCodeCallFlow, typeInfo *types.Info) []ast.Expr {
	if callee, ok := call.Fun.(*ast.Ident); ok {
		if callee.Name == "formatCLIHumanCode" {
			return nil
		}
		if summary, ok := flow[cliHumanCodeCalledFunction(call, typeInfo)]; ok {
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
	case "Write", "Print", "Printf", "Println", "PrintErr", "PrintErrf", "PrintErrln":
		return call.Args
	}
	if summary, ok := flow[cliHumanCodeCalledFunction(call, typeInfo)]; ok {
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

func rawPositionsForKey(target map[string]map[cliHumanCodeRawFinding]struct{}, key string) map[cliHumanCodeRawFinding]struct{} {
	if target[key] == nil {
		target[key] = map[cliHumanCodeRawFinding]struct{}{}
	}
	return target[key]
}

func cliHumanCodeLocalAssignments(body *ast.BlockStmt) (map[string][]ast.Expr, map[string][]ast.Expr) {
	assignments := map[string][]ast.Expr{}
	receiverData := map[string][]ast.Expr{}
	aliases := map[string]map[string]bool{}
	type receiverMutation struct {
		key       string
		arguments []ast.Expr
	}
	mutations := make([]receiverMutation, 0)
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			callee, ok := value.Fun.(*ast.SelectorExpr)
			if !ok || !isCLIHumanCodeReceiverMutation(callee.Sel.Name) {
				return true
			}
			if key, ok := cliHumanCodeIdentityKey(callee.X); ok {
				mutations = append(mutations, receiverMutation{key: key, arguments: value.Args})
			}
		case *ast.AssignStmt:
			if len(value.Lhs) != len(value.Rhs) {
				return true
			}
			for i, lhs := range value.Lhs {
				if key, ok := cliHumanCodeIdentityKey(lhs); ok {
					assignments[key] = append(assignments[key], value.Rhs[i])
					cliHumanCodeRecordAlias(aliases, key, value.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(value.Names) != len(value.Values) {
				return true
			}
			for i, name := range value.Names {
				if name.Name != "_" {
					assignments[name.Name] = append(assignments[name.Name], value.Values[i])
					cliHumanCodeRecordAlias(aliases, name.Name, value.Values[i])
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
	for _, mutation := range mutations {
		for alias := range cliHumanCodeAliasClosure(mutation.key, aliases) {
			receiverData[alias] = append(receiverData[alias], mutation.arguments...)
		}
	}
	return assignments, receiverData
}

func cliHumanCodeRecordAlias(aliases map[string]map[string]bool, left string, right ast.Expr) {
	rightKey, ok := cliHumanCodeIdentityKey(right)
	if !ok || left == rightKey {
		return
	}
	if aliases[left] == nil {
		aliases[left] = map[string]bool{}
	}
	if aliases[rightKey] == nil {
		aliases[rightKey] = map[string]bool{}
	}
	aliases[left][rightKey] = true
	aliases[rightKey][left] = true
}

func cliHumanCodeAliasClosure(start string, aliases map[string]map[string]bool) map[string]bool {
	found := map[string]bool{}
	pending := []string{start}
	for len(pending) > 0 {
		key := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if found[key] {
			continue
		}
		found[key] = true
		for alias := range aliases[key] {
			pending = append(pending, alias)
		}
	}
	return found
}

func cliHumanCodeIdentityKey(expression ast.Expr) (string, bool) {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name, value.Name != "_"
	case *ast.ParenExpr:
		return cliHumanCodeIdentityKey(value.X)
	case *ast.StarExpr:
		return cliHumanCodeIdentityKey(value.X)
	case *ast.UnaryExpr:
		if value.Op == token.AND || value.Op == token.MUL {
			return cliHumanCodeIdentityKey(value.X)
		}
	case *ast.SelectorExpr:
		base, ok := cliHumanCodeIdentityKey(value.X)
		if ok {
			return base + "." + value.Sel.Name, true
		}
	case *ast.IndexExpr:
		base, baseOK := cliHumanCodeIdentityKey(value.X)
		index, indexOK := cliHumanCodeIdentityKey(value.Index)
		if baseOK && indexOK {
			return base + "[" + index + "]", true
		}
	case *ast.BasicLit:
		return value.Value, true
	}
	return "", false
}

func collectCLIHumanDataflowIdentifiers(
	node ast.Node,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[string]bool,
) {
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
		case *ast.KeyValueExpr:
			collectCLIHumanDataflowIdentifiers(value.Value, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			return false
		case *ast.CallExpr:
			if callee, ok := value.Fun.(*ast.Ident); ok && callee.Name == "formatCLIHumanCode" {
				return false
			}
			if summary, receiver, ok := cliHumanCodeReturnSummary(value, returnFlow, typeInfo); ok {
				for _, expression := range cliHumanCodeFlowingCallArguments(value, summary.cliHumanCodeCallFlow, receiver) {
					collectCLIHumanDataflowIdentifiers(expression, assignments, receiverData, returnFlow, typeInfo, visiting, found)
				}
				return false
			}
			if callee, ok := value.Fun.(*ast.SelectorExpr); ok {
				collectCLIHumanReceiverDataflow(callee.X, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			}
			for _, argument := range value.Args {
				collectCLIHumanDataflowIdentifiers(argument, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			}
			return false
		case *ast.SelectorExpr:
			if cliHumanCodeCollectAssignedDataflow(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			collectCLIHumanDataflowIdentifiers(value.X, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			return false
		case *ast.IndexExpr:
			if cliHumanCodeCollectAssignedDataflow(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			collectCLIHumanDataflowIdentifiers(value.X, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			collectCLIHumanDataflowIdentifiers(value.Index, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			return false
		case *ast.Ident:
			if cliHumanCodeCollectAssignedDataflow(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			found[value.Name] = true
			return false
		}
		return true
	})
}

func cliHumanCodeCollectAssignedDataflow(
	expression ast.Expr,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[string]bool,
) bool {
	key, ok := cliHumanCodeIdentityKey(expression)
	if !ok || visiting[key] || (len(assignments[key]) == 0 && len(receiverData[key]) == 0) {
		return false
	}
	visiting[key] = true
	for _, assigned := range assignments[key] {
		collectCLIHumanDataflowIdentifiers(assigned, assignments, receiverData, returnFlow, typeInfo, visiting, found)
	}
	for _, written := range receiverData[key] {
		collectCLIHumanDataflowIdentifiers(written, assignments, receiverData, returnFlow, typeInfo, visiting, found)
	}
	delete(visiting, key)
	return true
}

func collectCLIHumanReceiverDataflow(
	expression ast.Expr,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[string]bool,
) {
	key, ok := cliHumanCodeIdentityKey(expression)
	if ok && !visiting[key] && len(receiverData[key]) > 0 {
		visiting[key] = true
		for _, written := range receiverData[key] {
			collectCLIHumanDataflowIdentifiers(written, assignments, receiverData, returnFlow, typeInfo, visiting, found)
		}
		delete(visiting, key)
		return
	}
	collectCLIHumanDataflowIdentifiers(expression, map[string][]ast.Expr{}, map[string][]ast.Expr{}, returnFlow, typeInfo, visiting, found)
}

func collectRawCLIHumanCodeExpressions(
	node ast.Node,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[cliHumanCodeRawFinding]struct{},
) {
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
		case *ast.KeyValueExpr:
			collectRawCLIHumanCodeExpressions(value.Value, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			return false
		case *ast.CallExpr:
			if callee, ok := value.Fun.(*ast.Ident); ok && callee.Name == "formatCLIHumanCode" {
				return false
			}
			if summary, receiver, ok := cliHumanCodeReturnSummary(value, returnFlow, typeInfo); ok {
				for name := range summary.names {
					found[cliHumanCodeRawFinding{position: value.Pos(), name: name}] = struct{}{}
				}
				for _, expression := range cliHumanCodeFlowingCallArguments(value, summary.cliHumanCodeCallFlow, receiver) {
					collectRawCLIHumanCodeExpressions(expression, assignments, receiverData, returnFlow, typeInfo, visiting, found)
				}
				return false
			}
			if callee, ok := value.Fun.(*ast.SelectorExpr); ok {
				collectCLIHumanReceiverRaw(callee.X, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			}
			for _, argument := range value.Args {
				collectRawCLIHumanCodeExpressions(argument, assignments, receiverData, returnFlow, typeInfo, visiting, found)
			}
			return false
		case *ast.SelectorExpr:
			if cliHumanCodeCollectAssignedRaw(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			if name, ok := cliHumanCodeLikeName(value.Sel.Name); ok {
				found[cliHumanCodeRawFinding{position: value.Sel.Pos(), name: name}] = struct{}{}
			}
			// The selected field is the value expression. Its receiver is an
			// object/container identity, not another rendered value.
			return false
		case *ast.IndexExpr:
			if cliHumanCodeCollectAssignedRaw(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			return true
		case *ast.Ident:
			if cliHumanCodeCollectAssignedRaw(value, assignments, receiverData, returnFlow, typeInfo, visiting, found) {
				return false
			}
			if name, ok := cliHumanCodeLikeName(value.Name); ok {
				found[cliHumanCodeRawFinding{position: value.Pos(), name: name}] = struct{}{}
				return false
			}
		}
		return true
	})
}

func cliHumanCodeCollectAssignedRaw(
	expression ast.Expr,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[cliHumanCodeRawFinding]struct{},
) bool {
	key, ok := cliHumanCodeIdentityKey(expression)
	if !ok || visiting[key] || (len(assignments[key]) == 0 && len(receiverData[key]) == 0) {
		return false
	}
	visiting[key] = true
	for _, assigned := range assignments[key] {
		collectRawCLIHumanCodeExpressions(assigned, assignments, receiverData, returnFlow, typeInfo, visiting, found)
	}
	for _, written := range receiverData[key] {
		collectRawCLIHumanCodeExpressions(written, assignments, receiverData, returnFlow, typeInfo, visiting, found)
	}
	delete(visiting, key)
	return true
}

func collectCLIHumanReceiverRaw(
	expression ast.Expr,
	assignments map[string][]ast.Expr,
	receiverData map[string][]ast.Expr,
	returnFlow map[*types.Func]cliHumanCodeReturnFlow,
	typeInfo *types.Info,
	visiting map[string]bool,
	found map[cliHumanCodeRawFinding]struct{},
) {
	key, ok := cliHumanCodeIdentityKey(expression)
	if ok && !visiting[key] && len(receiverData[key]) > 0 {
		visiting[key] = true
		for _, written := range receiverData[key] {
			collectRawCLIHumanCodeExpressions(written, assignments, receiverData, returnFlow, typeInfo, visiting, found)
		}
		delete(visiting, key)
		return
	}
	collectRawCLIHumanCodeExpressions(expression, map[string][]ast.Expr{}, map[string][]ast.Expr{}, returnFlow, typeInfo, visiting, found)
}

func cliHumanCodeCalledFunction(call *ast.CallExpr, typeInfo *types.Info) *types.Func {
	if typeInfo == nil {
		return nil
	}
	switch callee := call.Fun.(type) {
	case *ast.Ident:
		function, _ := typeInfo.Uses[callee].(*types.Func)
		return function
	case *ast.SelectorExpr:
		if selection := typeInfo.Selections[callee]; selection != nil {
			function, _ := selection.Obj().(*types.Func)
			return function
		}
		function, _ := typeInfo.Uses[callee.Sel].(*types.Func)
		return function
	default:
		return nil
	}
}

func cliHumanCodeReturnSummary(call *ast.CallExpr, flow map[*types.Func]cliHumanCodeReturnFlow, typeInfo *types.Info) (cliHumanCodeReturnFlow, ast.Expr, bool) {
	switch callee := call.Fun.(type) {
	case *ast.Ident:
		summary, ok := flow[cliHumanCodeCalledFunction(call, typeInfo)]
		return summary, nil, ok && (len(summary.parameters) > 0 || len(summary.names) > 0)
	case *ast.SelectorExpr:
		summary, ok := flow[cliHumanCodeCalledFunction(call, typeInfo)]
		return summary, callee.X, ok && (len(summary.parameters) > 0 || len(summary.names) > 0)
	default:
		return cliHumanCodeReturnFlow{}, nil, false
	}
}

func isCLIHumanCodeReceiverMutation(method string) bool {
	switch method {
	case "Write", "WriteByte", "WriteRune", "WriteString", "ReadFrom":
		return true
	default:
		return false
	}
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
