package main

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type cliOutputConformanceRegistryRow struct {
	Key            string
	Command        string
	Classification string
	ExceptionRule  string
	OwnerIssue     string
	FactOwner      string
	Reason         string
	Node           *yaml.Node
}

type cliOutputSharedOwnerProof struct {
	Constructor string
	Runner      string
}

var cliOutputSharedOwnerProofs = map[string]cliOutputSharedOwnerProof{
	"swarm version":           {Constructor: "newVersionCommand", Runner: "runVersionCommand"},
	"swarm verify":            {Constructor: "newVerifyCommand", Runner: "runVerifyCommandWithOutput"},
	"swarm run list":          {Constructor: "newRunsCommand", Runner: "runDiagnosticRunListCommand"},
	"swarm run status":        {Constructor: "newStatusCommand", Runner: "runDiagnosticRunCommand"},
	"swarm run fork":          {Constructor: "newForkCommand", Runner: "runForkCommand"},
	"swarm health":            {Constructor: "newHealthCommand", Runner: "runDiagnosticHealthCommand"},
	"swarm conversation list": {Constructor: "newConversationsListCommand", Runner: "runConversationsListCommand"},
	"swarm conversation view": {Constructor: "newConversationViewCommand", Runner: "runConversationViewCommand"},
	"swarm conversation turn": {Constructor: "newConversationTurnCommand", Runner: "runConversationTurnCommand"},
	"swarm bundle list":       {Constructor: "newBundleListCommand", Runner: "runBundleListCommand"},
	"swarm bundle show":       {Constructor: "newBundleShowCommand", Runner: "runBundleShowCommand"},
	"swarm bundle agents":     {Constructor: "newBundleAgentsCommand", Runner: "runBundleAgentsCommand"},
	"swarm bundle register":   {Constructor: "newBundleRegisterCommand", Runner: "runBundleRegisterCommand"},
	"swarm bundle delete":     {Constructor: "newBundleDeleteCommand", Runner: "runBundleDeleteCommand"},
	"swarm forkchat new":      {Constructor: "newForkChatNewCommand", Runner: "runForkChatNewCommand"},
	"swarm forkchat resume":   {Constructor: "newForkChatResumeCommand", Runner: "runForkChatResumeCommand"},
	"swarm forkchat list":     {Constructor: "newForkChatListCommand", Runner: "runForkChatListCommand"},
	"swarm forkchat view":     {Constructor: "newForkChatViewCommand", Runner: "runForkChatViewCommand"},
	"swarm forkchat delete":   {Constructor: "newForkChatDeleteCommand", Runner: "runForkChatDeleteCommand"},
	"swarm describe":          {Constructor: "newDescribeCommand", Runner: "runDescribeCommandWithOutput"},
}

var cliOutputGrandfatheredNonSharedRows = map[string]string{
	"swarm":                        "exception",
	"swarm help":                   "exception",
	"swarm completion":             "exception",
	"swarm doctor":                 "split",
	"swarm test":                   "split",
	"swarm workspace":              "exception",
	"swarm workspace build":        "split",
	"swarm secrets":                "exception",
	"swarm secrets set":            "split",
	"swarm secrets list":           "split",
	"swarm secrets check":          "split",
	"swarm secrets rm":             "split",
	"swarm connections":            "exception",
	"swarm connections connect":    "split",
	"swarm connections callback":   "split",
	"swarm connections status":     "split",
	"swarm connections disconnect": "split",
	"swarm serve":                  "exception",
	"swarm run":                    "exception",
	"swarm run start":              "split",
	"swarm run trace":              "split",
	"swarm mailbox":                "exception",
	"swarm mailbox list":           "split",
	"swarm mailbox view":           "split",
	"swarm mailbox approve":        "split",
	"swarm mailbox reject":         "split",
	"swarm mailbox defer":          "split",
	"swarm control":                "exception",
	"swarm control pause":          "split",
	"swarm control continue":       "split",
	"swarm control stop":           "split",
	"swarm control nuke":           "split",
	"swarm agent":                  "exception",
	"swarm agent list":             "split",
	"swarm agent view":             "split",
	"swarm agent diagnose":         "split",
	"swarm agent deliveries":       "split",
	"swarm agent restart":          "split",
	"swarm agent replay":           "split",
	"swarm agent replay-backlog":   "split",
	"swarm agent directive":        "split",
	"swarm event":                  "exception",
	"swarm event list":             "split",
	"swarm event follow":           "split",
	"swarm event view":             "split",
	"swarm event replay":           "split",
	"swarm event publish":          "split",
	"swarm conversation":           "exception",
	"swarm bundle":                 "exception",
	"swarm bundle build":           "split",
	"swarm forkchat":               "exception",
	"swarm entity":                 "exception",
	"swarm entity list":            "split",
	"swarm entity view":            "split",
	"swarm entity aggregate":       "split",
	"swarm logs":                   "split",
	"swarm incidents":              "split",
	"swarm context":                "exception",
	"swarm context current":        "split",
	"swarm context list":           "split",
	"swarm context prune":          "split",
}

var cliOutputExpectedFactOwners = map[string]string{
	"swarm bundle list":     "/v1/rpc bundle.list",
	"swarm bundle show":     "/v1/rpc bundle.get",
	"swarm bundle agents":   "/v1/rpc bundle.agents",
	"swarm bundle register": "/v1/rpc bundle.register",
	"swarm bundle delete":   "/v1/rpc bundle.delete",
}

var cliOutputSharedDisplayProofs = map[string][]string{
	"writeAgentDeliveryLifecycleListResult":  {"writeCLITable"},
	"writeAgentListResult":                   {"writeCLITable"},
	"writeBundleAgentsHuman":                 {"writeCLITable"},
	"writeBundleDetailHuman":                 {"writeCLIFieldLine"},
	"writeBundleListHuman":                   {"writeCLITable"},
	"writeConnectionsTable":                  {"writeCLITable"},
	"writeContextListText":                   {"writeCLITable"},
	"writeConversationDetailResult":          {"writeCLITable", "writeCLIFieldLine"},
	"writeConversationListResult":            {"writeCLITable"},
	"writeConversationTurnDetailResult":      {"writeCLIFieldLine"},
	"writeDiagnosticRunList":                 {"writeCLITable"},
	"writeDiagnosticRunTrace":                {"writeCLITable"},
	"writeDiagnosticRunTraceDeliveryDetail":  {"writeCLITable"},
	"writeDiagnosticRunTraceDeliverySummary": {"writeCLITable"},
	"writeEntityAggregateResult":             {"writeCLITable"},
	"writeEntityFullResult":                  {"writeCLIFieldLine"},
	"writeEntityListResult":                  {"writeCLITable"},
	"writeEventDeadLetterLine":               {"writeCLIFieldLine"},
	"writeEventDetailResult":                 {"writeCLIFieldLine"},
	"writeEventListResult":                   {"writeCLITable"},
	"writeForkChatListResult":                {"writeCLITable"},
	"writeForkChatSessionDetail":             {"writeCLITable"},
	"writeForkChatSessionHeader":             {"writeCLIFieldLine"},
	"writeMailboxDetailResult":               {"writeCLIFieldLine"},
	"writeMailboxListResult":                 {"writeCLITable"},
	"writeRuntimeIncidentListResult":         {"writeCLITable"},
	"writeSecretsTable":                      {"writeCLITable"},
}

func cliOutputConformanceRegistryRows(t *testing.T) map[string]cliOutputConformanceRegistryRow {
	t.Helper()
	spec := loadCLISpecification(t)
	foundations := driftMappingValue(spec, "foundations")
	outputContract := driftMappingValue(foundations, "output_contract")
	commandSupport := driftMappingValue(outputContract, "command_support")
	registry := driftMappingValue(commandSupport, "output_conformance_registry")
	if registry == nil {
		t.Fatal("output_conformance_registry not found")
	}
	rows := driftMappingValue(registry, "rows")
	if rows == nil || rows.Kind != yaml.MappingNode {
		t.Fatal("output_conformance_registry.rows not found")
	}
	out := map[string]cliOutputConformanceRegistryRow{}
	for i := 0; i+1 < len(rows.Content); i += 2 {
		key := rows.Content[i].Value
		node := rows.Content[i+1]
		command := strings.TrimSpace(cliOutputRegistryScalar(node, "command"))
		if command == "" {
			t.Errorf("registry row %s: missing command", key)
			continue
		}
		if _, exists := out[command]; exists {
			t.Errorf("registry command %q appears more than once", command)
		}
		out[command] = cliOutputConformanceRegistryRow{
			Key:            key,
			Command:        command,
			Classification: strings.TrimSpace(cliOutputRegistryScalar(node, "classification")),
			ExceptionRule:  strings.TrimSpace(cliOutputRegistryScalar(node, "exception_rule")),
			OwnerIssue:     strings.TrimSpace(cliOutputRegistryScalar(node, "owner_issue")),
			FactOwner:      strings.TrimSpace(cliOutputRegistryScalar(node, "fact_owner")),
			Reason:         strings.TrimSpace(cliOutputRegistryScalar(node, "reason")),
			Node:           node,
		}
	}
	return out
}

func cliOutputRegistryScalar(node *yaml.Node, key string) string {
	value := driftMappingValue(node, key)
	if value == nil {
		return ""
	}
	return value.Value
}

func visibleCLICommandPaths(t *testing.T) map[string]*cobra.Command {
	t.Helper()
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	root.InitDefaultHelpCmd()
	paths := map[string]*cobra.Command{}
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		if cmd.Hidden {
			return
		}
		key := strings.Join(path, " ")
		paths[key] = cmd
		for _, child := range cmd.Commands() {
			if child.Hidden {
				continue
			}
			walk(child, append(path, child.Name()))
		}
	}
	walk(root, []string{root.Name()})
	return paths
}

func TestCLIOutputConformanceRegistryCoversVisibleCommandTree(t *testing.T) {
	rows := cliOutputConformanceRegistryRows(t)
	visible := visibleCLICommandPaths(t)
	for command := range visible {
		if _, ok := rows[command]; !ok {
			t.Errorf("visible command %q missing from output_conformance_registry", command)
		}
	}
	for command := range rows {
		if _, ok := visible[command]; !ok {
			t.Errorf("output_conformance_registry command %q is not visible in the Cobra tree", command)
		}
	}
}

func TestCLIOutputConformanceRegistryCoversImplementedCommandCatalogRows(t *testing.T) {
	rows := cliOutputConformanceRegistryRows(t)
	catalog := driftMappingValue(loadCLISpecification(t), "command_catalog")
	if catalog == nil {
		t.Fatal("command_catalog not found")
	}
	checked := 0
	for i := 0; i+1 < len(catalog.Content); i += 2 {
		rowName := catalog.Content[i].Value
		row := catalog.Content[i+1]
		command := driftMappingValue(row, "command")
		status := driftMappingValue(row, "implementation_status")
		if command == nil || status == nil {
			continue
		}
		if !strings.HasPrefix(status.Value, "implemented") && status.Value != "partial" {
			continue
		}
		if rowName == "root" {
			checked++
			if _, ok := rows["swarm"]; !ok {
				t.Errorf("command_catalog.root: implemented command %q missing output_conformance_registry row", command.Value)
			}
			continue
		}
		path := commandPathTokens(command.Value)
		if len(path) == 0 {
			t.Errorf("command_catalog.%s: cannot derive command path from %q", rowName, command.Value)
			continue
		}
		checked++
		registryCommand := "swarm " + strings.Join(path, " ")
		if _, ok := rows[registryCommand]; !ok {
			t.Errorf("command_catalog.%s: implemented command %q missing output_conformance_registry row", rowName, registryCommand)
		}
	}
	if checked < 40 {
		t.Fatalf("implemented command_catalog rows checked = %d, want >= 40; row detection broken", checked)
	}
}

func TestCLIOutputConformanceRegistryRowsAreWellFormed(t *testing.T) {
	rows := cliOutputConformanceRegistryRows(t)
	visible := visibleCLICommandPaths(t)
	exceptionRules := cliOutputExceptionRuleNames(t)
	issueRef := regexp.MustCompile(`^#\d+$`)
	for command, row := range rows {
		switch row.Classification {
		case "shared_output":
			if _, ok := cliOutputGrandfatheredNonSharedRows[command]; ok {
				t.Errorf("%s: command %q migrated to shared_output but still appears in the non-shared grandfather list", row.Key, command)
			}
			cmd := visible[command]
			if cmd == nil {
				t.Errorf("%s: shared_output row does not resolve to a visible command", row.Key)
				continue
			}
			for _, flag := range []string{cliOutputJSONFlag, cliOutputQuietFlag, cliOutputNoColorFlag} {
				if cmd.Flags().Lookup(flag) == nil {
					t.Errorf("%s: shared_output command %q missing --%s flag from the shared output owner", row.Key, command, flag)
				}
			}
			for _, key := range []string{"fact_owner", "json_shape", "quiet_values"} {
				if driftMappingValue(row.Node, key) == nil {
					t.Errorf("%s: shared_output row missing %s", row.Key, key)
				}
			}
			if want := cliOutputExpectedFactOwners[command]; want != "" && row.FactOwner != want {
				t.Errorf("%s: shared_output command %q fact_owner = %q, want %q", row.Key, command, row.FactOwner, want)
			}
		case "exception":
			if got := cliOutputGrandfatheredNonSharedRows[command]; got != row.Classification {
				t.Errorf("%s: exception command %q is not in the grandfathered non-shared allowlist with classification exception", row.Key, command)
			}
			if row.ExceptionRule == "" {
				t.Errorf("%s: exception row missing exception_rule", row.Key)
			} else if !exceptionRules[row.ExceptionRule] {
				t.Errorf("%s: exception_rule %q is not declared under output_contract.exception_rules", row.Key, row.ExceptionRule)
			}
			if row.OwnerIssue != "" {
				t.Errorf("%s: exception row must not carry owner_issue %q", row.Key, row.OwnerIssue)
			}
		case "split":
			if got := cliOutputGrandfatheredNonSharedRows[command]; got != row.Classification {
				t.Errorf("%s: split command %q is not in the grandfathered non-shared allowlist with classification split", row.Key, command)
			}
			if !issueRef.MatchString(row.OwnerIssue) {
				t.Errorf("%s: split row owner_issue = %q, want #<digits>", row.Key, row.OwnerIssue)
			}
			if strings.TrimSpace(row.Reason) == "" {
				t.Errorf("%s: split row missing reason", row.Key)
			}
		default:
			t.Errorf("%s: unknown classification %q", row.Key, row.Classification)
		}
	}
	for command, classification := range cliOutputGrandfatheredNonSharedRows {
		row, ok := rows[command]
		if !ok {
			t.Errorf("grandfathered non-shared command %q has no registry row", command)
			continue
		}
		if row.Classification != classification {
			t.Errorf("grandfathered non-shared command %q classification = %q, want %q", command, row.Classification, classification)
		}
	}
}

func TestCLIOutputConformanceExceptionRulesDoNotContradictRegistry(t *testing.T) {
	rows := cliOutputConformanceRegistryRows(t)
	for _, command := range cliOutputAbsentCommandRows(t) {
		if _, ok := rows["swarm "+command]; ok {
			t.Errorf("absent_command_rows lists %q, but output_conformance_registry has a visible row for %q", command, "swarm "+command)
		}
	}
}

func cliOutputExceptionRuleNames(t *testing.T) map[string]bool {
	t.Helper()
	spec := loadCLISpecification(t)
	rules := driftMappingValue(driftMappingValue(driftMappingValue(spec, "foundations"), "output_contract"), "exception_rules")
	if rules == nil || rules.Kind != yaml.MappingNode {
		t.Fatal("output_contract.exception_rules not found")
	}
	out := map[string]bool{}
	for i := 0; i+1 < len(rules.Content); i += 2 {
		out[rules.Content[i].Value] = true
	}
	return out
}

func cliOutputAbsentCommandRows(t *testing.T) []string {
	t.Helper()
	spec := loadCLISpecification(t)
	foundations := driftMappingValue(spec, "foundations")
	outputContract := driftMappingValue(foundations, "output_contract")
	exceptionRules := driftMappingValue(outputContract, "exception_rules")
	absent := driftMappingValue(exceptionRules, "absent_command_rows")
	if absent == nil || absent.Kind != yaml.MappingNode {
		t.Fatal("output_contract.exception_rules.absent_command_rows not found")
	}
	commands := driftMappingValue(absent, "commands")
	if commands == nil {
		return nil
	}
	if commands.Kind != yaml.SequenceNode {
		t.Fatalf("output_contract.exception_rules.absent_command_rows.commands kind = %v, want sequence", commands.Kind)
	}
	out := make([]string, 0, len(commands.Content))
	for _, command := range commands.Content {
		out = append(out, strings.TrimSpace(command.Value))
	}
	return out
}

func TestCLIOutputConformanceSharedRowsConsumeSharedOwner(t *testing.T) {
	rows := cliOutputConformanceRegistryRows(t)
	calls := cliOutputFunctionCalls(t)
	for command, row := range rows {
		proof, hasProof := cliOutputSharedOwnerProofs[command]
		if row.Classification == "shared_output" {
			if !hasProof {
				t.Errorf("%s: shared_output command %q missing shared owner proof metadata", row.Key, command)
				continue
			}
			if !calls[proof.Constructor]["bindCLIOutputFlags"] {
				t.Errorf("%s: constructor %s does not call bindCLIOutputFlags", row.Key, proof.Constructor)
			}
			if !calls[proof.Runner]["renderCLIOutput"] {
				t.Errorf("%s: runner %s does not call renderCLIOutput", row.Key, proof.Runner)
			}
			continue
		}
		if hasProof {
			t.Errorf("%s: non-shared command %q has stale shared owner proof metadata", row.Key, command)
		}
	}
	for command := range cliOutputSharedOwnerProofs {
		row, ok := rows[command]
		if !ok {
			t.Errorf("shared owner proof for %q has no registry row", command)
			continue
		}
		if row.Classification != "shared_output" {
			t.Errorf("shared owner proof for %q points at %s row", command, row.Classification)
		}
	}
}

func TestCLIOutputConformanceMigratedDisplayWritersConsumeSharedRenderer(t *testing.T) {
	calls := cliOutputFunctionCalls(t)
	for fn, requiredCalls := range cliOutputSharedDisplayProofs {
		fnCalls, ok := calls[fn]
		if !ok {
			t.Errorf("display proof function %s not found", fn)
			continue
		}
		for _, required := range requiredCalls {
			if !fnCalls[required] {
				t.Errorf("%s does not call %s", fn, required)
			}
		}
	}
}

func TestCLIOutputConformanceNoStaleActiveSplitOwners(t *testing.T) {
	staleOwners := map[string]string{
		"#1814": "display renderer migration is complete",
		"#1816": "run trace fidelity migration is complete",
		"#1821": "registry-ratchet parent must be closeable after active rows move to #1913",
	}
	rows := cliOutputConformanceRegistryRows(t)
	for command, row := range rows {
		if reason, ok := staleOwners[row.OwnerIssue]; ok {
			t.Errorf("%s: command %q still points at stale split owner %s (%s); split rows need a live active owner", row.Key, command, row.OwnerIssue, reason)
		}
	}
}

func TestCLIOutputConformanceNoUnclassifiedLiteralTabTables(t *testing.T) {
	root := driftTestRepoRoot(t)
	dir := filepath.Join(root, "cmd", "swarm")
	allowedFiles := map[string]string{
		"logs.go": "split to #1819 logs formatting",
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			if imp.Path != nil && imp.Path.Value == strconv.Quote("text/tabwriter") && allowedFiles[name] == "" {
				t.Errorf("%s imports text/tabwriter; table/list display must consume writeCLITable", name)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !cliOutputIsFmtPrintCall(call) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil || !strings.Contains(value, "\t") {
					continue
				}
				if allowedFiles[name] != "" {
					continue
				}
				t.Errorf("%s contains fmt print literal tab %q; table/list display must consume writeCLITable", name, value)
			}
			return true
		})
	}
}

func cliOutputIsFmtPrintCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "fmt" {
		return false
	}
	return strings.HasPrefix(sel.Sel.Name, "Fprint")
}

func cliOutputFunctionCalls(t *testing.T) map[string]map[string]bool {
	t.Helper()
	root := driftTestRepoRoot(t)
	dir := filepath.Join(root, "cmd", "swarm")
	calls := map[string]map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if calls[fn.Name.Name] == nil {
				calls[fn.Name.Name] = map[string]bool{}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				ident, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch ident.Name {
				case "bindCLIOutputFlags", "renderCLIOutput", "writeCLITable", "writeCLIFieldLine", "writeCLITitle", "writeCLIEmptyState":
					calls[fn.Name.Name][ident.Name] = true
				}
				return true
			})
		}
	}
	return calls
}
