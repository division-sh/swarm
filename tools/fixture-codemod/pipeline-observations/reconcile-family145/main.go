package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const base = "c96e20aa7484b23b017f1787a3ee74eced517c54"
const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const transitionLedger = "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
const targetDigest = "0ca51ec94eedf1a4d7009a01abe50dc89a7f0cd9b611f23b7faf7484e0b41fa3"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string    `json:"Successor,omitempty"`
	Removed                               bool      `json:"Removed,omitempty"`
	Mechanical                            *snapshot `json:"Mechanical,omitempty"`
}
type snapshot struct{ SourceCommit, BeforeSHA256, After string }
type transition struct {
	File, Function, Before, After string
	Removed                       bool `json:"Removed,omitempty"`
}

var files = []string{
	"internal/apiv1/operator_agent_control_test.go",
	"internal/apiv1/operator_data_test.go",
	"internal/apiv1/operator_event_publish_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/operator_run_start_test.go",
	"internal/apiv1/operator_runtime_context_test.go",
	"internal/apiv1/operator_stage_completion_journey_test.go",
	"internal/apiv1/operator_stage_completion_read_test.go",
	"internal/apiv1/operator_test_setup_test.go",
	"internal/apiv1/sqlite_observability_supported_surface_test.go",
	"internal/cliapp/data_show_selected_test.go",
	"internal/cliapp/runtime_log_filter_supported_test.go",
	"internal/runtime/conformance/reply_resolution_conformance_test.go",
	"internal/runtime/conformance/delivery_lifecycle_conformance_test.go",
	"internal/runtime/conformance/empty_handler_construction_test.go",
	"internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go",
	"internal/runtime/conformance/fan_out_drain_failure_diagnostic_test.go",
	"internal/runtime/conformance/fan_out_expired_evaluation_conformance_test.go",
	"internal/runtime/conformance/fan_out_nested_serving_helpers_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_test.go",
	"internal/runtime/conformance/fan_out_resource_source_proof_test.go",
	"internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go",
	"internal/runtime/conformance/fan_out_serving_generation_replacement_test.go",
	"internal/runtime/conformance/fan_out_serving_matrix_helpers_test.go",
	"internal/runtime/conformance/fan_out_shared_serving_conformance_test.go",
	"internal/runtime/conformance/fan_out_supplemental_helpers_test.go",
	"internal/runtime/conformance/lifecycle_diagnostic_overlap_test.go",
	"internal/runtime/conformance/notify_all_children_runtime_conformance_test.go",
	"internal/runtime/deliverylifecycle/claim_commit_runtime_test.go",
	"internal/runtime/inbound_postgres_test.go",
	"internal/runtime/inbound_raw_settlement_store_test.go",
	"internal/runtime/manager/receipt_selected_store_test.go",
	"internal/runtime/manager/terminal_retirement_store_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/runforkexecution/activity_fork_execution_test.go",
	"internal/runtime/runforkexecution/activation_gate_test.go",
	"internal/runtime/runforkexecution/constructed_source_fixture_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/runforkexecution/runtime_outcome_test.go",
	"internal/runtime/runforkexecution/selected_execution_authority_fixture_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/reset_projection_real_docker_test.go",
	"internal/serveapp/startup_integrity_test.go",
}

func main() {
	var rows []recipe
	must(json.Unmarshal(commit(ledger), &rows))
	var changes []transition
	for _, path := range files {
		before := functions(commit(path))
		data, err := os.ReadFile(path)
		must(err)
		after := functions(data)
		keys := make([]string, 0, len(before))
		for key := range before {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			original := before[key]
			replacement, removed := currentReplacement(path, key, after)
			if original == replacement {
				continue
			}
			change := transition{File: path, Function: functionName(original), Before: original, After: replacement, Removed: removed}
			changes = append(changes, change)
			rows = extend(rows, change)
		}
	}
	raw, err := json.Marshal(changes)
	must(err)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	fmt.Printf("transitions=%d digest=%s\n", len(changes), digest)
	for _, change := range changes {
		fmt.Printf("%s\t%s\n", change.File, change.Function)
	}
	if len(os.Args) == 2 && os.Args[1] == "-write" {
		if digest != targetDigest {
			panic("family145 transition differs from its inspected exact source")
		}
		writeJSON(ledger, rows)
		writeJSON(transitionLedger, changes)
	} else if len(os.Args) != 1 {
		panic("usage: reconcile-family145 [-write]")
	}
}

func currentReplacement(path, key string, after map[string]string) (string, bool) {
	if replacement := after[key]; replacement != "" {
		return replacement, false
	}
	if path == "internal/apiv1/operator_event_publish_test.go" {
		if key == "sqliteEventDeliveryTargetRoutes" {
			return "", true
		}
		if key == "postgresEventDeliveryTargetRoutes" && after["publicationDeliveryTargetRoutes"] != "" {
			return after["publicationDeliveryTargetRoutes"], false
		}
	}
	if path == "internal/apiv1/operator_event_replay_test.go" && (key == "updateCompleteReplayChainDepth" || key == "updateCompleteReplayTargetRoute") {
		return "", true
	}
	panic("unreviewed retirement: " + path + "/" + key)
}

func extend(rows []recipe, change transition) []recipe {
	match := -1
	for i, row := range rows {
		if row.File != change.File || functionKey(row.After) != functionKey(change.Before) {
			continue
		}
		if match != -1 {
			panic("ambiguous current recipe: " + change.File + "/" + change.Function)
		}
		match = i
	}
	if match < 0 {
		if change.Removed {
			return append(rows, recipe{Family: "native-postgres-admission-family145", File: change.File, Function: change.Function, Before: change.Before, After: change.Before, Removed: true})
		}
		return append(rows, recipe{Family: "native-postgres-admission-family145", File: change.File, Function: change.Function, Before: change.Before, After: change.After})
	}
	row := &rows[match]
	current := row.After
	if row.Successor != "" {
		current = row.Successor
	}
	if node(parseFunction(current)) != change.Before || row.Removed {
		panic("noncanonical predecessor: " + change.File + "/" + change.Function)
	}
	if change.Removed {
		row.Successor, row.Removed = "", true
	} else {
		row.Successor = change.After
	}
	return rows
}

func functions(source []byte) map[string]string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	must(err)
	result := make(map[string]string)
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		key := key(fn)
		if _, duplicate := result[key]; duplicate {
			panic("ambiguous function: " + key)
		}
		result[key] = node(fn)
	}
	return result
}

func functionKey(source string) string {
	fn := parseFunction(source)
	return key(fn)
}

func functionName(source string) string { return parseFunction(source).Name.Name }

func parseFunction(source string) *ast.FuncDecl {
	file, err := parser.ParseFile(token.NewFileSet(), "function.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	if len(file.Decls) != 1 {
		panic("noncanonical function snapshot")
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		panic("snapshot is not a function")
	}
	return fn
}

func key(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return fn.Name.Name
	}
	return fn.Name.Name + "/" + node(fn.Recv.List[0].Type)
}

func node(n ast.Node) string {
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), n))
	return strings.TrimSpace(out.String())
}

func commit(path string) []byte {
	data, err := exec.Command("git", "show", base+":"+path).Output()
	must(err)
	return data
}

func writeJSON(path string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	must(err)
	must(os.WriteFile(path, append(data, '\n'), 0644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
