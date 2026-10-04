package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// Negative/parser snippets and exact mutation strings are migrated explicitly,
// not normalized by this one-shot positive-producer rewrite.
var manualGoSources = map[string]bool{
	"parser_snippets.go":            true,
	"negative.go":                   true,
	"schema_admission_source.go":    true,
	"harness_injection.go":          true,
	"arrival_join_guard_sources.go": true,
	"publication_sites.go":          true,
	"pin_rewrite_syntax.go":         true,
}

var inlineGoSources = []string{
	"internal/runtime/accprojection/resolver_test.go",
	"internal/runtime/authoringview/view_test.go",
	"internal/runtime/bootverify/report_test.go",
	"internal/runtime/bootverify/workflow_singleton_coordinator_checks_test.go",
	"internal/runtime/bus/root_subscription_verify_test.go",
	"internal/runtime/engine/executor_test.go",
	"internal/runtime/pipeline/engine_adapter_test.go",
	"internal/runtime/pipeline/a2_accumulator_persistence_external_test.go",
	"internal/runtime/pipeline/a2_count_join_execution_external_test.go",
	"internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go",
	"internal/runtime/pipeline/a2_join_result_types_external_test.go",
	"internal/runtime/pipeline/a2_same_commit_join_binding_external_test.go",
	"internal/runtime/pipeline/fan_out_backlog_test.go",
	"internal/runtime/pipeline/sqlite_dynamic_activation_test.go",
	"internal/runtime/pipeline/workflow_guard_reachability_proof_test.go",
	"internal/runtime/pipeline/workflow_instance_activation_test.go",
	"internal/runtime/pipeline/workflow_nodes_test.go",
	"internal/runtime/runforkexecution/workflow_receiver_readiness_test.go",
	"internal/store/internal/backend/runforkpersistence/receiver_config_history_test.go",
	"internal/runtime/workflow_timer_startup_recovery_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/eventbus_receiver_authority_e2e_test.go",
	"internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go",
	"internal/apiv1/operator_event_publish_test.go",
	"internal/runtime/bus/root_subscription_authority_test.go",
	"internal/runtime/bus/routing_derivation_test.go",
	"internal/runtime/bus/eventbus_target_routes_test.go",
	"internal/runtime/contracts/workflow_contract_wave1_test.go",
	"internal/runtime/contracts/workflow_contract_schema_presence_test.go",
	"internal/runtime/contracts/workflow_contract_connect_test.go",
	"internal/runtime/contracts/workflow_contracts_tree_test.go",
	"internal/runtime/contracts/load_validation_test.go",
	"internal/serveapp/semantic_numeric_schedule_test.go",
	"internal/serveapp/semantic_numeric_gate_test.go",
	"internal/serveapp/provider_alias_authority_test.go",
	"internal/releasee2e/run_observer_overflow_test.go",
	"internal/store/internal/runtimepersistence/dynamic_flow_creation_atomicity_test.go",
	"internal/store/internal/runtimepersistence/receiver_config_activation_atomicity_test.go",
	"internal/store/internal/runtimepersistence/sqlite_runtime_test.go",
}

func runGoSources(root string, write bool) error {
	directory := "internal/runtime/testfixtures/canonicalrouting"
	files, err := os.ReadDir(filepath.Join(root, directory))
	if err != nil {
		return err
	}
	var plan []change
	for _, entry := range files {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || manualGoSources[name] {
			continue
		}
		name = filepath.ToSlash(filepath.Join(directory, name))
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		after, err := rewriteGoSource(name, data)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, after) {
			plan = append(plan, change{Path: name, After: after})
		}
	}
	for _, name := range inlineGoSources {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		after, err := rewriteGoSource(name, data)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, after) {
			plan = append(plan, change{Path: name, After: after})
		}
	}
	if write {
		if err := applyPlan(root, plan, nil); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(plan)
}

func rewriteGoSource(name string, data []byte) ([]byte, error) {
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, name, data, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var edits []struct {
		start, end int
		value      string
	}
	fragments := map[*ast.BasicLit]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if declaration, ok := node.(*ast.FuncDecl); ok && declaration.Name.Name == "RetiredFanInCoordinatorSchema" {
			markLiteralFragments(declaration, fragments)
		}
		if _, ok := node.(*ast.BinaryExpr); ok {
			markLiteralFragments(node, fragments)
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "strings" && selector.Sel.Name != "TrimSpace" {
					markLiteralFragments(call, fragments)
				}
			}
		}
		return true
	})
	var rewriteErr error
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || rewriteErr != nil {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			rewriteErr = err
			return false
		}
		after := []byte(rewritePinMutationLiteral(name, text))
		if !fragments[literal] {
			after, err = rewriteGoPinLiteral(after)
		}
		if err != nil {
			rewriteErr = fmt.Errorf("%s: %w", positions.Position(literal.Pos()), err)
			return false
		}
		if !bytes.Equal([]byte(text), after) {
			value := strconv.Quote(string(after))
			if strings.HasPrefix(literal.Value, "`") && !bytes.ContainsRune(after, '`') {
				value = "`" + string(after) + "`"
			}
			edits = append(edits, struct {
				start, end int
				value      string
			}{positions.Position(literal.Pos()).Offset, positions.Position(literal.End()).Offset, value})
		}
		return true
	})
	if rewriteErr != nil {
		return nil, rewriteErr
	}
	if len(edits) == 0 {
		return data, nil
	}
	for i := len(edits) - 1; i >= 0; i-- {
		edit := edits[i]
		data = append(append(append([]byte{}, data[:edit.start]...), edit.value...), data[edit.end:]...)
	}
	return format.Source(data)
}

func markLiteralFragments(node ast.Node, fragments map[*ast.BasicLit]bool) {
	ast.Inspect(node, func(child ast.Node) bool {
		if literal, ok := child.(*ast.BasicLit); ok {
			fragments[literal] = true
		}
		return true
	})
}

func rewriteGoPinLiteral(data []byte) ([]byte, error) {
	doc, err := parse(data)
	if err != nil {
		// Most Go strings are paths, CEL, or partial non-schema templates.
		// They do not participate in this closed source rewrite.
		return data, nil
	}
	pins, err := lookupValue(doc.root, "pins")
	if err != nil {
		return nil, err
	}
	if pins.Presence() != yamlsource.PresenceMapping {
		return data, nil
	}
	fields, err := pins.Mapping()
	if err != nil {
		return nil, err
	}
	wrapped := false
	for _, field := range fields {
		wrapped = wrapped || field.Value.Presence() == yamlsource.PresenceMapping
	}
	if !wrapped {
		return data, nil
	}
	converted, replies, err := rewritePins("generated-positive", pins)
	if err != nil {
		return nil, err
	}
	if len(replies) != 0 {
		return nil, fmt.Errorf("reply needs an explicit paired-connect rewrite")
	}
	doc.edit["pins"] = converted
	return render(doc)
}
