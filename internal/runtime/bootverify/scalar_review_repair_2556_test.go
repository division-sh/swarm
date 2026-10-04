package bootverify

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func scalar2556LoadedSource(t *testing.T, handler string) (*checkerContext, contracts.ScopedNodeRecord, string) {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: diagnostic\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "proof.requested:\n  items: list<text>\nproof.done:\n  value: text?\n  inherited: text?\n")
	nodes := "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n" + handler
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), nodes)
	repo := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
	for _, record := range bundle.ScopedNodeRecords() {
		if record.LogicalID == "worker" {
			return &checkerContext{source: semanticview.Wrap(bundle)}, record, nodes
		}
	}
	t.Fatal("loaded worker missing")
	return nil, contracts.ScopedNodeRecord{}, ""
}

func scalar2556AssertDiagnosticCoordinates(t *testing.T, ctx *checkerContext, record contracts.ScopedNodeRecord, source string, reader expressionReference) {
	t.Helper()
	line, column := 0, 0
	for index, text := range strings.Split(source, "\n") {
		if position := strings.Index(text, "missing_root"); position >= 0 {
			if line != 0 {
				t.Fatal("witness must have exactly one offending scalar")
			}
			line, column = index+1, position+1
		}
	}
	cause := workflowexpr.ValidateValueExpression(reader.Expression)
	if cause == nil {
		t.Fatal("offending scalar unexpectedly passed canonical CEL validation")
	}
	err := ctx.authoredExpressionError(record, "proof.requested", reader, cause)
	if want := fmt.Sprintf("nodes.yaml:%d:%d:", line, column); line == 0 || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s (source slot %s) expected %s: %v", reader.Kind, reader.SourceSlot, want, err)
	}
}

func TestScalar2556RuleDiagnosticCoordinatesUsePositions(t *testing.T) {
	for _, collection := range []string{"rules", "on_complete"} {
		for _, label := range []string{"", "named", "1", "0", "bracket]label"} {
			t.Run(collection+"/"+label, func(t *testing.T) {
				field, tail := "when", "        - else: true\n"
				if collection == "on_complete" {
					field, tail = "condition", "        - condition: true\n          emit: proof.done\n"
				}
				row := "        - " + field + ": missing_root\n"
				if label != "" {
					row = "        - id: '" + label + "'\n          " + field + ": missing_root\n"
				}
				ctx, record, source := scalar2556LoadedSource(t, "      "+collection+":\n"+row+tail)
				node, _ := record.Identity()
				readers := handlerConditionExpressionsForSource(ctx.source, node, "proof.requested", record.Entry.EventHandlers["proof.requested"])
				found := false
				for _, reader := range readers {
					if reader.Expression == "missing_root" {
						scalar2556AssertDiagnosticCoordinates(t, ctx, record, source, reader)
						found = true
					}
				}
				if !found {
					t.Fatal("offending condition not enumerated")
				}
				findings := checkConditionExpressionValidation(ctx)
				matched := false
				for _, finding := range findings {
					if strings.Contains(finding.Message, "missing_root") && strings.Contains(finding.Message, "nodes.yaml:") {
						matched = true
					}
				}
				if !matched {
					t.Fatalf("actual verifier lost coordinate-bearing CEL error: %#v", findings)
				}
			})
		}
	}
}

func TestScalar2556NestedDiagnosticSourceOwnership(t *testing.T) {
	for _, tc := range []struct{ name, handler string }{
		{"rule activity escaped key", "      rules:\n        - id: '1'\n          else: true\n          activity:\n            tool: send\n            input:\n              'value.part': missing_root\n"},
		{"rule write", "      rules:\n        - id: named\n          else: true\n          data_accumulation:\n            writes:\n              - target_field: note\n                value: missing_root\n"},
		{"completion write", "      on_complete:\n        - id: '1'\n          data_accumulation:\n            writes:\n              - target_field: note\n                value: missing_root\n"},
		{"rule emit escaped key", "      rules:\n        - id: '1'\n          else: true\n          emit:\n            event: proof.done\n            fields:\n              'value.part': missing_root\n"},
		{"completion emit", "      on_complete:\n        - id: named\n          emit:\n            event: proof.done\n            fields:\n              value: missing_root\n"},
		{"template specialized", "      emit:\n        event: proof.done\n        fields:\n          inherited: 'text'\n      rules:\n        - id: '1'\n          else: true\n          emit:\n            fields:\n              value: missing_root\n"},
		{"template inherited", "      emit:\n        event: proof.done\n        fields:\n          inherited: missing_root\n      rules:\n        - id: named\n          else: true\n          emit:\n            fields:\n              value: 'text'\n"},
		{"keyed rule", "      rules:\n        chosen:\n          when: missing_root\n        fallback:\n          else: true\n"},
		{"singleton rule write", "      rules:\n        else: true\n        data_accumulation:\n          writes:\n            - target_field: note\n              value: missing_root\n"},
		{"rule fanout identity", "      rules:\n        - id: '1'\n          else: true\n          fan_out:\n            items_from: payload.items\n            as: element\n            identity: missing_root\n            emit:\n              event: proof.done\n              fields:\n                value: element\n"},
		{"completion fanout emit", "      on_complete:\n        - id: named\n          fan_out:\n            items_from: payload.items\n            as: element\n            identity: element\n            emit:\n              event: proof.done\n              fields:\n                value: missing_root\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, record, source := scalar2556LoadedSource(t, tc.handler)
			node, _ := record.Identity()
			matched := false
			for _, reader := range handlerExecutableReaderExpressionsForSource(ctx.source, node, "proof.requested", record.Entry.EventHandlers["proof.requested"]) {
				if reader.Expression == "missing_root" {
					scalar2556AssertDiagnosticCoordinates(t, ctx, record, source, reader)
					matched = true
				}
			}
			if !matched {
				t.Fatal("nested executable reader missing")
			}
		})
	}
}

func TestScalar2556LoadedPredicatesRefuseBeforeExecution(t *testing.T) {
	for _, tc := range []struct{ slot, handler string }{
		{"rule.when", "rules:\n  - when: %s\n  - else: true"},
		{"on_complete.condition", "on_complete:\n  - condition: %s\n    emit: proof.done"},
		{"guard.check", "guard: {check: %s}"},
		{"guard.checks.check", "guard:\n  checks:\n    - check: %s"},
		{"query.filter", "query:\n  entities: account\n  filter: %s"},
		{"filter.condition", "filter:\n  items_from: payload.items\n  condition: %s"},
		{"count.condition", "count:\n  items_from: payload.items\n  condition: %s"},
	} {
		for _, sentinel := range []string{"else", "ELSE", "eLsE"} {
			t.Run(tc.slot+"/"+sentinel, func(t *testing.T) {
				root := t.TempDir()
				writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: diagnostic\n")
				handler := "      " + strings.ReplaceAll(fmt.Sprintf(tc.handler, sentinel), "\n", "\n      ") + "\n"
				writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n"+handler)
				repo := repoRootForBootverifyTest(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err == nil || bundle != nil || !strings.Contains(err.Error(), tc.slot+" must be a CEL predicate") || !strings.Contains(err.Error(), "nodes.yaml:") {
					t.Fatalf("invalid predicate reached an executable bundle: %v, %v", bundle, err)
				}
			})
		}
	}
}

func TestScalar2556ReaderCensusExcludesOnlyInternalDefaults(t *testing.T) {
	for _, expression := range []string{"else", "ELSE", "eLsE"} {
		var readers []expressionReference
		appendExecutableReader(&readers, "activity.input.value.cel", expression, pipeline.WorkflowEntityFieldLifecycleRule)
		appendConditionExecutableReader(&readers, "guard.check", expression, pipeline.WorkflowEntityFieldLifecycleGuard, pipeline.WorkflowConditionContextGuard)
		appendConditionExecutableReader(&readers, "rules[0].condition", expression, pipeline.WorkflowEntityFieldLifecycleRule, pipeline.WorkflowConditionContextRule)
		if len(readers) != 2 || readers[0].Expression != expression || readers[1].ConditionContext != pipeline.WorkflowConditionContextGuard {
			t.Fatalf("non-default expression hidden from validation: %#v", readers)
		}
	}
}
