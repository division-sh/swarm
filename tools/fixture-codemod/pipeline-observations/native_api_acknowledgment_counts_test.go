package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func nativeAPIAcknowledgmentCountRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes": true,
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":       true,
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":    true,
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence": true,
		"TestOperatorEventPublishQueuesWhileRuntimePaused":                              true,
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                   true,
		"TestOperatorEventReplayQueuesWhenDispatchGated":                                true,
		"countEventDeliveriesForEvent":                                                  true,
		"countPipelineReceiptsForEvent":                                                 true}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 9 {
		t.Fatalf("ack count recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}
func TestNativeAPIAcknowledgmentCountsRetainAllWorkAndContext(t *testing.T) {
	for _, row := range nativeAPIAcknowledgmentCountRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("ack/replay/paused/source/workload/assertions changed: %s", row.Function)
			}
			continue
		}
		cfg := map[string][2]string{
			"countEventDeliveriesForEvent":  {"CountAgentEventDeliveryStorage", "count event deliveries for %s: %v"},
			"countPipelineReceiptsForEvent": {"CountPipelineEventReceiptStorage", "count pipeline receipts for %s: %v"},
		}[row.Function]
		want := "func " + row.Function + "(t *testing.T,ctx context.Context,selected any,eventID string)int{t.Helper();count,err:=storetest." + cfg[0] + "(ctx,selected,eventID);if err!=nil{t.Fatalf(" + strconv.Quote(cfg[1]) + ",eventID,err)};return count}"
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatalf("ack key/context/owner/refusal changed: %s", row.Function)
		}
	}
}
func TestNativeAPIAcknowledgmentCountsRejectWrongContextOwnerOrKey(t *testing.T) {
	probes := 0
	for _, row := range nativeAPIAcknowledgmentCountRecipes(t) {
		for _, change := range [][2]string{{", ctx, pg,", ", wrongContext, pg,"}, {", ctx, pg,", ", ctx, wrongOwner,"}, {"eventID);", "otherEventID);"}, {"got != 1", "got < 1"}} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPIPhysicalCardinalityWorkload(t, row.After) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("changed ack proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes < 5 {
		t.Fatalf("ack adversaries missing: %d", probes)
	}
}
func TestNativeAPIAcknowledgmentCountCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]map[string]int{}
	for _, path := range paths {
		bytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, bytes, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := formattedNativeReadNode(call.Fun)
				if name != "countEventDeliveriesForEvent" && name != "countPipelineReceiptsForEvent" {
					return true
				}
				if actual[fn.Name.Name] == nil {
					actual[fn.Name.Name] = map[string]int{}
				}
				actual[fn.Name.Name][name]++
				if len(call.Args) != 4 || formattedNativeReadNode(call.Args[2]) != "pg" {
					t.Fatalf("raw/foreign ack count owner: %s", fn.Name.Name)
				}
				return true
			})
		}
	}
	expected := map[string]map[string]int{"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes": {"countPipelineReceiptsForEvent": 2},
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":       {"countEventDeliveriesForEvent": 1, "countPipelineReceiptsForEvent": 1},
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":    {"countEventDeliveriesForEvent": 1},
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence": {"countEventDeliveriesForEvent": 1},
		"TestOperatorEventPublishQueuesWhileRuntimePaused":                              {"countEventDeliveriesForEvent": 1, "countPipelineReceiptsForEvent": 2},
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                   {"countEventDeliveriesForEvent": 2},
		"TestOperatorEventReplayQueuesWhenDispatchGated":                                {"countEventDeliveriesForEvent": 1, "countPipelineReceiptsForEvent": 1}}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("ack consumers differ: got=%v want=%v", actual, expected)
	}
}
