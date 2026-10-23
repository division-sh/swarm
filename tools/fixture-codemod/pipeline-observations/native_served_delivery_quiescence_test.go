package main

import (
	"encoding/json"

	"go/ast"

	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestServedDeliveryQuiescenceCallerInventoryConsumesOnlyDomainReadPorts(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	counts := map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "serveapp"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return walkErr
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || formattedNativeReadNode(call.Fun) != "waitServedRunDeliveryQuiescence" {
					return true
				}
				counts[fn.Name.Name]++
				if len(call.Args) != 3 || strings.Contains(formattedNativeReadNode(call.Args[1]), ".DB") || formattedNativeReadNode(call.Args[1]) == "db" {
					t.Errorf("delivery wait regained raw/dialect authority: %s/%s", path, formattedNativeReadNode(call))
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if len(counts) != 82 || total != 155 || counts["TestServedRunDeliveryQuiescenceReaderPreservesStableStatusCut"] != 1 ||
		counts["proveServedRetainedJoinOutcomeHeld"] != 3 || counts["testServedJoinWriterCompletedHistoryRefusal"] != 5 {
		t.Fatalf("delivery family inventory functions=%d,calls=%d,want81/150 plus three held-witness and two retained-outcome waits", len(counts), total)
	}
}

func TestServedDeliveryQuiescenceOwnerRecipesRemainSourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-served-delivery-quiescence-owner" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		if row.File == "internal/serveapp/receiver_restart_test.go" && row.Function == "TestReceiverCompositionRestartBothStores" {
			const original = "`SELECT count(*) FROM entity_state WHERE run_id=$1 AND current_state='done'`"
			const paired = "`SELECT count(*) FROM entity_state es JOIN flow_instances fi\n" +
				"ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\n" +
				"WHERE es.run_id=$1 AND fi.current_state='done'`"
			if strings.Count(current, original) != 1 {
				t.Fatal("missing exact historical restart construction-count read")
			}
			historical := current
			current = strings.Replace(current, original, paired, 1)
			guard := row
			guard.Before, guard.After, guard.Successor = current, current, ""
			if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+historical), guard); err == nil || changed {
				t.Fatal("retired data-row stage count admitted as the current restart reader")
			}
			for _, cut := range []string{"fi.run_id=es.run_id", "fi.instance_path=es.flow_instance", "fi.entity_id=es.entity_id", "fi.current_state='done'", "entities != 2"} {
				mutant := strings.Replace(current, cut, "foreignCut", 1)
				if mutant == current {
					t.Fatalf("missing exact restart construction binding: %s", cut)
				}
				if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
					t.Fatalf("restart lost lifecycle owner, composite membership or exact count: %s", cut)
				}
			}
		}
		want, err := canonicalFunction(current)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got {
			t.Fatalf("construction/lifetime/delivery predicate changed: %s", row.Function)
		}
		if row.Function != "waitServedRunDeliveryQuiescence" && !strings.Contains(current, "p.deps.DeliveryStore.SummarizeRun") && !strings.Contains(current, "InspectionDeliveryReader(t,") {
			t.Fatalf("constructor did not bind an original delivery or inspection owner: %s", row.Function)
		}
	}
	if matched != 20 {
		t.Fatalf("delivery owner recipes=%d,want20", matched)
	}
}
