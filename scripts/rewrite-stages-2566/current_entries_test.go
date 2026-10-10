package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestRewrite2566RetiredDiagnosticRetainsSurvivingNativeConsumer(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	const retired = "internal/runtime/pipeline/testdata/diagnostics/authored_rule_receiver_retry_test.go"
	if _, err := os.Stat(filepath.Join(root, retired)); !os.IsNotExist(err) {
		t.Fatalf("retired diagnostic source reappeared or could not be checked: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "internal/runtime/pipeline/delivery_native_consumers_external_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "TestAuthoredRuleReceiverPreparationRetryBothStores" {
			continue
		}
		assertDiagnosticNativeConsumerCall(t, function)
		return
	}
	t.Fatal("surviving authored-rule native consumer disappeared")
}

func assertDiagnosticNativeConsumerCall(t *testing.T, function *ast.FuncDecl) {
	t.Helper()
	if len(function.Body.List) != 1 {
		t.Fatal("native consumer is not the exact direct proof")
	}
	statement, ok := function.Body.List[0].(*ast.ExprStmt)
	if !ok {
		t.Fatal("native proof call missing")
	}
	call, ok := statement.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 2 {
		t.Fatal("native proof arguments changed")
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "VerifyNativeAuthoredRuleReceiverPreparationRetryBothStoresForTest" {
		t.Fatal("native proof no longer executes the surviving canonical source owner")
	}
	owner, ok := selector.X.(*ast.Ident)
	if !ok || owner.Name != "pipeline" {
		t.Fatal("native consumer changed its source owner")
	}
	argument, ok := call.Args[0].(*ast.Ident)
	if !ok || argument.Name != "t" {
		t.Fatal("native consumer dropped its live proof context")
	}
	fixture, ok := call.Args[1].(*ast.Ident)
	if !ok || fixture.Name != "pipelineDeliveryNativeFixture" {
		t.Fatal("native store fixture changed")
	}
}

func TestRewrite2566CurrentSelectorsRetainExactSourceCorrespondence(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	body, err := os.ReadFile("entries.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entryGolden
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Current == nil {
			continue
		}
		t.Run(entry.File+"/"+entry.Flow, func(t *testing.T) {
			original := reviewedEntryLiteralForCurrent(t, root, entry)
			current, err := readEntryLiteral(root, entry.Current.File, entry.Current.Function, entry.Current.Literal)
			if err != nil {
				t.Fatal(err)
			}
			assertEntryLiteralStageGolden(t, entry, original)
			assertEntryLiteralStageGolden(t, entry, current)
			t.Logf("source correspondence: original_sha256=%s current_sha256=%s byte_equal=%t", digest([]byte(original.Body)), digest([]byte(current.Body)), original.Body == current.Body)
		})
	}
}

func reviewedEntryLiteralForCurrent(t *testing.T, root string, entry entryGolden) goLiteralSite {
	t.Helper()
	// This master ancestor predates the fixture migration and stays reachable
	// after compaction. It supplies proof evidence, never a reader fallback.
	body, err := exec.Command("git", "-C", root, "show", "055bbbaba13e8d604ed80b73af98ca97c3b7acfb:"+entry.File).Output()
	if err != nil {
		t.Fatal(err)
	}
	sites, err := goLiteralSites(entry.File, body)
	if err != nil {
		t.Fatal(err)
	}
	var found []goLiteralSite
	for _, site := range sites {
		if site.Function == entry.Function && site.Ordinal == entry.Literal {
			found = append(found, site)
		}
	}
	if len(found) != 1 {
		t.Fatal("missing or ambiguous independently reviewed original source")
	}
	return found[0]
}

func assertEntryLiteralStageGolden(t *testing.T, entry entryGolden, site goLiteralSite) {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte(site.Body))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document(entry.File).Root())
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEntryGolden(entry, schema); err != nil {
		t.Fatal(err)
	}
}

func TestRewrite2566CurrentSelectorsPreserveHistoricalDecisionsAndRegeneration(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	reviewed, err := baselineEntryGoldens(root, readPlan(t))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("entries.json")
	if err != nil {
		t.Fatal(err)
	}
	var retained []entryGolden
	if err := json.Unmarshal(body, &retained); err != nil {
		t.Fatal(err)
	}
	merged, err := retainCurrentEntrySelectors(reviewed, retained)
	if err != nil || !reflect.DeepEqual(merged, retained) {
		t.Fatalf("regeneration changed a reviewed tuple or current selector: %v", err)
	}
	count := 0
	for _, entry := range merged {
		if entry.Current != nil {
			count++
		}
	}
	if count != 36 {
		t.Fatalf("current source remaps=%d, want all36 classified historical rows", count)
	}
}

func TestRewrite2566CurrentSelectorsRefuseUnmatchedDuplicatesAndChangedDecisions(t *testing.T) {
	reviewed := entryGolden{File: "old.go", Source: "old.go", Flow: "old/literal-1", Function: "old", Literal: 1,
		Entry: "queued", Order: []string{"queued", "done"}, Finals: []string{"done"}}
	current := reviewed
	current.Current = &currentEntrySelector{File: "current.go", Function: "current", Literal: 1}
	for _, tc := range []struct {
		name   string
		change func(*entryGolden)
	}{
		{"unmatched", func(e *entryGolden) { e.Flow = "unknown/literal-1" }},
		{"entry", func(e *entryGolden) { e.Entry = "done" }},
		{"order", func(e *entryGolden) { e.Order = []string{"done", "queued"} }},
		{"final", func(e *entryGolden) { e.Finals = []string{"queued"} }},
		{"historical identity", func(e *entryGolden) { e.Function = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := current
			tc.change(&changed)
			if _, err := retainCurrentEntrySelectors([]entryGolden{reviewed}, []entryGolden{changed}); err == nil {
				t.Fatal("current coordinates authorized a missing or changed independent decision")
			}
		})
	}
	if _, err := retainCurrentEntrySelectors([]entryGolden{reviewed}, []entryGolden{current, current}); err == nil {
		t.Fatal("duplicate relocation admitted")
	}
	if _, err := retainCurrentEntrySelectors([]entryGolden{reviewed, reviewed}, []entryGolden{current}); err == nil {
		t.Fatal("ambiguous original decision admitted")
	}
}

func TestRewrite2566CurrentSourceRefusesMissingAmbiguousAndChangedStageFacts(t *testing.T) {
	root := t.TempDir()
	expected := entryGolden{File: "retired.go", Source: "retired.go", Flow: "retired/literal-1", Function: "retired", Literal: 1,
		Entry: "queued", Order: []string{"queued", "done"}, Finals: []string{"done"}}
	const source = "package fixture;func current(){ _ = `stages:\n  queued: {}\n  done: {final: true}\n` }"
	if err := os.WriteFile(filepath.Join(root, "current.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	for _, selector := range []currentEntrySelector{
		{File: "missing.go", Function: "current", Literal: 1},
		{File: "current.go", Function: "missing", Literal: 1},
		{File: "current.go", Function: "current", Literal: 2},
	} {
		if _, err := readEntryLiteral(root, selector.File, selector.Function, selector.Literal); err == nil {
			t.Fatal("missing current target admitted")
		}
	}
	for _, tc := range []struct{ name, source string }{
		{"entry/order", strings.Replace(source, "  queued: {}\n  done: {final: true}", "  done: {final: true}\n  queued: {}", 1)},
		{"final membership", strings.Replace(source, "{final: true}", "{}", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(root, "current.go"), []byte(tc.source), 0600); err != nil {
				t.Fatal(err)
			}
			site, err := readEntryLiteral(root, "current.go", "current", 1)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := yamlsource.Load([]byte(site.Body))
			if err != nil {
				t.Fatal(err)
			}
			schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document("current.go").Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := assertEntryGolden(expected, schema); err == nil {
				t.Fatal("remap concealed changed stage facts")
			}
		})
	}
	if err := os.WriteFile(filepath.Join(root, "current.go"), []byte(source+";func current(){}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readEntryLiteral(root, "current.go", "current", 1); err == nil {
		t.Fatal("ambiguous current function admitted")
	}
}

func TestRewrite2566CurrentSelectorShapeRemainsClosed(t *testing.T) {
	entry := entryGolden{Function: "historical", Current: &currentEntrySelector{File: "current.go", Function: "current", Literal: 1}}
	if err := validateCurrentEntrySelector(entry); err != nil {
		t.Fatal(err)
	}
	for _, selector := range []currentEntrySelector{
		{File: "../outside.go", Function: "current", Literal: 1},
		{File: "current.go", Function: "", Literal: 1},
		{File: "current.go", Function: "current", Literal: 0},
		{File: "current.go", Function: "current", Literal: 1, Materialize: true},
		{File: deliveryEntryOwner, Function: "CopyPipelineDeliveryAuthority", Literal: 2},
		{File: deliveryEntryOwner, Function: "CopyOtherSource", Literal: 2, Materialize: true},
	} {
		changed := entry
		changed.Current = &selector
		if err := validateCurrentEntrySelector(changed); err == nil {
			t.Fatalf("invalid selector admitted: %+v", selector)
		}
	}
	retained := []entryGolden{entry}
	original := entry
	original.Current = nil
	first, err := retainCurrentEntrySelectors([]entryGolden{original}, retained)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retainCurrentEntrySelectors([]entryGolden{original}, first)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("remap was not idempotent: %v", err)
	}
}
