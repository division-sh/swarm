package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145TemplateWaitAssertions(before, after string) error {
	original, err := runSetupAssertions(before)
	if err != nil {
		return err
	}
	current, err := runSetupAssertions(after)
	if err != nil {
		return err
	}
	for i, assertion := range original {
		for _, replacement := range [][2]string{{"for query %s", "for observation %s"}, {"from query %s", "from observation %s"}, {"strings.TrimSpace(query)", "observation"}, {"runtimeTestEventDiagnostics(ctx, db)", "runtimeTestEventDiagnostics(ctx, selected)"}} {
			assertion = strings.ReplaceAll(assertion, replacement[0], replacement[1])
		}
		original[i] = assertion
	}
	if strings.Join(original, "\n") != strings.Join(current, "\n") || strings.Count(after, "read()") != 1 || family145PublicationRawEscape([]byte("package proof\n"+after)) != nil {
		return fmt.Errorf("template wait/assertion changed an original refusal, cardinality or closed read")
	}
	if strings.Contains(before, "10 * time.Second") && (!strings.Contains(after, "10 * time.Second") || !strings.Contains(after, "25 * time.Millisecond")) {
		return fmt.Errorf("template polling changed original deadline or spacing")
	}
	return nil
}

func TestFamily145TemplateWaitHelpersPreserveEveryRefusalAndDeadline(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range rows {
		if row.File != "internal/runtime/template_instance_delivery_test.go" || (row.Function != "waitRuntimeDBCount" && row.Function != "assertRuntimeDBCount" && row.Function != "waitRuntimeEventID") {
			continue
		}
		checked++
		if err := family145TemplateWaitAssertions(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		assertions, err := runSetupAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			if family145TemplateWaitAssertions(row.Before, strings.Replace(row.After, assertion, "ignore()", 1)) == nil {
				t.Fatal("dropped template assertion accepted")
			}
		}
		if strings.Contains(row.After, "10 * time.Second") && family145TemplateWaitAssertions(row.Before, strings.Replace(row.After, "10 * time.Second", "20 * time.Second", 1)) == nil {
			t.Fatal("inflated template deadline accepted")
		}
	}
	if checked != 3 {
		t.Fatalf("template assertion helper census=%d want3", checked)
	}
}

func TestFamily145TemplateOrdinaryOpeningAndWholeHelperChainAreClosed(t *testing.T) {
	rootCount := 0
	for _, path := range []string{"template_instance_delivery_test.go", "template_flow_pilot_runtime_test.go", "final_flow_instance_authoring_runtime_test.go"} {
		data, err := os.ReadFile("../../../internal/runtime/" + path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			source := family145NodeString(fn)
			if strings.HasPrefix(fn.Name.Name, "Test") {
				rootCount += strings.Count(source, "storetest.StartPostgresRuntimeStore(t)")
				if fn.Name.Name == "TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute" {
					if !family145TemplateNativePublicationFault(source) {
						t.Fatal("exact rollback fault was hidden or erased")
					}
					for _, cut := range []string{"storetest.PublicationAfterMaterialization", `"consumer"`, "fault.RestoreForTest(context.Background())", "fault.InstallForTest(ctx)"} {
						if family145TemplateNativePublicationFault(strings.Replace(source, cut, "unreviewedFaultCut", 1)) {
							t.Fatal("native rollback guard accepted a changed owner/cut/cleanup:", cut)
						}
					}
				}
			}
			if strings.Contains(source, "*sql.DB") || strings.Contains(source, "assertRuntimeDBCount(") || strings.Contains(source, "waitRuntimeDBCount(") || strings.Contains(source, "waitRuntimeEventID(") || family145PublicationRawEscape([]byte("package proof\n"+source)) != nil {
				t.Fatalf("template function retains raw authority: %s", fn.Name.Name)
			}
		}
	}
	if rootCount != 9 {
		t.Fatalf("native template openings=%d want9 (eight ordinary plus the native rollback fault)", rootCount)
	}
	data, err := os.ReadFile("../../../internal/runtime/artifact_action_result_delivery_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "waitRuntimeEventID(") || strings.Contains(string(data), "waitRuntimeNodeDeliveryOutcome(t, ctx, db,") {
		t.Fatal("indirect artifact callers retained shared raw helper ownership")
	}
}

func family145TemplateNativePublicationFault(source string) bool {
	create := "storetest.NewPublicationFailure(pg, storetest.PublicationAfterMaterialization, \"consumer\")"
	restore := "fault.RestoreForTest(context.Background())"
	install := "fault.InstallForTest(ctx)"
	cleanup := strings.Index(source, "t.Cleanup(func()")
	return strings.Count(source, create) == 1 && strings.Count(source, restore) == 1 &&
		strings.Count(source, install) == 1 && cleanup >= 0 &&
		strings.Index(source, create) < cleanup && cleanup < strings.Index(source, restore) &&
		strings.Index(source, restore) < strings.Index(source, install)
}
