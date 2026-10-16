package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

func family145ProspectivePreflightError(before, after string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "preflight.go", "package proof\n"+after, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]int{
		"runfork.SelectedPreparationSourceFingerprint(loaded.EffectiveSourceIdentity)": 1,
		"selectedPreparationFingerprint(cfg)":                                          1,
		"catalog.Fingerprint()":                                                        2,
		"plan.Fingerprint()":                                                           1,
		"storetest.ReadRunlessSelectedPreparationSurfaceStorage(ctx, selected, surfaceIDs[0], preparationID, \"selected-health-agent\")": 1,
		"witness.read(ctx, selected)": 1,
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if _, exact := want[family145NodeString(call)]; exact {
				want[family145NodeString(call)]--
			}
		}
		return true
	})
	for call, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("prospective preflight lost exact source/configuration/catalog/read owner: %s", call)
		}
	}
	for _, required := range []string{
		"prospectiveCatalogFingerprint := catalog.Fingerprint()",
		"BundleHash: loaded.SourceArtifactFact.BundleHash(), SourceFingerprint: sourceFingerprint",
		"AdmittedPlanFingerprint: prospectiveCatalogFingerprint, ConfigurationFingerprint: configurationFingerprint, CatalogFingerprint: catalog.Fingerprint()",
		"ActorPlanFingerprint: planFingerprint",
		"surface.Authority.Preparation == nil || *surface.Authority.Preparation != probe.Authority",
		"err != nil || count != 0",
		"{\"runs\", storetest.CountGlobalRunStorage}",
		"{\"agents\", storetest.CountGlobalAgentStorage}",
		"{\"run_fork_selected_contract_runtime_executions\", storetest.CountGlobalSelectedRuntimeExecutionStorage}",
		"{\"runtime_generation_grants\", storetest.CountGlobalGenerationGrantStorage}",
	} {
		if !strings.Contains(after, required) {
			return fmt.Errorf("prospective component lost exact evidence or global-zero witness: %s", required)
		}
	}
	for _, forbidden := range []string{"*sql.DB", "db.", "storetest.Database", "strings.Repeat", "MaterializeRunFork", "CommitSemanticEvent", "AcquireRuntimeGenerationGrant"} {
		if strings.Contains(after, forbidden) {
			return fmt.Errorf("prospective component retained raw/fabricated/executable setup: %s", forbidden)
		}
	}
	return family145ProspectivePreflightAssertions(before, after)
}

func family145ProspectivePreflightAssertions(before, after string) error {
	old, oldErr := runSetupAssertions(before)
	native, nativeErr := runSetupAssertions(after)
	if oldErr != nil || nativeErr != nil {
		return fmt.Errorf("parse prospective assertions: %v/%v", oldErr, nativeErr)
	}
	want := make(map[string]int)
	for _, assertion := range old {
		want[strings.Replace(assertion, "table, count, err)", "witness.table, count, err)", 1)]++
	}
	want[`t.Fatalf("load prospective fixture source: %v", err)`]++
	want[`t.Fatalf("persisted preparation coordinates differ from exact supplied component evidence: %+v", surface.Authority.Preparation)`]++
	want["t.Fatal(err)"] += 2
	for _, assertion := range native {
		want[assertion]--
	}
	for assertion, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("prospective component changed an original assertion or an unreviewed addition: %s", assertion)
		}
	}
	return nil
}
