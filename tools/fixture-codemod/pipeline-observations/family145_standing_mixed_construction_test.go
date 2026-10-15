package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func family145StandingMixedConstructionProofError(before, after string) error {
	for _, old := range []string{"var opened", "store.NewPostgresStore(", "BootstrapPostgresRuntimeStore(", "DatabaseForTest(", "openSelectedPostgresOwner("} {
		if strings.Contains(after, old) {
			return fmt.Errorf("mixed standing retained a detached constructor: %s", old)
		}
	}
	begin := strings.Index(before, "buildStoresForServe = func(")
	if begin < 0 {
		return fmt.Errorf("missing original mixed-standing factory")
	}
	indent := before[strings.LastIndex(before[:begin], "\n")+1 : begin]
	end := strings.Index(before[begin:], "\n"+indent+"cliapp.ConfiguredWorkspaceLifecycleForServe =")
	native := "buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {\n" + indent + "\treturn storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})\n" + indent + "}"
	if end < 0 || strings.Count(after, native) != 1 {
		return fmt.Errorf("mixed-standing factory changed native context/selection/DSN/TTL")
	}
	normalized := strings.Replace(after, native, before[begin:begin+end], 1)
	const marker = "\n\t\t\t\toldBuildStores := buildStoresForServe"
	if strings.Count(before, "var opened *store.PostgresStore") != 1 || strings.Count(normalized, marker) != 1 {
		return fmt.Errorf("changed original mixed-standing construction boundary")
	}
	normalized = strings.Replace(normalized, marker, "\n\t\t\t\tvar opened *store.PostgresStore"+marker, 1)
	if normalized != before {
		return fmt.Errorf("mixed-standing rewrite changed healthy/terminal identity, real terminalization, recovery, calls or cleanup")
	}
	return nil
}

func TestFamily145StandingMixedConstructorPreservesBothRealLifecyclesAndTerminalSibling(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	previous, err := selectedreadrole.EarlierStandingIngressHistory("../../..", family145TransitionBytes, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(previous, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestStandingRestartMixedHealthyAndTerminalProcessParity" {
			continue
		}
		matched++
		if err := family145StandingMixedConstructionProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"storeselected.OpenRuntime(ctx,", "Selection: selection", "PostgresDSN: dsn", "SessionLockTTL: runtimeSessionLockTTL(cfg)", "db, postgresStore, sqliteStore = selectedRuntimeStoreForTest", "terminalizeStandingRunFromRuntime(t, firstRuntime", "first.stop()", "second.stop()", "enableServeRuntimeRecovery", "healthyGeneration", "terminalGeneration", "assertNoStandingTelegramCall", "buildStoresForServe = oldBuildStores", "cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace", "t.Cleanup(cleanup)", "t.Fatalf"} {
			if !strings.Contains(row.After, cut) || family145StandingMixedConstructionProofError(row.Before, strings.Replace(row.After, cut, "foreignStandingScope()", 1)) == nil {
				t.Fatalf("lost mixed-standing owner/identity/terminal/recovery/teardown assertion: %s", cut)
			}
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/standing_ingress_supported_surface_test.go", row.Function)
		if actual != row.After {
			t.Fatal("mixed-standing construction escaped its finite source")
		}
	}
	if matched != 1 {
		t.Fatalf("mixed-standing constructor cohort=%d/1", matched)
	}
}
