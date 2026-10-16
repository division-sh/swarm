package main

import (
	"fmt"
	"strings"
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
