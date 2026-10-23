package main

import (
	"strings"
)

func nativeExecutionConstructionSource(source string) string {
	selected := "pg"
	if strings.Contains(source, "func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(") {
		selected = "selected"
	}
	source = strings.Replace(source, "\t_, db, cleanup := testutil.StartPostgres(t)", "\t"+selected+" := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\t_, db, _ := testutil.StartPostgres(t)", "\t"+selected+" := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\tt.Cleanup(cleanup)\n", "", 1)
	return strings.Replace(source, "\t"+selected+" := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
}
