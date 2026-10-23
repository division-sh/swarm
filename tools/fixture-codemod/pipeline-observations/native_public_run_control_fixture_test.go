package main

import (
	"strings"
)

func nativePublicRunControlSource(source string) string {
	source = strings.Replace(source, "\t_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin()", "storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin()", 1)
	return strings.ReplaceAll(source, "assertRunControlState(t, db,", "assertRunControlState(t, pg,")
}
