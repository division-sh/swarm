package main

import (
	"strings"
)

func nativeDiagnosisPaginationSource(source string) string {
	for _, cut := range [][2]string{
		{"(agentDiagnosePaginationStore, *sql.DB, bool)", "agentDiagnosePaginationStore"},
		{"\t\t\t\tselected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)\n\t\t\t\treturn selected, storetest.Database(selected), true", "\t\t\t\treturn storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
		{"\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected := storetest.AdmitPostgresRuntimeStore(t, db)\n\t\t\t\treturn selected, db, false", "\t\t\t\treturn storetest.StartPostgresRuntimeStore(t)"},
		{"selected, db, sqlite := backend.open(t, ctx)", "selected := backend.open(t, ctx)"},
		{"\t\t\tif sqlite {\n\t\t\t\trunlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),\n\t\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t\t})\n\t\t\t} else {\n\t\t\t\trunlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),\n\t\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t\t})\n\t\t\t}\n", "\t\t\tstoretest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),\n\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t})\n"},
	} {
		source = strings.ReplaceAll(source, cut[0], cut[1])
	}
	return source
}
