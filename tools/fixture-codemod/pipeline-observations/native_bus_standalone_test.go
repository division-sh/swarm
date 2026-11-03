package main

import (
	"strings"
)

func normalizedNativeStandaloneSetup(source string) string {
	for _, replacement := range [][2]string{
		{"_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\n\tctx := testAuthorActivityContext(context.Background())\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := testAuthorActivityContext(context.Background())"},
		{"executeStandaloneCompletionCandidate(t, ctx, db, pg,", "executeStandaloneCompletionCandidate(t, ctx, pg,"},
		{"loadRunStateForEvent(t, ctx, db,", "loadRunStateForEvent(t, ctx, pg,"},
		{"loadAgentDeliveryForEvent(t, ctx, db,", "loadAgentDeliveryForEvent(t, ctx, pg,"},
		{"runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{\n\t\t\t\tOrigin:     runlifecyclefixture.EventOrigin(t, tc.eventID, string(tc.eventType)),", "storetest.RequireRun(t, ctx, pg, storetest.RunFixture{\n\t\t\t\tOrigin:     storetest.EventOrigin(t, tc.eventID, string(tc.eventType)),"},
	} {
		source = strings.ReplaceAll(source, replacement[0], replacement[1])
	}
	return source
}
