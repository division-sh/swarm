package schemastore

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPlatformFanOutOutcomeSchemaRetainsCanceledRecipientEvidence(t *testing.T) {
	repo := repoRootForRuntimeWriterGuard(t)
	spec := loadPlatformSpecDocumentForStoreTest(t, runtimecontracts.DefaultPlatformSpecFile(repo))
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.TableName != "fan_out_outcomes" {
			continue
		}
		ddl := strings.Join(plan.Statements, "\n")
		for _, contract := range []string{
			"inherited_disposition IN ('succeeded', 'dead_lettered', 'canceled', 'no_route')",
			"((event_id IS NOT NULL) <> (source_event_id IS NOT NULL))",
			"source_event_id IS NOT NULL AND inherited_disposition IS NOT NULL",
		} {
			if !strings.Contains(ddl, contract) {
				t.Fatalf("canonical outcome schema omits %q", contract)
			}
		}
		return
	}
	t.Fatal("canonical schema omits fan_out_outcomes")
}
