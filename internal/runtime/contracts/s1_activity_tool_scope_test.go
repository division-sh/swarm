package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ActivityOutcomeSchemasUseDeclaringFlowTools(t *testing.T) {
	artifact, err := sourceartifact.AdmitMembers(packfixture.ScopedActivityMembers())
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "scoped-activity", artifact)
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleFromRootedSource(repo, rooted, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ambiguous := bundle.ToolEntries()["shared"]; ambiguous {
		t.Fatal("fixture did not establish an ambiguous global alias")
	}
	generated := bundle.GeneratedActivityEventEntries()
	for _, cell := range []struct{ flow, event, origin string }{
		{".", "send.succeeded", "root"},
		{"child", "child/send.succeeded", "child"},
		{"sibling", "sibling/send.succeeded", "sibling"},
		{"inherited", "inherited/send.succeeded", "root"},
	} {
		t.Run(cell.flow, func(t *testing.T) {
			entry, present := generated[cell.event]
			if !present {
				t.Fatalf("declaring-flow activity lost generated outcome %s", cell.event)
			}
			result := entry.Payload.Properties["result"].ExactSchema
			if result == nil {
				t.Fatal("generated outcome lost exact tool output schema")
			}
			origin, present := result.Property("origin")
			if !present || !origin.Equal(MustToolInputSchema(ToolSchemaString, ToolSchemaEnum(cell.origin))) {
				t.Fatalf("generated outcome borrowed another flow's tool: %+v", origin.Projection())
			}
		})
	}
}
