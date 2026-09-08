package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestNodeIdentityMissingMapKeyDoesNotTeachRetiredField(t *testing.T) {
	for _, key := range []string{"", " "} {
		checker := &checkerContext{}
		checker.appendInvalidExecutableNodeFindings(contracts.ScopedNodeRecord{
			LogicalID: key,
			Source:    contracts.ContractItemSource{FlowPath: "left"},
		})
		if len(checker.invalidFindings) != 1 {
			t.Fatalf("invalid key %q: %#v", key, checker.invalidFindings)
		}
		finding := checker.invalidFindings[0]
		if finding.CheckID != "invalid_field_detection" || finding.Severity != "error" || !strings.HasSuffix(finding.Message, "requires a nonempty map key") {
			t.Fatalf("invalid key %q teaches wrong identity: %#v", key, finding)
		}
	}
}

func TestNodeIdentityActivityControlRemainsValid(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	root := filepath.Join(repo, "internal/releasee2e/testdata/node_identity_activity")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if invalid := report.HardInvalidities(); len(invalid) != 0 {
		t.Fatalf("activity identity control is not valid: %#v", invalid)
	}
}

func TestNodeIdentityMetadataProjectionAcrossScopes(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	root := filepath.Join(repo, "internal/releasee2e/testdata/node_identity")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	report := Run(context.Background(), source, Options{})
	if invalid := report.HardInvalidities(); len(invalid) != 0 {
		t.Fatalf("identity fixture is not executable: %#v", invalid)
	}
	names := eventMetadataInternalActorNames(source)
	for _, record := range bundle.ScopedNodeRecords() {
		ref, err := record.Identity()
		if err != nil {
			t.Fatal(err)
		}
		if ref.NodeID() != "worker" || bundle.Semantics.EffectiveNodes[ref.Key()].ID != "worker" {
			t.Fatalf("effective identity disagrees: %v", ref)
		}
		if label, ok := names.match(ref.Key()); !ok || label != "system node "+ref.Key() {
			t.Fatalf("metadata lost scoped identity %s: %#v", ref.Key(), names)
		}
	}
	if _, ok := names["worker"]; !ok {
		t.Fatal("metadata lost local rejection vocabulary")
	}
}
