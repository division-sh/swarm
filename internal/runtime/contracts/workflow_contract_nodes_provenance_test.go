package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeAdmissionProvenanceComposesWithEvents(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples", "routing", "template-create-minted-key")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var nodeFound, eventFound bool
	for _, entry := range bundle.EffectiveProvenance().Entries() {
		if strings.HasPrefix(entry.Path, `nodes["producer:producer-node"].event_handlers["validation.triggered"].emit.fields.candidate`) {
			nodeFound = entry.Provenance.Origin == EffectiveValueOriginAuthored &&
				strings.HasSuffix(entry.Provenance.SourceFile, "producer/nodes.yaml") && entry.Provenance.SourceLine == 10
		}
		if strings.HasPrefix(entry.Path, "events[") {
			eventFound = true
		}
	}
	if !nodeFound || !eventFound {
		t.Fatalf("composed provenance missing node or event entry: node=%t event=%t", nodeFound, eventFound)
	}
}
