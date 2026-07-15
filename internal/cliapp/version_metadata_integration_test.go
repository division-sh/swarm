package cliapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/versionmetadata"
)

func currentTestVersionMetadata(t *testing.T) versionmetadata.Metadata {
	t.Helper()
	metadata, err := versionmetadata.Resolve(versionmetadata.Injected{Version: binaryVersion, Commit: binaryCommit, Date: binaryDate})
	if err != nil {
		t.Fatalf("resolve version metadata: %v", err)
	}
	return metadata
}
