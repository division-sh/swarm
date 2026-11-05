package cliapp

import "testing"

func currentTestVersionMetadata(t *testing.T) versionMetadata {
	t.Helper()
	metadata, err := resolveLocalVersionMetadata(BuildMetadata{Version: binaryVersion, Commit: binaryCommit, Date: binaryDate})
	if err != nil {
		t.Fatalf("resolve version metadata: %v", err)
	}
	return metadata
}
