package pipeline

import (
	"path/filepath"
	"runtime"
	"testing"
)

func contractComplianceRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func remainingCampaignModes(initialMode string) []string {
	return RemainingCampaignModes(initialMode)
}
