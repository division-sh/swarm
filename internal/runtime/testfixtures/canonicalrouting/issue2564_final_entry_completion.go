package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyIssue2564FinalEntryCompletion is a dedicated temporal proof corpus, not
// an alteration of the frozen reconstructed H2 workloads.
func CopyIssue2564FinalEntryCompletion(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/final-entry-completion"), root)
	return root
}
