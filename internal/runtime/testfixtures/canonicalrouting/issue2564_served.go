package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyIssue2564H3CollectionOperations copies the two-writer collection corpus.
func CopyIssue2564H3CollectionOperations(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h3"), root)
	return root
}

// CopyIssue2564M33NonterminalDeadline copies the single-writer late-result
// corpus with the authored five-second working -> review nonterminal deadline.
func CopyIssue2564M33NonterminalDeadline(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/m33"), root)
	return root
}
