package contracts

import (
	"path/filepath"
	"testing"
)

func handlerRuleIdentityGuardRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
