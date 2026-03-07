package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNoShadowInternalToolsPackage(t *testing.T) {
	path := filepath.Join("..", "tools")
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("%s should not exist; use internal/runtime/tools", path)
	}
}
