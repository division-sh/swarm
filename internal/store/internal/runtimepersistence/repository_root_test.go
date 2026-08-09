package runtimepersistence

import (
	"path/filepath"
	"runtime"
	"testing"
)

func repoRootForRuntimeWriterGuard(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime persistence test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
