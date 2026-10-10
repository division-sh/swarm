package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeConversationSessionRejectsSameDatabaseRegistryReconstruction(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal/runtime/conformance/persisted_surfaces_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := "lease, err := registry.Acquire(ctx, identity, \"test-owner\")"
	if strings.Count(string(source), before) != 1 {
		t.Fatal("original registry acquisition missing or ambiguous")
	}
	work := t.TempDir()
	mutantPath := filepath.Join(work, "persisted_surfaces_test.go")
	after := "lease, err := storetest.AdmitPostgresRuntimeStore(t, storetest.Database(registry)).Acquire(ctx, identity, \"test-owner\")"
	if err := os.WriteFile(mutantPath, []byte(strings.Replace(string(source), before, after, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{path: mutantPath}})
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(work, "overlay.json")
	if err := os.WriteFile(overlay, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-overlay", overlay, "-count=1", "-timeout=2m", "-json", "./internal/runtime/conformance", "-run", "^TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader$")
	command.Dir = root
	output, runErr := command.CombinedOutput()
	if runErr == nil || ctx.Err() != nil || !strings.Contains(string(output), "conversation acquisition escaped original selected coordinator") ||
		!strings.Contains(string(output), `"Action":"fail"`) || strings.Contains(string(output), "release conversation proof grant:") {
		t.Fatalf("same-database reconstruction did not fail at original ownership with successful deferred release: %v/%v\n%s", runErr, ctx.Err(), output)
	}
	t.Logf("same-database coordinator reconstruction refused at acquisition; deferred release did not report an error:\n%s", output)
}
