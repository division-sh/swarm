package serveapp

import (
	"encoding/json"
	"os"
	"testing"
)

func writeRunCommandPayloadFile(t *testing.T, payload map[string]any) string {
	t.Helper()
	path := t.TempDir() + "/payload.json"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	return path
}
