package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTelegramAgentConsumesEmbeddedPackInventory(t *testing.T) {
	exampleRoot := ExampleRoot(t, TelegramAgent)
	if _, err := os.Stat(filepath.Join(exampleRoot, "provider-triggers")); !os.IsNotExist(err) {
		t.Fatalf("Telegram example must not own a provider-trigger snapshot: %v", err)
	}
	for _, relative := range []string{"swarm.yaml", "swarm.live.yaml", ".swarm"} {
		if _, err := os.Stat(filepath.Join(exampleRoot, relative)); !os.IsNotExist(err) {
			t.Fatalf("Telegram example must not ship deployment artifact %s: %v", relative, err)
		}
	}
}
