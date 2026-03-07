package dashboard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDashboardArchitecture_NoOmnibusTestDump(t *testing.T) {
	path := filepath.Join("zzz_more_consolidated_test.go")
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("%s should not exist", path)
	}
}

func TestDashboardArchitecture_ServerLineCountCeilings(t *testing.T) {
	check := func(path string, maxLines int) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		lines := strings.Count(string(data), "\n") + 1
		if lines > maxLines {
			t.Fatalf("%s has %d lines, want <= %d", path, lines, maxLines)
		}
	}

	check("server.go", 800)
	check("server_control_runtime.go", 400)
	check("server_control_mailbox.go", 400)
	check("server_control_seed.go", 400)
	check("server_graph_handlers.go", 350)
	check("server_graph_builders.go", 700)
}
