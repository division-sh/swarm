package cliapp

import "testing"

func TestCLIReadWindowRegistryCoversEveryPublicSinceUntilFlag(t *testing.T) {
	want := map[string]string{
		"swarm run list":      "read_window",
		"swarm run trace":     "read_window",
		"swarm event list":    "read_window",
		"swarm logs":          "read_window",
		"swarm mailbox defer": "future_deadline",
	}
	got := map[string]string{}
	for path, cmd := range visibleCLICommandPaths(t) {
		if cmd.Flags().Lookup("since") == nil && cmd.Flags().Lookup("until") == nil {
			continue
		}
		classification, ok := want[path]
		if !ok {
			t.Errorf("public command %q exposes --since/--until without a read-window classification", path)
			continue
		}
		got[path] = classification
	}
	for path, classification := range want {
		if got[path] != classification {
			t.Errorf("classified command %q = %q, want %q", path, got[path], classification)
		}
	}
}
