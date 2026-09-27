package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyCommandR2LiteralEmitUsesDestinationSchema(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		wantOK      bool
	}{
		{"invalid bare integer", "7", false},
		{"invalid CEL integer", "${7}", false},
		{"valid bare text", "ready", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), "examples", "routing", "root-ingress"))); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "nodes.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			updated := strings.Replace(string(raw), "item_id: ${payload.item_id}", "item_id: "+tc.value, 1)
			if updated == string(raw) {
				t.Fatal("R2 fixture mutation did not match")
			}
			if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if (code == 0) != tc.wantOK {
				t.Fatalf("verify code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !tc.wantOK && !strings.Contains(stdout.String(), "item_id") {
				t.Fatalf("verify failure did not identify the typed field: %s", stdout.String())
			}
		})
	}
}

func TestVerifyCommandR2RecursiveRecordUsesNamedDestination(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		wantOK      bool
	}{
		{"valid", `{name: Ada, count: "${1}"}`, true},
		{"missing", `{name: Ada}`, false},
		{"extra", `{name: Ada, count: "${1}", extra: true}`, false},
		{"wrong type", `{name: Ada, count: "${'one'}"}`, false},
		{"null sibling of dynamic field", `{name: "${'Ada'}", count: null}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), "examples", "routing", "root-ingress"))); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "types.yaml"), []byte("types:\n  Report:\n    name: text\n    count: integer\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			eventsPath := filepath.Join(root, "events.yaml")
			events, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			updatedEvents := strings.Replace(string(events), "item_id: text?", "item_id: Report", 1)
			if updatedEvents == string(events) {
				t.Fatal("event fixture mutation did not match")
			}
			if err := os.WriteFile(eventsPath, []byte(updatedEvents), 0o600); err != nil {
				t.Fatal(err)
			}
			nodesPath := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodesPath)
			if err != nil {
				t.Fatal(err)
			}
			updatedNodes := strings.Replace(string(nodes), "item_id: ${payload.item_id}", "item_id: "+tc.value, 1)
			if updatedNodes == string(nodes) {
				t.Fatal("node fixture mutation did not match")
			}
			if err := os.WriteFile(nodesPath, []byte(updatedNodes), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if (code == 0) != tc.wantOK {
				t.Fatalf("verify code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !tc.wantOK && !strings.Contains(stdout.String(), "item_id") && !strings.Contains(stdout.String(), "result") {
				t.Fatalf("verify failure did not identify the record: %s", stdout.String())
			}
		})
	}
}
