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
		{"invalid CEL integer", "7 + 0", false},
		{"valid quoted text", `"ready"`, true},
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
			updated := strings.Replace(string(raw), "item_id: payload.item_id", "item_id: "+tc.value, 1)
			if updated == string(raw) {
				t.Fatal("R2 fixture mutation did not match")
			}
			if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
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
		{"valid", `{name: "Ada", count: 1 + 0}`, true},
		{"missing", `{name: "Ada"}`, false},
		{"extra", `{name: "Ada", count: 1 + 0, extra: true}`, false},
		{"wrong type", `{name: "Ada", count: "one"}`, false},
		{"null sibling of dynamic field", `{name: string("Ada"), count: null}`, false},
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
			updatedNodes := strings.Replace(string(nodes), "item_id: payload.item_id", "item_id: "+tc.value, 1)
			if updatedNodes == string(nodes) {
				t.Fatal("node fixture mutation did not match")
			}
			if err := os.WriteFile(nodesPath, []byte(updatedNodes), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if (code == 0) != tc.wantOK {
				t.Fatalf("verify code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !tc.wantOK && !strings.Contains(stdout.String(), "item_id") && !strings.Contains(stdout.String(), "result") {
				t.Fatalf("verify failure did not identify the record: %s", stdout.String())
			}
		})
	}
}

func TestVerifyCommandR2ConstructorAndCommentCrossProduct(t *testing.T) {
	for _, tc := range []struct {
		name, destination, value string
		valid                    bool
	}{
		{"one-field record", "Child", ` {n: 1 + 0}`, true},
		{"same-typed record", "Pair", ` {left: 1 + 0, right: 2 + 0}`, true},
		{"nested literal record", "Envelope", ` {child: {n: 1 + 0}, label: "ok"}`, true},
		{"nested typed record", "Envelope", ` {child: payload.item_id, label: "ok"}`, true},
		{"optional present", "OptionalReport", " |-\n            {\"label\":\"ok\", ?\"n\": optional.of(1)}", true},
		{"optional absent", "OptionalReport", " |-\n            {\"label\":\"ok\", ?\"n\": optional.none()}", true},
		{"optional wrong type", "OptionalReport", " |-\n            {\"label\":\"ok\", ?\"n\": optional.of(\"one\")}", false},
		{"mixed trailing comment", "text", ` "v=${1 // comment\n}"`, true},
		{"nested trailing comment", "Child", "\n            n: |-\n              1 // comment", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), "examples", "routing", "root-ingress"))); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "types.yaml"), []byte("types:\n  Child:\n    n: integer\n  Pair:\n    left: integer\n    right: integer\n  Envelope:\n    child: Child\n    label: text\n  OptionalReport:\n    label: text\n    n: integer?\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			eventsPath := filepath.Join(root, "events.yaml")
			events, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			updatedEvents := strings.Replace(string(events), "item.received:\n  item_id: text", "item.received:\n  item_id: Child", 1)
			updatedEvents = strings.Replace(updatedEvents, "item.processed:\n  item_id: text?", "item.processed:\n  item_id: "+tc.destination, 1)
			if err := os.WriteFile(eventsPath, []byte(updatedEvents), 0o600); err != nil {
				t.Fatal(err)
			}
			nodesPath := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodesPath)
			if err != nil {
				t.Fatal(err)
			}
			updatedNodes := strings.Replace(string(nodes), "item_id: payload.item_id", "item_id:"+tc.value, 1)
			if updatedNodes == string(nodes) {
				t.Fatal("R2 fixture mutation did not match")
			}
			if err := os.WriteFile(nodesPath, []byte(updatedNodes), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if (code == 0) != tc.valid {
				t.Fatalf("verify code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestVerifyScalar2556SourceSlotDiagnosticsBeforeEffects(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        []string
		wantOK      bool
	}{
		{"undeclared plain text", "ready", []string{"nodes.yaml", "expression slot", "item_id", "undeclared reference to", "quote text or use a declared reference"}, false},
		{"unsupported wrapper", "${payload.item_id}", []string{"nodes.yaml", "expression slot", "item_id"}, false},
		{"single quoted reference is text", "'payload.item_id'", nil, true},
		{"double quoted reference is text", `"payload.item_id"`, nil, true},
		{"sole interpolation is text", `'${7}'`, nil, true},
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
			updated := strings.Replace(string(raw), "item_id: payload.item_id", "item_id: "+tc.value, 1)
			if updated == string(raw) {
				t.Fatal("source mutation did not match")
			}
			if err := os.WriteFile(path, []byte(updated), 0600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if (code == 0) != tc.wantOK {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
			for _, want := range tc.want {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("missing %q: %s", want, &stdout)
				}
			}
			if tc.name == "unsupported wrapper" && strings.Contains(stdout.String(), "quote text or use a declared reference") {
				t.Fatal("parse error incorrectly taught undeclared-root fix")
			}
			if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
				t.Fatalf("portable verification created runtime state: %v", err)
			}
		})
	}
}
