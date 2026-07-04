package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// Read-only conformance drift test binding the cobra command tree to
// platform-spec.yaml#cli_specification.command_catalog rows, mandated by
// cli_specification.topology_revision_v2_2.conformance_binding. Identifiers
// are compared directly — no translation table.

func driftTestRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
}

func loadCLISpecification(t *testing.T) *yaml.Node {
	t.Helper()
	raw, err := os.ReadFile(platform.DefaultPlatformSpecFile(driftTestRepoRoot(t)))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "cli_specification" {
			return root.Content[i+1]
		}
	}
	t.Fatal("cli_specification not found")
	return nil
}

func driftMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

// commandPathTokens derives the command path from a catalog `command:` string:
// tokens after "swarm" up to the first flag/placeholder/alternation token.
func commandPathTokens(command string) []string {
	fields := strings.Fields(command)
	if len(fields) == 0 || fields[0] != "swarm" {
		return nil
	}
	var path []string
	for _, tok := range fields[1:] {
		if strings.HasPrefix(tok, "-") || strings.HasPrefix(tok, "[") || strings.HasPrefix(tok, "<") || strings.Contains(tok, "|") {
			break
		}
		path = append(path, tok)
	}
	return path
}

func findCommandByPath(root *cobra.Command, path []string) *cobra.Command {
	cmd := root
	for _, name := range path {
		var next *cobra.Command
		for _, sub := range cmd.Commands() {
			if sub.Name() == name {
				next = sub
				break
			}
		}
		if next == nil {
			return nil
		}
		cmd = next
	}
	return cmd
}

func TestCobraTreeMatchesCommandCatalog(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	root.InitDefaultHelpCmd() // cobra adds the help command lazily at Execute time
	catalog := driftMappingValue(loadCLISpecification(t), "command_catalog")
	if catalog == nil {
		t.Fatal("command_catalog not found")
	}
	checked := 0
	for i := 0; i+1 < len(catalog.Content); i += 2 {
		rowName := catalog.Content[i].Value
		row := catalog.Content[i+1]
		if row.Kind != yaml.MappingNode {
			continue
		}
		command := driftMappingValue(row, "command")
		status := driftMappingValue(row, "implementation_status")
		group := driftMappingValue(row, "group")
		if command == nil || status == nil || !strings.HasPrefix(status.Value, "implemented") {
			continue // ledgers, policies, retired/backlog rows
		}
		if rowName == "root" {
			continue
		}
		path := commandPathTokens(command.Value)
		if len(path) == 0 {
			t.Errorf("row %s: cannot derive command path from %q", rowName, command.Value)
			continue
		}
		checked++
		cmd := findCommandByPath(root, path)
		if cmd == nil {
			t.Errorf("row %s: command path %v not found in cobra tree", rowName, path)
			continue
		}
		if cmd.Hidden {
			t.Errorf("row %s: implemented command %v is hidden", rowName, path)
		}
		if group != nil {
			top := findCommandByPath(root, path[:1])
			if top.GroupID != group.Value {
				t.Errorf("row %s: cobra GroupID %q != catalog group %q (direct comparison; no translation table)", rowName, top.GroupID, group.Value)
			}
		}
	}
	if checked < 40 {
		t.Fatalf("implemented rows checked = %d, want >= 40; row detection broken", checked)
	}

	// External-row exceptions, enumerated explicitly per the conformance_binding
	// rule rather than silently skipped: `describe` (semanticview authoring
	// section) and `context` (foundations.local_context_registry_authority).
	for _, exception := range []struct {
		name  string
		group string
	}{
		{"describe", commandGroupAuthor},
		{"context", commandGroupStart},
	} {
		cmd := findCommandByPath(root, []string{exception.name})
		if cmd == nil || cmd.Hidden {
			t.Errorf("%s: expected visible command with external spec row", exception.name)
		} else if cmd.GroupID != exception.group {
			t.Errorf("%s: GroupID %q, want %q", exception.name, cmd.GroupID, exception.group)
		}
	}

	// Reverse direction: every visible top-level command must be accounted for
	// by a catalog row path head or the enumerated external-row exception.
	rowHeads := map[string]bool{"describe": true, "context": true, "help": true}
	for i := 0; i+1 < len(catalog.Content); i += 2 {
		row := catalog.Content[i+1]
		command := driftMappingValue(row, "command")
		if command == nil {
			continue
		}
		if path := commandPathTokens(command.Value); len(path) > 0 {
			rowHeads[path[0]] = true
		}
	}
	for _, sub := range root.Commands() {
		if sub.Hidden || sub.Name() == "help" {
			continue
		}
		if !rowHeads[sub.Name()] {
			t.Errorf("visible command %q has no command_catalog row or enumerated exception", sub.Name())
		}
	}
}

func TestRetiredSpellingsFailClosedWithPromotedMessages(t *testing.T) {
	spec := loadCLISpecification(t)
	retired := driftMappingValue(driftMappingValue(spec, "retired_namespaces"), "topology_v2_2_retired_spellings")
	if retired == nil {
		t.Fatal("topology_v2_2_retired_spellings not found")
	}
	spellings := driftMappingValue(retired, "spellings")
	for i := 0; i+1 < len(spellings.Content); i += 2 {
		name := spellings.Content[i].Value
		message := spellings.Content[i+1].Value
		args := []string{name}
		if name == "run_bare_start" {
			args = []string{"run", "--event", "x"}
		}
		var stdout, stderr bytes.Buffer
		code := executeRootCommand(context.Background(), t.TempDir(), args, &stdout, &stderr)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
		if !strings.Contains(stderr.String(), message) {
			t.Errorf("%v: stderr %q missing promoted message %q", args, stderr.String(), message)
		}
	}
}

// The non-cobra command-path interpreter tables must not retain retired
// spellings (gate-required seams: doctor target classes, API-flag placement,
// log-level placement).
func TestCommandPathTablesCarryNoRetiredSpellings(t *testing.T) {
	retiredHeads := []string{"swarm runs", "swarm status", "swarm trace", "swarm fork ", "swarm agents", "swarm events", "swarm entities", "swarm conversations"}
	for _, class := range doctorTargetCommandClasses() {
		for _, command := range class.Commands {
			for _, retired := range retiredHeads {
				if strings.HasPrefix(command+" ", retired) || command == strings.TrimSpace(retired) {
					t.Errorf("doctorTargetCommandClasses %s: retired spelling %q", class.Name, command)
				}
			}
		}
	}
	// Behavioral checks: new paths eligible, retired paths not.
	for _, tc := range []struct {
		prefix []string
		want   bool
	}{
		{[]string{"run", "list"}, true},
		{[]string{"run", "trace"}, true},
		{[]string{"run", "fork"}, true},
		{[]string{"agent", "list"}, true},
		{[]string{"event", "list"}, true},
		{[]string{"runs"}, false},
		{[]string{"trace"}, false},
		{[]string{"fork"}, false},
		{[]string{"agents", "list"}, false},
	} {
		if got := cliAPIConnectionFlagAfterLeafCommand(tc.prefix); got != tc.want {
			t.Errorf("cliAPIConnectionFlagAfterLeafCommand(%v) = %v, want %v", tc.prefix, got, tc.want)
		}
	}
	for _, tc := range []struct {
		prefix []string
		want   bool
	}{
		{[]string{"run", "list"}, true},
		{[]string{"run", "status"}, true},
		{[]string{"conversation", "list"}, true},
		{[]string{"runs"}, false},
		{[]string{"status"}, false},
		{[]string{"conversations", "list"}, false},
	} {
		if got := cliLoggingFlagAfterSupportedLeafCommand(tc.prefix); got != tc.want {
			t.Errorf("cliLoggingFlagAfterSupportedLeafCommand(%v) = %v, want %v", tc.prefix, got, tc.want)
		}
	}
}
