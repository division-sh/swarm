package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
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

// Every retirement/pointer message must reference commands that are alive and
// visible in the current tree — a retirement pointing at another retirement
// (the #1686 review finding on investigate) is a topology drift class of its
// own.
func TestRetirementPointerMessagesReferenceLiveCommands(t *testing.T) {
	commandRef := regexp.MustCompile("`swarm ([^`]+)`")
	nameToken := regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	retiredInvocations := [][]string{
		{"runs"}, {"status"}, {"trace"}, {"fork"}, {"agents"}, {"events"}, {"entities"}, {"conversations"},
		{"run", "--event", "x"},
		{"fork", "--dry-run"},
		{"investigate"}, {"investigate", "runs"}, {"investigate", "run"}, {"investigate", "trace"}, {"investigate", "health"},
	}
	for _, args := range retiredInvocations {
		var stdout, stderr bytes.Buffer
		code := executeRootCommand(context.Background(), t.TempDir(), args, &stdout, &stderr)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
			continue
		}
		refs := commandRef.FindAllStringSubmatch(stderr.String(), -1)
		if len(refs) == 0 {
			t.Errorf("%v: retirement message has no `swarm ...` pointer: %q", args, stderr.String())
			continue
		}
		for _, ref := range refs {
			var path []string
			for _, tok := range strings.Fields(ref[1]) {
				if !nameToken.MatchString(tok) {
					break
				}
				path = append(path, tok)
			}
			if len(path) == 0 {
				continue // reference like `swarm run` handled above; bare flags skipped
			}
			if path[0] == args[0] {
				continue // messages name the retired spelling itself before the pointer
			}
			target := findCommandByPath(root, path)
			if target == nil {
				t.Errorf("%v: pointer references `swarm %s` which does not resolve to a command", args, strings.Join(path, " "))
			} else if target.Hidden {
				t.Errorf("%v: pointer references `swarm %s` which is hidden/retired", args, strings.Join(path, " "))
			}
		}
	}
}

// Retired spellings invoked with pre-dispatch-validated flags (API connection,
// log level) must still reach their pointer stubs instead of dying on a
// generic flag-placement error (#1686 review finding).
func TestRetiredSpellingsWithConnectionFlagsStillPointToReplacement(t *testing.T) {
	for _, tc := range []struct {
		args        []string
		wantPointer string
	}{
		{[]string{"runs", "--api-server", "http://127.0.0.1:9"}, "swarm run list"},
		{[]string{"fork", "--context", "c"}, "swarm run fork"},
		{[]string{"trace", "--log-level", "debug"}, "swarm run trace"},
		{[]string{"status", "--api-token-file", "/dev/null"}, "swarm run status"},
		{[]string{"run", "--api-server", "http://127.0.0.1:9"}, "swarm run start"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommand(context.Background(), t.TempDir(), tc.args, &stdout, &stderr)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2 (stderr=%q)", tc.args, code, stderr.String())
			continue
		}
		if !strings.Contains(stderr.String(), tc.wantPointer) {
			t.Errorf("%v: stderr %q missing pointer %q (generic flag error instead of promoted message?)", tc.args, stderr.String(), tc.wantPointer)
		}
	}
}

// Retired topology spellings must not survive anywhere topology is expressed
// as unstructured text: Go sources (guidance strings, bind metadata,
// comments), platform-spec present-truth prose, README, and CI workflows.
// Structured surfaces are covered by the tests above; this scan closes the
// unstructured-string class that three review cycles on #1686 kept finding.
// Lines carrying an explicit retirement/historical marker are exempt — a
// retirement message may name the spelling it retires, and historical
// ledgers may record superseded decisions.
func TestNoRetiredSpellingsInUnstructuredSources(t *testing.T) {
	retiredWords := regexp.MustCompile("swarm (runs|agents|events|entities|conversations)([^a-z]|$)" +
		"|swarm (status|trace)([^a-z]|$)" +
		"|swarm fork([^a-z]|$)") // forkchat excluded by the non-letter guard
	// The bare-run start form is scanned only in user-visible surfaces; spec
	// prose references run-start flags in contexts where the flag, not the
	// spelling, is the subject.
	bareRunStart := regexp.MustCompile("swarm run --")
	historicalMarker := regexp.MustCompile("(?i)renamed|retired|no longer|historical|superseded|restore|previous tracked prose|unpromoted|candidate backlog|v1 retirement|v2\\.2|legacy|remain split|#[0-9]{3}|--dry-run\\|" +
		"|^\\s*action: '|^\\s*current: swarm ")

	root := driftTestRepoRoot(t)
	var targets []string
	goFiles, err := filepath.Glob(filepath.Join(root, "cmd", "swarm", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range goFiles {
		if !strings.HasSuffix(f, "_test.go") { // tests exercise retired spellings on purpose
			targets = append(targets, f)
		}
	}
	targets = append(targets, filepath.Join(root, "platform-spec.yaml"), filepath.Join(root, "README.md"))
	workflows, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	targets = append(targets, workflows...)

	for _, path := range targets {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		isSpec := strings.HasSuffix(path, "platform-spec.yaml")
		for i, line := range strings.Split(string(raw), "\n") {
			if !retiredWords.MatchString(line) && (isSpec || !bareRunStart.MatchString(line)) {
				continue
			}
			if historicalMarker.MatchString(line) {
				continue
			}
			rel, _ := filepath.Rel(root, path)
			t.Errorf("%s:%d: retired topology spelling in unstructured text (update to the v2.2 spelling, or mark the line historical): %s", rel, i+1, strings.TrimSpace(line))
		}
	}
}
