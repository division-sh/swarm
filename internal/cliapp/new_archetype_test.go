package cliapp

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestScaffoldAdmittedArchetypesAndTeachNextCommands(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.TelegramAgent,
		canonicalrouting.ArtifactID("internal/cliapp/archetypes/zero-agent-automation"),
	)
	for _, archetype := range []string{"zero-agent-automation", "webhook-responder"} {
		t.Run(archetype, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), archetype)
			var out bytes.Buffer
			if err := scaffoldArchetype(&out, archetype, destination); err != nil {
				t.Fatal(err)
			}
			requiredFiles := []string{"package.yaml"}
			if archetype == "webhook-responder" {
				requiredFiles = append(requiredFiles, "swarm.live.yaml", "bot/swarm.yaml", "bot/.swarm/swarm.yaml", "bot/tests/smoke.yaml")
			} else {
				requiredFiles = append(requiredFiles, "swarm.yaml", ".swarm/swarm.yaml", "tests/smoke.yaml")
			}
			for _, required := range requiredFiles {
				if _, err := os.Stat(filepath.Join(destination, required)); err != nil {
					t.Fatalf("missing %s: %v", required, err)
				}
			}
			if archetype == "webhook-responder" {
				assertArchetypeTreeEqual(t, canonicalrouting.ExampleRoot(t, canonicalrouting.TelegramAgent), destination)
				if !strings.Contains(out.String(), "cd ./bot") {
					t.Fatalf("output %q does not enter the runnable bot package", out.String())
				}
			}
			for _, command := range []string{"swarm verify", "swarm serve", "swarm test"} {
				if !strings.Contains(out.String(), command) {
					t.Fatalf("output %q does not teach %s", out.String(), command)
				}
			}
			if err := scaffoldArchetype(&out, archetype, destination); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("existing destination error = %v", err)
			}
		})
	}
}

func assertArchetypeTreeEqual(t *testing.T, wantRoot, gotRoot string) {
	t.Helper()
	want := map[string][]byte{}
	err := filepath.WalkDir(wantRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(wantRoot, path)
		if err != nil {
			return err
		}
		want[filepath.ToSlash(rel)], err = os.ReadFile(path)
		return err
	})
	if err != nil {
		t.Fatalf("read canonical archetype: %v", err)
	}
	got := map[string][]byte{}
	err = filepath.WalkDir(gotRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(gotRoot, path)
		if err != nil {
			return err
		}
		got[filepath.ToSlash(rel)], err = os.ReadFile(path)
		return err
	})
	if err != nil {
		t.Fatalf("read scaffolded archetype: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("scaffold file count = %d, want %d", len(got), len(want))
	}
	for name, wantBody := range want {
		gotBody, ok := got[name]
		if !ok || !bytes.Equal(gotBody, wantBody) {
			t.Fatalf("scaffolded %s does not equal canonical bytes", name)
		}
	}
}

func TestScaffoldRejectsUnadmittedArchetype(t *testing.T) {
	if err := scaffoldArchetype(&bytes.Buffer{}, "approval-gate", filepath.Join(t.TempDir(), "approval")); err == nil || !strings.Contains(err.Error(), "admitted archetypes") {
		t.Fatalf("unadmitted archetype error = %v", err)
	}
}
