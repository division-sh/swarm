package cliapp

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	swarmassets "github.com/division-sh/swarm"
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
			root := mustInvocationRootForTest(filepath.Dir(destination))
			if err := scaffoldArchetype(root, &out, archetype, destination); err != nil {
				t.Fatal(err)
			}
			source := admittedArchetypes[archetype]
			assertArchetypeTreeEqual(t, source.Files, source.SourceRoot, destination)
			requiredFiles := []string{"manifest.yaml"}
			if archetype == "webhook-responder" {
				requiredFiles = append(requiredFiles, "tests/smoke.yaml", "telegram-chat/schema.yaml", "telegram-ingress/schema.yaml")
			} else {
				requiredFiles = append(requiredFiles, "schema.yaml", "nodes.yaml", "events.yaml", "tests/smoke.yaml")
			}
			for _, required := range requiredFiles {
				if _, err := os.Stat(filepath.Join(destination, required)); err != nil {
					t.Fatalf("missing %s: %v", required, err)
				}
			}
			if strings.Contains(out.String(), "cd ./bot") {
				t.Fatalf("output %q retains the retired nested source-root handoff", out.String())
			}
			for _, wrapper := range []string{"bot", "automation", "swarm.yaml", "swarm.live.yaml", ".swarm"} {
				if _, err := os.Stat(filepath.Join(destination, wrapper)); !os.IsNotExist(err) {
					t.Fatalf("scaffold retains redundant %s wrapper: %v", wrapper, err)
				}
			}
			for _, command := range []string{"swarm verify", "swarm serve", "swarm test"} {
				if !strings.Contains(out.String(), command) {
					t.Fatalf("output %q does not teach %s", out.String(), command)
				}
			}
			if err := scaffoldArchetype(root, &out, archetype, destination); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("existing destination error = %v", err)
			}
		})
	}
}

func TestScaffoldEmbedsNoHiddenDeploymentConfig(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve scaffold test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	for path, forbidden := range map[string]string{
		"platform_artifacts.go":            "all:examples/integrations/telegram-agent",
		"internal/cliapp/new_archetype.go": "all:archetypes",
	} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("%s restores ignored-file embedding through %q", path, forbidden)
		}
	}

	for _, test := range []struct {
		name string
		fs   fs.FS
		root string
	}{
		{
			name: "telegram-agent",
			fs:   swarmassets.EmbeddedTelegramAgentExample(),
			root: ".",
		},
		{
			name: "zero-agent-automation",
			fs:   archetypeFiles,
			root: "archetypes/zero-agent-automation",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var hidden []string
			err := fs.WalkDir(test.fs, test.root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				if strings.Contains("/"+path+"/", "/.swarm/") {
					hidden = append(hidden, path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(hidden) != 0 {
				t.Fatalf("embedded .swarm files = %v, want none", hidden)
			}
		})
	}
}

func assertArchetypeTreeEqual(t *testing.T, wantFS fs.FS, wantRoot, gotRoot string) {
	t.Helper()
	want := map[string][]byte{}
	err := fs.WalkDir(wantFS, wantRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(filepath.FromSlash(wantRoot), filepath.FromSlash(path))
		if err != nil {
			return err
		}
		want[filepath.ToSlash(rel)], err = fs.ReadFile(wantFS, path)
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
	root := mustInvocationRootForTest(t.TempDir())
	if err := scaffoldArchetype(root, &bytes.Buffer{}, "approval-gate", root.Resolve("approval")); err == nil || !strings.Contains(err.Error(), "admitted archetypes") {
		t.Fatalf("unadmitted archetype error = %v", err)
	}
}
