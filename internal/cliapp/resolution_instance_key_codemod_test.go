package cliapp

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestMigrateResolutionInstanceKeyCommandMigratesCanonicalCensusAndVerifies(t *testing.T) {
	tests := []struct {
		name         string
		artifact     canonicalrouting.ArtifactID
		declarations int
	}{
		{name: "select existing", artifact: canonicalrouting.TemplateSelectExisting, declarations: 2},
		{name: "select or create", artifact: canonicalrouting.TemplateSelectOrCreate, declarations: 1},
		{name: "reply requester", artifact: canonicalrouting.TemplateReply, declarations: 2},
		{name: "generated uuid create", artifact: canonicalrouting.TemplateCreateMintedKey, declarations: 1},
		{name: "notify all children", artifact: canonicalrouting.ArtifactID("examples/routing/notify-all-children"), declarations: 2},
		{name: "fan in barrier create", artifact: canonicalrouting.FanInBarrier, declarations: 1},
		{name: "fan in stream create", artifact: canonicalrouting.FanInStream, declarations: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := copyResolutionMigrationArtifact(t, repo, tc.artifact)
			canonicalrouting.ApplyRetiredResolutionInstanceKeyMutation(t, root, tc.artifact)

			var out bytes.Buffer
			var errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repo, []string{
				"migrate-resolution-instance-key", "--contracts", root,
			}, &out, &errOut)
			if code != 0 {
				t.Fatalf("migrate code = %d, stderr = %q", code, errOut.String())
			}
			if !strings.Contains(out.String(), "migrated "+strconv.Itoa(tc.declarations)+" resolution.instance_key declaration") {
				t.Fatalf("stdout = %q, want %d migrated declarations", out.String(), tc.declarations)
			}
			assertNoRetiredResolutionInstanceKey(t, root)

			out.Reset()
			errOut.Reset()
			code = executeRootCommand(context.Background(), repo, []string{"verify", "--contracts", root}, &out, &errOut)
			if code != 0 {
				t.Fatalf("verify migrated artifact code = %d, stdout/stderr = %q/%q", code, out.String(), errOut.String())
			}

			out.Reset()
			errOut.Reset()
			code = executeRootCommand(context.Background(), repo, []string{
				"migrate-resolution-instance-key", "--contracts", root,
			}, &out, &errOut)
			if code != 0 || !strings.Contains(out.String(), "no resolution.instance_key declarations found") {
				t.Fatalf("idempotent migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
			}
		})
	}
}

func TestMigrateResolutionInstanceKeyCommandLeavesWholeTreeByteExactOnBlocker(t *testing.T) {
	tests := []struct {
		name     string
		artifact canonicalrouting.ArtifactID
		blocker  canonicalrouting.RetiredResolutionInstanceKeyBlocker
		want     string
	}{
		{name: "identity disagreement", artifact: canonicalrouting.TemplateSelectExisting, blocker: canonicalrouting.RetiredResolutionInstanceKeyMismatch, want: "does not match instance.by"},
		{name: "unknown mint", artifact: canonicalrouting.TemplateCreateMintedKey, blocker: canonicalrouting.RetiredResolutionInstanceKeyUnknownMint, want: "requires manual migration"},
		{name: "selecting synthetic source", artifact: canonicalrouting.TemplateSelectOrCreate, blocker: canonicalrouting.RetiredResolutionInstanceKeySelectingSyntheticSource, want: "selecting pins require one top-level payload source"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := copyResolutionMigrationArtifact(t, repo, tc.artifact)
			canonicalrouting.ApplyRetiredResolutionInstanceKeyMutation(t, root, tc.artifact)
			canonicalrouting.ApplyRetiredResolutionInstanceKeyBlocker(t, root, tc.blocker)
			before := snapshotResolutionMigrationTree(t, root)

			var out bytes.Buffer
			var errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repo, []string{
				"migrate-resolution-instance-key", "--contracts", root,
			}, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), tc.want) {
				t.Fatalf("migrate code/stderr = %d/%q, want failure containing %q", code, errOut.String(), tc.want)
			}
			assertResolutionMigrationTreeUnchanged(t, root, before)
		})
	}
}

func TestMigrateResolutionInstanceKeyCommandAllowsScalarInputEventRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		migrate bool
	}{
		{name: "mixed tree", migrate: true},
		{name: "no-op tree", migrate: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := copyResolutionMigrationArtifact(t, repo, canonicalrouting.TemplateSelectExisting)
			if tc.migrate {
				canonicalrouting.ApplyRetiredResolutionInstanceKeyMutation(t, root, canonicalrouting.TemplateSelectExisting)
			}
			writeScalarInputEventSchema(t, root)

			var out bytes.Buffer
			var errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repo, []string{
				"migrate-resolution-instance-key", "--contracts", root,
			}, &out, &errOut)
			if code != 0 {
				t.Fatalf("migrate code = %d, stdout/stderr = %q/%q", code, out.String(), errOut.String())
			}
			if tc.migrate {
				if !strings.Contains(out.String(), "migrated 2 resolution.instance_key declarations") {
					t.Fatalf("stdout = %q, want mixed-tree migration", out.String())
				}
				assertNoRetiredResolutionInstanceKey(t, root)
				return
			}
			if !strings.Contains(out.String(), "no resolution.instance_key declarations found") {
				t.Fatalf("stdout = %q, want no-op result", out.String())
			}
		})
	}
}

func TestMigrateResolutionInstanceKeyCommandRejectsInvalidCandidateAndPreservesOriginalTree(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(testing.TB, string)
	}{
		{
			name: "unrelated package blocker",
			corrupt: func(t testing.TB, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte("name: [\n"), 0o644); err != nil {
					t.Fatalf("write invalid package: %v", err)
				}
			},
		},
		{
			name: "unrelated flow blocker",
			corrupt: func(t testing.TB, root string) {
				t.Helper()
				path := filepath.Join(root, "flows", "producer", "nodes.yaml")
				if err := os.WriteFile(path, []byte("producer-node: [\n"), 0o644); err != nil {
					t.Fatalf("write invalid unrelated flow: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := copyResolutionMigrationArtifact(t, repo, canonicalrouting.TemplateSelectExisting)
			canonicalrouting.ApplyRetiredResolutionInstanceKeyMutation(t, root, canonicalrouting.TemplateSelectExisting)
			tc.corrupt(t, root)
			before := snapshotResolutionMigrationTree(t, root)

			var out bytes.Buffer
			var errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repo, []string{
				"migrate-resolution-instance-key", "--contracts", root,
			}, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), "rewritten candidate failed production validation") {
				t.Fatalf("migrate code/stderr = %d/%q, want production candidate rejection", code, errOut.String())
			}
			assertResolutionMigrationTreeUnchanged(t, root, before)
		})
	}
}

func copyResolutionMigrationArtifact(t testing.TB, repo string, id canonicalrouting.ArtifactID) string {
	t.Helper()
	if id != canonicalrouting.ArtifactID("examples/routing/notify-all-children") {
		return canonicalrouting.CopyExample(t, id)
	}
	source := filepath.Join(repo, filepath.FromSlash(string(id)))
	target := filepath.Join(t.TempDir(), "notify-all-children")
	if err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, raw, 0o644)
	}); err != nil {
		t.Fatalf("copy notify-all-children artifact: %v", err)
	}
	return target
}

func snapshotResolutionMigrationTree(t testing.TB, root string) map[string][]byte {
	t.Helper()
	snapshot := map[string][]byte{}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot[relative] = raw
		return nil
	}); err != nil {
		t.Fatalf("snapshot migration tree: %v", err)
	}
	return snapshot
}

func assertResolutionMigrationTreeUnchanged(t testing.TB, root string, before map[string][]byte) {
	t.Helper()
	after := snapshotResolutionMigrationTree(t, root)
	if len(after) != len(before) {
		t.Fatalf("blocked migration changed file count: before=%d after=%d", len(before), len(after))
	}
	for path, raw := range before {
		if !bytes.Equal(after[path], raw) {
			t.Fatalf("blocked migration changed %s\nbefore:\n%s\nafter:\n%s", path, raw, after[path])
		}
	}
}

func writeScalarInputEventSchema(t testing.TB, root string) {
	t.Helper()
	path := filepath.Join(root, "parser-only", "schema.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parser-only schema directory: %v", err)
	}
	const raw = `name: parser-only-scalar-input
mode: static
pins:
  inputs:
    events:
      - pipeline.started
`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("write parser-only scalar schema: %v", err)
	}
}

func assertNoRetiredResolutionInstanceKey(t testing.TB, root string) {
	t.Helper()
	for path, raw := range snapshotResolutionMigrationTree(t, root) {
		if filepath.Base(path) != "schema.yaml" {
			continue
		}
		if strings.Contains(string(raw), "instance_key:") || strings.Contains(string(raw), "instance.key.") {
			t.Fatalf("migrated schema %s retains retired authority:\n%s", path, raw)
		}
	}
}
