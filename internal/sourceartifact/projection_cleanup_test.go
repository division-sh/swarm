package sourceartifact

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestProjectionCleanupIntentSurvivesProcessHandlesWithoutTouchingSuccessor(t *testing.T) {
	artifactRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(artifactRoot, "schema.yaml"), []byte("name: admitted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := AdmitDirectory(artifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	old, err := MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Release() })
	next, err := MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Release() })
	intent, err := old.CleanupIntent()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	var recovered RuntimeProjectionCleanup
	if err := json.Unmarshal(encoded, &recovered); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := SettleRuntimeProjectionCleanup(recovered); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(intent.Root); !os.IsNotExist(err) {
		t.Fatalf("predecessor projection not removed: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(next.PrivateRoot(), "schema.yaml"))
	if err != nil || string(content) != "name: admitted\n" {
		t.Fatalf("cleanup affected successor or confused authored bytes with marker: %v", err)
	}
	if _, err := os.Stat(filepath.Join(next.PrivateRoot(), "identity.json")); !os.IsNotExist(err) {
		t.Fatalf("ownership marker leaked into mounted admitted source: %v", err)
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	if got, err := old.CleanupIntent(); err != nil || got != intent || old.PrivateRoot() != "" {
		t.Fatalf("released disposal identity changed or reopened source access: %v", err)
	}
}

func TestProjectionCleanupRejectsChangedOrSymlinkedOwnership(t *testing.T) {
	for _, change := range []string{"marker", "marker_missing", "marker_symlink", "root_symlink", "unexpected_entry"} {
		t.Run(change, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			artifactRoot := t.TempDir()
			if err := os.WriteFile(filepath.Join(artifactRoot, "schema.yaml"), []byte("source\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			artifact, err := AdmitDirectory(artifactRoot)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := MaterializeRuntimeProjection(artifact)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = projection.Release() })
			intent, err := projection.CleanupIntent()
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(intent.Root, "identity.json")
			original, _ := json.Marshal(intent)
			t.Cleanup(func() {
				_ = os.Remove(marker)
				if err := os.WriteFile(marker, original, 0o400); err != nil {
					t.Error(err)
				}
			})
			if change == "root_symlink" {
				link := filepath.Join(t.TempDir(), "swarm-source-link")
				if err := os.Symlink(intent.Root, link); err != nil {
					t.Fatal(err)
				}
				intent.Root = link
			} else if change == "unexpected_entry" {
				unexpected := filepath.Join(intent.Root, "unowned")
				if err := os.WriteFile(unexpected, []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Remove(unexpected) })
			} else {
				if err := os.Remove(marker); err != nil {
					t.Fatal(err)
				}
				if change == "marker_symlink" {
					outside := filepath.Join(t.TempDir(), "identity.json")
					encoded, _ := json.Marshal(intent)
					if err := os.WriteFile(outside, encoded, 0o400); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(outside, marker); err != nil {
						t.Fatal(err)
					}
				} else if change != "marker_missing" {
					if err := os.WriteFile(marker, []byte(`{"wrong_owner":true}`), 0o400); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := SettleRuntimeProjectionCleanup(intent); err == nil {
				t.Fatal("unproven projection ownership accepted")
			}
			if _, err := os.Stat(projection.PrivateRoot()); err != nil {
				t.Fatalf("unproven cleanup removed source: %v", err)
			}
		})
	}
}

func TestProjectionCleanupResumesEveryFilesystemSettlementWindow(t *testing.T) {
	for _, phase := range []string{"source-partial", "source-removed", "marker-removed"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: cleanup-window\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			artifact, err := AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := MaterializeRuntimeProjection(artifact)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := projection.Release(); err != nil {
					t.Error(err)
				}
			})
			intent, err := projection.CleanupIntent()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(projection.PrivateRoot(), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(projection.PrivateRoot(), "schema.yaml")); err != nil {
				t.Fatal(err)
			}
			if phase != "source-partial" {
				if err := os.Remove(projection.PrivateRoot()); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "marker-removed" {
				if err := os.Remove(filepath.Join(intent.Root, "identity.json")); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := SettleRuntimeProjectionCleanup(intent); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(intent.Root); !os.IsNotExist(err) {
				t.Fatalf("interrupted settlement left its envelope: %v", err)
			}
		})
	}
}
