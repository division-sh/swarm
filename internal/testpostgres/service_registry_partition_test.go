package testpostgres

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const neighborServiceOwnerID = "22222222-2222-4222-8222-222222222222"

func proveIndependentRegistryNamespaces(t *testing.T) {
	for _, phase := range []string{"foreign_only", "mixed_same_lease", "neighbor_creating", "neighbor_retiring"} {
		t.Run(phase, func(t *testing.T) {
			local, record := testRegistryRecord(t, ServiceReady)
			attachContainerIdentity(t, &record, "local-id")
			if err := local.putRecord(record); err != nil {
				t.Fatal(err)
			}
			holdServiceTestLock(t, local.leasePath(record.LeaseID))
			neighbor, other := testRegistryRecord(t, ServiceReady)
			other.OwnerID, other.RunnerID, other.Name = neighborServiceOwnerID, "neighbor", "swarm-test-postgres-v1-neighbor"
			other.Labels = serviceLabels(other.OwnerID, other.DaemonID, other.RunnerID, other.LeaseID, other.SpecHash)
			if err := os.WriteFile(filepath.Join(neighbor.StateRoot, "owner-id"), []byte(other.OwnerID+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			attachContainerIdentity(t, &other, "neighbor-id")
			if err := neighbor.putRecord(other); err != nil {
				t.Fatal(err)
			}
			otherLease := holdServiceTestLock(t, neighbor.leasePath(other.LeaseID))
			foreign := testDockerInspect(other.ContainerID, other.LeaseID, other.RunnerID)
			foreign.Config.Labels = other.Labels
			values := []dockerInspect{testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID), foreign}
			if phase == "foreign_only" {
				deleteRegistryRecord(t, local, record.LeaseID)
				values = values[1:]
			}
			docker := dockerWithContainers(t, values...)
			local.docker, neighbor.docker = docker, docker
			before := snapshotServiceFiles(t, neighbor.StateRoot)
			changed := false
			if phase == "neighbor_creating" {
				other.State, other.ContainerID, other.CIDFileSHA256 = ServiceCreating, "", ""
				if err := neighbor.putRecord(other); err != nil {
					t.Fatal(err)
				}
				creator := holdServiceTestLock(t, neighbor.creatorPath(other.LeaseID))
				docker.afterCall = func(command string) {
					if command != "inspect neighbor-id" || changed {
						return
					}
					changed = true
					attachContainerIdentity(t, &other, "neighbor-id")
					other.State = ServiceCreateSucceeded
					if err := errors.Join(neighbor.putRecord(other), creator.Close()); err != nil {
						t.Fatal(err)
					}
					before = snapshotServiceFiles(t, neighbor.StateRoot)
				}
			}
			if phase == "neighbor_retiring" {
				docker.afterCall = func(command string) {
					if command != "inspect neighbor-id" || changed {
						return
					}
					changed = true
					if err := (&Service{registry: neighbor, record: other, lease: otherLease}).Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					before = snapshotServiceFiles(t, neighbor.StateRoot)
				}
			}
			if err := local.Reconcile(context.Background()); err != nil {
				t.Fatalf("independent namespace refused: %v", err)
			}
			assertServiceFilesUnchanged(t, neighbor.StateRoot, before)
			removals := 0
			for _, command := range docker.calls {
				if strings.HasPrefix(command, "rm ") {
					if phase != "neighbor_retiring" || command != "rm --force neighbor-id" {
						t.Fatalf("local reconciliation mutated another service: %s", command)
					}
					removals++
				}
			}
			if phase == "neighbor_retiring" && (!changed || removals != 1) || phase == "neighbor_creating" && !changed {
				t.Fatalf("controlled neighbor phase not reached: changed=%t removals=%d", changed, removals)
			}
			if phase == "mixed_same_lease" || phase == "neighbor_creating" {
				beforeLocal := snapshotServiceFiles(t, local.StateRoot)
				if err := neighbor.Reconcile(context.Background()); err != nil {
					t.Fatalf("reverse namespace refused: %v", err)
				}
				assertServiceFilesUnchanged(t, local.StateRoot, beforeLocal)
			}
		})
	}
}

func proveRegistryNamespaceRefusals(t *testing.T) {
	for _, fault := range []string{"same_owner_rowless", "duplicate", "missing_owner_label", "invalid_owner_label", "nil_owner_label", "foreign_missing_managed_label", "foreign_owner_local_cid", "foreign_owner_local_name", "foreign_owner_new_local_row", "missing_owner_file", "missing_owner_file_rowless", "replaced_owner_file", "invalid_owner_file", "inspection_error"} {
		t.Run(fault, func(t *testing.T) {
			registry, record := testRegistryRecord(t, ServiceReady)
			attachContainerIdentity(t, &record, "local-id")
			if err := registry.putRecord(record); err != nil {
				t.Fatal(err)
			}
			holdServiceTestLock(t, registry.leasePath(record.LeaseID))
			candidate := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
			ownerPath := filepath.Join(registry.StateRoot, "owner-id")
			switch fault {
			case "same_owner_rowless":
				deleteRegistryRecord(t, registry, record.LeaseID)
			case "missing_owner_label":
				delete(candidate.Config.Labels, "com.division.swarm.test-postgres.owner-id")
			case "invalid_owner_label":
				candidate.Config.Labels["com.division.swarm.test-postgres.owner-id"] = "invalid"
			case "nil_owner_label":
				candidate.Config.Labels["com.division.swarm.test-postgres.owner-id"] = "00000000-0000-0000-0000-000000000000"
			case "foreign_missing_managed_label":
				candidate.Config.Labels["com.division.swarm.test-postgres.owner-id"] = neighborServiceOwnerID
				delete(candidate.Config.Labels, "com.division.swarm.test-postgres.managed")
			case "foreign_owner_local_cid", "foreign_owner_local_name", "foreign_owner_new_local_row":
				candidate.Config.Labels["com.division.swarm.test-postgres.owner-id"] = neighborServiceOwnerID
				if fault != "foreign_owner_local_name" {
					candidate.Name = "/swarm-test-postgres-v1-neighbor"
					candidate.Config.Labels["com.division.swarm.test-postgres.runner-id"] = "neighbor"
				} else {
					candidate.ID = "other-id"
				}
			case "missing_owner_file", "missing_owner_file_rowless":
				if fault == "missing_owner_file_rowless" {
					deleteRegistryRecord(t, registry, record.LeaseID)
				}
				if err := os.Remove(ownerPath); err != nil {
					t.Fatal(err)
				}
			case "replaced_owner_file", "invalid_owner_file":
				value := neighborServiceOwnerID
				if fault == "invalid_owner_file" {
					value = "invalid"
				}
				if err := os.WriteFile(ownerPath, []byte(value+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			values := []dockerInspect{candidate}
			if fault == "duplicate" {
				values = append(values, testDockerInspect("duplicate-id", record.LeaseID, record.RunnerID))
			}
			docker := dockerWithContainers(t, values...)
			if fault == "inspection_error" {
				docker.errors["inspect "+candidate.ID] = errors.New("permission denied")
			}
			registry.docker = docker
			before := snapshotServiceFiles(t, registry.StateRoot)
			if fault == "foreign_owner_new_local_row" {
				deleteRegistryRecord(t, registry, record.LeaseID)
				published := false
				docker.afterCall = func(command string) {
					if command == "inspect "+candidate.ID && !published {
						published = true
						if err := registry.putRecord(record); err != nil {
							t.Fatal(err)
						}
						before = snapshotServiceFiles(t, registry.StateRoot)
					}
				}
			}
			if err := registry.Reconcile(context.Background()); err == nil {
				t.Fatal("contradictory or missing namespace evidence admitted")
			}
			if containsCall(docker.calls, "rm --force") {
				t.Fatal("refused namespace was mutated")
			}
			assertServiceFilesUnchanged(t, registry.StateRoot, before)
		})
	}
}

type serviceFileSnapshot struct {
	info os.FileInfo
	raw  []byte
}

func snapshotServiceFiles(t *testing.T, root string) map[string]serviceFileSnapshot {
	t.Helper()
	files := map[string]serviceFileSnapshot{}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		files[path] = serviceFileSnapshot{info: info, raw: raw}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func assertServiceFilesUnchanged(t *testing.T, root string, before map[string]serviceFileSnapshot) {
	t.Helper()
	after := snapshotServiceFiles(t, root)
	if len(before) != len(after) {
		t.Fatalf("namespace authority file count changed: before=%d after=%d", len(before), len(after))
	}
	for path, want := range before {
		got, ok := after[path]
		if !ok || !os.SameFile(want.info, got.info) || want.info.Mode() != got.info.Mode() || !bytes.Equal(want.raw, got.raw) {
			t.Fatalf("namespace authority changed: %s", path)
		}
	}
}
