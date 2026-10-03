package testpostgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func holdServiceTestLock(t *testing.T, path string) *fileLock {
	t.Helper()
	lock, acquired, err := acquireFileLock(path, false)
	if err != nil || !acquired {
		t.Fatalf("hold %s: acquired=%v err=%v", path, acquired, err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	return lock
}

func TestServiceRegistryConcurrentCreationPublication(t *testing.T) {
	for _, phase := range []string{"pre_id", "new_row_after_snapshot", "terminal_after_discovery"} {
		t.Run(phase, func(t *testing.T) {
			publisher, record := testRegistryRecord(t, ServiceCreating)
			lease := holdServiceTestLock(t, publisher.leasePath(record.LeaseID))
			creator := holdServiceTestLock(t, publisher.creatorPath(record.LeaseID))
			docker := dockerWithContainers(t, testDockerInspect("container-id", record.LeaseID, record.RunnerID))
			reader := NewServiceRegistry(publisher.StateRoot, "unused")
			reader.docker = docker
			if phase == "new_row_after_snapshot" {
				attachContainerIdentity(t, &record, "container-id")
				record.State = ServiceReady
				if err := creator.Close(); err != nil {
					t.Fatal(err)
				}
				deleteRegistryRecord(t, publisher, record.LeaseID)
				published := false
				docker.beforeCall = func(command string) {
					if command == canonicalPSCommand() && !published {
						published = true
						if err := publisher.putRecord(record); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if phase == "terminal_after_discovery" {
				published := false
				docker.afterCall = func(command string) {
					if command == "inspect container-id" && !published {
						published = true
						attachContainerIdentity(t, &record, "container-id")
						record.State = ServiceCreateSucceeded
						if err := publisher.putRecord(record); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := reader.Reconcile(context.Background()); err != nil {
				t.Fatalf("live publication refused: %v", err)
			}
			if containsCall(docker.calls, "rm --force") {
				t.Fatal("live service removed")
			}
			if _, err := publisher.record(record.LeaseID); err != nil {
				t.Fatal("live row retired", err)
			}
			if phase != "pre_id" {
				var creatorErr error
				if phase != "new_row_after_snapshot" {
					creatorErr = creator.Close()
				}
				if err := errors.Join(creatorErr, lease.Close()); err != nil {
					t.Fatal(err)
				}
				if err := reader.Reconcile(context.Background()); err != nil {
					t.Fatal("released terminal handoff did not converge", err)
				}
				assertServiceAuthorityAbsent(t, reader, record)
			}
		})
	}
}

func TestServiceRegistryPreIDRequiresLiveExactConstruction(t *testing.T) {
	for _, fault := range []string{"prepared", "creator_starting", "create_failed", "no_service_owner", "no_creator_owner", "missing_creator", "duplicate", "name", "image", "owner_label", "daemon_label", "runner_label", "spec_label", "management_label", "contract_label", "lease_label", "live_to_dead"} {
		t.Run(fault, func(t *testing.T) {
			publisher, record := testRegistryRecord(t, ServiceCreating)
			lease := holdServiceTestLock(t, publisher.leasePath(record.LeaseID))
			creator := holdServiceTestLock(t, publisher.creatorPath(record.LeaseID))
			candidate := testDockerInspect("container-id", record.LeaseID, record.RunnerID)
			switch fault {
			case "prepared", "creator_starting", "create_failed":
				record.State = ServiceState(fault)
				if err := publisher.putRecord(record); err != nil {
					t.Fatal(err)
				}
			case "no_service_owner":
				if err := lease.Close(); err != nil {
					t.Fatal(err)
				}
			case "no_creator_owner", "missing_creator":
				if err := creator.Close(); err != nil {
					t.Fatal(err)
				}
				if fault == "missing_creator" {
					if err := os.Remove(publisher.creatorPath(record.LeaseID)); err != nil {
						t.Fatal(err)
					}
				}
			case "name":
				candidate.Name += "-forged"
			case "image":
				candidate.Image = "foreign-image"
			default:
				labels := map[string]string{"owner_label": "owner-id", "daemon_label": "daemon-id", "runner_label": "runner-id", "spec_label": "spec-sha256", "management_label": "managed", "contract_label": "contract", "lease_label": "lease-id"}
				if label, ok := labels[fault]; ok {
					candidate.Config.Labels["com.division.swarm.test-postgres."+label] = "forged"
				}
			}
			docker := dockerWithContainers(t, candidate)
			if fault == "duplicate" {
				second := testDockerInspect("second-id", record.LeaseID, record.RunnerID)
				docker = dockerWithContainers(t, candidate, second)
			}
			if fault == "live_to_dead" {
				released := false
				docker.afterCall = func(command string) {
					if command == "inspect container-id" && !released {
						released = true
						if err := errors.Join(creator.Close(), lease.Close()); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			reader := NewServiceRegistry(publisher.StateRoot, "unused")
			reader.docker = docker
			if err := reader.Reconcile(context.Background()); err == nil {
				t.Fatal("invalid pre-ID construction accepted")
			}
			if containsCall(docker.calls, "rm --force") {
				t.Fatal("invalid namespace was mutated")
			}
			if _, err := publisher.record(record.LeaseID); err != nil {
				t.Fatal("invalid evidence was retired", err)
			}
			if fault == "missing_creator" {
				if _, err := os.Stat(publisher.creatorPath(record.LeaseID)); !os.IsNotExist(err) {
					t.Fatal("authority observation recreated a missing creator fence", err)
				}
			}
		})
	}
}

func TestServiceRegistryTerminalNamespaceRequiresPublishedCID(t *testing.T) {
	for _, fault := range []string{"missing", "malformed", "wrong_digest", "wrong_id", "failed_with_id", "creating_with_id"} {
		t.Run(fault, func(t *testing.T) {
			publisher, record := testRegistryRecord(t, ServiceReady)
			attachContainerIdentity(t, &record, "container-id")
			holdServiceTestLock(t, publisher.leasePath(record.LeaseID))
			switch fault {
			case "missing":
				if err := os.Remove(record.CIDFile); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(record.CIDFile, []byte("foreign\nextra\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "wrong_digest":
				record.CIDFileSHA256 = "foreign-digest"
			case "wrong_id":
				record.ContainerID = "foreign-id"
			case "failed_with_id":
				record.State = ServiceCreateFailed
			case "creating_with_id":
				record.State = ServiceCreating
			}
			if err := publisher.putRecord(record); err != nil {
				t.Fatal(err)
			}
			docker := dockerWithContainers(t, testDockerInspect("container-id", record.LeaseID, record.RunnerID))
			reader := NewServiceRegistry(publisher.StateRoot, "unused")
			reader.docker = docker
			if err := reader.Reconcile(context.Background()); err == nil {
				t.Fatal("invalid terminal identity accepted")
			}
			if containsCall(docker.calls, "rm --force") {
				t.Fatal("corrupt terminal namespace removed")
			}
		})
	}
}

func TestServiceRegistryConcurrentRetirementRequiresExactAbsence(t *testing.T) {
	for _, survives := range []bool{false, true} {
		t.Run(map[bool]string{false: "retired_absent", true: "rowless_survivor"}[survives], func(t *testing.T) {
			publisher, record := testRegistryRecord(t, ServiceReady)
			attachContainerIdentity(t, &record, "container-id")
			if err := publisher.putRecord(record); err != nil {
				t.Fatal(err)
			}
			lease := holdServiceTestLock(t, publisher.leasePath(record.LeaseID))
			creator := holdServiceTestLock(t, publisher.creatorPath(record.LeaseID))
			if err := creator.Close(); err != nil {
				t.Fatal(err)
			}
			docker := dockerWithContainers(t, testDockerInspect("container-id", record.LeaseID, record.RunnerID))
			publisher.docker = docker
			retired := false
			docker.afterCall = func(command string) {
				if command != "inspect container-id" || retired {
					return
				}
				retired = true
				if survives {
					deleteRegistryRecord(t, publisher, record.LeaseID)
					return
				}
				service := &Service{registry: publisher, record: record, lease: lease}
				if err := service.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			reader := NewServiceRegistry(publisher.StateRoot, "unused")
			reader.docker = docker
			err := reader.Reconcile(context.Background())
			if survives {
				if err == nil || !strings.Contains(err.Error(), "has no registry row") || containsCall(docker.calls, "rm --force") {
					t.Fatalf("surviving rowless resource was not fenced: %v calls=%v", err, docker.calls)
				}
			} else {
				if err != nil {
					t.Fatal("stale retired discovery refused", err)
				}
				assertServiceAuthorityAbsent(t, reader, record)
			}
		})
	}
}

func TestServiceRegistryProvisionPreservesPrimaryAndCleanupFailure(t *testing.T) {
	registry, _ := testRegistryRecord(t, ServicePrepared)
	primary, cleanup := errors.New("injected creator-publication failure"), errors.New("injected cleanup write failure")
	docker := registry.docker.(*fakeDocker)
	docker.outputs["info --format {{.ID}}"] = []byte("daemon")
	docker.outputs["image inspect --format {{.Id}} postgres:16"] = []byte("image")
	registry.beforeRegistrySave = func(doc registryDocument) error {
		for _, record := range doc.Services {
			if record.State == ServiceCreatorStarting {
				return primary
			}
			if record.State == ServiceTearingDown {
				return cleanup
			}
		}
		return nil
	}
	service, err := registry.Provision(context.Background(), "must-not-launch")
	if service != nil || !errors.Is(err, primary) || !errors.Is(err, cleanup) {
		t.Fatalf("independent provision/cleanup failure lost: service=%v err=%v", service, err)
	}
	doc, err := registry.registrySnapshot()
	if err != nil || len(doc.Services) != 1 {
		t.Fatalf("ambiguous authority lost: %+v %v", doc, err)
	}
	registry.beforeRegistrySave = nil
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal("failed cleanup did not remain reconcilable", err)
	}
}
