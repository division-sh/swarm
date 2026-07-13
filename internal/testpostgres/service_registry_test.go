package testpostgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeDocker struct {
	outputs               map[string][]byte
	errors                map[string]error
	calls                 []string
	removeLeavesContainer bool
	beforeCall            func(string)
}

func (d *fakeDocker) CombinedOutput(_ context.Context, args ...string) ([]byte, error) {
	key := strings.Join(args, " ")
	d.calls = append(d.calls, key)
	if d.beforeCall != nil {
		d.beforeCall(key)
	}
	if len(args) == 3 && args[0] == "rm" && args[1] == "--force" && !d.removeLeavesContainer {
		inspectKey := "inspect " + args[2]
		delete(d.outputs, inspectKey)
		d.outputs[inspectKey] = []byte("Error: No such object: " + args[2])
		d.errors[inspectKey] = errors.New("exit status 1")
	}
	return d.outputs[key], d.errors[key]
}

func TestServiceRegistryPreparedStateClearsWithoutDockerAuthority(t *testing.T) {
	registry, record := testRegistryRecord(t, ServicePrepared)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("prepared record survives: %v", err)
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestServiceRegistryCreatorStartingFenceBlocksReconciliation(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreatorStarting)
	creator, acquired, err := acquireFileLock(registry.creatorPath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire creator lock: acquired=%v err=%v", acquired, err)
	}
	defer creator.Close()
	err = registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still starting") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestServiceRegistryCreatorStartingWithoutFenceClears(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreatorStarting)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("creator-starting record survives without a fence: %v", err)
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestServiceRegistryCreatingFenceBlocksReconciliation(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreating)
	creator, acquired, err := acquireFileLock(registry.creatorPath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire creator lock: acquired=%v err=%v", acquired, err)
	}
	defer creator.Close()
	err = registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still in flight") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestServiceRegistryAmbiguousCreatorDeathBlocksAndRetainsEvidence(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreating)
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "without a terminal result") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("ambiguous record removed: %v", err)
	}
}

func TestServiceRegistryActiveLeaseIsUntouched(t *testing.T) {
	registry, record := testRegistryRecord(t, ServicePrepared)
	lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire lease: acquired=%v err=%v", acquired, err)
	}
	defer lease.Close()
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("active record removed: %v", err)
	}
}

func TestServiceRegistryRowlessActiveProvisionLeaseIsUntouched(t *testing.T) {
	registry, record := testRegistryRecord(t, ServicePrepared)
	deleteRegistryRecord(t, registry, record.LeaseID)
	lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire pre-row provision lease: acquired=%v err=%v", acquired, err)
	}
	defer lease.Close()
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(registry.leasePath(record.LeaseID)); err != nil {
		t.Fatalf("active pre-row provision authority removed: %v", err)
	}
}

func TestServiceRegistryFailedCreatorRequiresNoContainerEvidence(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreateFailed)
	if err := os.WriteFile(record.CIDFile, []byte("ambiguous"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestServiceRegistryFailedCreatorWithoutEvidenceClears(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreateFailed)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("failed creator record survives proven absence: %v", err)
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestServiceRegistryTerminalPublicationWaitsForCreatorFenceRelease(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceCreateFailed)
	creator, acquired, err := acquireFileLock(registry.creatorPath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire creator lock: acquired=%v err=%v", acquired, err)
	}
	defer creator.Close()
	err = registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still owns terminal handoff") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("terminal evidence removed before fence release: %v", err)
	}
}

func TestServiceRegistryNoRowContainerBlocksAndIsUntouched(t *testing.T) {
	registry, _ := testRegistryRecord(t, ServicePrepared)
	deleteRegistryRecord(t, registry, "lease")
	inspect := testDockerInspect("foreign-container", "foreign-lease", "foreign-runner")
	registry.docker = dockerWithContainers(t, inspect)
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "has no registry row") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("no-row container was removed")
	}
}

func TestServiceRegistryExactTerminalContainerIsRemoved(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	registry.docker = dockerWithContainers(t, inspect)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("terminal record survives exact teardown: %v", err)
	}
	if !containsCall(registry.docker.(*fakeDocker).calls, "rm --force "+record.ContainerID) {
		t.Fatal("exact container was not removed")
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestServiceRegistryTearingDownConvergesAfterContainerAlreadyRemoved(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceTearingDown)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	fake := registry.docker.(*fakeDocker)
	fake.outputs["inspect "+record.ContainerID] = []byte("Error: No such object: " + record.ContainerID)
	fake.errors["inspect "+record.ContainerID] = errors.New("exit status 1")
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("tearing_down record survives exact absence: %v", err)
	}
}

func TestServiceRegistryRetainsAuthorityUntilRemovalAbsenceIsVerified(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	fake := dockerWithContainers(t, inspect)
	fake.removeLeavesContainer = true
	registry.docker = fake
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Fatalf("Reconcile() error = %v, want retained-authority blocker", err)
	}
	got, err := registry.record(record.LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != ServiceTearingDown {
		t.Fatalf("record state = %q, want tearing_down", got.State)
	}
}

func TestServiceRegistryPersistsTearingDownBeforeDockerRemoval(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	fake := dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
	var stateAtRemoval ServiceState
	fake.beforeCall = func(command string) {
		if command != "rm --force "+record.ContainerID {
			return
		}
		doc, err := registry.loadRegistry()
		if err == nil {
			stateAtRemoval = doc.Services[record.LeaseID].State
		}
	}
	registry.docker = fake
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateAtRemoval != ServiceTearingDown {
		t.Fatalf("state at Docker removal = %q, want tearing_down", stateAtRemoval)
	}
}

func TestServiceRegistryForgedLabelBlocksBeforeRemoval(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	inspect.Config.Labels["com.division.swarm.test-postgres.owner-id"] = "forged-owner"
	registry.docker = dockerWithContainers(t, inspect)
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "label") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("forged container was removed")
	}
}

func TestServiceRegistryCanonicalNameWithoutManagedLabelBlocks(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	delete(inspect.Config.Labels, "com.division.swarm.test-postgres.managed")
	registry.docker = dockerWithContainers(t, inspect)
	registry.docker.(*fakeDocker).outputs[managedPSCommand()] = nil
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing or invalid management labels") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("canonical container without labels was removed")
	}
}

func TestServiceRegistryNoRowCanonicalNameWithoutLabelsBlocks(t *testing.T) {
	registry, _ := testRegistryRecord(t, ServicePrepared)
	deleteRegistryRecord(t, registry, "lease")
	inspect := testDockerInspect("foreign-container", "foreign-lease", "foreign-runner")
	inspect.Config.Labels = nil
	registry.docker = dockerWithContainers(t, inspect)
	registry.docker.(*fakeDocker).outputs[managedPSCommand()] = nil
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing or invalid management labels") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("no-row canonical container without labels was removed")
	}
}

func TestServiceRegistryInvalidManagedLabelBlocks(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	inspect.Config.Labels["com.division.swarm.test-postgres.managed"] = "true"
	registry.docker = dockerWithContainers(t, inspect)
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing or invalid management labels") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestServiceRegistryContainerUnionDeduplicatesFullIdentity(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	registry.docker = dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, call := range registry.docker.(*fakeDocker).calls {
		if call == "inspect "+record.ContainerID {
			count++
		}
	}
	// One discovery inspection plus one exact pre-remove inspection and one
	// absence verification. The namespace union must not add a fourth.
	if count != 3 {
		t.Fatalf("inspect count = %d, want 3", count)
	}
}

func TestServiceCloseInspectsBeforeRemoval(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	inspect := testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID)
	inspect.Config.Labels["com.division.swarm.test-postgres.spec-sha256"] = "forged"
	registry.docker = dockerWithContainers(t, inspect)
	service := &Service{registry: registry, record: record}
	err := service.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "label") {
		t.Fatalf("Close() error = %v", err)
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("Close removed a mismatched container")
	}
}

func TestServiceCloseRemovesAllAuthorityFiles(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire lease: acquired=%v err=%v", acquired, err)
	}
	creator, acquired, err := acquireFileLock(registry.creatorPath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire creator: acquired=%v err=%v", acquired, err)
	}
	if err := creator.Close(); err != nil {
		t.Fatal(err)
	}
	registry.docker = dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
	service := &Service{registry: registry, record: record, lease: lease}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestServiceRegistryRetirementWriteFailureIsReplayable(t *testing.T) {
	for _, path := range []string{"close", "reconcile"} {
		t.Run(path, func(t *testing.T) {
			state := ServiceTearingDown
			if path == "close" {
				state = ServiceReady
			}
			registry, record := terminalRegistryRecord(t, state)
			var service *Service
			if path == "close" {
				lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
				if err != nil || !acquired {
					t.Fatalf("acquire service lease: acquired=%v err=%v", acquired, err)
				}
				service = &Service{registry: registry, record: record, lease: lease}
			}
			injected := errors.New("injected registry commit failure")
			registry.beforeRegistrySave = func(doc registryDocument) error {
				if _, exists := doc.Services[record.LeaseID]; !exists {
					return injected
				}
				return nil
			}
			var err error
			if path == "close" {
				err = service.Close(context.Background())
			} else {
				err = registry.Reconcile(context.Background())
			}
			if !errors.Is(err, injected) {
				t.Fatalf("terminal %s error = %v, want injected commit failure", path, err)
			}
			if _, err := registry.record(record.LeaseID); err != nil {
				t.Fatalf("registry row lost after failed commit: %v", err)
			}
			assertServiceAuthorityPresent(t, registry, record)

			registry.beforeRegistrySave = nil
			if err := registry.Reconcile(context.Background()); err != nil {
				t.Fatalf("reconcile after failed %s retirement: %v", path, err)
			}
			assertServiceAuthorityAbsent(t, registry, record)
		})
	}
}

func TestServiceRegistryRetirementCrashAfterRowCommitIsReplayable(t *testing.T) {
	for _, path := range []string{"close", "reconcile"} {
		t.Run(path, func(t *testing.T) {
			state := ServiceTearingDown
			if path == "close" {
				state = ServiceReady
			}
			registry, record := terminalRegistryRecord(t, state)
			var service *Service
			if path == "close" {
				lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
				if err != nil || !acquired {
					t.Fatalf("acquire service lease: acquired=%v err=%v", acquired, err)
				}
				service = &Service{registry: registry, record: record, lease: lease}
			}
			injected := errors.New("injected process death after row commit")
			registry.afterRecordDelete = func(ServiceRecord) error { return injected }
			var err error
			if path == "close" {
				err = service.Close(context.Background())
			} else {
				err = registry.Reconcile(context.Background())
			}
			if !errors.Is(err, injected) {
				t.Fatalf("terminal %s error = %v, want injected crash window", path, err)
			}
			if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
				t.Fatalf("registry row survived committed retirement: %v", err)
			}
			assertServiceAuthorityPresent(t, registry, record)

			registry.afterRecordDelete = nil
			if err := registry.Reconcile(context.Background()); err != nil {
				t.Fatalf("reconcile after %s retirement crash: %v", path, err)
			}
			assertServiceAuthorityAbsent(t, registry, record)
		})
	}
}

func TestServiceRegistryRejectsUnknownState(t *testing.T) {
	registry, record := testRegistryRecord(t, ServicePrepared)
	record.State = "future_state"
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unknown Postgres service state") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestServiceRegistryReportsDockerEnumerationFailure(t *testing.T) {
	registry, _ := testRegistryRecord(t, ServicePrepared)
	fake := registry.docker.(*fakeDocker)
	fake.errors[managedPSCommand()] = errors.New("daemon unavailable")
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "enumerate managed Postgres services") {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestManagedContainersIgnoresContainerRemovedAfterEnumeration(t *testing.T) {
	registry := NewServiceRegistry(t.TempDir(), "docker-unused")
	fake := &fakeDocker{outputs: map[string][]byte{
		canonicalPSCommand():  []byte("container-1 swarm-test-postgres-v1-runner-1\n"),
		managedPSCommand():    []byte("container-1\n"),
		"inspect container-1": []byte("Error: No such object: container-1"),
	}, errors: map[string]error{"inspect container-1": errors.New("exit status 1")}}
	registry.docker = fake

	candidates, err := registry.managedContainers(context.Background())
	if err != nil {
		t.Fatalf("managedContainers: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("managed containers after concurrent removal = %#v, want none", candidates)
	}
}

func TestServiceRegistryRejectsUnsafeStateRootBeforeDocker(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatal(err)
	}
	registry := NewServiceRegistry(root, "docker-unused")
	fake := &fakeDocker{outputs: make(map[string][]byte), errors: make(map[string]error)}
	registry.docker = fake
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsafe mode") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("Docker called with unsafe authority: %v", fake.calls)
	}
}

func TestServiceRegistryRejectsSymlinkedAuthorityBeforeDocker(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := NewServiceRegistry(root, "docker-unused")
	if err := registry.initialize(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(target, []byte(`{"version":1,"services":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "services-v1.json")); err != nil {
		t.Fatal(err)
	}
	fake := &fakeDocker{outputs: make(map[string][]byte), errors: make(map[string]error)}
	registry.docker = fake
	err := registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("Docker called with symlinked authority: %v", fake.calls)
	}
}

func TestServiceRegistryOwnerIDFirstCreationIsAtomic(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := NewServiceRegistry(root, "docker-unused")
	if err := registry.initialize(); err != nil {
		t.Fatal(err)
	}
	const workers = 32
	values := make(chan string, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			value, err := registry.ownerID()
			values <- value
			errs <- err
		}()
	}
	group.Wait()
	close(values)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var want string
	for value := range values {
		if want == "" {
			want = value
		}
		if value != want {
			t.Fatalf("concurrent owner IDs differ: %q != %q", value, want)
		}
	}
}

func testRegistryRecord(t *testing.T, state ServiceState) (*ServiceRegistry, ServiceRecord) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := NewServiceRegistry(root, filepath.Join(root, "docker-unused"))
	registry.docker = &fakeDocker{outputs: map[string][]byte{managedPSCommand(): nil, canonicalPSCommand(): nil}, errors: make(map[string]error)}
	if err := registry.initialize(); err != nil {
		t.Fatal(err)
	}
	record := ServiceRecord{
		Version: 1, State: state, OwnerID: "owner", DaemonID: "daemon", ImageID: "image",
		RunnerID: "runner", LeaseID: "lease", Name: "swarm-test-postgres-v1-runner",
		CIDFile: filepath.Join(root, "handoff", "lease.cid"), SpecHash: "hash",
		Labels: serviceLabels("owner", "daemon", "runner", "lease", "hash"),
	}
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	return registry, record
}

func terminalRegistryRecord(t *testing.T, state ServiceState) (*ServiceRegistry, ServiceRecord) {
	t.Helper()
	registry, record := testRegistryRecord(t, state)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	creator, acquired, err := acquireFileLock(registry.creatorPath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("create terminal creator authority: acquired=%v err=%v", acquired, err)
	}
	if err := creator.Close(); err != nil {
		t.Fatal(err)
	}
	fake := registry.docker.(*fakeDocker)
	fake.outputs["inspect "+record.ContainerID] = []byte("Error: No such object: " + record.ContainerID)
	fake.errors["inspect "+record.ContainerID] = errors.New("exit status 1")
	return registry, record
}

func assertServiceAuthorityPresent(t *testing.T, registry *ServiceRegistry, record ServiceRecord) {
	t.Helper()
	for _, path := range []string{record.CIDFile, registry.creatorPath(record.LeaseID), registry.leasePath(record.LeaseID)} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("terminal service authority %q missing: %v", path, err)
		}
	}
}

func assertServiceAuthorityAbsent(t *testing.T, registry *ServiceRegistry, record ServiceRecord) {
	t.Helper()
	for _, path := range []string{record.CIDFile, registry.creatorPath(record.LeaseID), registry.leasePath(record.LeaseID)} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("terminal service authority %q survives: %v", path, err)
		}
	}
}

func managedPSCommand() string {
	return "ps --all --filter label=com.division.swarm.test-postgres.managed=1 --format {{.ID}}"
}

func canonicalPSCommand() string {
	return "ps --all --format {{.ID}} {{.Names}}"
}

func dockerWithContainers(t *testing.T, values ...dockerInspect) *fakeDocker {
	t.Helper()
	fake := &fakeDocker{outputs: make(map[string][]byte), errors: make(map[string]error)}
	var ids []string
	var names []string
	for _, value := range values {
		ids = append(ids, value.ID)
		names = append(names, value.ID+" "+strings.TrimPrefix(value.Name, "/"))
		raw, err := json.Marshal([]dockerInspect{value})
		if err != nil {
			t.Fatal(err)
		}
		fake.outputs["inspect "+value.ID] = raw
	}
	fake.outputs[managedPSCommand()] = []byte(strings.Join(ids, "\n"))
	fake.outputs[canonicalPSCommand()] = []byte(strings.Join(names, "\n"))
	return fake
}

func testDockerInspect(id, leaseID, runnerID string) dockerInspect {
	labels := serviceLabels("owner", "daemon", runnerID, leaseID, "hash")
	var result dockerInspect
	result.ID = id
	result.Name = "/swarm-test-postgres-v1-" + runnerID
	result.Image = "image"
	result.Config.Labels = labels
	return result
}

func deleteRegistryRecord(t *testing.T, registry *ServiceRegistry, leaseID string) {
	t.Helper()
	if err := registry.deleteRecord(leaseID); err != nil {
		t.Fatal(err)
	}
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if strings.Contains(call, want) {
			return true
		}
	}
	return false
}

func attachContainerIdentity(t *testing.T, record *ServiceRecord, id string) {
	t.Helper()
	raw := []byte(id + "\n")
	if err := os.WriteFile(record.CIDFile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	record.ContainerID = id
	record.CIDFileSHA256 = hex.EncodeToString(digest[:])
}
