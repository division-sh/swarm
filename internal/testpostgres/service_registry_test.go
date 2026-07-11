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
	"testing"
)

type fakeDocker struct {
	outputs map[string][]byte
	errors  map[string]error
	calls   []string
}

func (d *fakeDocker) CombinedOutput(_ context.Context, args ...string) ([]byte, error) {
	key := strings.Join(args, " ")
	d.calls = append(d.calls, key)
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

func testRegistryRecord(t *testing.T, state ServiceState) (*ServiceRegistry, ServiceRecord) {
	t.Helper()
	root := t.TempDir()
	registry := NewServiceRegistry(root, filepath.Join(root, "docker-unused"))
	registry.docker = &fakeDocker{outputs: map[string][]byte{managedPSCommand(): nil}, errors: make(map[string]error)}
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

func managedPSCommand() string {
	return "ps --all --filter label=com.division.swarm.test-postgres.managed=1 --format {{.ID}}"
}

func dockerWithContainers(t *testing.T, values ...dockerInspect) *fakeDocker {
	t.Helper()
	fake := &fakeDocker{outputs: make(map[string][]byte), errors: make(map[string]error)}
	var ids []string
	for _, value := range values {
		ids = append(ids, value.ID)
		raw, err := json.Marshal([]dockerInspect{value})
		if err != nil {
			t.Fatal(err)
		}
		fake.outputs["inspect "+value.ID] = raw
	}
	fake.outputs[managedPSCommand()] = []byte(strings.Join(ids, "\n"))
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
