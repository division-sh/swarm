package testpostgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	serviceRegistryVersion = 1
	postgresImage          = "postgres:16"
)

type ServiceState string

const (
	ServicePrepared        ServiceState = "prepared"
	ServiceCreatorStarting ServiceState = "creator_starting"
	ServiceCreating        ServiceState = "creating"
	ServiceCreateSucceeded ServiceState = "create_succeeded"
	ServiceCreateFailed    ServiceState = "create_failed"
	ServiceStarting        ServiceState = "starting"
	ServiceReady           ServiceState = "ready"
	ServiceChildRunning    ServiceState = "child_running"
	ServiceTearingDown     ServiceState = "tearing_down"
)

type ServiceRecord struct {
	Version       int               `json:"version"`
	State         ServiceState      `json:"state"`
	OwnerID       string            `json:"owner_id"`
	DaemonID      string            `json:"daemon_id"`
	ImageID       string            `json:"image_id"`
	RunnerID      string            `json:"runner_id"`
	LeaseID       string            `json:"lease_id"`
	Name          string            `json:"name"`
	ContainerID   string            `json:"container_id,omitempty"`
	CIDFileSHA256 string            `json:"cidfile_sha256,omitempty"`
	CIDFile       string            `json:"cidfile"`
	SpecHash      string            `json:"spec_hash"`
	Labels        map[string]string `json:"labels"`
	CreateError   string            `json:"create_error,omitempty"`
	CreateResult  string            `json:"create_result,omitempty"`
	CreatedAtUTC  string            `json:"created_at_utc"`
}

type registryDocument struct {
	Version  int                      `json:"version"`
	Services map[string]ServiceRecord `json:"services"`
}

type ServiceRegistry struct {
	StateRoot          string
	DockerBin          string
	docker             dockerExecutor
	beforeRegistrySave func(registryDocument) error
	afterRecordDelete  func(ServiceRecord) error
}

type dockerExecutor interface {
	CombinedOutput(context.Context, ...string) ([]byte, error)
}

type commandDocker struct{ bin string }

func (d commandDocker) CombinedOutput(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, d.bin, args...).CombinedOutput()
}

type Service struct {
	registry   *ServiceRegistry
	record     ServiceRecord
	lease      *fileLock
	Connection Connection
	closed     bool
}

type dockerInspect struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Image  string `json:"Image"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
}

func DefaultServiceRegistry() (*ServiceRegistry, error) {
	stateRoot, err := defaultServiceStateRoot()
	if err != nil {
		return nil, err
	}
	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		return nil, fmt.Errorf("Docker is required for the test runner; configure host Postgres using internal/testutil/POSTGRES.md: %w", err)
	}
	return NewServiceRegistry(stateRoot, dockerBin), nil
}

func NewServiceRegistry(stateRoot, dockerBin string) *ServiceRegistry {
	return &ServiceRegistry{StateRoot: stateRoot, DockerBin: dockerBin, docker: commandDocker{bin: dockerBin}}
}

func (r *ServiceRegistry) Provision(ctx context.Context, executable string) (*Service, error) {
	if err := validateCreatorProcessSupport(); err != nil {
		return nil, err
	}
	if err := r.initialize(); err != nil {
		return nil, err
	}
	if err := r.Reconcile(ctx); err != nil {
		return nil, err
	}
	if out, err := r.runDocker(ctx, "pull", postgresImage); err != nil {
		return nil, fmt.Errorf("pull %s: %v output=%s", postgresImage, err, strings.TrimSpace(string(out)))
	}
	daemonID, err := r.dockerOutput(ctx, "info", "--format", "{{.ID}}")
	if err != nil {
		return nil, err
	}
	imageID, err := r.dockerOutput(ctx, "image", "inspect", "--format", "{{.Id}}", postgresImage)
	if err != nil {
		return nil, err
	}
	ownerID, err := r.ownerID()
	if err != nil {
		return nil, err
	}
	runnerID := uuid.NewString()
	leaseID := uuid.NewString()
	name := "swarm-test-postgres-v1-" + runnerID
	specHash := serviceSpecHash(imageID)
	labels := serviceLabels(ownerID, daemonID, runnerID, leaseID, specHash)
	record := ServiceRecord{
		Version: serviceRegistryVersion, State: ServicePrepared, OwnerID: ownerID,
		DaemonID: daemonID, ImageID: imageID, RunnerID: runnerID, LeaseID: leaseID,
		Name: name, CIDFile: filepath.Join(r.StateRoot, "handoff", leaseID+".cid"),
		SpecHash: specHash, Labels: labels, CreatedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := os.Remove(record.CIDFile); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	lease, acquired, err := acquireFileLock(r.leasePath(leaseID), false)
	if err != nil || !acquired {
		return nil, fmt.Errorf("acquire service lease: %w", err)
	}
	service := &Service{registry: r, record: record, lease: lease}
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			_ = service.Close(context.Background())
		}
	}()
	if err := r.putRecord(record); err != nil {
		return nil, err
	}
	creator, acquired, err := acquireFileLock(r.creatorPath(leaseID), false)
	if err != nil || !acquired {
		return nil, fmt.Errorf("acquire creator fence: %w", err)
	}
	record.State = ServiceCreatorStarting
	if err := r.putRecord(record); err != nil {
		_ = creator.Close()
		return nil, err
	}
	cmd, err := creatorProcessCommand(executable, r.StateRoot, leaseID, creator)
	if err != nil {
		_ = creator.Close()
		return nil, err
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = creator.Close()
		return nil, fmt.Errorf("start Postgres service creator: %w", err)
	}
	// Close only the runner's descriptor. Unlocking would also release the
	// inherited flock held by the helper's shared open-file description.
	_ = creator.File().Close()
	waitErr := cmd.Wait()
	record, err = r.record(leaseID)
	if err != nil {
		return nil, err
	}
	service.record = record
	if waitErr != nil {
		cleanupOnError = false
		_ = service.lease.Close()
		service.lease = nil
		return nil, fmt.Errorf("Postgres service creator failed in state %q: %w", record.State, waitErr)
	}
	if record.State != ServiceCreateSucceeded || record.ContainerID == "" {
		cleanupOnError = false
		_ = service.lease.Close()
		service.lease = nil
		return nil, fmt.Errorf("Postgres service creator ended in state %q: %s", record.State, record.CreateError)
	}
	service.record = record
	if err := r.transition(leaseID, ServiceStarting); err != nil {
		return nil, err
	}
	if out, err := r.runDocker(ctx, "start", record.ContainerID); err != nil {
		return nil, fmt.Errorf("start Postgres service %s: %v output=%s", record.ContainerID, err, strings.TrimSpace(string(out)))
	}
	portOutput, err := r.dockerOutput(ctx, "port", record.ContainerID, "5432/tcp")
	if err != nil {
		return nil, err
	}
	port, err := parseDockerPort(portOutput)
	if err != nil {
		return nil, err
	}
	connection, err := NewOwnedDockerConnection(port)
	if err != nil {
		return nil, err
	}
	db, err := connection.Open()
	if err != nil {
		return nil, err
	}
	if err := waitForDatabase(ctx, db, 90*time.Second); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("wait for runner-owned Postgres: %w", err)
	}
	if err := hardenOwnedPostgres(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := verifyOwnedPostgresSettings(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	_ = db.Close()
	if err := r.transition(leaseID, ServiceReady); err != nil {
		return nil, err
	}
	record, _ = r.record(leaseID)
	service.record = record
	service.Connection = connection
	cleanupOnError = false
	return service, nil
}

func hardenOwnedPostgres(ctx context.Context, db *sql.DB) error {
	for _, name := range []string{"postgres", "template1"} {
		if _, err := db.ExecContext(ctx, `REVOKE CONNECT, TEMPORARY ON DATABASE `+quoteIdent(name)+` FROM PUBLIC`); err != nil {
			return fmt.Errorf("harden runner-owned Postgres database %q: %w", name, err)
		}
	}
	return nil
}

func (s *Service) MarkChildRunning() error {
	if err := s.registry.transition(s.record.LeaseID, ServiceChildRunning); err != nil {
		return err
	}
	s.record.State = ServiceChildRunning
	return nil
}

func (s *Service) Close(ctx context.Context) error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	retired := false
	if err := s.registry.initialize(); err != nil {
		errs = append(errs, err)
	}
	if s.record.LeaseID != "" {
		if len(errs) == 0 {
			if err := s.registry.transition(s.record.LeaseID, ServiceTearingDown); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
		}
		if len(errs) == 0 && s.record.ContainerID != "" {
			if _, exists, err := s.registry.inspectExactMaybe(ctx, s.record); err != nil {
				errs = append(errs, err)
			} else if exists {
				if out, err := s.registry.runDocker(ctx, "rm", "--force", s.record.ContainerID); err != nil {
					errs = append(errs, fmt.Errorf("remove Postgres service %s: %v output=%s", s.record.ContainerID, err, strings.TrimSpace(string(out))))
				}
			}
			if len(errs) == 0 {
				if err := s.registry.verifyContainerAbsent(ctx, s.record.ContainerID); err != nil {
					errs = append(errs, err)
				}
			}
		}
		if len(errs) == 0 {
			if err := s.registry.retireRecordAuthority(s.record); err != nil {
				errs = append(errs, err)
			} else {
				retired = true
			}
		}
	}
	leaseClosed := s.lease == nil
	if s.lease != nil {
		if err := s.lease.Close(); err != nil {
			errs = append(errs, err)
		} else {
			leaseClosed = true
		}
	}
	if retired && leaseClosed {
		if err := removeAuthorityFile(s.registry.leasePath(s.record.LeaseID), "service lease"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *ServiceRegistry) RunCreator(ctx context.Context, leaseID string, creatorFD uintptr) error {
	restrictCreatorProcessFileMode()
	if err := r.initialize(); err != nil {
		return err
	}
	creatorFile := os.NewFile(creatorFD, "creator-fence")
	if creatorFile == nil {
		return fmt.Errorf("creator fence descriptor is invalid")
	}
	defer creatorFile.Close()
	record, err := r.record(leaseID)
	if err != nil {
		return err
	}
	if record.State != ServiceCreatorStarting {
		return fmt.Errorf("creator record state = %q, want %q", record.State, ServiceCreatorStarting)
	}
	if err := r.validateCreatorFence(record, creatorFile); err != nil {
		return err
	}
	if err := validateServiceRecord(leaseID, record); err != nil {
		return err
	}
	daemonID, err := r.dockerOutput(ctx, "info", "--format", "{{.ID}}")
	if err != nil {
		return err
	}
	imageID, err := r.dockerOutput(ctx, "image", "inspect", "--format", "{{.Id}}", postgresImage)
	if err != nil {
		return err
	}
	if daemonID != record.DaemonID || imageID != record.ImageID || serviceSpecHash(imageID) != record.SpecHash {
		return fmt.Errorf("creator identity changed before Docker mutation; state retained")
	}
	if err := r.transition(leaseID, ServiceCreating); err != nil {
		return err
	}
	args := []string{"create", "--cidfile", record.CIDFile, "--name", record.Name, "--rm", "--tmpfs", "/var/lib/postgresql/data:rw", "-e", "POSTGRES_PASSWORD=postgres", "-e", "POSTGRES_DB=postgres", "-p", "127.0.0.1::5432"}
	keys := make([]string, 0, len(record.Labels))
	for key := range record.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--label", key+"="+record.Labels[key])
	}
	args = append(args, record.ImageID, "-c", "max_connections=300", "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off")
	out, createErr := r.runDocker(ctx, args...)
	if createErr != nil {
		record.State = ServiceCreateFailed
		record.CreateError = fmt.Sprintf("%v output=%s", createErr, strings.TrimSpace(string(out)))
		record.CreateResult = "docker create exited with failure"
		return r.putRecord(record)
	}
	if err := os.Chmod(record.CIDFile, 0o600); err != nil {
		return fmt.Errorf("secure Docker cidfile: %w", err)
	}
	rawID, err := os.ReadFile(record.CIDFile)
	if err != nil {
		return fmt.Errorf("read Docker cidfile: %w", err)
	}
	record.ContainerID = strings.TrimSpace(string(rawID))
	if record.ContainerID == "" {
		return fmt.Errorf("Docker cidfile is empty")
	}
	cidDigest := sha256.Sum256(rawID)
	record.CIDFileSHA256 = hex.EncodeToString(cidDigest[:])
	if _, err := r.inspectExact(ctx, record); err != nil {
		return err
	}
	record.State = ServiceCreateSucceeded
	record.CreateError = ""
	record.CreateResult = "docker create exited successfully and exact identity was inspected"
	return r.putRecord(record)
}

func (r *ServiceRegistry) Reconcile(ctx context.Context) error {
	if err := r.initialize(); err != nil {
		return err
	}
	doc, err := r.registrySnapshot()
	if err != nil {
		return err
	}
	candidates, err := r.managedContainers(ctx)
	if err != nil {
		return err
	}
	if err := validateManagedNamespace(&doc, candidates); err != nil {
		return err
	}
	if err := r.reconcileOrphanAuthority(doc); err != nil {
		return err
	}
	leaseIDs := make([]string, 0, len(doc.Services))
	for leaseID := range doc.Services {
		leaseIDs = append(leaseIDs, leaseID)
	}
	sort.Strings(leaseIDs)
	for _, leaseID := range leaseIDs {
		record := doc.Services[leaseID]
		if err := validateServiceRecord(leaseID, record); err != nil {
			return err
		}
		lease, acquired, err := acquireFileLock(r.leasePath(leaseID), true)
		if err != nil {
			return err
		}
		if !acquired {
			continue
		}
		record, err = r.record(leaseID)
		if errors.Is(err, os.ErrNotExist) {
			_ = lease.Close()
			continue
		}
		retired := false
		if err == nil {
			retired, err = r.reconcileRecord(ctx, record, candidates[leaseID])
		}
		closeErr := lease.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if retired {
			if err := removeAuthorityFile(r.leasePath(leaseID), "service lease"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ServiceRegistry) registrySnapshot() (registryDocument, error) {
	var snapshot registryDocument
	err := r.readRegistry(func(doc registryDocument) error {
		snapshot = doc
		snapshot.Services = make(map[string]ServiceRecord, len(doc.Services))
		for leaseID, record := range doc.Services {
			snapshot.Services[leaseID] = record
		}
		return nil
	})
	return snapshot, err
}

func (r *ServiceRegistry) reconcileRecord(ctx context.Context, record ServiceRecord, candidates []dockerInspect) (bool, error) {
	leaseID := record.LeaseID
	if record.State == ServiceCreateSucceeded || record.State == ServiceCreateFailed {
		creator, free, err := acquireFileLock(r.creatorPath(leaseID), true)
		if err != nil {
			return false, err
		}
		if !free {
			return false, fmt.Errorf("Postgres service creator %s published %q but still owns terminal handoff", leaseID, record.State)
		}
		if err := creator.Close(); err != nil {
			return false, err
		}
	}
	switch record.State {
	case ServicePrepared:
		return true, r.retireRecordAuthority(record)
	case ServiceCreatorStarting:
		creator, free, err := acquireFileLock(r.creatorPath(leaseID), true)
		if err != nil {
			return false, err
		}
		if !free {
			return false, fmt.Errorf("Postgres service creator %s is still starting", leaseID)
		}
		if err := creator.Close(); err != nil {
			return false, err
		}
		return true, r.retireRecordAuthority(record)
	case ServiceCreating:
		creator, free, err := acquireFileLock(r.creatorPath(leaseID), true)
		if err != nil {
			return false, err
		}
		if !free {
			return false, fmt.Errorf("Postgres service creator %s is still in flight", leaseID)
		}
		if err := creator.Close(); err != nil {
			return false, err
		}
		return false, fmt.Errorf("Postgres service creator %s died without a terminal result; state retained and resources left untouched", leaseID)
	case ServiceCreateFailed:
		if record.ContainerID != "" || fileExists(record.CIDFile) || len(candidates) != 0 {
			return false, fmt.Errorf("failed Postgres creator %s has ambiguous container evidence; left untouched", leaseID)
		}
		return true, r.retireRecordAuthority(record)
	default:
		if record.ContainerID == "" {
			return false, fmt.Errorf("Postgres service %s state %q has no container ID", leaseID, record.State)
		}
		if record.State != ServiceTearingDown {
			if err := r.transition(leaseID, ServiceTearingDown); err != nil {
				return false, err
			}
		}
		_, exists, err := r.inspectExactMaybe(ctx, record)
		if err != nil {
			return false, err
		}
		if exists {
			if out, err := r.runDocker(ctx, "rm", "--force", record.ContainerID); err != nil {
				return false, fmt.Errorf("reconcile Postgres service %s: %v output=%s", record.ContainerID, err, strings.TrimSpace(string(out)))
			}
		}
		if err := r.verifyContainerAbsent(ctx, record.ContainerID); err != nil {
			return false, err
		}
		return true, r.retireRecordAuthority(record)
	}
}

func (r *ServiceRegistry) retireRecordAuthority(record ServiceRecord) error {
	if err := r.deleteRecord(record.LeaseID); err != nil {
		return fmt.Errorf("retire Postgres service registry record %s: %w", record.LeaseID, err)
	}
	if r.afterRecordDelete != nil {
		if err := r.afterRecordDelete(record); err != nil {
			return err
		}
	}
	return errors.Join(
		removeAuthorityFile(record.CIDFile, "container ID handoff"),
		removeAuthorityFile(r.creatorPath(record.LeaseID), "creator fence"),
	)
}

func (r *ServiceRegistry) reconcileOrphanAuthority(snapshot registryDocument) error {
	leaseIDs, err := r.authorityLeaseIDs()
	if err != nil {
		return err
	}
	for _, leaseID := range leaseIDs {
		if _, tracked := snapshot.Services[leaseID]; tracked {
			continue
		}
		lease, acquired, err := acquireFileLock(r.leasePath(leaseID), true)
		if err != nil {
			return err
		}
		if !acquired {
			continue
		}
		if _, err := r.record(leaseID); err == nil {
			_ = lease.Close()
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = lease.Close()
			return err
		}
		creator, free, err := acquireFileLock(r.creatorPath(leaseID), true)
		if err != nil {
			_ = lease.Close()
			return err
		}
		if !free {
			_ = lease.Close()
			return fmt.Errorf("rowless Postgres service creator %s is still active; authority retained", leaseID)
		}
		if err := creator.Close(); err != nil {
			_ = lease.Close()
			return err
		}
		if err := errors.Join(
			removeAuthorityFile(filepath.Join(r.StateRoot, "handoff", leaseID+".cid"), "orphan container ID handoff"),
			removeAuthorityFile(r.creatorPath(leaseID), "orphan creator fence"),
		); err != nil {
			_ = lease.Close()
			return err
		}
		if err := lease.Close(); err != nil {
			return err
		}
		if err := removeAuthorityFile(r.leasePath(leaseID), "orphan service lease"); err != nil {
			return err
		}
	}
	return nil
}

func (r *ServiceRegistry) authorityLeaseIDs() ([]string, error) {
	ids := make(map[string]struct{})
	for _, source := range []struct {
		dir    string
		suffix string
	}{
		{dir: "leases", suffix: ".lock"},
		{dir: "creators", suffix: ".lock"},
		{dir: "handoff", suffix: ".cid"},
	} {
		entries, err := os.ReadDir(filepath.Join(r.StateRoot, source.dir))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, source.suffix) || len(name) == len(source.suffix) {
				return nil, fmt.Errorf("invalid Postgres service authority entry %q in %s", name, source.dir)
			}
			ids[strings.TrimSuffix(name, source.suffix)] = struct{}{}
		}
	}
	values := make([]string, 0, len(ids))
	for id := range ids {
		values = append(values, id)
	}
	sort.Strings(values)
	return values, nil
}

func removeAuthorityFile(path, kind string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove Postgres %s authority %q: %w", kind, path, err)
	}
	return nil
}

func (r *ServiceRegistry) inspectExact(ctx context.Context, record ServiceRecord) (dockerInspect, error) {
	got, exists, err := r.inspectExactMaybe(ctx, record)
	if err != nil {
		return dockerInspect{}, err
	}
	if !exists {
		return dockerInspect{}, fmt.Errorf("Postgres service %s is absent; exact identity cannot be verified", record.ContainerID)
	}
	return got, nil
}

func (r *ServiceRegistry) inspectExactMaybe(ctx context.Context, record ServiceRecord) (dockerInspect, bool, error) {
	if err := validateCIDFile(record); err != nil {
		return dockerInspect{}, false, err
	}
	out, err := r.runDocker(ctx, "inspect", record.ContainerID)
	if err != nil {
		if dockerObjectAbsent(out) {
			return dockerInspect{}, false, nil
		}
		return dockerInspect{}, false, fmt.Errorf("inspect Postgres service %s: %w output=%s", record.ContainerID, err, strings.TrimSpace(string(out)))
	}
	var values []dockerInspect
	if err := json.Unmarshal(out, &values); err != nil || len(values) != 1 {
		return dockerInspect{}, false, fmt.Errorf("decode Docker inspect for %s: %w", record.ContainerID, err)
	}
	got := values[0]
	if got.ID != record.ContainerID || strings.TrimPrefix(got.Name, "/") != record.Name || got.Image != record.ImageID {
		return dockerInspect{}, false, fmt.Errorf("Postgres service %s identity mismatch; left untouched", record.ContainerID)
	}
	for key, want := range record.Labels {
		if got.Config.Labels[key] != want {
			return dockerInspect{}, false, fmt.Errorf("Postgres service %s label %s mismatch; left untouched", record.ContainerID, key)
		}
	}
	return got, true, nil
}

func (r *ServiceRegistry) verifyContainerAbsent(ctx context.Context, containerID string) error {
	out, err := r.runDocker(ctx, "inspect", containerID)
	if err != nil && dockerObjectAbsent(out) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify Postgres service %s absence: %w output=%s", containerID, err, strings.TrimSpace(string(out)))
	}
	return fmt.Errorf("Postgres service %s still exists after teardown; authority retained", containerID)
}

func dockerObjectAbsent(out []byte) bool {
	message := strings.ToLower(string(out))
	return strings.Contains(message, "no such object") || strings.Contains(message, "no such container")
}

func validateCIDFile(record ServiceRecord) error {
	if record.ContainerID == "" || record.CIDFileSHA256 == "" {
		return fmt.Errorf("Postgres service %s lacks durable cidfile identity; left untouched", record.LeaseID)
	}
	raw, err := os.ReadFile(record.CIDFile)
	if err != nil {
		return fmt.Errorf("read Postgres service %s cidfile: %w; left untouched", record.LeaseID, err)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != record.CIDFileSHA256 || strings.TrimSpace(string(raw)) != record.ContainerID {
		return fmt.Errorf("Postgres service %s cidfile identity mismatch; left untouched", record.LeaseID)
	}
	return nil
}

func (r *ServiceRegistry) initialize() error {
	if _, err := os.Lstat(r.StateRoot); err == nil {
		if err := validatePrivateStateRoot(r.StateRoot); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, dir := range []string{r.StateRoot, filepath.Join(r.StateRoot, "leases"), filepath.Join(r.StateRoot, "creators"), filepath.Join(r.StateRoot, "handoff")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return validatePrivateStateRoot(r.StateRoot)
}

func (r *ServiceRegistry) withRegistry(fn func(*registryDocument) error) error {
	lock, acquired, err := acquireFileLock(filepath.Join(r.StateRoot, "services-v1.lock"), false)
	if err != nil || !acquired {
		return fmt.Errorf("acquire service registry lock: %w", err)
	}
	defer lock.Close()
	doc, err := r.loadRegistry()
	if err != nil {
		return err
	}
	if err := fn(&doc); err != nil {
		return err
	}
	if r.beforeRegistrySave != nil {
		if err := r.beforeRegistrySave(doc); err != nil {
			return err
		}
	}
	return r.saveRegistry(doc)
}

func (r *ServiceRegistry) readRegistry(fn func(registryDocument) error) error {
	lock, acquired, err := acquireFileLock(filepath.Join(r.StateRoot, "services-v1.lock"), false)
	if err != nil || !acquired {
		return fmt.Errorf("acquire service registry lock: %w", err)
	}
	defer lock.Close()
	doc, err := r.loadRegistry()
	if err != nil {
		return err
	}
	return fn(doc)
}

func (r *ServiceRegistry) loadRegistry() (registryDocument, error) {
	path := filepath.Join(r.StateRoot, "services-v1.json")
	if err := validateExistingAuthorityFile(path); err != nil {
		return registryDocument{}, err
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return registryDocument{Version: serviceRegistryVersion, Services: make(map[string]ServiceRecord)}, nil
	}
	if err != nil {
		return registryDocument{}, err
	}
	var doc registryDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return registryDocument{}, fmt.Errorf("decode service registry: %w", err)
	}
	if doc.Version != serviceRegistryVersion || doc.Services == nil {
		return registryDocument{}, fmt.Errorf("unsupported service registry version %d", doc.Version)
	}
	for leaseID, record := range doc.Services {
		if err := validateServiceRecord(leaseID, record); err != nil {
			return registryDocument{}, err
		}
	}
	return doc, nil
}

func (r *ServiceRegistry) saveRegistry(doc registryDocument) error {
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(r.StateRoot, "services-v1.json")
	tmp := path + ".tmp-" + uuid.NewString()
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return validateExistingAuthorityFile(path)
}

func (r *ServiceRegistry) putRecord(record ServiceRecord) error {
	return r.withRegistry(func(doc *registryDocument) error { doc.Services[record.LeaseID] = record; return nil })
}

func (r *ServiceRegistry) record(leaseID string) (ServiceRecord, error) {
	var record ServiceRecord
	err := r.readRegistry(func(doc registryDocument) error {
		var ok bool
		record, ok = doc.Services[leaseID]
		if !ok {
			return os.ErrNotExist
		}
		return nil
	})
	return record, err
}

func (r *ServiceRegistry) transition(leaseID string, state ServiceState) error {
	return r.withRegistry(func(doc *registryDocument) error {
		record, ok := doc.Services[leaseID]
		if !ok {
			return os.ErrNotExist
		}
		if !validServiceTransition(record.State, state) {
			return fmt.Errorf("invalid Postgres service transition %q -> %q", record.State, state)
		}
		record.State = state
		doc.Services[leaseID] = record
		return nil
	})
}

func validServiceTransition(from, to ServiceState) bool {
	if to == ServiceTearingDown {
		return from != ServiceCreating && from != ServiceTearingDown
	}
	switch from {
	case ServicePrepared:
		return to == ServiceCreatorStarting
	case ServiceCreatorStarting:
		return to == ServiceCreating
	case ServiceCreateSucceeded:
		return to == ServiceStarting
	case ServiceStarting:
		return to == ServiceReady
	case ServiceReady:
		return to == ServiceChildRunning
	default:
		return false
	}
}

func (r *ServiceRegistry) deleteRecord(leaseID string) error {
	return r.withRegistry(func(doc *registryDocument) error { delete(doc.Services, leaseID); return nil })
}

func (r *ServiceRegistry) ownerID() (string, error) {
	path := filepath.Join(r.StateRoot, "owner-id")
	for {
		if err := validateExistingAuthorityFile(path); err != nil {
			return "", err
		}
		if raw, err := os.ReadFile(path); err == nil {
			value := strings.TrimSpace(string(raw))
			if _, err := uuid.Parse(value); err != nil {
				return "", fmt.Errorf("invalid service owner ID: %w", err)
			}
			return value, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		value := uuid.NewString()
		tmp := filepath.Join(r.StateRoot, ".owner-id-"+uuid.NewString()+".tmp")
		file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return "", err
		}
		_, writeErr := file.WriteString(value + "\n")
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil {
			_ = os.Remove(tmp)
			return "", writeErr
		}
		if syncErr != nil {
			_ = os.Remove(tmp)
			return "", syncErr
		}
		if closeErr != nil {
			_ = os.Remove(tmp)
			return "", closeErr
		}
		linkErr := os.Link(tmp, path)
		_ = os.Remove(tmp)
		if os.IsExist(linkErr) {
			continue
		}
		if linkErr != nil {
			return "", fmt.Errorf("publish service owner ID: %w", linkErr)
		}
		if err := validateExistingAuthorityFile(path); err != nil {
			return "", err
		}
		return value, nil
	}
}

func (r *ServiceRegistry) dockerOutput(ctx context.Context, args ...string) (string, error) {
	out, err := r.runDocker(ctx, args...)
	if err != nil {
		return "", fmt.Errorf("docker %s: %v output=%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func (r *ServiceRegistry) runDocker(ctx context.Context, args ...string) ([]byte, error) {
	if r.docker == nil {
		r.docker = commandDocker{bin: r.DockerBin}
	}
	return r.docker.CombinedOutput(ctx, args...)
}

func (r *ServiceRegistry) managedContainers(ctx context.Context) (map[string][]dockerInspect, error) {
	nameOut, err := r.runDocker(ctx, "ps", "--all", "--format", "{{.ID}} {{.Names}}")
	if err != nil {
		return nil, fmt.Errorf("enumerate canonical Postgres service names: %w output=%s", err, strings.TrimSpace(string(nameOut)))
	}
	labelOut, err := r.runDocker(ctx, "ps", "--all", "--filter", "label=com.division.swarm.test-postgres.managed=1", "--format", "{{.ID}}")
	if err != nil {
		return nil, fmt.Errorf("enumerate managed Postgres services by label: %w output=%s", err, strings.TrimSpace(string(labelOut)))
	}
	ids := make(map[string]struct{})
	for _, line := range strings.Split(string(nameOut), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.HasPrefix(fields[1], "swarm-test-postgres-v1-") {
			ids[fields[0]] = struct{}{}
		}
	}
	for _, id := range strings.Fields(string(labelOut)) {
		ids[id] = struct{}{}
	}
	result := make(map[string][]dockerInspect)
	seenFullIDs := make(map[string]bool)
	for id := range ids {
		inspectOut, err := r.runDocker(ctx, "inspect", id)
		if err != nil {
			return nil, fmt.Errorf("inspect managed Postgres service %s: %w", id, err)
		}
		var values []dockerInspect
		if err := json.Unmarshal(inspectOut, &values); err != nil || len(values) != 1 {
			return nil, fmt.Errorf("decode managed Postgres service %s inspection", id)
		}
		got := values[0]
		if seenFullIDs[got.ID] {
			continue
		}
		seenFullIDs[got.ID] = true
		labels := got.Config.Labels
		if strings.TrimPrefix(got.Name, "/") == "" || !strings.HasPrefix(strings.TrimPrefix(got.Name, "/"), "swarm-test-postgres-v1-") {
			return nil, fmt.Errorf("managed Postgres service %s has non-canonical name %q; left untouched", got.ID, got.Name)
		}
		if labels["com.division.swarm.test-postgres.managed"] != "1" || labels["com.division.swarm.test-postgres.contract"] != "v1" {
			return nil, fmt.Errorf("canonical Postgres service %s has missing or invalid management labels; left untouched", got.ID)
		}
		for _, key := range []string{"owner-id", "daemon-id", "runner-id", "lease-id", "spec-sha256"} {
			if labels["com.division.swarm.test-postgres."+key] == "" {
				return nil, fmt.Errorf("managed Postgres service %s has no %s label; left untouched", got.ID, key)
			}
		}
		runnerID := labels["com.division.swarm.test-postgres.runner-id"]
		if strings.TrimPrefix(got.Name, "/") != "swarm-test-postgres-v1-"+runnerID {
			return nil, fmt.Errorf("managed Postgres service %s name does not match its runner label; left untouched", got.ID)
		}
		leaseID := labels["com.division.swarm.test-postgres.lease-id"]
		result[leaseID] = append(result[leaseID], got)
	}
	return result, nil
}

func validateManagedNamespace(doc *registryDocument, candidates map[string][]dockerInspect) error {
	for leaseID, values := range candidates {
		record, ok := doc.Services[leaseID]
		if !ok {
			return fmt.Errorf("managed Postgres service lease %s has no registry row; %d resource(s) left untouched", leaseID, len(values))
		}
		if len(values) != 1 {
			return fmt.Errorf("managed Postgres service lease %s has %d candidates; all left untouched", leaseID, len(values))
		}
		got := values[0]
		if record.ContainerID == "" || got.ID != record.ContainerID {
			return fmt.Errorf("managed Postgres service lease %s does not match its registry container; left untouched", leaseID)
		}
	}
	return nil
}

func validateServiceRecord(key string, record ServiceRecord) error {
	if record.Version != serviceRegistryVersion || record.LeaseID == "" || key != record.LeaseID {
		return fmt.Errorf("invalid Postgres service registry identity for row %q", key)
	}
	if !knownServiceState(record.State) {
		return fmt.Errorf("unknown Postgres service state %q for lease %s", record.State, key)
	}
	if record.OwnerID == "" || record.DaemonID == "" || record.ImageID == "" || record.RunnerID == "" || record.Name == "" || record.CIDFile == "" || record.SpecHash == "" {
		return fmt.Errorf("incomplete Postgres service registry row %s", key)
	}
	wantLabels := serviceLabels(record.OwnerID, record.DaemonID, record.RunnerID, record.LeaseID, record.SpecHash)
	for label, want := range wantLabels {
		if record.Labels[label] != want {
			return fmt.Errorf("Postgres service registry row %s label %s is invalid", key, label)
		}
	}
	return nil
}

func knownServiceState(state ServiceState) bool {
	switch state {
	case ServicePrepared, ServiceCreatorStarting, ServiceCreating, ServiceCreateSucceeded, ServiceCreateFailed, ServiceStarting, ServiceReady, ServiceChildRunning, ServiceTearingDown:
		return true
	default:
		return false
	}
}

func (r *ServiceRegistry) validateCreatorFence(record ServiceRecord, inherited *os.File) error {
	passed, err := inherited.Stat()
	if err != nil {
		return fmt.Errorf("stat inherited creator fence: %w", err)
	}
	expected, err := os.Stat(r.creatorPath(record.LeaseID))
	if err != nil {
		return fmt.Errorf("stat expected creator fence: %w", err)
	}
	if !os.SameFile(passed, expected) {
		return fmt.Errorf("inherited creator fence does not match lease %s", record.LeaseID)
	}
	probe, acquired, err := acquireFileLock(r.creatorPath(record.LeaseID), true)
	if err != nil {
		return err
	}
	if acquired {
		_ = probe.Close()
		return fmt.Errorf("inherited creator fence is not held")
	}
	return nil
}

func (r *ServiceRegistry) leasePath(id string) string {
	return filepath.Join(r.StateRoot, "leases", id+".lock")
}
func (r *ServiceRegistry) creatorPath(id string) string {
	return filepath.Join(r.StateRoot, "creators", id+".lock")
}

func serviceLabels(owner, daemon, runnerID, leaseID, specHash string) map[string]string {
	return map[string]string{
		"com.division.swarm.test-postgres.managed":     "1",
		"com.division.swarm.test-postgres.contract":    "v1",
		"com.division.swarm.test-postgres.owner-id":    owner,
		"com.division.swarm.test-postgres.daemon-id":   daemon,
		"com.division.swarm.test-postgres.runner-id":   runnerID,
		"com.division.swarm.test-postgres.lease-id":    leaseID,
		"com.division.swarm.test-postgres.spec-sha256": specHash,
	}
}

func serviceSpecHash(imageID string) string {
	hash := sha256.Sum256([]byte(strings.Join([]string{imageID, "postgres:16", "tmpfs-pgdata", "random-loopback-port", "max_connections=300", "fsync=off", "synchronous_commit=off", "full_page_writes=off"}, "\x00")))
	return hex.EncodeToString(hash[:])
}

func parseDockerPort(value string) (uint16, error) {
	line := strings.TrimSpace(value)
	if index := strings.LastIndex(line, "\n"); index >= 0 {
		line = strings.TrimSpace(line[index+1:])
	}
	index := strings.LastIndex(line, ":")
	if index < 0 {
		return 0, fmt.Errorf("unexpected Docker port output %q", value)
	}
	port, err := strconv.ParseUint(strings.TrimSpace(line[index+1:]), 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("invalid Docker port output %q", value)
	}
	return uint16(port), nil
}

func verifyOwnedPostgresSettings(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	for setting, want := range map[string]string{
		"max_connections":    "300",
		"fsync":              "off",
		"synchronous_commit": "off",
		"full_page_writes":   "off",
	} {
		var got string
		if err := db.QueryRowContext(ctx, `SELECT current_setting($1)`, setting).Scan(&got); err != nil {
			return fmt.Errorf("verify runner-owned Postgres setting %s: %w", setting, err)
		}
		if got != want {
			return fmt.Errorf("runner-owned Postgres setting %s=%q, want %q", setting, got, want)
		}
	}
	return nil
}

func defaultServiceStateRoot() (string, error) {
	if root := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); root != "" {
		return filepath.Join(root, "swarm", "test-postgres"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "swarm", "test-postgres"), nil
	}
	return filepath.Join(home, ".local", "state", "swarm", "test-postgres"), nil
}

func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
