package devscratch

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	storeRelativePath = ".swarm/stores/dev-scratch.db"
	authorityFileName = "dev-scratch.epoch.lock"
	diagnosticVersion = 1
)

var predecessorSuffixes = []string{"", "-wal", "-shm", "-journal"}

// Coordinate is the canonical project-local dev scratch ownership coordinate.
type Coordinate struct {
	ProjectRoot   string
	DatabasePath  string
	AuthorityPath string
}

// Resolve derives the one scratch coordinate owned by a canonical project.
func Resolve(projectRoot string) (Coordinate, error) {
	projectRoot = strings.TrimSpace(projectRoot)
	if projectRoot == "" {
		return Coordinate{}, errors.New("dev scratch requires a canonical project root")
	}
	abs, err := filepath.Abs(filepath.Clean(projectRoot))
	if err != nil {
		return Coordinate{}, fmt.Errorf("resolve dev scratch project root: %w", err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return Coordinate{}, fmt.Errorf("resolve dev scratch project identity: %w", err)
	}
	real = filepath.Clean(real)
	info, err := os.Stat(real)
	if err != nil {
		return Coordinate{}, fmt.Errorf("stat dev scratch project root: %w", err)
	}
	if !info.IsDir() {
		return Coordinate{}, errors.New("dev scratch project root must be a directory")
	}
	databasePath := filepath.Join(real, filepath.FromSlash(storeRelativePath))
	return Coordinate{
		ProjectRoot:   real,
		DatabasePath:  databasePath,
		AuthorityPath: filepath.Join(filepath.Dir(databasePath), authorityFileName),
	}, nil
}

type authorityState uint8

const (
	authorityAcquired authorityState = iota
	authorityPrepared
	authorityBound
	authorityReleased
)

// EpochAuthority owns replacement authority before the SQLite store opens.
// BindOpenedStore consumes it into StoreEpoch, whose only release operation is
// explicitly ordered after selected-store close.
type EpochAuthority struct {
	mu         sync.Mutex
	coordinate Coordinate
	epochID    string
	lock       platformLock
	state      authorityState
}

// RegistrationGrant proves that descriptor reconciliation is running under
// the retained canonical project epoch rather than descriptor liveness guesses.
type RegistrationGrant struct {
	projectRoot string
	epochID     string
}

// storeEpoch is the retained epoch lock after selected-store construction.
// It is private so callers cannot release an opened epoch independently of
// the selected-store lifecycle.
type storeEpoch struct {
	mu         sync.Mutex
	coordinate Coordinate
	epochID    string
	lock       platformLock
	released   bool
}

type diagnostic struct {
	Version     int    `json:"version"`
	EpochID     string `json:"epoch_id"`
	ProjectRoot string `json:"project_root"`
	Database    string `json:"database_path"`
	PID         int    `json:"pid"`
	AcquiredAt  string `json:"acquired_at"`
}

// Acquire retains the nonblocking kernel authority lock. It does not replace
// predecessor files until PrepareFreshStore is called.
func Acquire(coordinate Coordinate) (*EpochAuthority, error) {
	canonical, err := Resolve(coordinate.ProjectRoot)
	if err != nil {
		return nil, err
	}
	if filepath.Clean(coordinate.DatabasePath) != canonical.DatabasePath || filepath.Clean(coordinate.AuthorityPath) != canonical.AuthorityPath {
		return nil, errors.New("dev scratch coordinate does not match its canonical project owner")
	}
	if err := establishStateDirectory(canonical); err != nil {
		return nil, err
	}
	lock, err := acquirePlatformLock(canonical.AuthorityPath)
	if err != nil {
		return nil, err
	}
	epochID, err := newEpochID()
	if err != nil {
		return nil, errors.Join(err, lock.release())
	}
	metadata, err := json.Marshal(diagnostic{
		Version: diagnosticVersion, EpochID: epochID, ProjectRoot: canonical.ProjectRoot,
		Database: canonical.DatabasePath, PID: os.Getpid(), AcquiredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, errors.Join(err, lock.release())
	}
	if err := lock.rewrite(append(metadata, '\n')); err != nil {
		return nil, errors.Join(fmt.Errorf("write dev scratch epoch diagnostic: %w", err), lock.release())
	}
	return &EpochAuthority{coordinate: canonical, epochID: epochID, lock: lock, state: authorityAcquired}, nil
}

type directoryIdentity struct {
	path string
	info os.FileInfo
}

func establishStateDirectory(coordinate Coordinate) error {
	project, err := inspectCanonicalDirectory(coordinate.ProjectRoot, "project root")
	if err != nil {
		return err
	}
	statePath := filepath.Join(coordinate.ProjectRoot, ".swarm")
	state, err := ensureCanonicalDirectory(statePath, "state directory")
	if err != nil {
		return err
	}
	if err := revalidateDirectoryIdentities(project, state); err != nil {
		return err
	}
	storesPath := filepath.Dir(coordinate.DatabasePath)
	stores, err := ensureCanonicalDirectory(storesPath, "store directory")
	if err != nil {
		return err
	}
	return revalidateDirectoryIdentities(project, state, stores)
}

func ensureCanonicalDirectory(path, label string) (directoryIdentity, error) {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0o700); err != nil {
			return directoryIdentity{}, fmt.Errorf("create dev scratch %s: %w", label, err)
		}
	} else if err != nil {
		return directoryIdentity{}, fmt.Errorf("inspect dev scratch %s: %w", label, err)
	}
	return inspectCanonicalDirectory(path, label)
}

func inspectCanonicalDirectory(path, label string) (directoryIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return directoryIdentity{}, fmt.Errorf("inspect dev scratch %s: %w", label, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return directoryIdentity{}, fmt.Errorf("dev scratch %s must be an unaliased directory", label)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return directoryIdentity{}, fmt.Errorf("resolve dev scratch %s identity: %w", label, err)
	}
	if filepath.Clean(resolved) != filepath.Clean(path) {
		return directoryIdentity{}, fmt.Errorf("dev scratch %s aliases are not ownership authority", label)
	}
	return directoryIdentity{path: path, info: info}, nil
}

func revalidateDirectoryIdentities(identities ...directoryIdentity) error {
	for _, identity := range identities {
		current, err := inspectCanonicalDirectory(identity.path, "state ancestor")
		if err != nil {
			return err
		}
		if !os.SameFile(identity.info, current.info) {
			return fmt.Errorf("dev scratch state ancestor %s changed during acquisition", identity.path)
		}
	}
	return nil
}

// PrepareFreshStore validates every predecessor artifact before removing any
// of them, then removes the complete database epoch while retaining the lock.
func (a *EpochAuthority) PrepareFreshStore() error {
	if a == nil {
		return errors.New("dev scratch epoch authority is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != authorityAcquired {
		return errors.New("dev scratch replacement requires a newly acquired epoch")
	}
	paths := predecessorPaths(a.coordinate.DatabasePath)
	for _, path := range paths {
		if err := validatePredecessorArtifact(path); err != nil {
			return err
		}
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove dev scratch predecessor artifact %s: %w", path, err)
		}
	}
	a.state = authorityPrepared
	return nil
}

// AbortBeforeStoreOpen releases an epoch that never opened a selected store.
func (a *EpochAuthority) AbortBeforeStoreOpen() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == authorityBound {
		return errors.New("opened dev scratch epoch must be released after selected-store close")
	}
	if a.state == authorityReleased {
		return nil
	}
	a.state = authorityReleased
	err := a.lock.release()
	a.lock = nil
	return err
}

// BindOpenedStore transfers the retained lock into the selected-store lifetime.
func (a *EpochAuthority) bindOpenedStore() (*storeEpoch, error) {
	if a == nil {
		return nil, errors.New("dev scratch epoch authority is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != authorityPrepared || a.lock == nil {
		return nil, errors.New("dev scratch epoch must replace predecessor state before store binding")
	}
	bound := &storeEpoch{coordinate: a.coordinate, epochID: a.epochID, lock: a.lock}
	a.lock = nil
	a.state = authorityBound
	return bound, nil
}

func (a *EpochAuthority) Coordinate() Coordinate {
	if a == nil {
		return Coordinate{}
	}
	return a.coordinate
}

func (a *EpochAuthority) EpochID() string {
	if a == nil {
		return ""
	}
	return a.epochID
}

func (a *EpochAuthority) RegistrationGrant() (RegistrationGrant, error) {
	if a == nil {
		return RegistrationGrant{}, errors.New("dev scratch epoch authority is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != authorityAcquired || a.lock == nil {
		return RegistrationGrant{}, errors.New("dev scratch descriptor registration requires a retained pre-replacement epoch")
	}
	return RegistrationGrant{projectRoot: a.coordinate.ProjectRoot, epochID: a.epochID}, nil
}

func (g RegistrationGrant) ValidateProject(projectRoot string) error {
	if strings.TrimSpace(g.epochID) == "" || strings.TrimSpace(g.projectRoot) == "" {
		return errors.New("dev scratch descriptor registration grant is required")
	}
	resolved, err := Resolve(projectRoot)
	if err != nil {
		return err
	}
	if resolved.ProjectRoot != g.projectRoot {
		return errors.New("dev scratch descriptor registration grant belongs to another canonical project")
	}
	return nil
}

// ReleaseAfterStoreClose releases the coordinate only after its selected
// store owner reports a successful close.
func (e *storeEpoch) releaseAfterStoreClose() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.released {
		return nil
	}
	e.released = true
	err := e.lock.release()
	e.lock = nil
	return err
}

func (e *storeEpoch) coordinateValue() Coordinate {
	if e == nil {
		return Coordinate{}
	}
	return e.coordinate
}

func predecessorPaths(databasePath string) []string {
	paths := make([]string, 0, len(predecessorSuffixes))
	for _, suffix := range predecessorSuffixes {
		paths = append(paths, databasePath+suffix)
	}
	return paths
}

func validatePredecessorArtifact(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect dev scratch predecessor artifact %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || !isSingleLink(info) {
		return fmt.Errorf("dev scratch predecessor artifact %s is not one unaliased regular file", path)
	}
	return nil
}

func newEpochID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate dev scratch epoch id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
