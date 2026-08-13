package credentials

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Store interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string) error
	List(ctx context.Context) ([]string, error)
	Delete(ctx context.Context, key string) error
}

type Inspector interface {
	Store
	Inspect(ctx context.Context, key string) (Metadata, error)
}

// AtomicSnapshot is one observation of a credential value and its metadata.
// The value is deliberately private to process memory and has no JSON/YAML
// representation.
type AtomicSnapshot struct {
	Key       string
	Present   bool
	Source    string
	Writable  bool
	Shadowed  bool
	UpdatedAt *time.Time
	value     string
}

// Snapshotter is the strict credential boundary used when a value and its
// currentness metadata must describe the same store read.
type Snapshotter interface {
	Snapshot(context.Context, string) (AtomicSnapshot, error)
}

func (s AtomicSnapshot) Metadata() Metadata {
	return Metadata{Key: s.Key, Present: s.Present, Source: s.Source, Writable: s.Writable, Shadowed: s.Shadowed, UpdatedAt: timePtrValue(s.UpdatedAt)}
}

func (s AtomicSnapshot) CredentialValue() string { return s.value }

// AdmittedSnapshot adds a process-private epoch. The epoch changes whenever
// the observed value or metadata changes and is safe to include in redacted
// registration identity; it is not derived from secret bytes.
type AdmittedSnapshot struct {
	AtomicSnapshot
	epoch string
}

func (s AdmittedSnapshot) Epoch() string { return s.epoch }

type SnapshotOwner struct {
	store Snapshotter
	mu    sync.Mutex
	seen  map[string]snapshotObservation
}

type snapshotObservation struct {
	snapshot AtomicSnapshot
	epoch    string
}

func NewSnapshotOwner(store Store) (*SnapshotOwner, error) {
	snapshotter, ok := store.(Snapshotter)
	if !ok || snapshotter == nil {
		return nil, fmt.Errorf("credential store does not provide atomic snapshots")
	}
	return &SnapshotOwner{store: snapshotter, seen: map[string]snapshotObservation{}}, nil
}

func (o *SnapshotOwner) Observe(ctx context.Context, key string) (AdmittedSnapshot, error) {
	if o == nil || o.store == nil {
		return AdmittedSnapshot{}, fmt.Errorf("credential snapshot owner is required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return AdmittedSnapshot{}, fmt.Errorf("credential snapshot key is required")
	}
	snapshot, err := o.store.Snapshot(ctx, key)
	if err != nil {
		return AdmittedSnapshot{}, err
	}
	snapshot = cloneAtomicSnapshot(snapshot)
	o.mu.Lock()
	defer o.mu.Unlock()
	observation, exists := o.seen[key]
	if !exists || !sameAtomicSnapshot(observation.snapshot, snapshot) {
		observation = snapshotObservation{snapshot: cloneAtomicSnapshot(snapshot), epoch: uuid.NewString()}
		o.seen[key] = observation
	}
	return AdmittedSnapshot{AtomicSnapshot: cloneAtomicSnapshot(snapshot), epoch: observation.epoch}, nil
}

func cloneAtomicSnapshot(snapshot AtomicSnapshot) AtomicSnapshot {
	snapshot.UpdatedAt = timePtrValue(snapshot.UpdatedAt)
	return snapshot
}

func sameAtomicSnapshot(left, right AtomicSnapshot) bool {
	leftMeta, _ := json.Marshal(left.Metadata())
	rightMeta, _ := json.Marshal(right.Metadata())
	return subtle.ConstantTimeCompare([]byte(left.value), []byte(right.value)) == 1 &&
		subtle.ConstantTimeCompare(leftMeta, rightMeta) == 1
}

func timePtrValue(value *time.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	copy := value.UTC()
	return &copy
}

type Metadata struct {
	Key       string
	Present   bool
	Source    string
	Writable  bool
	Shadowed  bool
	UpdatedAt *time.Time
}

const (
	SourceEnv  = "env"
	SourceFile = "file"
)

type EnvStore struct{}

func NewEnvStore() Store {
	return EnvStore{}
}

func (EnvStore) Get(_ context.Context, key string) (string, bool, error) {
	for _, candidate := range credentialEnvCandidates(key) {
		value, ok := os.LookupEnv(candidate)
		if !ok {
			continue
		}
		if strings.HasPrefix(candidate, "SWARM_") {
			return "", false, fmt.Errorf("credential env %s is not accepted through dynamic credential lookup; declare a typed source or store the secret with swarm secrets", candidate)
		}
		return value, true, nil
	}
	return "", false, nil
}

func (EnvStore) Set(_ context.Context, _, _ string) error {
	return fmt.Errorf("env credential store is read-only")
}

func (EnvStore) List(_ context.Context) ([]string, error) {
	return nil, nil
}

func (EnvStore) Delete(_ context.Context, _ string) error {
	return fmt.Errorf("env credential store is read-only")
}

func (EnvStore) Inspect(ctx context.Context, key string) (Metadata, error) {
	snapshot, err := EnvStore{}.Snapshot(ctx, key)
	if err != nil {
		return Metadata{}, err
	}
	return snapshot.Metadata(), nil
}

func (EnvStore) Snapshot(ctx context.Context, key string) (AtomicSnapshot, error) {
	value, ok, err := EnvStore{}.Get(ctx, key)
	if err != nil {
		return AtomicSnapshot{}, err
	}
	snapshot := AtomicSnapshot{Key: strings.TrimSpace(key), Present: ok}
	if ok {
		snapshot.Source = SourceEnv
		snapshot.value = value
	}
	return snapshot, nil
}

func credentialEnvCandidates(key string) []string {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	normalized := strings.NewReplacer(".", "_", "-", "_", " ", "_").Replace(key)
	upper := strings.ToUpper(normalized)
	candidates := []string{key}
	if normalized != key {
		candidates = append(candidates, normalized)
	}
	if upper != normalized {
		candidates = append(candidates, upper)
	}
	return dedupeStrings(candidates)
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
