package credentials

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"sort"
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

type WriteReceipt struct {
	Key       string
	Receipt   string
	Epoch     string
	UpdatedAt time.Time
}

type ReceiptWriter interface {
	Store
	AdmitWithReceipt(context.Context, string, string, string) (WriteReceipt, error)
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
	// occurrenceEpoch is store-owned, non-secret currentness metadata. It is
	// deliberately excluded from JSON and public credential metadata.
	occurrenceEpoch string
}

// Snapshotter is the strict credential boundary used when a value and its
// currentness metadata must describe the same store read.
type Snapshotter interface {
	Snapshot(context.Context, string) (AtomicSnapshot, error)
}

// NewAtomicSnapshot lets Store implementations outside this package satisfy
// Snapshotter without exposing the credential value as a struct field.
func NewAtomicSnapshot(metadata Metadata, value string) AtomicSnapshot {
	return AtomicSnapshot{
		Key: strings.TrimSpace(metadata.Key), Present: metadata.Present, Source: strings.TrimSpace(metadata.Source),
		Writable: metadata.Writable, Shadowed: metadata.Shadowed, UpdatedAt: timePtrValue(metadata.UpdatedAt), value: value,
	}
}

// NewAtomicSnapshotWithOccurrence is for credential stores that persist an
// opaque occurrence identity alongside the credential value.
func NewAtomicSnapshotWithOccurrence(metadata Metadata, value, occurrenceEpoch string) AtomicSnapshot {
	snapshot := NewAtomicSnapshot(metadata, value)
	snapshot.occurrenceEpoch = strings.TrimSpace(occurrenceEpoch)
	return snapshot
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

type SecretBindingStatus string

const (
	SecretBindingBound   SecretBindingStatus = "BOUND"
	SecretBindingUnbound SecretBindingStatus = "UNBOUND"
)

// SecretBinding is the one exact-key usability projection used by ingress
// readback and authenticated execution. Its admitted snapshot remains private
// to process memory and has no serialization surface.
type SecretBinding struct {
	status   SecretBindingStatus
	snapshot AdmittedSnapshot
}

func (b SecretBinding) Status() SecretBindingStatus { return b.status }
func (b SecretBinding) Bound() bool                 { return b.status == SecretBindingBound }
func (b SecretBinding) Epoch() string               { return b.snapshot.Epoch() }

func (b SecretBinding) CredentialValue() string {
	if !b.Bound() {
		return ""
	}
	return b.snapshot.CredentialValue()
}

func (b SecretBinding) AdmittedSnapshot() (AdmittedSnapshot, error) {
	if !b.Bound() {
		return AdmittedSnapshot{}, fmt.Errorf("credential %q is unbound", b.snapshot.Key)
	}
	return b.snapshot, nil
}

type SecretBindingObservationError struct {
	Key string
	Err error
}

func (e *SecretBindingObservationError) Error() string {
	return fmt.Sprintf("observe credential binding %q: %v", e.Key, e.Err)
}

func (e *SecretBindingObservationError) Unwrap() error { return e.Err }

type SecretBindingProjectionStaleError struct {
	Key string
}

func (e *SecretBindingProjectionStaleError) Error() string {
	return fmt.Sprintf("credential binding projection became stale while %q was observed", e.Key)
}

// SecretBindingProjection is one readback-scoped observation session. Each
// exact key is observed once regardless of how many targets consume it, then
// revalidated before the caller publishes the projection.
type SecretBindingProjection struct {
	owner    *SnapshotOwner
	bindings map[string]SecretBinding
}

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
		epoch := strings.TrimSpace(snapshot.occurrenceEpoch)
		if epoch == "" {
			epoch = uuid.NewString()
		}
		observation = snapshotObservation{snapshot: cloneAtomicSnapshot(snapshot), epoch: epoch}
		o.seen[key] = observation
	}
	return AdmittedSnapshot{AtomicSnapshot: cloneAtomicSnapshot(snapshot), epoch: observation.epoch}, nil
}

func (o *SnapshotOwner) ObserveSecretBinding(ctx context.Context, key string) (SecretBinding, error) {
	key = strings.TrimSpace(key)
	snapshot, err := o.Observe(ctx, key)
	if err != nil {
		return SecretBinding{}, &SecretBindingObservationError{Key: key, Err: err}
	}
	status := SecretBindingUnbound
	if snapshot.Present && strings.TrimSpace(snapshot.CredentialValue()) != "" {
		status = SecretBindingBound
	}
	return SecretBinding{status: status, snapshot: snapshot}, nil
}

func (o *SnapshotOwner) BeginSecretBindingProjection() *SecretBindingProjection {
	return &SecretBindingProjection{owner: o, bindings: map[string]SecretBinding{}}
}

func (p *SecretBindingProjection) ObserveSecretBinding(ctx context.Context, key string) (SecretBinding, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return SecretBinding{}, fmt.Errorf("credential snapshot key is required")
	}
	if p == nil || p.owner == nil {
		return SecretBinding{}, fmt.Errorf("credential snapshot owner is required")
	}
	if p.bindings == nil {
		p.bindings = map[string]SecretBinding{}
	}
	if binding, ok := p.bindings[key]; ok {
		return binding, nil
	}
	binding, err := p.owner.ObserveSecretBinding(ctx, key)
	if err != nil {
		return SecretBinding{}, err
	}
	p.bindings[key] = binding
	return binding, nil
}

func (p *SecretBindingProjection) ValidateCurrent(ctx context.Context) error {
	if p == nil || len(p.bindings) == 0 {
		return nil
	}
	keys := make([]string, 0, len(p.bindings))
	for key := range p.bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		current, err := p.owner.ObserveSecretBinding(ctx, key)
		if err != nil {
			return err
		}
		if current.Epoch() != p.bindings[key].Epoch() {
			return &SecretBindingProjectionStaleError{Key: key}
		}
	}
	return nil
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
