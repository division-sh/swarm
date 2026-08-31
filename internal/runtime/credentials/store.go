package credentials

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	UpdatedAt time.Time
}

type ReceiptWriter interface {
	Store
	AdmitWithReceipt(context.Context, string, string, string) (WriteReceipt, error)
}

// ReceiptObserver reports whether one exact receipt still owns the current
// writable occurrence without exposing the credential value.
type ReceiptObserver interface {
	Store
	ObserveReceipt(context.Context, string, string) (WriteReceipt, bool, error)
}

// ReceiptDeleter removes only the exact writable occurrence created by a
// receipt-bearing admission. A stale receipt can never delete its successor.
type ReceiptDeleter interface {
	Store
	DeleteWithReceipt(context.Context, string, string) (bool, error)
}

const valueSealPrefix = "credential-value-seal-v1:"

var ErrValueSealKeyUnavailable = errors.New("credential value seal key is unavailable")

// ValueSeal is opaque, non-secret evidence that one exact credential key had
// one exact value when a consumer admitted it.
type ValueSeal string

func ParseValueSeal(raw string) (ValueSeal, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, valueSealPrefix) {
		return "", fmt.Errorf("invalid credential value seal")
	}
	digest := strings.TrimPrefix(raw, valueSealPrefix)
	if len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("invalid credential value seal")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("invalid credential value seal")
	}
	return ValueSeal(raw), nil
}

func (s ValueSeal) String() string { return string(s) }

// ValueEvidence is the typed durable currentness evidence consumed by channel
// admissions and provider identity. Public DTOs must not serialize it.
type ValueEvidence struct {
	Key  string    `json:"-"`
	Seal ValueSeal `json:"-"`
}

func (e ValueEvidence) Validate() error {
	if strings.TrimSpace(e.Key) == "" {
		return fmt.Errorf("credential value evidence key is required")
	}
	_, err := ParseValueSeal(e.Seal.String())
	return err
}

type valueSealStore interface {
	hasDurableValueSealKeyHome() bool
	sealCurrentValue(context.Context, string) (ValueEvidence, error)
	currentValueMatchesSeal(context.Context, ValueEvidence) (bool, error)
}

type valueSealKeyHome interface {
	sealExactValue(context.Context, string, string) (ValueSeal, error)
	matchExactValue(context.Context, string, string, ValueSeal) (bool, error)
}

// RequireDurableValueSealKeyHome performs a mutation-free capability check for
// durable credential admission. It never creates a seal key.
func RequireDurableValueSealKeyHome(store Store) error {
	owner, ok := store.(valueSealStore)
	if !ok || owner == nil || !owner.hasDurableValueSealKeyHome() {
		return fmt.Errorf("%w: configure a writable credential file tier, then run swarm channel connect <provider> --credential-stdin", ErrValueSealKeyUnavailable)
	}
	return nil
}

func SealCurrentValue(ctx context.Context, store Store, key string) (ValueEvidence, error) {
	if err := RequireDurableValueSealKeyHome(store); err != nil {
		return ValueEvidence{}, err
	}
	owner := store.(valueSealStore)
	return owner.sealCurrentValue(ctx, key)
}

func CurrentValueMatchesSeal(ctx context.Context, store Store, evidence ValueEvidence) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	owner, ok := store.(valueSealStore)
	if !ok || owner == nil {
		return false, fmt.Errorf("%w: configure a writable credential file tier before validating durable channel credentials", ErrValueSealKeyUnavailable)
	}
	return owner.currentValueMatchesSeal(ctx, evidence)
}

func credentialValueSeal(key []byte, storeKey, value string) ValueSeal {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("swarm/credential-currentness/value-seal/v1"))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(storeKey)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write([]byte(storeKey))
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write([]byte(value))
	return ValueSeal(valueSealPrefix + hex.EncodeToString(mac.Sum(nil)))
}

func newValueSealKey() (string, error) {
	key := make([]byte, sha256.Size)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("create credential value seal key: %w", err)
	}
	return base64.RawStdEncoding.EncodeToString(key), nil
}

func decodeValueSealKey(encoded string) ([]byte, error) {
	if strings.TrimSpace(encoded) == "" {
		return nil, ErrValueSealKeyUnavailable
	}
	key, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(key) != sha256.Size {
		return nil, fmt.Errorf("credential value seal key is invalid")
	}
	return key, nil
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

// NewAtomicSnapshot lets Store implementations outside this package satisfy
// Snapshotter without exposing the credential value as a struct field.
func NewAtomicSnapshot(metadata Metadata, value string) AtomicSnapshot {
	return AtomicSnapshot{
		Key: strings.TrimSpace(metadata.Key), Present: metadata.Present, Source: strings.TrimSpace(metadata.Source),
		Writable: metadata.Writable, Shadowed: metadata.Shadowed, UpdatedAt: timePtrValue(metadata.UpdatedAt), value: value,
	}
}

func (s AtomicSnapshot) Metadata() Metadata {
	return Metadata{Key: s.Key, Present: s.Present, Source: s.Source, Writable: s.Writable, Shadowed: s.Shadowed, UpdatedAt: timePtrValue(s.UpdatedAt)}
}

func (s AtomicSnapshot) CredentialValue() string { return s.value }

// AdmittedSnapshot adds a process-private observation token. The token changes whenever
// the observed value or metadata changes and is safe to include in redacted
// registration identity; it is not derived from secret bytes.
type AdmittedSnapshot struct {
	AtomicSnapshot
	observationToken string
}

func (s AdmittedSnapshot) ObservationToken() string { return s.observationToken }

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
func (b SecretBinding) ObservationToken() string    { return b.snapshot.ObservationToken() }

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
	store  Snapshotter
	values Store
	mu     sync.Mutex
	seen   map[string]snapshotObservation
}

type snapshotObservation struct {
	snapshot AtomicSnapshot
	token    string
}

func NewSnapshotOwner(store Store) (*SnapshotOwner, error) {
	snapshotter, ok := store.(Snapshotter)
	if !ok || snapshotter == nil {
		return nil, fmt.Errorf("credential store does not provide atomic snapshots")
	}
	return &SnapshotOwner{store: snapshotter, values: store, seen: map[string]snapshotObservation{}}, nil
}

func (o *SnapshotOwner) SealCurrentValue(ctx context.Context, key string) (ValueEvidence, error) {
	if o == nil || o.values == nil {
		return ValueEvidence{}, fmt.Errorf("credential snapshot owner is required")
	}
	return SealCurrentValue(ctx, o.values, key)
}

func (o *SnapshotOwner) CurrentValueMatchesSeal(ctx context.Context, evidence ValueEvidence) (bool, error) {
	if o == nil || o.values == nil {
		return false, fmt.Errorf("credential snapshot owner is required")
	}
	return CurrentValueMatchesSeal(ctx, o.values, evidence)
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
		observation = snapshotObservation{snapshot: cloneAtomicSnapshot(snapshot), token: uuid.NewString()}
		o.seen[key] = observation
	}
	return AdmittedSnapshot{AtomicSnapshot: cloneAtomicSnapshot(snapshot), observationToken: observation.token}, nil
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
		if current.ObservationToken() != p.bindings[key].ObservationToken() {
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
