package credentials

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type FileStore struct {
	path string
	mu   sync.Mutex
}

type fileCredentialSet struct {
	Version      int                           `json:"version"`
	ValueSealKey string                        `json:"value_seal_key,omitempty"`
	Entries      map[string]fileCredentialItem `json:"entries"`
}

type fileCredentialItem struct {
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
	Receipt   string    `json:"receipt,omitempty"`
}

func NewFileStore(path string) (*FileStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("credential file path is required")
	}
	return &FileStore{path: filepath.Clean(path)}, nil
}

func DefaultFilePath() (string, error) {
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(root, "swarm", "credentials.json"), nil
}

func (s *FileStore) hasDurableValueSealKeyHome() bool { return s != nil }

func (s *FileStore) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return "", false, err
	}
	item, ok := doc.Entries[strings.TrimSpace(key)]
	if !ok {
		return "", false, nil
	}
	return item.Value, true, nil
}

func (s *FileStore) Set(_ context.Context, key, value string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("credential key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		doc.Entries[key] = fileCredentialItem{
			Value:     value,
			UpdatedAt: time.Now().UTC(),
		}
		return s.saveLocked(doc)
	})
}

func (s *FileStore) AdmitWithReceipt(_ context.Context, key, value, receipt string) (WriteReceipt, error) {
	key = strings.TrimSpace(key)
	receipt = strings.TrimSpace(receipt)
	if key == "" || receipt == "" {
		return WriteReceipt{}, fmt.Errorf("credential key and write receipt are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out WriteReceipt
	err := s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		if item, ok := doc.Entries[key]; ok && item.Receipt == receipt {
			out = WriteReceipt{Key: key, Receipt: receipt, UpdatedAt: item.UpdatedAt.UTC()}
			return nil
		}
		now := time.Now().UTC()
		item := fileCredentialItem{Value: value, UpdatedAt: now, Receipt: receipt}
		doc.Entries[key] = item
		if err := s.saveLocked(doc); err != nil {
			return err
		}
		out = WriteReceipt{Key: key, Receipt: receipt, UpdatedAt: now}
		return nil
	})
	return out, err
}

func (s *FileStore) ObserveReceipt(_ context.Context, key, receipt string) (WriteReceipt, bool, error) {
	key = strings.TrimSpace(key)
	receipt = strings.TrimSpace(receipt)
	if key == "" || receipt == "" {
		return WriteReceipt{}, false, fmt.Errorf("credential key and write receipt are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return WriteReceipt{}, false, err
	}
	item, found := doc.Entries[key]
	if !found || strings.TrimSpace(item.Receipt) != receipt {
		return WriteReceipt{}, false, nil
	}
	return WriteReceipt{Key: key, Receipt: receipt, UpdatedAt: item.UpdatedAt.UTC()}, true, nil
}

func (s *FileStore) List(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(doc.Entries))
	for key := range doc.Entries {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *FileStore) Delete(_ context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		delete(doc.Entries, key)
		return s.saveLocked(doc)
	})
}

func (s *FileStore) DeleteWithReceipt(_ context.Context, key, receipt string) (bool, error) {
	key = strings.TrimSpace(key)
	receipt = strings.TrimSpace(receipt)
	if key == "" || receipt == "" {
		return false, fmt.Errorf("credential key and write receipt are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := false
	err := s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		item, found := doc.Entries[key]
		if !found || strings.TrimSpace(item.Receipt) != receipt {
			return nil
		}
		delete(doc.Entries, key)
		if err := s.saveLocked(doc); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}

func (s *FileStore) Inspect(_ context.Context, key string) (Metadata, error) {
	snapshot, err := s.Snapshot(context.Background(), key)
	if err != nil {
		return Metadata{}, err
	}
	return snapshot.Metadata(), nil
}

func (s *FileStore) Snapshot(_ context.Context, key string) (AtomicSnapshot, error) {
	key = strings.TrimSpace(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return AtomicSnapshot{}, err
	}
	snapshot := AtomicSnapshot{
		Key:      key,
		Writable: true,
	}
	if item, ok := doc.Entries[key]; ok {
		snapshot.Present = true
		snapshot.Source = SourceFile
		snapshot.UpdatedAt = timePtr(item.UpdatedAt)
		snapshot.value = item.Value
	}
	return snapshot, nil
}

func (s *FileStore) sealCurrentValue(_ context.Context, key string) (ValueEvidence, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return ValueEvidence{}, fmt.Errorf("credential key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var evidence ValueEvidence
	err := s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		item, found := doc.Entries[key]
		if !found {
			return fmt.Errorf("credential %q is not present", key)
		}
		if !credentialValueUsable(item.Value) {
			return fmt.Errorf("%w: credential %q cannot be admitted", ErrCredentialValueUnusable, key)
		}
		seal, changed, err := sealValueInDocument(&doc, key, item.Value)
		if err != nil {
			return err
		}
		if changed {
			if err := s.saveLocked(doc); err != nil {
				return err
			}
		}
		evidence = ValueEvidence{Key: key, Seal: seal}
		return nil
	})
	return evidence, err
}

func (s *FileStore) currentValueMatchesSeal(_ context.Context, evidence ValueEvidence) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return false, err
	}
	key, err := decodeValueSealKey(doc.ValueSealKey)
	if err != nil {
		return false, fmt.Errorf("%w: restore the credential key home and repeat the channel identity ceremony", err)
	}
	item, found := doc.Entries[strings.TrimSpace(evidence.Key)]
	if !found {
		return false, nil
	}
	if !credentialValueUsable(item.Value) {
		return false, nil
	}
	want := credentialValueSeal(key, evidence.Key, item.Value)
	return subtleSealEqual(want, evidence.Seal), nil
}

func (s *FileStore) observedValueMatchesSeal(ctx context.Context, evidence ValueEvidence, value string) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	return s.matchExactValue(ctx, evidence.Key, value, evidence.Seal)
}

func (s *FileStore) sealExactValue(_ context.Context, key, value string) (ValueSeal, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("credential key is required")
	}
	if !credentialValueUsable(value) {
		return "", fmt.Errorf("%w: credential %q cannot be admitted", ErrCredentialValueUnusable, key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var seal ValueSeal
	err := s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		var changed bool
		seal, changed, err = sealValueInDocument(&doc, key, value)
		if err != nil {
			return err
		}
		if changed {
			return s.saveLocked(doc)
		}
		return nil
	})
	return seal, err
}

func (s *FileStore) matchExactValue(_ context.Context, key, value string, seal ValueSeal) (bool, error) {
	if _, err := ParseValueSeal(seal.String()); err != nil {
		return false, err
	}
	if !credentialValueUsable(value) {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return false, err
	}
	sealKey, err := decodeValueSealKey(doc.ValueSealKey)
	if err != nil {
		return false, fmt.Errorf("%w: restore the credential key home and repeat the channel identity ceremony", err)
	}
	return subtleSealEqual(credentialValueSeal(sealKey, strings.TrimSpace(key), value), seal), nil
}

func sealValueInDocument(doc *fileCredentialSet, key, value string) (ValueSeal, bool, error) {
	changed := false
	if strings.TrimSpace(doc.ValueSealKey) == "" {
		encoded, err := newValueSealKey()
		if err != nil {
			return "", false, err
		}
		doc.ValueSealKey = encoded
		changed = true
	}
	sealKey, err := decodeValueSealKey(doc.ValueSealKey)
	if err != nil {
		return "", false, err
	}
	return credentialValueSeal(sealKey, key, value), changed, nil
}

func subtleSealEqual(left, right ValueSeal) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func (s *FileStore) loadLocked() (fileCredentialSet, error) {
	doc := fileCredentialSet{
		Version: 1,
		Entries: map[string]fileCredentialItem{},
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return doc, nil
		}
		return fileCredentialSet{}, fmt.Errorf("read credential file %s: %w", s.path, err)
	}
	if len(raw) == 0 {
		return doc, nil
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fileCredentialSet{}, fmt.Errorf("parse credential file %s: %w", s.path, err)
	}
	if doc.Entries == nil {
		doc.Entries = map[string]fileCredentialItem{}
	}
	return doc, nil
}

func (s *FileStore) withWriteLockLocked(fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create credential dir: %w", err)
	}
	lockPath := s.path + ".lock"
	unlock, err := lockCredentialFile(lockPath)
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (s *FileStore) saveLocked(doc fileCredentialSet) error {
	if doc.Version == 0 {
		doc.Version = 1
	}
	if doc.Entries == nil {
		doc.Entries = map[string]fileCredentialItem{}
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create credential dir: %w", err)
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode credential file: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".credentials-*.json")
	if err != nil {
		return fmt.Errorf("create temp credential file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp credential file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp credential file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp credential file: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace credential file: %w", err)
	}
	return nil
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value.UTC()
	return &copy
}
