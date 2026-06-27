package credentials

import (
	"context"
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
	Version int                           `json:"version"`
	Entries map[string]fileCredentialItem `json:"entries"`
}

type fileCredentialItem struct {
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
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

func (s *FileStore) Inspect(_ context.Context, key string) (Metadata, error) {
	key = strings.TrimSpace(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return Metadata{}, err
	}
	meta := Metadata{
		Key:      key,
		Writable: true,
	}
	if item, ok := doc.Entries[key]; ok {
		meta.Present = true
		meta.Source = SourceFile
		meta.UpdatedAt = timePtr(item.UpdatedAt)
	}
	return meta, nil
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
