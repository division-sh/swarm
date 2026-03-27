package credentials

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
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

type Metadata struct {
	Key       string
	Present   bool
	Source    string
	Writable  bool
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
	_, ok, err := EnvStore{}.Get(ctx, key)
	if err != nil {
		return Metadata{}, err
	}
	meta := Metadata{
		Key:      strings.TrimSpace(key),
		Present:  ok,
		Writable: false,
	}
	if ok {
		meta.Source = SourceEnv
	}
	return meta, nil
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
