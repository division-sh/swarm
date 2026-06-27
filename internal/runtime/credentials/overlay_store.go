package credentials

import (
	"context"
	"sort"
	"strings"
)

type OverlayStore struct {
	primary  Store
	writable Store
}

func NewOverlayStore(primary, writable Store) *OverlayStore {
	return &OverlayStore{
		primary:  primary,
		writable: writable,
	}
}

func (s *OverlayStore) Get(ctx context.Context, key string) (string, bool, error) {
	if s == nil {
		return "", false, nil
	}
	if s.primary != nil {
		value, ok, err := s.primary.Get(ctx, key)
		if err != nil || ok {
			return value, ok, err
		}
	}
	if s.writable != nil {
		return s.writable.Get(ctx, key)
	}
	return "", false, nil
}

func (s *OverlayStore) Set(ctx context.Context, key, value string) error {
	if s == nil || s.writable == nil {
		return ErrNotWritable
	}
	return s.writable.Set(ctx, key, value)
}

func (s *OverlayStore) List(ctx context.Context) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	keys := make([]string, 0)
	seen := map[string]struct{}{}
	for _, store := range []Store{s.primary, s.writable} {
		if store == nil {
			continue
		}
		items, err := store.List(ctx)
		if err != nil {
			return nil, err
		}
		for _, key := range items {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *OverlayStore) Delete(ctx context.Context, key string) error {
	if s == nil || s.writable == nil {
		return ErrNotWritable
	}
	return s.writable.Delete(ctx, key)
}

func (s *OverlayStore) Inspect(ctx context.Context, key string) (Metadata, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Metadata{}, nil
	}
	var writableMeta Metadata
	var writableInspected bool
	if inspector, ok := s.writable.(Inspector); ok && inspector != nil {
		meta, err := inspector.Inspect(ctx, key)
		if err != nil {
			return Metadata{}, err
		}
		meta.Writable = true
		writableMeta = meta
		writableInspected = true
	}
	if inspector, ok := s.primary.(Inspector); ok && inspector != nil {
		meta, err := inspector.Inspect(ctx, key)
		if err != nil {
			return Metadata{}, err
		}
		if meta.Present {
			meta.Writable = false
			meta.Shadowed = writableInspected && writableMeta.Present
			return meta, nil
		}
	}
	if writableInspected {
		return writableMeta, nil
	}
	_, present, err := s.Get(ctx, key)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		Key:      key,
		Present:  present,
		Writable: s.writable != nil,
	}, nil
}
