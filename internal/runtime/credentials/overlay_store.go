package credentials

import (
	"context"
	"fmt"
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

func (s *OverlayStore) AdmitWithReceipt(ctx context.Context, key, value, receipt string) (WriteReceipt, error) {
	if s == nil || s.writable == nil {
		return WriteReceipt{}, ErrNotWritable
	}
	key = strings.TrimSpace(key)
	if key == "" || strings.TrimSpace(receipt) == "" {
		return WriteReceipt{}, fmt.Errorf("credential key and write receipt are required")
	}
	if s.primary != nil {
		snapshotter, ok := s.primary.(Snapshotter)
		if !ok || snapshotter == nil {
			return WriteReceipt{}, fmt.Errorf("primary credential store does not provide atomic snapshots")
		}
		primary, err := snapshotter.Snapshot(ctx, key)
		if err != nil {
			return WriteReceipt{}, err
		}
		if primary.Present {
			return WriteReceipt{}, fmt.Errorf("credential %q is owned by the read-only primary tier and cannot be admitted to the shadowed file tier", key)
		}
	}
	writer, ok := s.writable.(ReceiptWriter)
	if !ok || writer == nil {
		return WriteReceipt{}, fmt.Errorf("writable credential store does not support receipt admission")
	}
	return writer.AdmitWithReceipt(ctx, key, value, receipt)
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

func (s *OverlayStore) DeleteWithReceipt(ctx context.Context, key, receipt, epoch string) (bool, error) {
	if s == nil || s.writable == nil {
		return false, ErrNotWritable
	}
	deleter, ok := s.writable.(ReceiptDeleter)
	if !ok || deleter == nil {
		return false, fmt.Errorf("writable credential store does not support receipt-fenced deletion")
	}
	return deleter.DeleteWithReceipt(ctx, key, receipt, epoch)
}

func (s *OverlayStore) Inspect(ctx context.Context, key string) (Metadata, error) {
	snapshot, err := s.Snapshot(ctx, key)
	if err != nil {
		return Metadata{}, err
	}
	return snapshot.Metadata(), nil
}

func (s *OverlayStore) Snapshot(ctx context.Context, key string) (AtomicSnapshot, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return AtomicSnapshot{}, nil
	}
	var writableSnapshot AtomicSnapshot
	var writableInspected bool
	if snapshotter, ok := s.writable.(Snapshotter); ok && snapshotter != nil {
		snapshot, err := snapshotter.Snapshot(ctx, key)
		if err != nil {
			return AtomicSnapshot{}, err
		}
		snapshot.Writable = true
		writableSnapshot = snapshot
		writableInspected = true
	} else if s.writable != nil {
		return AtomicSnapshot{}, fmt.Errorf("writable credential store does not provide atomic snapshots")
	}
	if snapshotter, ok := s.primary.(Snapshotter); ok && snapshotter != nil {
		snapshot, err := snapshotter.Snapshot(ctx, key)
		if err != nil {
			return AtomicSnapshot{}, err
		}
		if snapshot.Present {
			snapshot.Writable = false
			snapshot.Shadowed = writableInspected && writableSnapshot.Present
			return snapshot, nil
		}
	} else if s.primary != nil {
		return AtomicSnapshot{}, fmt.Errorf("primary credential store does not provide atomic snapshots")
	}
	if writableInspected {
		return writableSnapshot, nil
	}
	return AtomicSnapshot{Key: key, Writable: s.writable != nil}, nil
}
