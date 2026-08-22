package startupownership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type sqlitePossession interface {
	ProveCurrent(context.Context) error
	Release() error
}

// SQLiteConstructionGuard keeps the selected file stable while the SQLite
// pool opens and records the exact file identity that later possession must
// protect.
type SQLiteConstructionGuard struct {
	mu         sync.Mutex
	possession sqlitePossession
}

// SQLiteBackendIdentity is the immutable file identity captured while the
// backend pool was opened under SQLiteConstructionGuard.
type SQLiteBackendIdentity struct {
	mu        sync.Mutex
	reference sqlitePossession
	initial   sqlitePossession
}

func AcquireSQLiteConstructionGuard(selectedPath string) (*SQLiteConstructionGuard, error) {
	selectedPath = strings.TrimSpace(selectedPath)
	if selectedPath == "" {
		return nil, errors.New("SQLite selected-store path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(selectedPath))
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite selected-store path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, fmt.Errorf("create SQLite selected-store parent: %w", err)
	}
	coordinate, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create SQLite selected-store coordinate: %w", err)
	}
	if err := coordinate.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite selected-store coordinate: %w", err)
	}
	possession, err := acquireSQLiteFilePossession(abs)
	if err != nil {
		return nil, err
	}
	return &SQLiteConstructionGuard{possession: possession}, nil
}

func (g *SQLiteConstructionGuard) BackendIdentity(ctx context.Context) (*SQLiteBackendIdentity, error) {
	if g == nil {
		return nil, errors.New("SQLite construction guard is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.possession == nil {
		return nil, errors.New("SQLite construction guard is released")
	}
	if err := g.possession.ProveCurrent(ctx); err != nil {
		return nil, err
	}
	possession := g.possession
	g.possession = nil
	return &SQLiteBackendIdentity{reference: possession, initial: possession}, nil
}

func (g *SQLiteConstructionGuard) Release() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	possession := g.possession
	g.possession = nil
	g.mu.Unlock()
	if possession == nil {
		return nil
	}
	return possession.Release()
}

func (i *SQLiteBackendIdentity) matches(possession sqlitePossession) bool {
	return i != nil && i.reference != nil && sameSQLitePossessionResource(i.reference, possession)
}

func (i *SQLiteBackendIdentity) takeInitial() sqlitePossession {
	if i == nil {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	possession := i.initial
	i.initial = nil
	return possession
}

func (i *SQLiteBackendIdentity) releaseInitial() error {
	possession := i.takeInitial()
	if possession == nil {
		return nil
	}
	return possession.Release()
}

// ReleaseConstructionPossession closes an identity that was not transferred
// into a composed startup owner.
func (i *SQLiteBackendIdentity) ReleaseConstructionPossession() error {
	return i.releaseInitial()
}

type testSQLitePossession struct {
	once    sync.Once
	release func()
}

func (p *testSQLitePossession) ProveCurrent(context.Context) error {
	if p == nil || p.release == nil {
		return errors.New("test SQLite possession is released")
	}
	return nil
}

func (p *testSQLitePossession) Release() error {
	if p != nil {
		p.once.Do(p.release)
	}
	return nil
}

func (s *StartupSQLiteOwner) acquirePossession(ctx context.Context) (sqlitePossession, error) {
	if s == nil {
		return nil, errors.New("SQLite startup owner is required")
	}
	if strings.TrimSpace(s.path) != "" {
		if s.backendIdentity != nil {
			if initial := s.backendIdentity.takeInitial(); initial != nil {
				if err := initial.ProveCurrent(ctx); err != nil {
					return nil, errors.Join(err, initial.Release())
				}
				return initial, nil
			}
		}
		possession, err := acquireSQLiteFilePossession(s.path)
		if err != nil {
			return nil, err
		}
		if s.backendIdentity != nil && !s.backendIdentity.matches(possession) {
			return nil, errors.Join(&runtimestartupownership.AcquisitionError{
				Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
				Detail:  "SQLite selected-store identity differs from the backend opened during construction",
			}, possession.Release())
		}
		return possession, nil
	}
	// Empty paths exist only in explicit externally-managed test-store
	// construction. Production file-backed stores always use the retained OS
	// possession owner below.
	if !s.ownerMu.TryLock() {
		return nil, &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionTakeoverRequired,
			Detail:  "selected test store is held by another process capability",
		}
	}
	return &testSQLitePossession{release: s.ownerMu.Unlock}, nil
}

func (s *StartupSQLiteOwner) ReleaseConstructionPossession() error {
	if s == nil || s.backendIdentity == nil {
		return nil
	}
	return s.backendIdentity.releaseInitial()
}
