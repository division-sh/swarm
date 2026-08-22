package startupownership

import (
	"context"
	"errors"
	"strings"
	"sync"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type sqlitePossession interface {
	ProveCurrent(context.Context) error
	Release() error
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

func (s *StartupSQLiteOwner) acquirePossession() (sqlitePossession, error) {
	if s == nil {
		return nil, errors.New("SQLite startup owner is required")
	}
	if strings.TrimSpace(s.path) != "" {
		return acquireSQLiteFilePossession(s.path)
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
