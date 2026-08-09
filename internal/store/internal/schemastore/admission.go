package schemastore

import (
	"fmt"
	"sync"
)

type schemaAdmissionState uint8

const (
	schemaUnaccepted schemaAdmissionState = iota
	schemaCurrent
)

type schemaAdmission struct {
	mu    sync.RWMutex
	state schemaAdmissionState
}

func (a *schemaAdmission) markCurrent() {
	a.mu.Lock()
	a.state = schemaCurrent
	a.mu.Unlock()
}

func (a *schemaAdmission) requireCurrent() error {
	a.mu.RLock()
	current := a.state == schemaCurrent
	a.mu.RUnlock()
	if !current {
		return fmt.Errorf("store: selected store schema is unaccepted; complete canonical schema bootstrap before runtime access")
	}
	return nil
}

func (s *Postgres) RequireCurrent() error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres schema owner is required")
	}
	return s.schemaAdmission.requireCurrent()
}

func (s *SQLite) RequireCurrent() error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite schema store is required")
	}
	return s.schemaAdmission.requireCurrent()
}
