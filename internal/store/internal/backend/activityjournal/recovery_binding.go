package activityjournal

import (
	"errors"
)

// ValidateDisplacedRecovery checks the fixed journal before recovering its rows.
// It exposes neither the transaction nor a callback seam.
func (s *ActivityPostgresOwner) ValidateDisplacedRecovery() error {
	if s == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return errors.New("activity recovery requires its original unbound PostgreSQL journal owner")
	}
	return s.schemaGuard()
}

func (s *ActivitySQLiteOwner) ValidateDisplacedRecovery() error {
	if s == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return errors.New("activity recovery requires its original unbound SQLite journal owner")
	}
	return s.schemaGuard()
}
