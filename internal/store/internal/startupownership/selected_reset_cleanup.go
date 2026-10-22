package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

type selectedResetHistory struct {
	validate func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) error
}

func (s *selectedResetHistory) BindSelectedResetHistory(validate func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) error) error {
	if validate == nil || s.validate != nil {
		return errors.New("selected reset history must be bound exactly once")
	}
	s.validate = validate
	return nil
}

func (s *selectedResetHistory) validateSelectedResetPreparationsTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) error {
	runIDs, err := adminpersistence.ValidateDestructiveResetCleanupRequest(req, req.RequestedAt)
	if err != nil {
		return err
	}
	planned := make(map[string]bool, len(runIDs))
	for _, id := range runIDs {
		planned[id] = true
	}
	seen := make(map[string]bool, len(req.SelectedHistory))
	for _, prepared := range req.SelectedHistory {
		id := prepared.Binding.ForkRunID
		if !planned[id] || seen[id] {
			return errors.New("selected reset preparation repeats or crosses the planned run set")
		}
		seen[id] = true
	}
	if s.validate == nil {
		if len(req.SelectedHistory) != 0 {
			return errors.New("selected reset history reader is not composed")
		}
		return nil
	}
	return s.validate(ctx, tx, req, sqlite)
}
