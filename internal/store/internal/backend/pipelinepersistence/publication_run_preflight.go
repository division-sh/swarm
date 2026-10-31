package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

func (s *PipelinePostgresOwner) LoadPublicationRunPreflight(ctx context.Context, eventID, runID string) (prepared bus.PreparedPublishEvent, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return prepared, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		prepared, found, err = loadPublicationRunPreflightTx(ctx, tx, s.events, eventID, runID, true)
		return err
	})
	return prepared, found, err
}

func (s *PipelineSQLiteOwner) LoadPublicationRunPreflight(ctx context.Context, eventID, runID string) (prepared bus.PreparedPublishEvent, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return prepared, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		prepared, found, err = loadPublicationRunPreflightTx(ctx, tx, s.events, eventID, runID, false)
		return err
	})
	return prepared, found, err
}

func loadPublicationRunPreflightTx(ctx context.Context, tx *sql.Tx, owner EventCommitOwner, eventID, runID string, postgres bool) (bus.PreparedPublishEvent, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || strings.TrimSpace(runID) == "" || owner == nil {
		return bus.PreparedPublishEvent{}, false, fmt.Errorf("publication run preflight requires its event, run and event owner")
	}
	prepared, found, err := owner.LoadPreparedPublishEventTx(ctx, tx, eventID)
	if err != nil {
		return bus.PreparedPublishEvent{}, false, fmt.Errorf("load publication event before run preflight: %w", err)
	}
	if found {
		return prepared, true, nil
	}
	load := sourceadmission.LoadSQLiteTx
	if postgres {
		load = sourceadmission.LoadPostgresTx
	}
	_, err = load(ctx, tx, runID, true, true)
	return bus.PreparedPublishEvent{}, false, err
}
