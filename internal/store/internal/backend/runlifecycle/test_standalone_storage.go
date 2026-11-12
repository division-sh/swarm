package runlifecycle

import (
	"context"
	"database/sql"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunLifecyclePostgresOwner) ReadStandaloneRunStorageForTest(ctx context.Context, eventID string) (StandaloneRunStorage, error) {
	var out StandaloneRunStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneRunStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ReadStandaloneRunStorageForTest(ctx context.Context, eventID string) (StandaloneRunStorage, error) {
	var out StandaloneRunStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneRunStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}

func (s *RunLifecyclePostgresOwner) ReadStandaloneCompletionCandidateForTest(ctx context.Context, eventID string) (runtimerunlifecycle.Candidate, error) {
	var out runtimerunlifecycle.Candidate
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneCompletionCandidateTx(ctx, tx, true, eventID)
		return err
	})
	if err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ReadStandaloneCompletionCandidateForTest(ctx context.Context, eventID string) (runtimerunlifecycle.Candidate, error) {
	var out runtimerunlifecycle.Candidate
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneCompletionCandidateTx(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	return out, nil
}
