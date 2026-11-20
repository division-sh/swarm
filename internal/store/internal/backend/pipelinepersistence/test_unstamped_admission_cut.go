package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

func (s *PipelinePostgresOwner) HoldUnstampedPipelineAdmissionTransactionForTest(ctx context.Context) (func() error, error) {
	return holdUnstampedPipelineAdmissionTransaction(ctx, func(write func(context.Context, *sql.Tx) error) error {
		if err := s.requireCurrent(); err != nil {
			return err
		}
		return s.backend.RunTransaction(ctx, write)
	})
}

func (s *PipelineSQLiteOwner) HoldUnstampedPipelineAdmissionTransactionForTest(ctx context.Context) (func() error, error) {
	return holdUnstampedPipelineAdmissionTransaction(ctx, func(write func(context.Context, *sql.Tx) error) error {
		if err := s.requireCurrent(); err != nil {
			return err
		}
		return s.backend.RunTransaction(ctx, "workflow projection shape fault fixture", write)
	})
}

func holdUnstampedPipelineAdmissionTransaction(ctx context.Context, enter func(func(context.Context, *sql.Tx) error) error) (func() error, error) {
	ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	aborted := errors.New("unstamped pipeline transaction cut released")
	var once sync.Once
	var result error
	closeCut := func() error {
		once.Do(func() { close(release) })
		<-done
		return result
	}
	go func() {
		err := enter(func(ctx context.Context, _ *sql.Tx) error {
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return aborted
		})
		if err != aborted {
			result = err
		}
		close(done)
	}()
	select {
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, closeCut())
		}
		return closeCut, nil
	case <-done:
		return nil, result
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), closeCut())
	}
}
