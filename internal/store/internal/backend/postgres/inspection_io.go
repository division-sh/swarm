package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/lib/pq"
)

// OpenForInspection owns a single-purpose read pool. Runtime pools and their
// retained authority sessions never inherit observation deadlines.
func OpenForInspection(cfg pq.Config) (*Backend, error) {
	connector, err := pq.NewConnectorConfig(cfg.Clone())
	if err != nil {
		return nil, err
	}
	dialer := &observationDialer{}
	connector.Dialer(dialer)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	b, err := NewWithInspectionConfig(db, cfg)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	b.inspectionDialer = dialer
	return b, nil
}

func (b *Backend) withInspectionIO(ctx context.Context, operation func(context.Context, func() error) error) (err error) {
	if !b.inspectionIOActive.CompareAndSwap(false, true) {
		return errors.New("PostgreSQL inspection transport already has an active observation")
	}
	defer b.inspectionIOActive.Store(false)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	stop, err := b.inspectionDialer.bind(ctx)
	if err != nil {
		if stop != nil {
			err = errors.Join(err, stop())
		}
		return errors.Join(err, b.inspectionDialer.close())
	}
	var once sync.Once
	var cleanup context.Context
	var cancelCleanup context.CancelFunc
	var finish func() error
	var phaseErr error
	settle := func() error {
		once.Do(func() {
			phaseErr = errors.Join(stop(), observationContextError(ctx))
			cleanup, cancelCleanup = context.WithTimeout(context.WithoutCancel(ctx), possessionObservationCleanupLimit)
			var bindErr error
			finish, bindErr = b.inspectionDialer.bind(cleanup)
			phaseErr = errors.Join(phaseErr, bindErr)
		})
		return phaseErr
	}
	defer func() {
		observationErr := settle()
		cleanupErr := errors.Join(b.inspectionDialer.phaseError(), observationContextError(cleanup))
		disposed := observationErr != nil || cleanupErr != nil
		if disposed {
			// Stop failed native I/O before evicting this single-purpose pool;
			// no unhealthy idle session may survive a completed observation.
			cleanupErr = errors.Join(cleanupErr, b.inspectionDialer.close(), b.db.Close())
		}
		if finish != nil {
			cleanupErr = errors.Join(cleanupErr, finish())
		}
		cleanupErr = errors.Join(cleanupErr, observationContextError(cleanup))
		if !disposed && cleanupErr != nil {
			// Finish can discover a late cleanup failure; the native deadline
			// remains installed while the failed pool and sockets are disposed.
			cleanupErr = errors.Join(cleanupErr, b.inspectionDialer.close(), b.db.Close())
		}
		err = errors.Join(err, observationErr, cleanupErr, b.inspectionDialer.phaseError(), observationContextError(cleanup))
		cancelCleanup()
	}()
	return operation(context.WithoutCancel(ctx), settle)
}
