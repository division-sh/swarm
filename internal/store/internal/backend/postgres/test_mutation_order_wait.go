package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// MutationOrderWaitForTest is confined to one live pooled workflow attempt.
// It observes catalog facts through a separate read-only connection; native
// session identifiers and transaction access never cross this boundary.
type MutationOrderWaitForTest struct {
	backend  *Backend
	pid      int
	started  time.Time
	xact     time.Time
	xid      string
	database int64
	mu       sync.Mutex
	closed   bool
}

func (b *Backend) CaptureMutationOrderWaitForTest(ctx context.Context, tx *sql.Tx) (*MutationOrderWaitForTest, error) {
	if !b.Valid() || b.inspectionConfig == nil || tx == nil {
		return nil, errors.New("mutation-order observation requires its live native attempt")
	}
	wait := &MutationOrderWaitForTest{backend: b}
	err := tx.QueryRowContext(ctx, `SELECT pid, backend_start, xact_start,
		(txid_current() & 4294967295)::text, datid::bigint
		FROM pg_stat_activity WHERE pid = pg_backend_pid() AND datname = current_database()`).
		Scan(&wait.pid, &wait.started, &wait.xact, &wait.xid, &wait.database)
	if err != nil {
		return nil, err
	}
	return wait, nil
}

func (w *MutationOrderWaitForTest) Close() {
	if w != nil {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
	}
}

func (w *MutationOrderWaitForTest) SuccessorWaiting(ctx context.Context) (bool, error) {
	if w == nil {
		return false, errors.New("original mutation-order observation is missing")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false, errors.New("original mutation-order observation has ended")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return false, errors.New("mutation-order observation requires a bounded context")
	}
	observer, err := OpenForInspection(*w.backend.inspectionConfig)
	if err != nil {
		return false, err
	}
	var original, holders, matching int
	err = observer.InspectSnapshot(ctx, func(read context.Context) error {
		return observer.QueryRowContext(read, `WITH original AS (
			SELECT a.pid FROM pg_stat_activity a
			WHERE a.pid = $1 AND a.backend_start = $2 AND a.xact_start = $3 AND a.datid = $5
			AND EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid AND l.granted
				AND l.locktype = 'transactionid' AND l.mode = 'ExclusiveLock' AND l.transactionid::text = $4)
		), holders AS (
			SELECT DISTINCT a.pid, a.query, a.state, a.wait_event_type FROM pg_stat_activity a
			JOIN pg_locks l ON l.pid = a.pid AND l.database = a.datid
			WHERE a.datid = $5 AND a.pid <> $1 AND l.locktype = 'advisory' AND l.granted
			AND l.mode = 'ExclusiveLock' AND l.objsubid = 1
			AND l.classid::bigint = ((hashtext('swarm:runtime:shared-store-owner')::bigint >> 32) & 4294967295)
			AND l.objid::bigint = (hashtext('swarm:runtime:shared-store-owner')::bigint & 4294967295)
		) SELECT (SELECT count(*) FROM original), (SELECT count(*) FROM holders),
			(SELECT count(*) FROM holders h WHERE h.state = 'active' AND h.wait_event_type = 'Lock'
			AND btrim(regexp_replace(h.query, '[[:space:]]+', ' ', 'g')) =
				'SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE'
			AND $1 = ANY(pg_blocking_pids(h.pid))
			AND EXISTS (SELECT 1 FROM pg_locks waiting WHERE waiting.pid = h.pid AND NOT waiting.granted
				AND waiting.locktype = 'transactionid' AND waiting.transactionid::text = $4))`,
			w.pid, w.started, w.xact, w.xid, w.database).Scan(&original, &holders, &matching)
	})
	err = errors.Join(err, observer.Close())
	if err != nil {
		return false, err
	}
	if original != 1 || holders > 1 || matching > 1 {
		return false, errors.New("mutation-order observation has expired or ambiguous native identities")
	}
	return holders == 1 && matching == 1, nil
}
