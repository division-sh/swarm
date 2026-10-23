package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

type forkContentionWait struct {
	pid         int
	transaction string
	query       string
	blockers    string
	fenced      bool
}

// Identify the one real native waiter in this otherwise idle fixture database,
// not a pool wait or an arbitrary connection blocked somewhere by the winner.
func forkContentionPostgresWait(ctx context.Context, db *sql.DB, winnerPID, observerPID, contenderPID int) (forkContentionWait, error) {
	var wait forkContentionWait
	var count int
	err := db.QueryRowContext(ctx, `SELECT pid,
		COALESCE(backend_xid::text, '') || '/' || COALESCE(xact_start::text, ''),
		query, pg_blocking_pids(pid)::text, $2=ANY(pg_blocking_pids(pid)),
		count(*) OVER ()
		FROM pg_stat_activity
		WHERE datname=current_database() AND pid<>$2
		  AND (($3<>0 AND pid=$3) OR ($3=0 AND
		    ($1=ANY(pg_blocking_pids(pid)) OR $2=ANY(pg_blocking_pids(pid)))))`, winnerPID, observerPID, contenderPID).
		Scan(&wait.pid, &wait.transaction, &wait.query, &wait.blockers, &wait.fenced, &count)
	if err != nil {
		return wait, err
	}
	if count != 1 || wait.transaction == "/" {
		return wait, fmt.Errorf("identify exact native contender: count=%d wait=%+v", count, wait)
	}
	return wait, nil
}
