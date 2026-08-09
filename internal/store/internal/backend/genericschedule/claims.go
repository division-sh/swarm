package genericschedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

const claimNamespace = "swarm:generic_schedule:"

func claimKey(wakeup runtimegenericschedule.Wakeup) string {
	return claimNamespace + wakeup.ActivationID()
}

func (o *PostgresOwner) ClaimGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (bool, error) {
	if err := o.requireSchema(); err != nil {
		return false, err
	}
	if err := wakeup.Validate(); err != nil {
		return false, err
	}
	key := claimKey(wakeup)
	o.claims.mu.Lock()
	defer o.claims.mu.Unlock()
	if _, ok := o.claims.keys[key]; ok {
		if o.claims.conn == nil {
			delete(o.claims.keys, key)
		} else {
			active, err := activeWakeupOnConn(ctx, o.claims.conn, wakeup)
			if err != nil {
				return false, err
			}
			if active {
				return true, nil
			}
			if _, err := o.claims.conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
				return false, err
			}
			delete(o.claims.keys, key)
		}
	}
	conn, err := o.ensureClaimConn(ctx)
	if err != nil {
		return false, err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&acquired); err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	active, err := activeWakeupOnConn(ctx, conn, wakeup)
	if err != nil || !active {
		_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key)
		return false, err
	}
	if o.claims.keys == nil {
		o.claims.keys = map[string]struct{}{}
	}
	o.claims.keys[key] = struct{}{}
	return true, nil
}

func (o *SQLiteOwner) ClaimGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (bool, error) {
	if err := o.requireSchema(); err != nil {
		return false, err
	}
	if err := wakeup.Validate(); err != nil {
		return false, err
	}
	activation, found, err := o.LoadGenericScheduleActivation(ctx, wakeup.ActivationID())
	if err != nil || !found {
		return false, err
	}
	return activation.Status == runtimegenericschedule.StatusActive && activation.CurrentDueAt.Equal(wakeup.DueAt()), nil
}

func (o *PostgresOwner) ReleaseGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	if o == nil {
		return nil
	}
	if err := wakeup.Validate(); err != nil {
		return err
	}
	key := claimKey(wakeup)
	o.claims.mu.Lock()
	defer o.claims.mu.Unlock()
	if _, ok := o.claims.keys[key]; !ok {
		return nil
	}
	if o.claims.conn == nil {
		delete(o.claims.keys, key)
		return nil
	}
	if _, err := o.claims.conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
		return err
	}
	delete(o.claims.keys, key)
	if len(o.claims.keys) == 0 {
		return o.closeClaimConn()
	}
	return nil
}

func (*SQLiteOwner) ReleaseGenericScheduleWakeup(context.Context, runtimegenericschedule.Wakeup) error {
	return nil
}

func (o *PostgresOwner) ReleaseGenericScheduleClaims(context.Context) error {
	if o == nil {
		return nil
	}
	o.claims.mu.Lock()
	defer o.claims.mu.Unlock()
	return o.closeClaimConn()
}

func (*SQLiteOwner) ReleaseGenericScheduleClaims(context.Context) error { return nil }

func (o *PostgresOwner) ensureClaimConn(ctx context.Context) (*sql.Conn, error) {
	if o.claims.conn != nil {
		return o.claims.conn, nil
	}
	conn, err := o.backend.Conn(ctx)
	if err != nil {
		return nil, err
	}
	o.claims.conn = conn
	return conn, nil
}

func (o *PostgresOwner) closeClaimConn() error {
	conn := o.claims.conn
	o.claims.conn = nil
	o.claims.keys = nil
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func activeWakeupOnConn(ctx context.Context, conn *sql.Conn, wakeup runtimegenericschedule.Wakeup) (bool, error) {
	if conn == nil {
		return false, errors.New("generic schedule claim connection is required")
	}
	var active bool
	err := conn.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM timers t LEFT JOIN runs r ON r.run_id = t.run_id
		WHERE t.timer_id = $1::uuid AND t.task_type IN ('timer','scheduled_task','global_recurring')
		AND t.status = 'active' AND t.fire_at = $2
		AND (t.run_id IS NULL OR r.status IN ('running','paused'))
	)`, strings.TrimSpace(wakeup.ActivationID()), wakeup.DueAt()).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("check generic schedule wakeup claim: %w", err)
	}
	return active, nil
}
