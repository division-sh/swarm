package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
)

type conversationForkDialect uint8

const (
	conversationForkPostgres conversationForkDialect = iota
	conversationForkSQLite
	sqliteCurrentLeaseSQL = "datetime(substr(replace(CAST(lease_expires_at AS TEXT),'T',' '),1,19))>CURRENT_TIMESTAMP"
)

type conversationForkStore struct {
	db        *sql.DB
	dialect   conversationForkDialect
	sqlite    *SQLiteRuntimeStore
	admission schemaAdmissionOwner
}

type conversationForkQueryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type conversationForkTimeValue struct {
	Time  time.Time
	Valid bool
}

func (v *conversationForkTimeValue) Scan(src any) error {
	parsed, valid, err := sqliteTimeValue(src)
	if err != nil {
		return err
	}
	v.Time = parsed
	v.Valid = valid
	return nil
}

func postgresConversationForkStore(s *PostgresStore) (conversationForkStore, error) {
	if s == nil || s.DB == nil {
		return conversationForkStore{}, fmt.Errorf("postgres store is required")
	}
	return conversationForkStore{db: s.DB, dialect: conversationForkPostgres, admission: s}, nil
}

func sqliteConversationForkStore(s *SQLiteRuntimeStore) (conversationForkStore, error) {
	if s == nil || s.DB == nil {
		return conversationForkStore{}, fmt.Errorf("sqlite runtime store is required")
	}
	return conversationForkStore{db: s.DB, dialect: conversationForkSQLite, sqlite: s, admission: s}, nil
}

func (s conversationForkStore) requireCurrentSchema() error {
	if s.admission == nil {
		return fmt.Errorf("conversation fork store requires accepted schema ownership")
	}
	return s.admission.requireCurrentSchema()
}

func (s conversationForkStore) bind(query string) string {
	if s.dialect == conversationForkSQLite {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 16)
	index := 1
	for _, r := range query {
		if r == '?' {
			fmt.Fprintf(&out, "$%d", index)
			index++
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func (s conversationForkStore) queryRow(ctx context.Context, q conversationForkQueryer, query string, args ...any) *sql.Row {
	return q.QueryRowContext(ctx, s.bind(query), args...)
}

func (s conversationForkStore) query(ctx context.Context, q conversationForkQueryer, query string, args ...any) (*sql.Rows, error) {
	return q.QueryContext(ctx, s.bind(query), args...)
}

func (s conversationForkStore) exec(ctx context.Context, q conversationForkQueryer, query string, args ...any) (sql.Result, error) {
	return q.ExecContext(ctx, s.bind(query), args...)
}

func (s conversationForkStore) forUpdate() string {
	if s.dialect == conversationForkPostgres {
		return " FOR UPDATE"
	}
	return ""
}

func (s conversationForkStore) currentLeaseSQL() string {
	if s.dialect == conversationForkPostgres {
		return "lease_expires_at>CURRENT_TIMESTAMP"
	}
	return sqliteCurrentLeaseSQL
}

func (s conversationForkStore) conversationQuerySources() []string {
	if s.dialect == conversationForkSQLite {
		return sqliteOperatorConversationQuerySources()
	}
	return operatorConversationQuerySources()
}

func (s conversationForkStore) runMutation(ctx context.Context, serializable bool, fn func(context.Context, *sql.Tx) error) error {
	if s.dialect == conversationForkSQLite {
		return s.sqlite.RunRuntimeMutation(ctx, fn)
	}
	return s.runPostgresMutation(ctx, s.db, serializable, fn)
}

func (s conversationForkStore) runForkMutation(ctx context.Context, forkID string, serializable bool, fn func(context.Context, *sql.Tx) error) (err error) {
	if s.dialect == conversationForkSQLite {
		return s.sqlite.RunRuntimeMutation(ctx, fn)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, forkID); err != nil {
		return fmt.Errorf("lock postgres conversation fork %s: %w", forkID, err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, forkID).Scan(&unlocked)
		if unlockErr == nil && unlocked {
			return
		}
		if unlockErr == nil {
			unlockErr = fmt.Errorf("postgres conversation fork advisory lock was not held")
		}
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		err = errors.Join(err, fmt.Errorf("unlock postgres conversation fork %s: %w", forkID, unlockErr))
	}()
	err = s.runPostgresMutation(ctx, conn, serializable, fn)
	return err
}

func (s conversationForkStore) runPostgresMutation(ctx context.Context, q interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}, serializable bool, fn func(context.Context, *sql.Tx) error) error {
	opts := &sql.TxOptions{}
	if serializable {
		opts.Isolation = sql.LevelSerializable
	}
	tx, err := q.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func nullableConversationForkID(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
