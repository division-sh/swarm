package timerobligation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	. "github.com/division-sh/swarm/internal/runtime/timerobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type SchedulePostgresOwner struct {
	backend        *postgresbackend.Backend
	schemaGuard    func() error
	scheduleClaims postgresScheduleClaimOwner
}

type postgresScheduleClaimOwner struct {
	mu   sync.Mutex
	conn *sql.Conn
	keys map[string]struct{}
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*SchedulePostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("timer obligation postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("timer obligation postgres schema guard is required")
	}
	return &SchedulePostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (o *SchedulePostgresOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("timer obligation postgres schema guard is required")
	}
	return o.schemaGuard()
}

func (o *SchedulePostgresOwner) Read(ctx context.Context, scope Scope, observedAt time.Time) (Snapshot, error) {
	if o == nil || o.backend == nil {
		return Snapshot{}, fmt.Errorf("timer obligation postgres owner is required")
	}
	return read(ctx, o.backend, true, scope, observedAt)
}

func ReadPostgresTx(ctx context.Context, tx *sql.Tx, scope Scope, observedAt time.Time) (Snapshot, error) {
	if tx == nil {
		return Snapshot{}, fmt.Errorf("timer obligation postgres transaction is required")
	}
	return read(ctx, tx, true, scope, observedAt)
}

type ScheduleSQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*ScheduleSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("timer obligation sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("timer obligation sqlite schema guard is required")
	}
	return &ScheduleSQLiteOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (o *ScheduleSQLiteOwner) requireCurrentSchema() error {
	if o == nil || o.schemaGuard == nil {
		return fmt.Errorf("timer obligation sqlite schema guard is required")
	}
	return o.schemaGuard()
}

func (o *ScheduleSQLiteOwner) Read(ctx context.Context, scope Scope, observedAt time.Time) (Snapshot, error) {
	if o == nil || o.backend == nil {
		return Snapshot{}, fmt.Errorf("timer obligation sqlite owner is required")
	}
	return read(ctx, o.backend, false, scope, observedAt)
}

func ReadSQLiteTx(ctx context.Context, tx *sql.Tx, scope Scope, observedAt time.Time) (Snapshot, error) {
	if tx == nil {
		return Snapshot{}, fmt.Errorf("timer obligation sqlite transaction is required")
	}
	return read(ctx, tx, false, scope, observedAt)
}

func read(ctx context.Context, queryer queryer, postgres bool, scope Scope, observedAt time.Time) (Snapshot, error) {
	if queryer == nil {
		return Snapshot{}, fmt.Errorf("timer obligation reader requires selected store")
	}
	observedAt = observedAt.UTC()
	if observedAt.IsZero() {
		return Snapshot{}, fmt.Errorf("timer obligation reader requires exact observation time")
	}

	query := `
		SELECT
			t.task_type,
			COALESCE(CAST(t.run_id AS TEXT), ''),
			t.status,
			t.fire_at,
			COALESCE(r.status, '')
		FROM timers t
		LEFT JOIN runs r ON r.run_id = t.run_id
		WHERE (? = '' OR t.run_id = ?)
		ORDER BY t.task_type, t.run_id, t.timer_id
	`
	args := []any{scope.RunID(), scope.RunID()}
	if postgres {
		query = `
			SELECT
				t.task_type,
				COALESCE(t.run_id::text, ''),
				t.status,
				t.fire_at,
				COALESCE(r.status, '')
			FROM timers t
			LEFT JOIN runs r ON r.run_id = t.run_id
			WHERE (NULLIF($1, '') IS NULL OR t.run_id = NULLIF($1, '')::uuid)
			ORDER BY t.task_type, t.run_id, t.timer_id
		`
		args = []any{scope.RunID()}
	}

	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read timer obligations: %w", err)
	}
	defer rows.Close()

	global := ZeroFamilies()
	runs := map[string][]FamilyObligation{}
	if scope.RunID() != "" {
		runs[scope.RunID()] = ZeroFamilies()
	}
	for rows.Next() {
		var (
			familyValue string
			runID       string
			status      string
			fireAtValue any
			runStatus   string
		)
		if err := rows.Scan(&familyValue, &runID, &status, &fireAtValue, &runStatus); err != nil {
			return Snapshot{}, fmt.Errorf("scan timer obligation: %w", err)
		}
		fireAt, err := timerTimeValue(fireAtValue)
		if err != nil {
			return Snapshot{}, fmt.Errorf("scan timer obligation fire_at: %w", err)
		}
		family, err := ParseFamily(familyValue)
		if err != nil {
			return Snapshot{}, err
		}
		runID = strings.TrimSpace(runID)
		status = strings.TrimSpace(status)
		runStatus = strings.TrimSpace(runStatus)
		if err := validateRow(family, runID, status, fireAt); err != nil {
			return Snapshot{}, err
		}

		target := global
		if runID != "" {
			if _, ok := runs[runID]; !ok {
				runs[runID] = ZeroFamilies()
			}
			target = runs[runID]
		}
		index := familyIndex(family)
		if status == "active" {
			target[index].ActiveCount++
			if !fireAt.After(observedAt) {
				target[index].DueCount++
			}
			runState, runStateErr := runtimerunlifecycle.ParseState(runStatus)
			if runID == "" || (runStateErr == nil && runState.Active()) {
				target[index].RecoverableCount++
			}
		}
		if runID == "" {
			global = target
		} else {
			runs[runID] = target
		}
	}
	if err := rows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate timer obligations: %w", err)
	}

	snapshot := Snapshot{
		ObservedAt:     observedAt,
		GlobalFamilies: global,
		Runs:           make([]RunObligations, 0, len(runs)),
	}
	for _, runID := range SortedRunIDs(runs) {
		snapshot.Runs = append(snapshot.Runs, RunObligations{
			RunID:    runID,
			Families: runs[runID],
		})
	}
	return snapshot, nil
}

func timerTimeValue(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		return parseTimerTime(value)
	case []byte:
		return parseTimerTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp value %T", raw)
	}
}

func parseTimerTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, format := range formats {
		parsed, err := time.Parse(format, raw)
		if err == nil {
			return parsed.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("parse timestamp %q: %w", raw, lastErr)
}

func familyIndex(family Family) int {
	for index, candidate := range AllFamilies() {
		if candidate == family {
			return index
		}
	}
	panic("validated timer family is absent from canonical ordering")
}

func validateRow(family Family, runID, status string, fireAt time.Time) error {
	switch status {
	case "active", "fired", "cancelled", "expired":
	default:
		return fmt.Errorf("timer family %s has invalid status %q", family, status)
	}
	if fireAt.IsZero() {
		return fmt.Errorf("timer family %s has zero fire_at", family)
	}
	if family == FamilyWorkflowTimer && runID == "" {
		return fmt.Errorf("workflow timer obligation requires run_id")
	}
	if family == FamilyWorkflowTimer && status == "expired" {
		return fmt.Errorf("workflow timer obligation cannot be expired")
	}
	return nil
}
