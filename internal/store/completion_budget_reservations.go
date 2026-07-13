package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type completionBudgetReservation struct {
	Period time.Time
	Kind   string
	Key    string
	CapUSD float64
	Amount float64
}

func prepareCompletionBudgetReservationsPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, now time.Time) ([]completionBudgetReservation, error) {
	scopes := canonicalCompletionBudgetScopes(authority.BudgetScopes)
	if len(scopes) == 0 {
		return nil, nil
	}
	period := completionBudgetPeriod(now)
	for _, scope := range scopes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO budget_admission_scopes (period_start_utc, scope_kind, scope_key, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$4) ON CONFLICT (period_start_utc, scope_kind, scope_key) DO NOTHING
		`, period, scope.Kind, scope.Key, now.UTC()); err != nil {
			return nil, fmt.Errorf("upsert completion budget scope: %w", err)
		}
	}
	reservations := make([]completionBudgetReservation, 0, len(scopes))
	for _, scope := range scopes {
		var locked int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM budget_admission_scopes WHERE period_start_utc=$1 AND scope_kind=$2 AND scope_key=$3 FOR UPDATE`, period, scope.Kind, scope.Key).Scan(&locked); err != nil {
			return nil, fmt.Errorf("lock completion budget scope: %w", err)
		}
		spent, active, err := completionBudgetUsagePostgres(ctx, tx, period, scope)
		if err != nil {
			return nil, err
		}
		remaining := scope.CapUSD - spent - active
		if remaining <= 0 {
			return nil, completionBudgetExhausted(scope, spent, active)
		}
		reservations = append(reservations, completionBudgetReservation{Period: period, Kind: scope.Kind, Key: scope.Key, CapUSD: scope.CapUSD, Amount: remaining})
	}
	return reservations, nil
}

func prepareCompletionBudgetReservationsSQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, now time.Time) ([]completionBudgetReservation, error) {
	scopes := canonicalCompletionBudgetScopes(authority.BudgetScopes)
	if len(scopes) == 0 {
		return nil, nil
	}
	period := completionBudgetPeriod(now)
	for _, scope := range scopes {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO budget_admission_scopes (period_start_utc, scope_kind, scope_key, created_at, updated_at)
			VALUES (?,?,?,?,?)
		`, period, scope.Kind, scope.Key, now.UTC(), now.UTC()); err != nil {
			return nil, fmt.Errorf("upsert sqlite completion budget scope: %w", err)
		}
	}
	reservations := make([]completionBudgetReservation, 0, len(scopes))
	for _, scope := range scopes {
		spent, active, err := completionBudgetUsageSQLite(ctx, tx, period, scope)
		if err != nil {
			return nil, err
		}
		remaining := scope.CapUSD - spent - active
		if remaining <= 0 {
			return nil, completionBudgetExhausted(scope, spent, active)
		}
		reservations = append(reservations, completionBudgetReservation{Period: period, Kind: scope.Kind, Key: scope.Key, CapUSD: scope.CapUSD, Amount: remaining})
	}
	return reservations, nil
}

func insertCompletionBudgetReservationsPostgres(ctx context.Context, tx *sql.Tx, attemptID string, reservations []completionBudgetReservation, now time.Time) error {
	for _, reservation := range reservations {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runtime_effect_budget_reservations
			(attempt_id, period_start_utc, scope_kind, scope_key, cap_usd, amount_usd, created_at)
			VALUES ($1::uuid,$2,$3,$4,$5,$6,$7)
		`, attemptID, reservation.Period, reservation.Kind, reservation.Key, reservation.CapUSD, reservation.Amount, now.UTC()); err != nil {
			return fmt.Errorf("insert completion budget reservation: %w", err)
		}
	}
	return nil
}

func insertCompletionBudgetReservationsSQLite(ctx context.Context, tx *sql.Tx, attemptID string, reservations []completionBudgetReservation, now time.Time) error {
	for _, reservation := range reservations {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runtime_effect_budget_reservations
			(attempt_id, period_start_utc, scope_kind, scope_key, cap_usd, amount_usd, created_at)
			VALUES (?,?,?,?,?,?,?)
		`, attemptID, reservation.Period, reservation.Kind, reservation.Key, reservation.CapUSD, reservation.Amount, now.UTC()); err != nil {
			return fmt.Errorf("insert sqlite completion budget reservation: %w", err)
		}
	}
	return nil
}

func completionBudgetUsagePostgres(ctx context.Context, tx *sql.Tx, period time.Time, scope runtimeeffects.BudgetAdmissionScope) (float64, float64, error) {
	var spent float64
	var err error
	switch scope.Kind {
	case "system":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE created_at >= $1`, period).Scan(&spent)
	case "global":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE entity_id IS NULL AND created_at >= $1`, period).Scan(&spent)
	case "entity":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE entity_id=$1::uuid AND created_at >= $2`, scope.Key, period).Scan(&spent)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read completion retained spend: %w", err)
	}
	var active float64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_usd),0) FROM runtime_effect_budget_reservations WHERE period_start_utc=$1 AND scope_kind=$2 AND scope_key=$3`, period, scope.Kind, scope.Key).Scan(&active); err != nil {
		return 0, 0, fmt.Errorf("read completion active reservations: %w", err)
	}
	return spent, active, nil
}

func completionBudgetUsageSQLite(ctx context.Context, tx *sql.Tx, period time.Time, scope runtimeeffects.BudgetAdmissionScope) (float64, float64, error) {
	var spent float64
	var err error
	switch scope.Kind {
	case "system":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE created_at >= ?`, period).Scan(&spent)
	case "global":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE entity_id IS NULL AND created_at >= ?`, period).Scan(&spent)
	case "entity":
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger WHERE entity_id=? AND created_at >= ?`, scope.Key, period).Scan(&spent)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read sqlite completion retained spend: %w", err)
	}
	var active float64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_usd),0) FROM runtime_effect_budget_reservations WHERE period_start_utc=? AND scope_kind=? AND scope_key=?`, period, scope.Kind, scope.Key).Scan(&active); err != nil {
		return 0, 0, fmt.Errorf("read sqlite completion active reservations: %w", err)
	}
	return spent, active, nil
}

func canonicalCompletionBudgetScopes(scopes []runtimeeffects.BudgetAdmissionScope) []runtimeeffects.BudgetAdmissionScope {
	out := append([]runtimeeffects.BudgetAdmissionScope(nil), scopes...)
	sort.Slice(out, func(i, j int) bool {
		left, right := completionBudgetScopeOrder(out[i].Kind), completionBudgetScopeOrder(out[j].Kind)
		if left != right {
			return left < right
		}
		return strings.TrimSpace(out[i].Key) < strings.TrimSpace(out[j].Key)
	})
	return out
}

func completionBudgetScopeOrder(kind string) int {
	switch strings.TrimSpace(kind) {
	case "system":
		return 0
	case "global":
		return 1
	case "entity":
		return 2
	default:
		return 3
	}
}

func completionBudgetPeriod(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func completionBudgetExhausted(scope runtimeeffects.BudgetAdmissionScope, spent, active float64) error {
	return runtimefailures.New(runtimefailures.ClassBudgetExhausted, "completion_spend_budget_exhausted", "llm-completion-authority", "authorize_attempt", map[string]any{
		"budget_kind":  "spend",
		"scope_kind":   scope.Kind,
		"scope_key":    scope.Key,
		"cap_usd":      scope.CapUSD,
		"spent_usd":    spent,
		"reserved_usd": active,
	})
}
