package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// InstalledSelectedJournalHistory is attached journal evidence only. A NULL
// execution binding requires the separate copied-lineage owner, not this leaf.
type InstalledSelectedJournalHistory struct {
	Record      runtimepipeline.ActivityAttemptRecord
	GrantFactID string
}

type selectedJournalGrantIndex map[string]map[uint64]SelectedGrantFact

// ReadInstalledSelectedJournalHistoryTx consumes the complete grant histories
// captured by the closed-generation owner in this same fenced transaction. It
// cannot issue an installed role or authorize reset, settlement or execution.
func ReadInstalledSelectedJournalHistoryTx(ctx context.Context, tx *sql.Tx, runID string, histories [][]SelectedGrantFact, sqlite bool) (facts []InstalledSelectedJournalHistory, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, parseErr := uuid.Parse(runID)
	if tx == nil || parseErr != nil || id == uuid.Nil || id.String() != runID {
		return nil, errors.New("installed journal history requires its native transaction and exact child UUID")
	}
	grants, err := indexSelectedJournalGrants(runID, histories)
	if err != nil {
		return nil, err
	}
	args := []any{runID}
	var placeholders []string
	for _, history := range histories {
		if len(history) == 0 {
			continue
		}
		args = append(args, history[0].Evidence.GrantID)
		placeholders = append(placeholders, "$"+strconv.Itoa(len(args)))
	}
	query := activityAttemptSelect + ` WHERE run_id=$1`
	if len(placeholders) != 0 {
		grantID := `execution_binding->>'grant_id'`
		if sqlite {
			grantID = `json_extract(execution_binding,'$.grant_id')`
		}
		query += ` OR ` + grantID + ` IN (` + strings.Join(placeholders, ",") + `)`
	}
	rows, err := tx.QueryContext(ctx, query+` ORDER BY request_event_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("read installed selected journals: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			facts = nil
		}
	}()
	seen := make(map[string]bool)
	for rows.Next() {
		record, err := scan(rows)
		if err != nil {
			return nil, err
		}
		if seen[record.RequestEventID] {
			return nil, errors.New("installed journal history has a foreign or repeated request")
		}
		factID, err := validateInstalledSelectedJournal(runID, record, grants)
		if err != nil {
			return nil, err
		}
		seen[record.RequestEventID] = true
		facts = append(facts, InstalledSelectedJournalHistory{Record: record, GrantFactID: factID})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate installed selected journals: %w", err)
	}
	return facts, ctx.Err()
}

func indexSelectedJournalGrants(runID string, histories [][]SelectedGrantFact) (selectedJournalGrantIndex, error) {
	grants := make(selectedJournalGrantIndex)
	for _, history := range histories {
		if len(history) == 0 {
			continue
		}
		grantID := history[0].Evidence.GrantID
		if grants[grantID] != nil {
			return nil, errors.New("installed journal history repeats a grant history")
		}
		versions := make(map[uint64]SelectedGrantFact)
		for i, fact := range history {
			if err := fact.Evidence.Validate(); err != nil {
				return nil, err
			}
			if err := validateSelectedGrantHistoryPrefix(history[:i], fact.Evidence); err != nil {
				return nil, err
			}
			if fact.Evidence.SelectedFork == nil || fact.Evidence.SelectedFork.ForkRunID != runID || fact.FactID == "" {
				return nil, errors.New("installed journal history has an unrelated grant frame")
			}
			versions[fact.Evidence.StateVersion] = fact
		}
		if history[len(history)-1].Evidence.State != runtimeownership.GrantRetired {
			return nil, errors.New("installed journal history requires retired grant histories")
		}
		grants[grantID] = versions
	}
	return grants, nil
}

func validateInstalledSelectedJournal(runID string, record runtimepipeline.ActivityAttemptRecord, grants selectedJournalGrantIndex) (string, error) {
	if record.RunID != runID {
		return "", errors.New("installed journal history has a foreign or repeated request")
	}
	if record.ExecutionBinding.IsZero() {
		return "", errors.New("copied journal requires its exact source-lineage membership owner")
	}
	if err := runtimepipeline.ValidateActivityAttemptStart(record); err != nil {
		return "", err
	}
	if err := runtimepipeline.ValidateActivityAttemptTerminal(record); err != nil {
		return "", err
	}
	if record.CompletedAt == nil || record.StartedAt.IsZero() || record.UpdatedAt.IsZero() || record.CompletedAt.Before(record.StartedAt) || record.UpdatedAt.Before(*record.CompletedAt) {
		return "", errors.New("installed journal history lacks its terminal physical clocks")
	}
	fact, found := grants[record.ExecutionBinding.GrantID][record.ExecutionBinding.StateVersion]
	if !found || fact.Evidence.State != runtimeownership.GrantAdmitted {
		return "", errors.New("installed journal binding has no exact admitted historical grant version")
	}
	return fact.FactID, nil
}
