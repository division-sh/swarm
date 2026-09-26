package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// ObserveFanOutExecutions is a bounded read projection, never an admission for
// claim or publication. All rows use one selected-store snapshot and clock.
func (s *fanOutServingStore) ObserveFanOutExecutions(ctx context.Context, registrations []startupownership.FanOutRegistrationObservation, keys []fanoutobligation.IntentKey) (snapshot startupownership.FanOutExecutionSnapshot, err error) {
	grants := fanOutRegistrationGrants(registrations, false)
	closing := make(map[string]bool, len(registrations))
	for _, registration := range registrations {
		closing[registration.Grant.GrantID] = registration.Closing
	}
	if len(keys) > 500 {
		return snapshot, errors.New("fan-out execution observation is limited to 500 exact keys")
	}
	for _, key := range keys {
		if err := key.Validate(); err != nil {
			return snapshot, err
		}
	}
	operation := func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutObservation)
		proofErr := s.admission.ObserveFanOutGrantsTx(ctx, tx, grants)
		pending := errors.Is(proofErr, ErrFanOutRegistrationPending)
		if proofErr != nil && !pending {
			return proofErr
		}
		now := time.Now
		if s.sqlite != nil {
			now = s.sqlite.now
		}
		var err error
		snapshot.ObservedAt, err = fanOutAdmissionTime(ctx, tx, s.postgres != nil, now)
		if err != nil || len(keys) == 0 {
			return err
		}
		var args []any
		query := `SELECT COALESCE(CAST(g.grant_id AS TEXT),''),` + fanOutExecutionReasonSQL + `,` +
			fanOutObservationHeaderColumns() + fanOutObservationFrom(grants, &args)
		filters := make([]string, 0, len(keys))
		for _, key := range keys {
			predicate, keyArgs := fanOutKeyPredicate("i", key, len(args)+1)
			args = append(args, keyArgs...)
			filters = append(filters, `(`+predicate+`)`)
		}
		rows, err := tx.QueryContext(ctx, query+` WHERE `+strings.Join(filters, " OR "), args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		observed := make(map[fanoutobligation.IntentKey]startupownership.FanOutExecutionObservation, len(keys))
		for rows.Next() {
			var row startupownership.FanOutExecutionObservation
			intent, err := scanFanOutIntent(fanOutPrefixedRow{row: rows, prefix: []any{&row.GrantID, &row.Reason}})
			if err != nil {
				return err
			}
			row.Key = intent.Request.Key
			row.Reason, err = fanOutCanonicalReadinessReason(intent, snapshot.ObservedAt, row.Reason)
			if err != nil {
				return err
			}
			if _, exists := observed[row.Key]; exists {
				return errors.New("fan-out execution observation found ambiguous exact authority")
			}
			if row.Reason == "selected_fork_reserved" || row.Reason == "run_not_owned" {
				row.GrantID = ""
			}
			if closing[row.GrantID] {
				row.Reason = "registration_closing"
			}
			if pending {
				row.GrantID, row.Reason = "", "registration_pending"
			}
			row.Eligible = row.GrantID != "" && row.Reason == "eligible"
			observed[row.Key] = row
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		byID := make(map[string]startupownership.GrantEvidence, len(grants))
		for _, grant := range grants {
			byID[grant.GrantID] = grant
		}
		checked := make(map[[2]string]bool)
		for _, key := range keys {
			row, exists := observed[key]
			if !exists {
				row = startupownership.FanOutExecutionObservation{Key: key, Reason: "intent_missing"}
			}
			if row.GrantID != "" {
				coordinate := [2]string{row.GrantID, key.RunID}
				owned, exists := checked[coordinate]
				if !exists {
					grant, found := byID[row.GrantID]
					if !found {
						return errors.New("fan-out execution observation has no exact registered grant")
					}
					owned, err = s.admission.ObserveFanOutRunTx(ctx, tx, grant, key.RunID)
					if err != nil {
						return err
					}
					checked[coordinate] = owned
				}
				if !owned {
					row.GrantID, row.Eligible, row.Reason = "", false, "run_not_owned"
				}
			}
			snapshot.Rows = append(snapshot.Rows, row)
		}
		return nil
	}
	if s.postgres != nil {
		err = s.postgres.backend.RunReadTransaction(ctx, operation)
	} else {
		err = s.sqlite.backend.RunReadTransaction(ctx, operation)
	}
	if err != nil {
		return startupownership.FanOutExecutionSnapshot{}, err
	}
	return snapshot, nil
}
