package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	nativeapi "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type MailboxGateDomain struct{ Method, Actor, Key, Hash string }
type MailboxGateEvidence struct {
	Phase                                                       string
	Domain                                                      MailboxGateDomain
	CardID, Status, Verdict, DecidedBy, DecisionEventID, Anchor string
	State, Accumulator, History                                 string
	DecisionEvents, OutcomeEvents                               int
	Changes, Receipts                                           []string
	API                                                         []nativeapi.MailboxGateCompletionRow
}

// These closures retain the original owner, never a store-valued return port.
type MailboxGateObservation struct {
	Observe            func(context.Context, string, string, MailboxGateDomain) (MailboxGateEvidence, error)
	Card               func(context.Context, string) (decisioncard.Card, error)
	Activity           func(context.Context, authoractivity.ListOptions) (authoractivity.ListResult, error)
	Prepared           func(context.Context, string) (bus.PreparedPublishEvent, bool, error)
	Deliveries         func(context.Context, string) (deliverylifecycle.RunSummary, error)
	IncompleteHandoffs func(context.Context, string) (int, error)
}

func BindMailboxGateObservationForTest(original any) (MailboxGateObservation, error) {
	if err := validateChannelObservationOwner(original); err != nil {
		return MailboxGateObservation{}, err
	}
	var out MailboxGateObservation
	var card func(context.Context, string) (decisionpersistence.MailboxGateCardStorage, error)
	var instance func(context.Context, string) (pipelinepersistence.MailboxGateInstanceStorage, error)
	var events func(context.Context, string, string) (int, error)
	var changes, receipts func(context.Context, string) ([]string, error)
	var completion func(context.Context, string, string, string) ([]nativeapi.MailboxGateCompletionRow, error)
	switch s := original.(type) {
	case *PostgresStore:
		card = s.decisionPostgresOwner.ReadMailboxGateCardStorageForTest
		changes = s.decisionPostgresOwner.ReadMailboxGateChangesStorageForTest
		instance = s.pipelinePostgresOwner.ReadMailboxGateInstanceStorageForTest
		receipts = s.pipelinePostgresOwner.ReadMailboxGateReceiptsStorageForTest
		events = s.eventPostgresOwner.CountRunEventNameStorageForTest
		completion = s.postgresOwner.ReadMailboxGateCompletionDomainForTest
		out.Card = s.decisionPostgresOwner.GetDecisionCard
		out.Activity = s.activityPostgresOwner.ListAuthorActivity
		out.Prepared = s.eventPostgresOwner.LoadPreparedPublishEvent
		out.Deliveries = s.deliveryPostgresOwner.ReadServedRunDeliverySummaryForTest
		out.IncompleteHandoffs = s.pipelinePostgresOwner.ReadServedIncompletePipelineHandoffCountForTest
	case *SQLiteRuntimeStore:
		card = s.decisionSQLiteOwner.ReadMailboxGateCardStorageForTest
		changes = s.decisionSQLiteOwner.ReadMailboxGateChangesStorageForTest
		instance = s.pipelineSQLiteOwner.ReadMailboxGateInstanceStorageForTest
		receipts = s.pipelineSQLiteOwner.ReadMailboxGateReceiptsStorageForTest
		events = s.eventSQLiteOwner.CountRunEventNameStorageForTest
		completion = s.sQLiteOwner.ReadMailboxGateCompletionDomainForTest
		out.Card = s.decisionSQLiteOwner.GetDecisionCard
		out.Activity = s.activitySQLiteOwner.ListAuthorActivity
		out.Prepared = s.eventSQLiteOwner.LoadPreparedPublishEvent
		out.Deliveries = s.deliverySQLiteOwner.ReadServedRunDeliverySummaryForTest
		out.IncompleteHandoffs = s.pipelineSQLiteOwner.ReadServedIncompletePipelineHandoffCountForTest

	default:
		return out, fmt.Errorf("mailbox gate evidence requires its original native owner")
	}
	out.Observe = func(ctx context.Context, runID, cardID string, domain MailboxGateDomain) (MailboxGateEvidence, error) {
		evidence := MailboxGateEvidence{Domain: domain, CardID: cardID}
		c, err := card(ctx, cardID)
		if err != nil {
			return evidence, err
		}
		evidence.Status, evidence.Verdict, evidence.DecidedBy, evidence.DecisionEventID, evidence.Anchor = c.Status, c.Verdict, c.DecidedBy, c.DecisionEventID, c.Anchor
		state, err := instance(ctx, runID)
		if err != nil {
			return evidence, err
		}
		evidence.State, evidence.Accumulator, evidence.History = state.State, state.Accumulator, state.Config
		evidence.DecisionEvents, err = events(ctx, runID, "mailbox.card_decided")
		if err != nil {
			return evidence, err
		}
		evidence.OutcomeEvents, err = events(ctx, runID, "work.completed")
		if err != nil {
			return evidence, err
		}
		evidence.Changes, err = changes(ctx, cardID)
		if err != nil {
			return evidence, err
		}
		evidence.Receipts, err = receipts(ctx, runID)
		if err != nil {
			return evidence, err
		}
		evidence.API, err = completion(ctx, cardID, domain.Method, domain.Key)
		return evidence, err
	}
	return out, nil
}
