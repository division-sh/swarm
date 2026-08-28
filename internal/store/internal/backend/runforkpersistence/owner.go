package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storeeffect "github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type schemaAdmissionOwner interface {
	requireCurrentSchema() error
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type eventCommitOwner interface {
	AppendAdmittedEventTxOutcome(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
}

type conversationForkSourceReader interface {
	LoadConversationForkSource(context.Context, string) (runtimerunfork.ConversationForkSource, error)
}

type runLifecycleCandidateHandoffReservation = storerunhandoff.CandidateHandoff
type persistedAgentProjection = storeagent.PersistedAgentProjection

var agentIdentityFields = storeagent.IdentityFields
var hydratePersistedAgentConfig = storeagent.HydrateAgentConfig
var DecodeConversationRuntimeStateDescriptor = storeoperatorsurface.DecodeConversationRuntimeStateDescriptor

func traceTimePtr(value time.Time) *time.Time { return storeoperatorsurface.TraceTimePtr(value) }

var cloneRawMessage = storeoperatorsurface.CloneRawMessage
var cloneConversationToolCalls = storeoperatorsurface.CloneConversationToolCalls
var cloneConversationToolResults = storeoperatorsurface.CloneConversationToolResults

type RunForkPostgresOwner struct {
	*storerunlifecycle.RunLifecyclePostgresOwner
	*storedecision.DecisionPostgresOwner
	*storedelivery.DeliveryPostgresOwner
	*storeeffect.EffectPostgresOwner
	*storepipeline.PipelinePostgresOwner

	backend        *postgresbackend.Backend
	requireCurrent func() error
	events         eventCommitOwner
	conversations  conversationForkSourceReader
	durableData    *storedurabledata.Owner
}

type RunForkSQLiteOwner struct {
	*storerunlifecycle.RunLifecycleSQLiteOwner
	*storedecision.DecisionSQLiteOwner
	*storedelivery.DeliverySQLiteOwner
	*storeeffect.EffectSQLiteOwner
	*storepipeline.PipelineSQLiteOwner

	backend        *sqlitebackend.Backend
	requireCurrent func() error
	nowFn          func() time.Time
	events         eventCommitOwner
	conversations  conversationForkSourceReader
	durableData    *storedurabledata.Owner
}

func NewPostgres(
	backend *postgresbackend.Backend,
	requireCurrent func() error,
	lifecycle *storerunlifecycle.RunLifecyclePostgresOwner,
	decision *storedecision.DecisionPostgresOwner,
	delivery *storedelivery.DeliveryPostgresOwner,
	effects *storeeffect.EffectPostgresOwner,
	pipeline *storepipeline.PipelinePostgresOwner,
	events eventCommitOwner,
	conversations conversationForkSourceReader,
	durableData *storedurabledata.Owner,
) (*RunForkPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || decision == nil || delivery == nil || effects == nil || pipeline == nil || events == nil || conversations == nil || durableData == nil {
		return nil, errors.New("run-fork PostgreSQL owner dependencies are required")
	}
	return &RunForkPostgresOwner{
		RunLifecyclePostgresOwner: lifecycle,
		DecisionPostgresOwner:     decision,
		DeliveryPostgresOwner:     delivery,
		EffectPostgresOwner:       effects,
		PipelinePostgresOwner:     pipeline,
		backend:                   backend,
		requireCurrent:            requireCurrent,
		events:                    events,
		conversations:             conversations,
		durableData:               durableData,
	}, nil
}

func NewSQLite(
	backend *sqlitebackend.Backend,
	requireCurrent func() error,
	lifecycle *storerunlifecycle.RunLifecycleSQLiteOwner,
	decision *storedecision.DecisionSQLiteOwner,
	delivery *storedelivery.DeliverySQLiteOwner,
	effects *storeeffect.EffectSQLiteOwner,
	pipeline *storepipeline.PipelineSQLiteOwner,
	events eventCommitOwner,
	conversations conversationForkSourceReader,
	durableData *storedurabledata.Owner,
	now func() time.Time,
) (*RunForkSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || decision == nil || delivery == nil || effects == nil || pipeline == nil || events == nil || conversations == nil || durableData == nil {
		return nil, errors.New("run-fork SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &RunForkSQLiteOwner{
		RunLifecycleSQLiteOwner: lifecycle,
		DecisionSQLiteOwner:     decision,
		DeliverySQLiteOwner:     delivery,
		EffectSQLiteOwner:       effects,
		PipelineSQLiteOwner:     pipeline,
		backend:                 backend,
		requireCurrent:          requireCurrent,
		nowFn:                   now,
		events:                  events,
		conversations:           conversations,
		durableData:             durableData,
	}, nil
}

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func workflowCommitJSONEqual(actual, expected []byte) bool {
	actualValue, actualErr := runtimecanonicaljson.Decode(actual)
	expectedValue, expectedErr := runtimecanonicaljson.Decode(expected)
	if actualErr != nil || expectedErr != nil {
		return false
	}
	actualCanonical, actualErr := runtimecanonicaljson.Encode(actualValue)
	expectedCanonical, expectedErr := runtimecanonicaljson.Encode(expectedValue)
	return actualErr == nil && expectedErr == nil && bytes.Equal(actualCanonical, expectedCanonical)
}

var postgresDeliveryAdapter = mustDeliveryAdapter(storedelivery.DialectPostgres)
var sqliteDeliveryAdapter = mustDeliveryAdapter(storedelivery.DialectSQLite)

func mustDeliveryAdapter(dialect storedelivery.Dialect) *storedelivery.Adapter {
	adapter, err := storedelivery.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func (s *RunForkPostgresOwner) requireCurrentSchema() error { return s.requireCurrent() }
func (s *RunForkSQLiteOwner) requireCurrentSchema() error   { return s.requireCurrent() }
func (s *RunForkSQLiteOwner) now() time.Time                { return s.nowFn().UTC() }

func (s *RunForkSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
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
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, lastErr
}
