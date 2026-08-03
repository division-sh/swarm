// Package mutationlog owns entity-mutation SQL inside the selected-store
// boundary. Runtime packages define semantic records and projections only.
package mutationlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/google/uuid"
)

type ActiveRunSourceOwner interface {
	RequireActiveRunSource(context.Context, string) (runtimecorrelation.BundleSourceFact, error)
}

func InsertWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, rec runtimemutationlog.Record) error {
	if tx == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("mutation log transaction is required")
	}
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	entityID := strings.TrimSpace(rec.EntityID)
	field := strings.TrimSpace(rec.Field)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || field == "" || writerType == "" || writerID == "" {
		return runtimemutationlog.ErrInvalidMutationLogWriter("entity_id, field, writer_type, and writer_id are required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter(err.Error())
	}
	if runLifecycle == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("run lifecycle owner is required")
	}
	runFact, err := runLifecycle.RequireActiveRunSource(ctx, runID)
	if err != nil {
		return err
	}
	contextFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return fmt.Errorf("mutation log bundle source fact is required")
	}
	if !runFact.Matches(contextFact) {
		return fmt.Errorf("mutation log bundle source fact does not match active run")
	}
	oldValue, err := jsonbArg(rec.OldValue)
	if err != nil {
		return err
	}
	newValue, err := jsonbArg(rec.NewValue)
	if err != nil {
		return err
	}
	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		if parsed := validUUIDString(inbound.ID()); parsed != "" {
			causedByEvent = parsed
		}
	}
	mutationID := uuid.NewString()
	occurredAt := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			mutation_id, run_id, entity_id, field, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5::jsonb, $6::jsonb,
			NULLIF($7, '')::uuid, $8, $9, NULLIF($10, ''), $11
		)
	`, mutationID, runID, entityID, field, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep), occurredAt); err != nil {
		return err
	}
	draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, occurredAt)
	if err != nil || !admitted {
		return err
	}
	return story.Record(ctx, draft)
}

func InsertEntityStateDiffWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := InsertWithStory(ctx, tx, runLifecycle, story, record); err != nil {
			return err
		}
	}
	return nil
}

func validUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func jsonbArg(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	case []byte:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(raw), nil
	}
}
