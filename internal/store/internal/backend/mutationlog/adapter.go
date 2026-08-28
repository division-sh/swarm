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
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type ActiveRunSourceOwner interface {
	RequireActiveRunSource(context.Context, string) (runtimecorrelation.BundleSourceFact, error)
}

func InsertWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, rec runtimemutationlog.Record) error {
	if tx == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("mutation log transaction is required")
	}
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	entityID := strings.TrimSpace(rec.EntityID)
	domain := rec.Domain
	path := strings.TrimSpace(rec.Path)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || writerType == "" || writerID == "" {
		return runtimemutationlog.ErrInvalidMutationLogWriter("entity_id, writer_type, and writer_id are required")
	}
	if err := runtimemutationlog.ValidateDomainPath(domain, path); err != nil {
		return err
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
			mutation_id, run_id, entity_id, domain, path, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb, $7::jsonb,
			NULLIF($8, '')::uuid, $9, $10, NULLIF($11, ''), $12
		)
	`, mutationID, runID, entityID, string(domain), path, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep), occurredAt); err != nil {
		return err
	}
	if err := effects.Add(runID, runforkrevision.FamilyEntityMutations); err != nil {
		return err
	}
	draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, occurredAt)
	if err != nil || !admitted {
		return err
	}
	return story.Record(ctx, draft)
}

func InsertEntityStateDiffWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := InsertWithStory(ctx, tx, runLifecycle, story, effects, record); err != nil {
			return err
		}
	}
	return nil
}

func InsertSQLiteWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, rec runtimemutationlog.Record) error {
	if tx == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("mutation log transaction is required")
	}
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	entityID := strings.TrimSpace(rec.EntityID)
	domain := rec.Domain
	path := strings.TrimSpace(rec.Path)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || writerType == "" || writerID == "" {
		return runtimemutationlog.ErrInvalidMutationLogWriter("entity_id, writer_type, and writer_id are required")
	}
	if err := runtimemutationlog.ValidateDomainPath(domain, path); err != nil {
		return err
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
		causedByEvent = validUUIDString(inbound.ID())
	}
	mutationID := uuid.NewString()
	occurredAt := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			mutation_id, run_id, entity_id, domain, path, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?)
	`, mutationID, runID, entityID, string(domain), path, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep), occurredAt); err != nil {
		return err
	}
	if err := effects.Add(runID, runforkrevision.FamilyEntityMutations); err != nil {
		return err
	}
	draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, occurredAt)
	if err != nil || !admitted {
		return err
	}
	return story.Record(ctx, draft)
}

func InsertSQLiteEntityStateDiffWithStory(ctx context.Context, tx *sql.Tx, runLifecycle ActiveRunSourceOwner, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
	if story == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("author activity owner is required")
	}
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := InsertSQLiteWithStory(ctx, tx, runLifecycle, story, effects, record); err != nil {
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
