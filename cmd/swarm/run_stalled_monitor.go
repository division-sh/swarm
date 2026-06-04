package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/runstalled"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

type runStalledReadStore interface {
	ListRunHeaders(context.Context, store.RunHeaderListOptions) ([]store.RunHeader, string, error)
	LoadRunDebugReport(context.Context, string, store.RunDebugQueryOptions) (store.RunDebugReport, error)
	ListOperatorEvents(context.Context, store.OperatorEventListOptions) (store.OperatorEventListResult, error)
}

type serveRunStalledReader struct {
	store    runStalledReadStore
	db       *sql.DB
	postgres bool
}

func startServeRunStalledEscalation(ctx context.Context, stores storeBundle, contexts []serveRuntimeBundleContext, eventBus *bus.EventBus) {
	reader, ok := newServeRunStalledReader(stores)
	if !ok || eventBus == nil {
		return
	}
	monitor := &runstalled.Monitor{
		Reader:         reader,
		Publisher:      eventBus,
		PolicyResolver: serveRunStalledPolicyResolver(contexts),
		OnError: func(err error) {
			log.Printf("run stalled escalation monitor: %v", err)
		},
	}
	go monitor.Run(ctx)
}

func newServeRunStalledReader(stores storeBundle) (*serveRunStalledReader, bool) {
	if stores.Postgres != nil {
		return &serveRunStalledReader{store: stores.Postgres, db: stores.SQLDB, postgres: true}, true
	}
	readStore, ok := stores.ObservabilityStore.(runStalledReadStore)
	if !ok || readStore == nil {
		return nil, false
	}
	return &serveRunStalledReader{store: readStore, db: stores.SQLDB}, true
}

func (r *serveRunStalledReader) ListRunningRuns(ctx context.Context, limit int, cursor string) ([]runstalled.RunRef, string, error) {
	if r == nil || r.store == nil {
		return nil, "", fmt.Errorf("run stalled reader requires store")
	}
	headers, next, err := r.store.ListRunHeaders(ctx, store.RunHeaderListOptions{
		Status: "running",
		Limit:  limit,
		Cursor: cursor,
	})
	if err != nil {
		return nil, "", err
	}
	refs := make([]runstalled.RunRef, 0, len(headers))
	for _, header := range headers {
		runID := strings.TrimSpace(header.RunID)
		if runID == "" {
			continue
		}
		refs = append(refs, runstalled.RunRef{RunID: runID})
	}
	return refs, next, nil
}

func (r *serveRunStalledReader) LoadRunSnapshot(ctx context.Context, runID string) (runstalled.RunSnapshot, error) {
	if r == nil || r.store == nil {
		return runstalled.RunSnapshot{}, fmt.Errorf("run stalled reader requires store")
	}
	report, err := r.store.LoadRunDebugReport(ctx, strings.TrimSpace(runID), store.RunDebugQueryOptions{})
	if err != nil {
		return runstalled.RunSnapshot{}, err
	}
	flowInstance, err := r.loadLatestFlowInstance(ctx, report.RunID)
	if err != nil {
		return runstalled.RunSnapshot{}, err
	}
	progressAt, err := r.loadLatestNonEscalationProgressAt(ctx, report.RunID)
	if err != nil {
		return runstalled.RunSnapshot{}, err
	}
	return runStalledSnapshotFromDebugReport(report, flowInstance, progressAt), nil
}

func (r *serveRunStalledReader) StalledRunEscalationExists(ctx context.Context, key runstalled.EscalationKey) (bool, error) {
	if r == nil || r.store == nil {
		return false, fmt.Errorf("run stalled reader requires store")
	}
	result, err := r.store.ListOperatorEvents(ctx, store.OperatorEventListOptions{
		Filter: store.OperatorEventListFilter{
			RunID:     key.RunID,
			EventName: runstalled.EventType,
		},
		Limit: 1000,
		Order: "desc",
	})
	if err != nil {
		return false, err
	}
	lastProgressAt := runstalled.LastProgressAtString(key.LastProgressAt)
	for _, evt := range result.Events {
		if payloadString(evt.Payload["blocking_layer"]) != key.BlockingLayer {
			continue
		}
		if payloadString(evt.Payload["blocking_reason"]) != key.BlockingReason {
			continue
		}
		if payloadString(evt.Payload["last_progress_at"]) != lastProgressAt {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (r *serveRunStalledReader) loadLatestFlowInstance(ctx context.Context, runID string) (string, error) {
	runID = strings.TrimSpace(runID)
	if r == nil || r.db == nil || runID == "" {
		return "", nil
	}
	query := `
		SELECT COALESCE(flow_instance, '')
		FROM events
		WHERE run_id = ?
		  AND COALESCE(flow_instance, '') <> ''
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`
	args := []any{runID}
	if r.postgres {
		query = `
			SELECT COALESCE(flow_instance, '')
			FROM events
			WHERE run_id = $1::uuid
			  AND COALESCE(flow_instance, '') <> ''
			ORDER BY created_at DESC, event_id::text DESC
			LIMIT 1
		`
	}
	var flowInstance string
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&flowInstance)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load latest run flow instance: %w", err)
	}
	return strings.Trim(flowInstance, "/"), nil
}

func (r *serveRunStalledReader) loadLatestNonEscalationProgressAt(ctx context.Context, runID string) (time.Time, error) {
	runID = strings.TrimSpace(runID)
	if r == nil || r.db == nil || runID == "" {
		return time.Time{}, nil
	}
	query := `
		SELECT MAX(created_at)
		FROM events
		WHERE run_id = ?
		  AND event_name <> ?
	`
	args := []any{runID, runstalled.EventType}
	if r.postgres {
		query = `
			SELECT MAX(created_at)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name <> $2
		`
	}
	var raw any
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("load latest non-escalation progress timestamp: %w", err)
	}
	progressAt, _, err := runStalledTimeValue(raw)
	if err != nil {
		return time.Time{}, err
	}
	return progressAt, nil
}

func runStalledSnapshotFromDebugReport(report store.RunDebugReport, flowInstance string, progressAt time.Time) runstalled.RunSnapshot {
	report.LastEventAt = progressAt
	status := store.ProjectRunOperationalStatus(report)
	return runstalled.RunSnapshot{
		RunID:          strings.TrimSpace(report.RunID),
		RunTableStatus: strings.TrimSpace(report.RunTableStatus),
		FlowInstance:   strings.Trim(flowInstance, "/"),
		LastProgressAt: progressAt,
		Diagnosis: runstalled.Diagnosis{
			OperationalState: strings.TrimSpace(status.State),
			BlockingLayer:    strings.TrimSpace(status.BlockingLayer),
			BlockingReason:   strings.TrimSpace(status.BlockingReason),
		},
	}
}

func serveRunStalledPolicyResolver(contexts []serveRuntimeBundleContext) runstalled.PolicyResolver {
	sources := make([]semanticview.Source, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.loaded.source != nil {
			sources = append(sources, contextDef.loaded.source)
		}
	}
	return func(flowInstance string) runstalled.Policy {
		policy := runstalled.DefaultPolicy()
		source, flowID := serveRunStalledPolicyScope(sources, flowInstance)
		if source == nil {
			return policy
		}
		if value, ok := semanticview.PolicyValueForFlow(source, flowID, "runtime.stalled_run_escalation.enabled"); ok {
			if enabled, ok := runStalledPolicyBool(value.Value); ok {
				policy.Enabled = enabled
			}
		}
		if value, ok := semanticview.PolicyValueForFlow(source, flowID, "runtime.stalled_run_escalation.threshold_seconds"); ok {
			if seconds, ok := runStalledPolicySeconds(value.Value); ok && seconds > 0 {
				policy.Threshold = time.Duration(seconds) * time.Second
			}
		}
		return policy
	}
}

func serveRunStalledPolicyScope(sources []semanticview.Source, flowInstance string) (semanticview.Source, string) {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	for _, source := range sources {
		if source == nil {
			continue
		}
		if flowID := runStalledFlowIDForInstance(source.FlowScopes(), flowInstance); flowID != "" {
			return source, flowID
		}
	}
	if len(sources) > 0 {
		return sources[0], ""
	}
	return nil, ""
}

func runStalledFlowIDForInstance(scopes []semanticview.FlowScope, flowInstance string) string {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return ""
	}
	bestFlowID := ""
	bestMatchLen := -1
	for _, scope := range scopes {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		for _, candidate := range []string{scope.Path, scope.ID} {
			candidate = strings.Trim(strings.TrimSpace(candidate), "/")
			if candidate == "" {
				continue
			}
			if flowInstance != candidate && !strings.HasPrefix(flowInstance, candidate+"/") {
				continue
			}
			if len(candidate) > bestMatchLen {
				bestMatchLen = len(candidate)
				bestFlowID = flowID
			}
		}
	}
	return bestFlowID
}

func runStalledPolicyBool(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return false, false
	}
}

func runStalledPolicySeconds(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case int32:
		return int(typed), true
	case float64:
		return int(typed), typed == float64(int(typed))
	case float32:
		return int(typed), typed == float32(int(typed))
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return 0, false
	}
}

func payloadString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func runStalledTimeValue(raw any) (time.Time, bool, error) {
	switch typed := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed.UTC(), true, nil
	case string:
		return parseRunStalledTimeString(typed)
	case []byte:
		return parseRunStalledTimeString(string(typed))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported run stalled time value %T", raw)
	}
}

func parseRunStalledTimeString(raw string) (time.Time, bool, error) {
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
	return time.Time{}, false, fmt.Errorf("parse run stalled time %q: %w", raw, lastErr)
}
