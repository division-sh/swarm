package serveapp

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runstalled"
)

type serveRunStalledReader struct {
	store runstalled.ProjectionReader
}

func startServeRunStalledEscalation(ctx context.Context, owner *worklifetime.Process, projection runstalled.ProjectionReader, eventBus *bus.EventBus, posture executionposture.Posture) (func(), error) {
	reader, ok := newServeRunStalledReader(projection)
	if !ok || eventBus == nil {
		return func() {}, nil
	}
	monitor := &runstalled.Monitor{
		Reader:           reader,
		Publisher:        eventBus,
		ExecutionPosture: posture,
		OnError: func(err error) {
			log.Printf("run stalled escalation monitor: %v", err)
		},
	}
	if owner == nil {
		return nil, fmt.Errorf("run stalled monitor requires a process work owner")
	}
	ctx, cancel := context.WithCancel(ctx)
	lease, err := owner.Begin(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("admit run stalled monitor: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		monitor.Run(lease.Context())
	}()
	return func() { cancel(); <-done }, nil
}

func newServeRunStalledReader(projection runstalled.ProjectionReader) (*serveRunStalledReader, bool) {
	if projection == nil {
		return nil, false
	}
	return &serveRunStalledReader{store: projection}, true
}

func (r *serveRunStalledReader) ListRunningRuns(ctx context.Context, limit int, cursor string) ([]runstalled.RunRef, string, error) {
	if r == nil || r.store == nil {
		return nil, "", fmt.Errorf("run stalled reader requires store")
	}
	headers, next, err := r.store.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{
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
	report, err := r.store.LoadRunDebugReport(ctx, strings.TrimSpace(runID), operatorread.RunDebugQueryOptions{})
	if err != nil {
		return runstalled.RunSnapshot{}, err
	}
	if report.OperationalFacts == nil {
		return runstalled.RunSnapshot{}, fmt.Errorf("run stalled monitor requires coherent operational evidence")
	}
	return runStalledSnapshotFromDebugReport(report, report.OperationalFacts.LastProgressAt), nil
}

func (r *serveRunStalledReader) StalledRunEscalationExists(ctx context.Context, key runstalled.EscalationKey) (bool, error) {
	if r == nil || r.store == nil {
		return false, fmt.Errorf("run stalled reader requires store")
	}
	result, err := r.store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{
			RunID:     key.RunID,
			EventName: runstalled.EventType,
		},
		Limit: 1000,
		Order: "desc",
	})
	if err != nil {
		return false, err
	}
	for _, evt := range result.Events {
		matches, err := key.MatchesPayload(evt.Payload)
		if err != nil {
			return false, err
		}
		if matches {
			return true, nil
		}
	}
	return false, nil
}

func runStalledSnapshotFromDebugReport(report operatorread.RunDebugReport, progressAt time.Time) runstalled.RunSnapshot {
	report.LastEventAt = progressAt
	status := operatorread.ProjectRunOperationalStatus(report)
	instances := make([]runstalled.StalledInstance, 0)
	for _, attribution := range status.Attributions {
		if !attribution.Historical {
			instances = append(instances, runstalled.StalledInstance{
				FlowInstance: attribution.FlowInstance, BlockingLayer: status.BlockingLayer,
				BlockingReason: status.BlockingReason, Attribution: attribution,
			})
		}
	}
	return runstalled.RunSnapshot{
		RunID:          strings.TrimSpace(report.RunID),
		RunTableStatus: strings.TrimSpace(report.RunTableStatus),
		LastProgressAt: progressAt,
		Diagnosis: runstalled.Diagnosis{
			OperationalState: strings.TrimSpace(status.State),
			BlockingLayer:    strings.TrimSpace(status.BlockingLayer),
			BlockingReason:   strings.TrimSpace(status.BlockingReason),
			StalledInstances: instances,
		},
	}
}
