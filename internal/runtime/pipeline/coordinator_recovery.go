package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type pipelineReceiptRecorder interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error
}

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
	runtimereplayclaim.RecipientReader
}

type Publisher interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error
}

type recoveryRuntimeLogger interface {
	LogRuntime(ctx context.Context, entry RuntimeLogEntry) error
}

type RecoveryManager struct {
	store  EventStore
	bus    Publisher
	window time.Duration
	limit  int
}

func NewRecoveryManager() *RecoveryManager {
	return &RecoveryManager{
		window: 24 * time.Hour,
		limit:  5000,
	}
}

func NewRecoveryManagerWith(store EventStore, bus Publisher) *RecoveryManager {
	rm := NewRecoveryManager()
	rm.store = store
	rm.bus = bus
	return rm
}

const (
	startupRecoveryPipelineReplayAction = "startup_recovery_pipeline_replay_aftermath"

	startupRecoveryPipelineReplayOutcomeReplayed = "replayed"
	startupRecoveryPipelineReplayOutcomeSkipped  = "skipped"
	startupRecoveryPipelineReplayOutcomeDropped  = "dropped"

	startupRecoveryPipelineReplayReasonReplayed              = "persisted_recipients_replayed"
	startupRecoveryPipelineReplayReasonClaimNotAcquired      = "replay_claim_not_acquired"
	startupRecoveryPipelineReplayReasonNoPersistedRecipients = "no_persisted_recipients"
	startupRecoveryPipelineReplayReasonQuarantined           = "replay_quarantined"
)

func startupRecoveryPipelineReplayDetail(evt events.Event, outcome, reason string, recipients []string) map[string]any {
	detail := map[string]any{
		"decision_family":           "startup_pipeline_replay",
		"decision_outcome":          strings.TrimSpace(outcome),
		"decision_reason_code":      strings.TrimSpace(reason),
		"event_id":                  strings.TrimSpace(evt.ID()),
		"event_type":                strings.TrimSpace(string(evt.Type())),
		"persisted_run_id":          strings.TrimSpace(evt.RunID()),
		"parent_event_id":           strings.TrimSpace(evt.ParentEventID()),
		"entity_id":                 evt.EntityID(),
		"flow_instance":             evt.FlowInstance(),
		"persisted_recipient_count": len(recipients),
	}
	if len(recipients) > 0 {
		copied := make([]string, 0, len(recipients))
		for _, recipient := range recipients {
			if trimmed := strings.TrimSpace(recipient); trimmed != "" {
				copied = append(copied, trimmed)
			}
		}
		detail["persisted_recipients"] = copied
		detail["persisted_recipient_count"] = len(copied)
	}
	return detail
}

func startupRecoveryPipelineReplayMessage(outcome string) string {
	switch strings.TrimSpace(outcome) {
	case startupRecoveryPipelineReplayOutcomeDropped:
		return "Startup recovery dropped persisted pipeline replay"
	case startupRecoveryPipelineReplayOutcomeSkipped:
		return "Startup recovery skipped persisted pipeline replay"
	default:
		return "Startup recovery replayed persisted pipeline event"
	}
}

func startupRecoveryPipelineReplayLevel(outcome string) diaglog.Level {
	switch strings.TrimSpace(outcome) {
	case startupRecoveryPipelineReplayOutcomeDropped:
		return diaglog.LevelWarn
	default:
		return diaglog.LevelInfo
	}
}

func logStartupRecoveryPipelineReplayAftermath(ctx context.Context, logger recoveryRuntimeLogger, evt events.Event, outcome, reason string, failure *runtimefailures.Envelope, recipients []string) {
	if logger == nil {
		return
	}
	if evt.Type() == events.EventType("platform.runtime_log") {
		return
	}
	_ = logger.LogRuntime(ctx, RuntimeLogEntry{
		Level:     startupRecoveryPipelineReplayLevel(outcome),
		Component: "pipeline-recovery",
		Action:    startupRecoveryPipelineReplayAction,
		Message:   startupRecoveryPipelineReplayMessage(outcome),
		EventID:   strings.TrimSpace(evt.ID()),
		EventType: strings.TrimSpace(string(evt.Type())),
		EntityID:  evt.EntityID(),
		Detail:    startupRecoveryPipelineReplayDetail(evt, outcome, reason, recipients),
		Failure:   runtimefailures.CloneEnvelope(failure),
	})
}

func (r *RecoveryManager) Recover(ctx context.Context) error {
	if r == nil || r.store == nil || r.bus == nil {
		return nil
	}
	replayStore, participates, err := runtimereplayclaim.RequireStore(r.store)
	if err != nil {
		return fmt.Errorf("recover pipeline receipts: %w", err)
	}
	if !participates {
		return nil
	}
	window := r.window
	if window <= 0 {
		window = 15 * time.Minute
	}
	limit := r.limit
	if limit <= 0 {
		limit = 500
	}
	logger, _ := r.bus.(recoveryRuntimeLogger)
	eventsToReplay, err := replayStore.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-window), limit)
	if err != nil {
		return err
	}
	recorder, _ := r.store.(pipelineReceiptRecorder)
	var firstErr error
	for _, record := range eventsToReplay {
		evt := record.Event
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(evt.ID()) == "" {
			continue
		}
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID())
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("claim replay event %s: %w", evt.ID(), err)
			}
			continue
		}
		if !claimed {
			logStartupRecoveryPipelineReplayAftermath(ctx, logger, evt, startupRecoveryPipelineReplayOutcomeSkipped, startupRecoveryPipelineReplayReasonClaimNotAcquired, nil, nil)
			continue
		}
		if record.ReplayFailure != nil {
			failure := *runtimefailures.CloneEnvelope(record.ReplayFailure)
			if recorder == nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: missing pipeline receipt recorder", evt.ID())
				}
				_ = lease.Release(ctx)
				continue
			}
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID(), "error", &failure); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: %w", evt.ID(), err)
				}
			}
			logStartupRecoveryPipelineReplayAftermath(ctx, logger, evt, startupRecoveryPipelineReplayOutcomeDropped, startupRecoveryPipelineReplayReasonQuarantined, &failure, nil)
			_ = lease.Release(ctx)
			continue
		}
		persistedRecipients, err := r.store.ListEventDeliveryRecipients(ctx, evt.ID())
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("load persisted recipients for replay event %s: %w", evt.ID(), err)
			}
			_ = lease.Release(ctx)
			continue
		}
		if len(persistedRecipients) == 0 {
			if scopeReader, ok := r.store.(runtimereplayclaim.ScopeReader); ok && scopeReader != nil {
				scope, err := scopeReader.LoadCommittedReplayScope(ctx, evt.ID())
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("load committed replay scope for replay event %s: %w", evt.ID(), err)
					}
					_ = lease.Release(ctx)
					continue
				}
				if scope == runtimereplayclaim.CommittedReplayScopeDirect {
					if recorder == nil {
						if firstErr == nil {
							firstErr = fmt.Errorf("mark replay event %s delivered receipt: missing pipeline receipt recorder", evt.ID())
						}
						_ = lease.Release(ctx)
						continue
					}
					if err := recorder.UpsertPipelineReceipt(ctx, evt.ID(), "processed", nil); err != nil {
						if firstErr == nil {
							firstErr = fmt.Errorf("mark replay event %s delivered receipt: %w", evt.ID(), err)
						}
					}
					logStartupRecoveryPipelineReplayAftermath(ctx, logger, evt, startupRecoveryPipelineReplayOutcomeSkipped, startupRecoveryPipelineReplayReasonNoPersistedRecipients, nil, nil)
					_ = lease.Release(ctx)
					continue
				}
			}
		}
		if err := r.bus.PublishPersistedRecipients(ctx, evt, persistedRecipients); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("replay event %s: %w", evt.ID(), err)
			}
			_ = lease.Release(ctx)
			continue
		}
		if recorder != nil {
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID(), "processed", nil); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("mark replay event %s delivered receipt: %w", evt.ID(), err)
			}
		}
		logStartupRecoveryPipelineReplayAftermath(ctx, logger, evt, startupRecoveryPipelineReplayOutcomeReplayed, startupRecoveryPipelineReplayReasonReplayed, nil, persistedRecipients)
		_ = lease.Release(ctx)
	}
	return firstErr
}
