package runstalled

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

const (
	EventType                = "platform.run_stalled"
	DefaultThresholdSeconds  = 300
	defaultPageLimit         = 100
	defaultPollInterval      = 30 * time.Second
	runningRunTableStatus    = "running"
	stalledOperationalStatus = "stalled"
)

type Diagnosis struct {
	OperationalState string
	BlockingLayer    string
	BlockingReason   string
}

type RunSnapshot struct {
	RunID          string
	RunTableStatus string
	FlowInstance   string
	LastProgressAt time.Time
	Diagnosis      Diagnosis
}

type RunRef struct {
	RunID string
}

type EscalationKey struct {
	RunID          string
	BlockingLayer  string
	BlockingReason string
	LastProgressAt time.Time
}

type Policy struct {
	Enabled   bool
	Threshold time.Duration
}

type Reader interface {
	ListRunningRuns(context.Context, int, string) ([]RunRef, string, error)
	LoadRunSnapshot(context.Context, string) (RunSnapshot, error)
	StalledRunEscalationExists(context.Context, EscalationKey) (bool, error)
}

type Publisher interface {
	Publish(context.Context, events.Event) error
}

type PolicyResolver func(flowInstance string) Policy

type Monitor struct {
	Reader         Reader
	Publisher      Publisher
	PolicyResolver PolicyResolver
	PageLimit      int
	PollInterval   time.Duration
	OnError        func(error)
}

type CheckResult struct {
	Scanned   int
	Published int
	Skipped   int
}

func DefaultPolicy() Policy {
	return Policy{Enabled: true, Threshold: time.Duration(DefaultThresholdSeconds) * time.Second}
}

func (m *Monitor) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	interval := m.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := m.CheckOnce(ctx, now.UTC()); err != nil {
				if ctx.Err() != nil {
					return
				}
				m.reportError(err)
			}
		}
	}
}

func (m *Monitor) CheckOnce(ctx context.Context, now time.Time) (CheckResult, error) {
	var result CheckResult
	if ctx == nil {
		ctx = context.Background()
	}
	if m == nil || m.Reader == nil || m.Publisher == nil {
		return result, fmt.Errorf("run stalled monitor requires reader and publisher")
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	limit := m.PageLimit
	if limit <= 0 {
		limit = defaultPageLimit
	}
	cursor := ""
	for {
		refs, next, err := m.Reader.ListRunningRuns(ctx, limit, cursor)
		if err != nil {
			return result, err
		}
		for _, ref := range refs {
			runID := strings.TrimSpace(ref.RunID)
			if runID == "" {
				result.Skipped++
				continue
			}
			result.Scanned++
			snapshot, err := m.Reader.LoadRunSnapshot(ctx, runID)
			if err != nil {
				return result, err
			}
			evt, key, ok, err := m.eventForSnapshot(snapshot, now)
			if err != nil {
				return result, err
			}
			if !ok {
				result.Skipped++
				continue
			}
			exists, err := m.Reader.StalledRunEscalationExists(ctx, key)
			if err != nil {
				return result, err
			}
			if exists {
				result.Skipped++
				continue
			}
			if err := m.Publisher.Publish(ctx, evt); err != nil {
				return result, err
			}
			result.Published++
		}
		if strings.TrimSpace(next) == "" {
			return result, nil
		}
		cursor = next
	}
}

func (m *Monitor) eventForSnapshot(snapshot RunSnapshot, now time.Time) (events.Event, EscalationKey, bool, error) {
	runID := strings.TrimSpace(snapshot.RunID)
	if runID == "" {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	if strings.ToLower(strings.TrimSpace(snapshot.RunTableStatus)) != runningRunTableStatus {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	if strings.ToLower(strings.TrimSpace(snapshot.Diagnosis.OperationalState)) != stalledOperationalStatus {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	if snapshot.LastProgressAt.IsZero() {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	policy := m.resolvePolicy(snapshot.FlowInstance)
	if !policy.Enabled || policy.Threshold <= 0 {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	lastProgressAt := snapshot.LastProgressAt.UTC()
	stalledFor := now.Sub(lastProgressAt)
	if stalledFor < policy.Threshold {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	blockingLayer := strings.TrimSpace(snapshot.Diagnosis.BlockingLayer)
	blockingReason := strings.TrimSpace(snapshot.Diagnosis.BlockingReason)
	if blockingLayer == "" || blockingReason == "" {
		return events.EmptyEvent(), EscalationKey{}, false, nil
	}
	key := EscalationKey{
		RunID:          runID,
		BlockingLayer:  blockingLayer,
		BlockingReason: blockingReason,
		LastProgressAt: lastProgressAt,
	}
	payload, err := json.Marshal(map[string]any{
		"run_id":              runID,
		"flow_instance":       strings.Trim(strings.TrimSpace(snapshot.FlowInstance), "/"),
		"operational_state":   stalledOperationalStatus,
		"blocking_layer":      blockingLayer,
		"blocking_reason":     blockingReason,
		"last_progress_at":    lastProgressAt.Format(time.RFC3339Nano),
		"threshold_seconds":   int(policy.Threshold / time.Second),
		"stalled_for_seconds": int(stalledFor / time.Second),
		"emitted_at":          now.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return events.EmptyEvent(), EscalationKey{}, false, err
	}
	evt := events.NewRuntimeDiagnosticEvent("", events.EventType(EventType), events.PlatformProducer("runtime"), "", payload, 0, runID, "", events.EventEnvelope{FlowInstance: snapshot.FlowInstance}, now.UTC())
	return evt, key, true, nil
}

func (m *Monitor) resolvePolicy(flowInstance string) Policy {
	if m != nil && m.PolicyResolver != nil {
		policy := m.PolicyResolver(flowInstance)
		if policy.Threshold > 0 {
			return policy
		}
		return Policy{Enabled: policy.Enabled, Threshold: DefaultPolicy().Threshold}
	}
	return DefaultPolicy()
}

func (m *Monitor) reportError(err error) {
	if err == nil {
		return
	}
	if m != nil && m.OnError != nil {
		m.OnError(err)
	}
}

func LastProgressAtString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
