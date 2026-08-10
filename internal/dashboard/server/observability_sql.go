package server

import (
	"context"
	"errors"
	"strings"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type EventFilter struct {
	Type           string
	Source         string
	EntityID       string
	SubscriberID   string
	SubscriberType string
	After          time.Time
}

type RuntimeLogFilter struct {
	Type      string
	Source    string
	EntityID  string
	Component string
	Level     string
	ErrorCode string
	Order     string
	After     time.Time
}

type IncidentFilter struct {
	SinceHours int
	MCPOnly    bool
	Level      string
	Component  string
	Limit      int
}

type eventDeliveryRecord struct {
	DeliveryID     string                    `json:"delivery_id,omitempty"`
	SubscriberType string                    `json:"subscriber_type,omitempty"`
	SubscriberID   string                    `json:"subscriber_id,omitempty"`
	Status         string                    `json:"status,omitempty"`
	Failure        *runtimefailures.Envelope `json:"failure,omitempty"`
	RetryCount     int                       `json:"retry_count,omitempty"`
}

type deliveryLifecycleSummary struct {
	Pending    int `json:"pending,omitempty"`
	InProgress int `json:"in_progress,omitempty"`
	Delivered  int `json:"delivered,omitempty"`
	Failed     int `json:"failed,omitempty"`
	DeadLetter int `json:"dead_letter,omitempty"`
}

func (s *deliveryLifecycleSummary) record(status string) {
	if s == nil {
		return
	}
	switch strings.TrimSpace(status) {
	case "pending":
		s.Pending++
	case "in_progress":
		s.InProgress++
	case "delivered":
		s.Delivered++
	case "failed":
		s.Failed++
	case "dead_letter":
		s.DeadLetter++
	}
}

type eventRecord struct {
	ID                string                   `json:"id"`
	EventID           string                   `json:"event_id,omitempty"`
	Type              string                   `json:"type,omitempty"`
	CreatedAt         string                   `json:"created_at,omitempty"`
	SourceAgent       string                   `json:"source_agent,omitempty"`
	EntityID          string                   `json:"entity_id,omitempty"`
	Scope             string                   `json:"scope,omitempty"`
	ParentEventID     string                   `json:"parent_event_id,omitempty"`
	Payload           any                      `json:"payload,omitempty"`
	DeliveryLifecycle deliveryLifecycleSummary `json:"delivery_lifecycle,omitempty"`
	Deliveries        []eventDeliveryRecord    `json:"deliveries,omitempty"`
	ErrorCount        int                      `json:"error_count,omitempty"`
	DeadCount         int                      `json:"dead_count,omitempty"`
	PendingCount      int                      `json:"pending_count,omitempty"`
}

type runtimeLogRecord struct {
	ID            string                    `json:"id"`
	EventID       string                    `json:"event_id,omitempty"`
	TS            string                    `json:"ts,omitempty"`
	Level         string                    `json:"level,omitempty"`
	Component     string                    `json:"component,omitempty"`
	Action        string                    `json:"action,omitempty"`
	EventType     string                    `json:"event_type,omitempty"`
	ParentEventID string                    `json:"parent_event_id,omitempty"`
	HandlerID     string                    `json:"handler_id,omitempty"`
	ErrorCode     string                    `json:"error_code,omitempty"`
	Failure       *runtimefailures.Envelope `json:"failure,omitempty"`
	AgentID       string                    `json:"agent_id,omitempty"`
	EntityID      string                    `json:"entity_id,omitempty"`
	SessionID     string                    `json:"session_id,omitempty"`
	DurationUS    int                       `json:"duration_us,omitempty"`
	Source        string                    `json:"source,omitempty"`
	Message       string                    `json:"message,omitempty"`
	DeliveryState string                    `json:"delivery_state,omitempty"`
	PreviousState string                    `json:"delivery_previous_state,omitempty"`
	Transition    string                    `json:"delivery_transition,omitempty"`
	Reason        string                    `json:"delivery_reason,omitempty"`
	Terminal      string                    `json:"delivery_terminal_outcome,omitempty"`
	RetryCount    int                       `json:"delivery_retry_count,omitempty"`
	Detail        any                       `json:"detail,omitempty"`
	Correlation   any                       `json:"correlation,omitempty"`
}

type incidentRecord struct {
	Code       string   `json:"code"`
	Count      int      `json:"count,omitempty"`
	RootCause  string   `json:"root_cause,omitempty"`
	Component  string   `json:"component,omitempty"`
	Level      string   `json:"level,omitempty"`
	Agents     []string `json:"agents,omitempty"`
	Components []string `json:"components,omitempty"`
	Actions    []string `json:"actions,omitempty"`
	FirstSeen  string   `json:"first_seen,omitempty"`
	LastSeen   string   `json:"last_seen,omitempty"`
}

type dashboardObservabilityReadOwner interface {
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
	ListOperatorRuntimeIncidents(context.Context, operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error)
}

type ObservabilityProjection struct {
	owner dashboardObservabilityReadOwner
}

func NewObservabilityProjection(owner dashboardObservabilityReadOwner) *ObservabilityProjection {
	if owner == nil {
		return nil
	}
	return &ObservabilityProjection{owner: owner}
}

func (r *ObservabilityProjection) ListEvents(ctx context.Context, filter EventFilter, limit int) ([]eventRecord, error) {
	if r == nil || r.owner == nil {
		return nil, errors.New("dashboard observability owner is required")
	}
	result, err := r.owner.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{
			EntityID:       filter.EntityID,
			EventName:      filter.Type,
			SubscriberID:   filter.SubscriberID,
			SubscriberType: filter.SubscriberType,
		},
		Source:             filter.Source,
		Since:              dashboardTimePtr(filter.After),
		Limit:              limit,
		ExcludeRuntimeLogs: true,
	})
	if err != nil {
		return nil, err
	}
	out := make([]eventRecord, 0, len(result.Events))
	for _, event := range result.Events {
		out = append(out, dashboardEventRecord(event))
	}
	return out, nil
}

func (r *ObservabilityProjection) GetEvent(ctx context.Context, id string) (eventRecord, bool, error) {
	if r == nil || r.owner == nil {
		return eventRecord{}, false, errors.New("dashboard observability owner is required")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return eventRecord{}, false, nil
	}
	event, err := r.owner.LoadOperatorEvent(ctx, id)
	if err == operatorread.ErrEventNotFound {
		return eventRecord{}, false, nil
	}
	if err != nil {
		return eventRecord{}, false, err
	}
	return dashboardEventRecord(event), true, nil
}

func (r *ObservabilityProjection) ListRuntimeLogs(ctx context.Context, filter RuntimeLogFilter, limit int) ([]runtimeLogRecord, error) {
	if r == nil || r.owner == nil {
		return nil, errors.New("dashboard observability owner is required")
	}
	result, err := r.owner.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		EntityID:          filter.EntityID,
		Component:         filter.Component,
		Level:             filter.Level,
		ErrorCode:         filter.ErrorCode,
		Source:            filter.Source,
		ActionOrEventType: filter.Type,
		Since:             dashboardTimePtr(filter.After),
		Limit:             limit,
		Order:             filter.Order,
	})
	if err != nil {
		return nil, err
	}
	out := make([]runtimeLogRecord, 0, len(result.Logs))
	for _, log := range result.Logs {
		out = append(out, dashboardRuntimeLogRecord(log))
	}
	return out, nil
}

func (r *ObservabilityProjection) ListIncidents(ctx context.Context, filter IncidentFilter) ([]incidentRecord, error) {
	if r == nil || r.owner == nil {
		return nil, errors.New("dashboard observability owner is required")
	}
	result, err := r.owner.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{
		SinceHours: filter.SinceHours,
		Component:  filter.Component,
		Level:      filter.Level,
		MCPOnly:    filter.MCPOnly,
		Limit:      filter.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]incidentRecord, 0, len(result.Incidents))
	for _, incident := range result.Incidents {
		out = append(out, dashboardIncidentRecord(incident))
	}
	return out, nil
}

func dashboardEventRecord(event operatorread.OperatorEventFull) eventRecord {
	record := eventRecord{
		ID:          event.EventID,
		EventID:     event.EventID,
		Type:        event.EventName,
		CreatedAt:   formatTime(event.CreatedAt),
		SourceAgent: event.Source,
		EntityID:    event.EntityID,
		Payload:     event.Payload,
		Deliveries:  make([]eventDeliveryRecord, 0, len(event.Deliveries)),
	}
	for _, delivery := range event.Deliveries {
		record.Deliveries = append(record.Deliveries, eventDeliveryRecord{
			DeliveryID:     delivery.DeliveryID,
			SubscriberType: delivery.SubscriberType,
			SubscriberID:   delivery.SubscriberID,
			Status:         delivery.Status,
			Failure:        runtimefailures.CloneEnvelope(delivery.Failure),
			RetryCount:     delivery.RetryCount,
		})
		record.DeliveryLifecycle.record(delivery.Status)
	}
	record.PendingCount = record.DeliveryLifecycle.Pending
	record.ErrorCount = record.DeliveryLifecycle.Failed
	record.DeadCount = record.DeliveryLifecycle.DeadLetter
	return record
}

func dashboardRuntimeLogRecord(log operatorread.OperatorRuntimeLogEntry) runtimeLogRecord {
	return runtimeLogRecord{
		ID:            log.LogID,
		EventID:       log.EventID,
		TS:            formatTime(log.TS),
		Level:         log.Level,
		Component:     log.Component,
		Action:        log.Action,
		EventType:     log.EventType,
		ParentEventID: log.ParentEventID,
		HandlerID:     log.HandlerID,
		ErrorCode:     log.ErrorCode,
		Failure:       runtimefailures.CloneEnvelope(log.Failure),
		AgentID:       log.AgentID,
		EntityID:      log.EntityID,
		SessionID:     log.SessionID,
		DurationUS:    log.DurationUS,
		Source:        log.Source,
		Message:       log.Message,
		DeliveryState: log.DeliveryState,
		PreviousState: log.PreviousState,
		Transition:    log.Transition,
		Reason:        log.Reason,
		Terminal:      log.Terminal,
		RetryCount:    log.RetryCount,
		Detail:        log.CanonicalDetail,
		Correlation:   log.Correlation,
	}
}

func dashboardIncidentRecord(incident operatorread.OperatorRuntimeIncident) incidentRecord {
	return incidentRecord{
		Code:       incident.ErrorCode,
		Count:      incident.Count,
		RootCause:  incident.SampleMessage,
		Component:  incident.Component,
		Level:      incident.Level,
		Agents:     incident.Agents,
		Components: incident.Components,
		Actions:    incident.Actions,
		FirstSeen:  formatTime(incident.FirstSeen),
		LastSeen:   formatTime(incident.LastSeen),
	}
}

func dashboardTimePtr(ts time.Time) *time.Time {
	if ts.IsZero() {
		return nil
	}
	value := ts.UTC()
	return &value
}
