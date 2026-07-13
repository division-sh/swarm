package runquiescence

import (
	"context"
	"time"
)

const (
	ServeAbandonOperationName = "swarm.serve.abandon_active_runs"
	ServeAbandonReasonCode    = "server_restart_abandon"
	ServeAbandonControlledBy  = ServeAbandonOperationName
	ServeAbandonDeliveryNote  = "server restart abandoned active delivery"
)

type Request struct {
	OperationName string
	DryRun        bool
	RequestedAt   time.Time
	RunIDs        []string
	AllActiveRuns bool
	ReasonCode    string
	ControlledBy  string
	DeliveryNote  string
}

type Result struct {
	OperationName        string
	DryRun               bool
	AppliedAt            time.Time
	ReasonCode           string
	ControlledBy         string
	Runs                 []QuiescedRun
	Deliveries           []QuiescedDelivery
	PipelineReceiptCount int
	SessionCount         int
	TimerCount           int
}

type ServeAbandonStore interface {
	ApplyServeAbandonActiveRunQuiescence(context.Context, time.Time) (Result, error)
}

type QuiescedRun struct {
	RunID          string
	PreviousStatus string
	Status         string
	ReasonCode     string
	Changed        bool
}

type QuiescedDelivery struct {
	DeliveryID      string
	RunID           string
	EventID         string
	SubscriberType  string
	SubscriberID    string
	PreviousStatus  string
	Status          string
	ReasonCode      string
	PreviousReason  string
	ActiveSessionID string
	Changed         bool
}
