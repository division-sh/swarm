package bundledelete

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/runtime/destructivereset"
	"swarm/internal/runtime/preservationcleanup"
)

const (
	DefaultOperationName = "bundle.delete"
	ForceControlledBy    = DefaultOperationName
)

var (
	ErrInvalidRequest      = errors.New("bundle delete invalid request")
	ErrBundleNotFound      = errors.New("bundle delete bundle not found")
	ErrOperationInProgress = errors.New("bundle delete operation already in progress")
	ErrActiveRunsRemain    = errors.New("bundle delete active runs remain before final mutation")
	ErrNonForceSplit       = errors.New("bundle delete non-force behavior is split")
)

type Request struct {
	ActorTokenID string
	RequestHash  string
	BundleHash   string
	Force        bool
	DryRun       bool
	RequestedAt  time.Time
}

type Result struct {
	OK                  bool                                  `json:"ok"`
	Status              string                                `json:"status"`
	OperationName       string                                `json:"operation_name"`
	BundleHash          string                                `json:"bundle_hash"`
	Force               bool                                  `json:"force"`
	Deleted             bool                                  `json:"deleted"`
	DryRun              bool                                  `json:"dry_run"`
	ActiveRunsStopped   int                                   `json:"active_runs_stopped"`
	DeliveriesCancelled int                                   `json:"deliveries_cancelled"`
	ContainersStopped   int                                   `json:"containers_stopped"`
	PartialFailure      bool                                  `json:"partial_failure"`
	Plan                Plan                                  `json:"plan"`
	Cleanup             preservationcleanup.Result            `json:"cleanup"`
	Containers          destructivereset.ContainerResetResult `json:"containers"`
	FinalMutation       FinalMutationResult                   `json:"final_mutation"`
	Errors              []PartialError                        `json:"errors,omitempty"`
}

type PartialError struct {
	Scope   string `json:"scope"`
	Message string `json:"message"`
}

type Plan struct {
	BundleHash       string                          `json:"bundle_hash"`
	PlannedAt        time.Time                       `json:"planned_at"`
	ActiveRuns       []RunRef                        `json:"active_runs"`
	NonActiveRuns    []RunRef                        `json:"non_active_runs"`
	AffectedRuns     []RunRef                        `json:"affected_runs"`
	ActiveDeliveries []DeliveryRef                   `json:"active_deliveries"`
	ActiveSessions   []SessionRef                    `json:"active_sessions"`
	ActiveTimers     []TimerRef                      `json:"active_timers"`
	EntityContainers []destructivereset.ContainerRef `json:"entity_containers"`
}

type RunRef struct {
	RunID             string `json:"run_id"`
	Status            string `json:"status"`
	BundleHash        string `json:"bundle_hash,omitempty"`
	BundleSource      string `json:"bundle_source,omitempty"`
	BundleFingerprint string `json:"bundle_fingerprint,omitempty"`
}

type DeliveryRef struct {
	DeliveryID     string `json:"delivery_id"`
	RunID          string `json:"run_id"`
	EventID        string `json:"event_id"`
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
}

type SessionRef struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
	AgentID   string `json:"agent_id,omitempty"`
	Status    string `json:"status"`
}

type TimerRef struct {
	TimerID   string `json:"timer_id"`
	RunID     string `json:"run_id"`
	TimerName string `json:"timer_name"`
	Status    string `json:"status"`
}

type FinalMutationRequest struct {
	OperationName string
	BundleHash    string
	RequestedAt   time.Time
}

type FinalMutationResult struct {
	OperationName         string    `json:"operation_name"`
	BundleHash            string    `json:"bundle_hash"`
	AppliedAt             time.Time `json:"applied_at"`
	RunsMarkedDeleted     int       `json:"runs_marked_deleted"`
	BundleRowsDeleted     int       `json:"bundle_rows_deleted"`
	RemainingActiveRuns   int       `json:"remaining_active_runs"`
	Deleted               bool      `json:"deleted"`
	SourceAuthorityOwner  string    `json:"source_authority_owner"`
	TransactionOrderProof []string  `json:"transaction_order_proof"`
}

type Planner interface {
	PlanBundleDelete(context.Context, Request) (Plan, error)
}

type PreservationCleaner interface {
	ApplyBundleForceDeletePreservationCleanup(context.Context, preservationcleanup.Request) (preservationcleanup.Result, error)
}

type Finalizer interface {
	ApplyBundleDeleteFinalMutation(context.Context, FinalMutationRequest) (FinalMutationResult, error)
}

type LockManager interface {
	TryAcquire(context.Context, string) (destructivereset.LockLease, bool, error)
}

type ManagedContainerInventoryReader interface {
	ManagedResetContainerInventory(context.Context) ([]destructivereset.ContainerRef, error)
}

type ManagedContainerStopper interface {
	Apply(context.Context, destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error)
}

func NormalizeRequest(req Request, now time.Time) (Request, error) {
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.BundleHash = strings.TrimSpace(req.BundleHash)
	if req.ActorTokenID == "" {
		return Request{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if req.BundleHash == "" {
		return Request{}, fmt.Errorf("%w: bundle_hash is required", ErrInvalidRequest)
	}
	if !req.Force {
		return Request{}, ErrNonForceSplit
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = now.UTC()
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = time.Now().UTC()
	}
	req.RequestedAt = req.RequestedAt.UTC()
	return req, nil
}

func ActiveRunTargets(plan Plan) []preservationcleanup.RunTarget {
	out := make([]preservationcleanup.RunTarget, 0, len(plan.ActiveRuns))
	for _, run := range plan.ActiveRuns {
		out = append(out, preservationcleanup.RunTarget{
			RunID:             strings.TrimSpace(run.RunID),
			BundleSource:      strings.TrimSpace(run.BundleSource),
			BundleHash:        strings.TrimSpace(run.BundleHash),
			BundleFingerprint: strings.TrimSpace(run.BundleFingerprint),
			ReasonCode:        preservationcleanup.BundleForceDeletedReason,
		})
	}
	return out
}

func AffectedRunIDs(plan Plan) map[string]struct{} {
	out := make(map[string]struct{}, len(plan.AffectedRuns))
	for _, run := range plan.AffectedRuns {
		if runID := strings.TrimSpace(run.RunID); runID != "" {
			out[runID] = struct{}{}
		}
	}
	return out
}

func AffectedRunIDList(plan Plan) []string {
	out := make([]string, 0, len(plan.AffectedRuns))
	for _, run := range plan.AffectedRuns {
		if runID := strings.TrimSpace(run.RunID); runID != "" {
			out = append(out, runID)
		}
	}
	return out
}
