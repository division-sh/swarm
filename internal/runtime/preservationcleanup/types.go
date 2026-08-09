package preservationcleanup

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
)

const (
	UnavailableBundleStartupOperationName = "swarm.serve.unavailable_bundle_startup_recovery"
	UnavailableBundleStartupControlledBy  = UnavailableBundleStartupOperationName
	BundleForceDeleteOperationName        = "bundle.delete.force"
	BundleForceDeleteControlledBy         = "bundle.delete"

	BundleEphemeralOrphanedReason = "bundle_ephemeral_orphaned"
	BundleDeletedOrphanedReason   = "bundle_deleted_orphaned"
	BundleForceDeletedReason      = "bundle_force_deleted"

	SessionTerminationReasonOrphaned = "orphaned"

	DeliveryOutcomeDeadLetter = "dead_letter"
	RunStatusCancelled        = "cancelled"
	RunControlStatusStopped   = "stopped"
	TimerStatusCancelled      = "cancelled"
)

type Request struct {
	OperationName string
	RequestedAt   time.Time
	ControlledBy  string
	Targets       []RunTarget
}

type RunTarget struct {
	RunID        string
	BundleSource runbundle.AvailabilitySource
	BundleHash   string
	ReasonCode   string
}

type Result struct {
	OperationName        string
	AppliedAt            time.Time
	ControlledBy         string
	Runs                 []RunResult
	Deliveries           []DeliveryResult
	PipelineReceiptCount int
	Sessions             []SessionResult
	Timers               []TimerResult
}

type RunResult struct {
	RunID          string
	BundleSource   runbundle.AvailabilitySource
	PreviousStatus string
	Status         string
	ReasonCode     string
	Changed        bool
}

type DeliveryResult struct {
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

type SessionResult struct {
	SessionID      string
	RunID          string
	AgentID        string
	PreviousStatus string
	Status         string
	ReasonCode     string
	Changed        bool
}

type TimerResult struct {
	Family         runtimetimercancellation.Family
	TimerID        string
	RunID          string
	TimerName      string
	PreviousStatus string
	Status         string
	ReasonCode     string
	Changed        bool
}

func CauseForBundleSource(source runbundle.AvailabilitySource) (string, bool) {
	switch source {
	case runbundle.AvailabilitySourceEphemeral:
		return BundleEphemeralOrphanedReason, true
	case runbundle.AvailabilitySourceDeleted:
		return BundleDeletedOrphanedReason, true
	default:
		return "", false
	}
}

func NormalizeRunTarget(target RunTarget) (RunTarget, error) {
	target.RunID = strings.TrimSpace(target.RunID)
	target.BundleHash = strings.TrimSpace(target.BundleHash)
	target.ReasonCode = strings.TrimSpace(target.ReasonCode)
	if target.RunID == "" {
		return RunTarget{}, fmt.Errorf("preservation cleanup run_id is required")
	}
	if target.ReasonCode == "" {
		cause, ok := CauseForBundleSource(target.BundleSource)
		if !ok {
			return RunTarget{}, fmt.Errorf("preservation cleanup unsupported bundle source %q", target.BundleSource.String())
		}
		target.ReasonCode = cause
	}
	return target, nil
}

func NormalizeTargets(targets []RunTarget) ([]RunTarget, error) {
	seen := map[string]struct{}{}
	out := make([]RunTarget, 0, len(targets))
	for _, target := range targets {
		normalized, err := NormalizeRunTarget(target)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized.RunID]; ok {
			continue
		}
		seen[normalized.RunID] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func TerminalReasonCodes() []string {
	return []string{
		BundleEphemeralOrphanedReason,
		BundleDeletedOrphanedReason,
		BundleForceDeletedReason,
	}
}
