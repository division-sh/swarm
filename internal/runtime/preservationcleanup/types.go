package preservationcleanup

import (
	"fmt"
	"strings"
	"time"

	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

const (
	UnavailableBundleStartupOperationName = "swarm.serve.unavailable_bundle_startup_recovery"
	UnavailableBundleStartupControlledBy  = UnavailableBundleStartupOperationName
	BundleForceDeleteOperationName        = "bundle.delete.force"
	BundleForceDeleteControlledBy         = "bundle.delete"

	BundleEphemeralOrphanedReason = "bundle_ephemeral_orphaned"
	BundleDeletedOrphanedReason   = "bundle_deleted_orphaned"
	BundleLegacyOrphanedReason    = "bundle_legacy_orphaned"
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
	RunID             string
	BundleSource      string
	BundleHash        string
	BundleFingerprint string
	ReasonCode        string
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
	BundleSource   string
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
	TimerID        string
	RunID          string
	TimerName      string
	PreviousStatus string
	Status         string
	ReasonCode     string
	Changed        bool
}

func CauseForBundleSource(source string) (string, bool) {
	switch strings.TrimSpace(source) {
	case storerunlifecycle.BundleSourceEphemeral:
		return BundleEphemeralOrphanedReason, true
	case storerunlifecycle.BundleSourceDeleted:
		return BundleDeletedOrphanedReason, true
	case storerunlifecycle.BundleSourceLegacy:
		return BundleLegacyOrphanedReason, true
	default:
		return "", false
	}
}

func NormalizeRunTarget(target RunTarget) (RunTarget, error) {
	target.RunID = strings.TrimSpace(target.RunID)
	target.BundleHash = strings.TrimSpace(target.BundleHash)
	target.BundleFingerprint = strings.TrimSpace(target.BundleFingerprint)
	target.ReasonCode = strings.TrimSpace(target.ReasonCode)
	source, err := storerunlifecycle.CanonicalBundleSource(target.BundleSource)
	if err != nil {
		return RunTarget{}, err
	}
	target.BundleSource = source
	if target.RunID == "" {
		return RunTarget{}, fmt.Errorf("preservation cleanup run_id is required")
	}
	if target.ReasonCode == "" {
		cause, ok := CauseForBundleSource(source)
		if !ok {
			return RunTarget{}, fmt.Errorf("preservation cleanup unsupported bundle source %q", source)
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
		BundleLegacyOrphanedReason,
		BundleForceDeletedReason,
	}
}
