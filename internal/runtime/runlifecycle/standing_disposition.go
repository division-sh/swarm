package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type StandingRestartDispositionKind string

const (
	StandingRestartOrdinary          StandingRestartDispositionKind = "ordinary"
	StandingRestartActiveIntrinsic   StandingRestartDispositionKind = "active_intrinsic"
	StandingRestartSuspended         StandingRestartDispositionKind = "suspended"
	StandingRestartCredentialDormant StandingRestartDispositionKind = "credential_dormant"
	StandingRestartRecoveryRequired  StandingRestartDispositionKind = "recovery_required"
	StandingRestartOrphaned          StandingRestartDispositionKind = "orphaned"
	StandingRestartTerminalDeclared  StandingRestartDispositionKind = "terminal_declared"
	StandingRestartTerminalOrphaned  StandingRestartDispositionKind = "terminal_orphaned"
	StandingRestartInvalidCurrent    StandingRestartDispositionKind = "invalid_current"
)

type StandingRestartRemediation string

const (
	StandingRestartNoRemediation        StandingRestartRemediation = "none"
	StandingRestartResumeOrReset        StandingRestartRemediation = "resume_or_reset"
	StandingRestartRestoreDeclaration   StandingRestartRemediation = "restore_declaration"
	StandingRestartReset                StandingRestartRemediation = "reset"
	StandingRestartRestoreThenReset     StandingRestartRemediation = "restore_then_reset"
	StandingRestartProvisionCredentials StandingRestartRemediation = "provision_credentials_then_restart"
	StandingRestartResumeChannel        StandingRestartRemediation = "resume_channel_onboarding"
)

type StandingBindingBlockReason string

const (
	StandingBindingCredentialsAbsent StandingBindingBlockReason = "credentials_absent"
	StandingBindingRecoveryRequired  StandingBindingBlockReason = "recovery_required"
)

func (r StandingBindingBlockReason) Validate(enabled bool) error {
	if enabled && r == "" || !enabled && (r == StandingBindingCredentialsAbsent || r == StandingBindingRecoveryRequired) {
		return nil
	}
	return fmt.Errorf("standing binding enabled=%t contradicts block reason %q", enabled, r)
}

func (r StandingBindingBlockReason) EffectiveState() string {
	switch r {
	case StandingBindingCredentialsAbsent:
		return "dormant"
	case StandingBindingRecoveryRequired:
		return "recovery_required"
	default:
		return ""
	}
}

func (r StandingBindingBlockReason) QuiescenceReason() string {
	switch r {
	case StandingBindingCredentialsAbsent:
		return "ingress_credentials_absent"
	case StandingBindingRecoveryRequired:
		return "ingress_authority_stale"
	default:
		return ""
	}
}

// StandingRestartFact is the complete durable state product required to
// classify one exact current standing generation. ExactCurrent=false is the
// only ordinary result and intentionally carries no partial standing facts.
type StandingRestartFact struct {
	ExactCurrent       bool
	ServiceID          string
	RunID              string
	Generation         int64
	DeclarationPresent bool
	BindingEnabled     bool
	EffectiveState     string
	OperatorOverride   string
	RunState           string
}

type StandingRestartDisposition struct {
	Kind               StandingRestartDispositionKind
	ServiceID          string
	RunID              string
	Generation         int64
	DeclarationPresent bool
	BindingEnabled     bool
	EffectiveState     string
	OperatorOverride   string
	RunState           string
	Remediation        StandingRestartRemediation
}

type StandingRestartDispositionReader interface {
	StandingRunRestartDisposition(context.Context, string) (StandingRestartDisposition, error)
}

func (d StandingRestartDisposition) ExactCurrent() bool {
	return d.Kind != "" && d.Kind != StandingRestartOrdinary
}

func (d StandingRestartDisposition) Executable() bool {
	return d.Kind == StandingRestartActiveIntrinsic
}

func (d StandingRestartDisposition) UsesGenericRecovery() bool {
	return d.Kind == StandingRestartOrdinary
}

func (d StandingRestartDisposition) RunControlGuidance() string {
	if d.Kind == StandingRestartActiveIntrinsic {
		return fmt.Sprintf("use `swarm standing suspend %s` or `swarm standing reset %s`", d.ServiceID, d.ServiceID)
	}
	switch d.Remediation {
	case StandingRestartNoRemediation:
		return "use standing service controls"
	case StandingRestartResumeOrReset:
		return fmt.Sprintf("use `swarm standing resume %s` or `swarm standing reset %s`", d.ServiceID, d.ServiceID)
	case StandingRestartRestoreDeclaration:
		return "restore the standing declaration"
	case StandingRestartReset:
		return fmt.Sprintf("use `swarm standing reset %s`", d.ServiceID)
	case StandingRestartRestoreThenReset:
		return fmt.Sprintf("restore the standing declaration, then use `swarm standing reset %s`", d.ServiceID)
	case StandingRestartProvisionCredentials:
		return "provision the named ingress credentials with `swarm secrets set <key>`, then restart serve or use explicit channel connect"
	case StandingRestartResumeChannel:
		return "inspect `swarm channel status`, then use `swarm channel resume <operation-id>` with fresh credentials and complete its required ceremony"
	default:
		return "repair the standing service disposition"
	}
}

func (d StandingRestartDisposition) Validate() error {
	switch d.Kind {
	case StandingRestartOrdinary:
		if d.ServiceID != "" || d.RunID != "" || d.Generation != 0 {
			return errors.New("ordinary standing restart disposition cannot carry a current owner")
		}
		return nil
	case StandingRestartActiveIntrinsic, StandingRestartSuspended, StandingRestartCredentialDormant, StandingRestartRecoveryRequired, StandingRestartOrphaned,
		StandingRestartTerminalDeclared, StandingRestartTerminalOrphaned, StandingRestartInvalidCurrent:
	default:
		return fmt.Errorf("invalid standing restart disposition %q", d.Kind)
	}
	if _, err := uuid.Parse(strings.TrimSpace(d.ServiceID)); err != nil {
		return fmt.Errorf("standing restart service_id: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(d.RunID)); err != nil {
		return fmt.Errorf("standing restart run_id: %w", err)
	}
	if d.Generation <= 0 {
		return errors.New("standing restart generation must be positive")
	}
	if _, err := ParseState(d.RunState); err != nil {
		return err
	}
	if d.Remediation == "" {
		return errors.New("standing restart remediation is required")
	}
	return nil
}

// ClassifyStandingRestart is the sole decision table for exact-current
// standing restart semantics. Selected-store readers prove the fact product;
// consumers receive only this typed result.
func ClassifyStandingRestart(fact StandingRestartFact) (StandingRestartDisposition, error) {
	if !fact.ExactCurrent {
		result := StandingRestartDisposition{Kind: StandingRestartOrdinary, Remediation: StandingRestartNoRemediation}
		return result, result.Validate()
	}
	fact.ServiceID = strings.TrimSpace(fact.ServiceID)
	fact.RunID = strings.TrimSpace(fact.RunID)
	fact.EffectiveState = strings.TrimSpace(fact.EffectiveState)
	fact.OperatorOverride = strings.TrimSpace(fact.OperatorOverride)
	fact.RunState = strings.TrimSpace(fact.RunState)
	if _, err := uuid.Parse(fact.ServiceID); err != nil {
		return StandingRestartDisposition{}, fmt.Errorf("standing restart service_id: %w", err)
	}
	if _, err := uuid.Parse(fact.RunID); err != nil {
		return StandingRestartDisposition{}, fmt.Errorf("standing restart run_id: %w", err)
	}
	if fact.Generation <= 0 {
		return StandingRestartDisposition{}, errors.New("standing restart generation must be positive")
	}
	state, err := ParseState(fact.RunState)
	if err != nil {
		return StandingRestartDisposition{}, err
	}
	if fact.EffectiveState != "active" && fact.EffectiveState != "suspended" && fact.EffectiveState != "orphaned" && fact.EffectiveState != "dormant" && fact.EffectiveState != "recovery_required" {
		return StandingRestartDisposition{}, fmt.Errorf("invalid standing restart effective_state %q", fact.EffectiveState)
	}
	if fact.OperatorOverride != "none" && fact.OperatorOverride != "suspended" {
		return StandingRestartDisposition{}, fmt.Errorf("invalid standing restart operator_override %q", fact.OperatorOverride)
	}
	desiredValid := (!fact.DeclarationPresent && !fact.BindingEnabled && fact.EffectiveState == "orphaned" &&
		(fact.OperatorOverride == "none" || fact.OperatorOverride == "suspended")) ||
		(fact.DeclarationPresent && fact.BindingEnabled && fact.EffectiveState == "active" && fact.OperatorOverride == "none") ||
		(fact.DeclarationPresent && fact.BindingEnabled && fact.EffectiveState == "suspended" && fact.OperatorOverride == "suspended") ||
		(fact.DeclarationPresent && !fact.BindingEnabled && (fact.EffectiveState == "dormant" || fact.EffectiveState == "recovery_required"))
	result := StandingRestartDisposition{
		ServiceID: fact.ServiceID, RunID: fact.RunID, Generation: fact.Generation,
		DeclarationPresent: fact.DeclarationPresent, BindingEnabled: fact.BindingEnabled, EffectiveState: fact.EffectiveState,
		OperatorOverride: fact.OperatorOverride, RunState: string(state),
	}
	if !desiredValid {
		return StandingRestartDisposition{}, fmt.Errorf(
			"standing restart desired-state product is inconsistent: declaration_present=%t effective_state=%s operator_override=%s",
			fact.DeclarationPresent,
			fact.EffectiveState,
			fact.OperatorOverride,
		)
	}
	switch {
	case state.Terminal() && fact.DeclarationPresent:
		result.Kind, result.Remediation = StandingRestartTerminalDeclared, StandingRestartReset
	case state.Terminal():
		result.Kind, result.Remediation = StandingRestartTerminalOrphaned, StandingRestartRestoreThenReset
	case fact.DeclarationPresent && !fact.BindingEnabled && fact.EffectiveState == "recovery_required" && state == StatePaused:
		result.Kind, result.Remediation = StandingRestartRecoveryRequired, StandingRestartResumeChannel
	case fact.DeclarationPresent && !fact.BindingEnabled && fact.EffectiveState == "dormant" && state == StatePaused:
		result.Kind, result.Remediation = StandingRestartCredentialDormant, StandingRestartProvisionCredentials
	case !fact.DeclarationPresent && fact.EffectiveState == "orphaned" &&
		(fact.OperatorOverride == "none" || fact.OperatorOverride == "suspended") &&
		state == StatePaused:
		result.Kind, result.Remediation = StandingRestartOrphaned, StandingRestartRestoreDeclaration
	case fact.DeclarationPresent && fact.EffectiveState == "suspended" &&
		fact.OperatorOverride == "suspended" && state == StatePaused:
		result.Kind, result.Remediation = StandingRestartSuspended, StandingRestartResumeOrReset
	case fact.DeclarationPresent && fact.EffectiveState == "active" &&
		fact.OperatorOverride == "none" && state.Active():
		result.Kind, result.Remediation = StandingRestartActiveIntrinsic, StandingRestartNoRemediation
	case fact.DeclarationPresent:
		result.Kind, result.Remediation = StandingRestartInvalidCurrent, StandingRestartReset
	default:
		result.Kind, result.Remediation = StandingRestartInvalidCurrent, StandingRestartRestoreThenReset
	}
	return result, result.Validate()
}
