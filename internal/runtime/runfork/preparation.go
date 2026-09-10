package runfork

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

var selectedPreparationRevisionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// SelectedForkPreparationBinding is immutable evidence in the selected execution
// row. It references original prospective receipts, never relabeled live actors.
type SelectedForkPreparationBinding struct {
	SelectedForkPreparation
	ForkRunID string `json:"fork_run_id"`
}

type SelectedForkPreparation struct {
	DeclarationPlanFingerprint string                                                 `json:"declaration_plan_fingerprint"`
	PreparationID              string                                                 `json:"preparation_id"`
	ProcessGeneration          uint64                                                 `json:"process_generation"`
	Coordinates                managedcapabilities.SelectedForkPreparationCoordinates `json:"coordinates"`
	SourceRunID                string                                                 `json:"source_run_id"`
	ForkEventID                string                                                 `json:"fork_event_id"`
	Actors                     []SelectedForkPreparedActor                            `json:"actors"`
}

type SelectedForkPreparedActor struct {
	Plan                  agentidentity.Plan `json:"plan"`
	ConfigurationRevision string             `json:"configuration_revision"`
	Backend               string             `json:"backend"`
	Mode                  executionmode.Mode `json:"mode"`
	SurfaceID             string             `json:"surface_id,omitempty"`
	SurfaceIntegrity      string             `json:"surface_integrity,omitempty"`
}

func (a SelectedForkPreparedActor) RequiresProbe() bool {
	return a.Backend == selection.BackendClaudeCLI && a.Mode == executionmode.Live
}

func (b SelectedForkPreparationBinding) Validate() error {
	if err := b.SelectedForkPreparation.Validate(); err != nil {
		return err
	}
	id, err := uuid.Parse(b.ForkRunID)
	if err != nil || id == uuid.Nil || id.String() != b.ForkRunID || b.ForkRunID == b.SourceRunID {
		return fmt.Errorf("selected preparation requires its distinct canonical fork run")
	}
	return nil
}

func (b SelectedForkPreparation) Validate() error {
	if !selectedPreparationRevisionPattern.MatchString(b.DeclarationPlanFingerprint) {
		return fmt.Errorf("selected preparation declaration fingerprint is invalid")
	}
	if b.ProcessGeneration == 0 {
		return fmt.Errorf("selected preparation requires a positive process generation")
	}
	for _, id := range []string{b.PreparationID, b.SourceRunID, b.ForkEventID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("selected preparation binding requires canonical nonzero coordinates")
		}
	}
	if err := b.Coordinates.Validate(); err != nil {
		return err
	}
	if b.Actors == nil {
		return fmt.Errorf("selected preparation requires an explicit actor census")
	}
	seen := make(map[string]bool, len(b.Actors))
	for i, actor := range b.Actors {
		if actor.Plan != actor.Plan.Normalize() {
			return fmt.Errorf("selected preparation actor plan is noncanonical")
		}
		if err := actor.Plan.Validate(); err != nil {
			return err
		}
		if i > 0 && !agentidentity.LessPlan(b.Actors[i-1].Plan, actor.Plan) {
			return fmt.Errorf("selected preparation actor census must be unique and ordered")
		}
		if !selectedPreparationRevisionPattern.MatchString("sha256:" + actor.ConfigurationRevision) {
			return fmt.Errorf("selected preparation actor configuration revision is invalid")
		}
		profile, err := selection.ResolveActiveBackend(actor.Backend)
		if err != nil || profile.ID != actor.Backend {
			return fmt.Errorf("selected preparation actor backend is invalid")
		}
		mode, err := selection.ExecutionModeForProfile(profile)
		if err != nil || mode != actor.Mode {
			return fmt.Errorf("selected preparation actor execution mode differs from its backend")
		}
		if !actor.RequiresProbe() {
			if actor.SurfaceID != "" || actor.SurfaceIntegrity != "" {
				return fmt.Errorf("selected preparation actor does not admit startup probe receipts")
			}
			continue
		}
		id, err := uuid.Parse(actor.SurfaceID)
		if err != nil || id == uuid.Nil || id.String() != actor.SurfaceID || seen[actor.SurfaceID] {
			return fmt.Errorf("selected preparation requires one distinct startup receipt per probed actor")
		}
		seen[actor.SurfaceID] = true
		// Surface integrity uses the capability owner's unprefixed digest format.
		if !selectedPreparationRevisionPattern.MatchString("sha256:" + actor.SurfaceIntegrity) {
			return fmt.Errorf("selected preparation surface integrity is invalid")
		}
	}
	return nil
}

// SelectedPreparationPlanFingerprint includes the fixed history certificate,
// which deliberately is not part of RunForkPlan's public JSON projection.
func SelectedPreparationPlanFingerprint(plan RunForkPlan, frontier RunForkContractFrontierAdmission, planning RunForkSelectedContractRecipientPlanning, declarationRevision string) (string, error) {
	history, ok := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !ok {
		return "", fmt.Errorf("selected preparation requires admitted fixed-revision history")
	}
	raw, err := canonicaljson.Bytes(struct {
		Plan         RunForkPlan
		History      []string
		Frontier     RunForkContractFrontierAdmission
		Recipients   RunForkSelectedContractRecipientPlanning
		Declarations string
	}{plan, history, frontier, planning, declarationRevision})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func SelectedPreparationSourceFingerprint(identity scenarioexecution.EffectiveSourceIdentity) (string, error) {
	value, err := identity.CanonicalValue()
	if err != nil {
		return "", err
	}
	raw, err := canonicaljson.Bytes(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func (b SelectedForkPreparationBinding) Fingerprint() (string, error) {
	if err := b.Validate(); err != nil {
		return "", err
	}
	return canonicaljson.Hash(b)
}

func (b SelectedForkPreparation) SurfaceIDs() []string {
	ids := make([]string, 0)
	for _, actor := range b.Actors {
		if actor.RequiresProbe() {
			ids = append(ids, actor.SurfaceID)
		}
	}
	sort.Strings(ids)
	return ids
}

func (b SelectedForkPreparation) ValidateSurface(actor SelectedForkPreparedActor, surface managedcapabilities.Surface) error {
	if err := surface.ValidateEffective(); err != nil {
		return err
	}
	if !actor.RequiresProbe() || surface.ID != actor.SurfaceID || surface.IntegrityHash != actor.SurfaceIntegrity ||
		surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe ||
		surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedForkPreparation ||
		surface.Authority.ExecutionAuthorityID != b.PreparationID || surface.Authority.Preparation == nil ||
		surface.Authority.Preparation.SelectedForkPreparationCoordinates != b.Coordinates || surface.ActorPlan != actor.Plan {
		return fmt.Errorf("selected preparation receipt differs from its exact bound actor or preparation")
	}
	return nil
}

func (b SelectedForkPreparation) ValidateSurfaceIDs(ids []string) error {
	expected := b.SurfaceIDs()
	if len(expected) != len(ids) || strings.Join(expected, "\n") != strings.Join(ids, "\n") {
		return fmt.Errorf("selected generation probe receipts differ from bound preparation")
	}
	return nil
}

// SelectedAgentPlans projects the admitted typed recipients without inferring
// declaration ownership from their display names.
func (p RunForkSelectedContractRecipientPlanning) SelectedAgentPlans() ([]agentidentity.Plan, error) {
	seen := make(map[agentidentity.Plan]struct{})
	for _, event := range p.RecipientPlanEvents {
		for _, recipient := range event.Recipients {
			if err := recipient.Validate(); err != nil {
				return nil, err
			}
			if recipient.Recipient.IsAgent() {
				seen[recipient.AgentPlan.Normalize()] = struct{}{}
			}
		}
	}
	plans := make([]agentidentity.Plan, 0, len(seen))
	for plan := range seen {
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool { return agentidentity.LessPlan(plans[i], plans[j]) })
	return plans, nil
}
