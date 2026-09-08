package runforkreadiness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// MaterializeRequest carries an admitted relation, never caller-authored states.
type MaterializeRequest struct {
	SourceRunID             string
	At                      string
	ContractSelection       runfork.RunForkContractSelection
	SourceArtifactFact      correlation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	FrontierAdmission       runfork.RunForkContractFrontierAdmission
	RouteTopology           runfork.RunForkSelectedContractRouteTopology
	RecipientPlanning       runfork.RunForkSelectedContractRecipientPlanning
	Readiness               Admission
	DataPinOverrides        []durabledata.ExplicitPin
	FanOutPlanRefs          []contracts.FanOutPlanRef
}

// Binding is checked again against the transaction's fixed plan and event rows.
type Binding struct {
	Plan                    runfork.RunForkPlan
	ContractSelection       runfork.RunForkContractSelection
	SourceArtifactFact      correlation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	FrontierAdmission       runfork.RunForkContractFrontierAdmission
	RecipientPlanning       runfork.RunForkSelectedContractRecipientPlanning
	SourceModes             map[string]executionmode.Mode
}

type AdmissionRequest struct {
	Binding
	Source       semanticview.Source
	ModelOptions manager.AgentManagerOptions
}

type Admission struct {
	sealed *admittedProjection
}

type admittedProjection struct {
	planBinding     string
	frontierBinding string
	selection       runfork.RunForkContractSelection
	sourceFact      correlation.SourceArtifactFact
	effectiveSource scenarioexecution.EffectiveSourceIdentity
	planning        []byte
	modes           []byte
	projection      []byte
}

func Admit(req AdmissionRequest) (Admission, error) {
	if req.Source == nil {
		return Admission{}, fmt.Errorf("selected-contract readiness requires selected semantic source")
	}
	bundle, ok := semanticview.Bundle(req.Source)
	if !ok {
		return Admission{}, fmt.Errorf("selected-contract readiness requires compiled artifact source")
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil || hash != req.SourceArtifactFact.BundleHash() {
		return Admission{}, fmt.Errorf("selected-contract readiness source disagrees with admitted source artifact: %v", err)
	}
	planBinding, frontierBinding, err := validateBinding(req.Binding)
	if err != nil {
		return Admission{}, err
	}
	canonical, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: req.Plan, Source: req.Source, ContractSelection: req.ContractSelection,
	})
	if err != nil {
		return Admission{}, err
	}
	_, _, canonicalBinding, err := runfork.RunForkContractFrontierEvidenceBinding(canonical)
	if err != nil || canonicalBinding != frontierBinding {
		return Admission{}, fmt.Errorf("selected-contract readiness frontier disagrees with canonical source admission: %v", err)
	}
	prepared, err := Project(req.Plan, req.Source, req.RecipientPlanning, req.SourceModes, req.ModelOptions)
	if err != nil {
		return Admission{}, err
	}
	projection, err := json.Marshal(prepared)
	if err != nil {
		return Admission{}, fmt.Errorf("seal selected-contract readiness projection: %w", err)
	}
	planning, err := json.Marshal(req.RecipientPlanning)
	if err != nil {
		return Admission{}, err
	}
	modes, err := json.Marshal(req.SourceModes)
	if err != nil {
		return Admission{}, err
	}
	return Admission{sealed: &admittedProjection{
		planBinding: planBinding, frontierBinding: frontierBinding,
		selection: req.ContractSelection, sourceFact: req.SourceArtifactFact,
		planning: planning, modes: modes, effectiveSource: req.EffectiveSourceIdentity, projection: projection,
	}}, nil
}

func (a Admission) Projection() (Projection, error) {
	if a.sealed == nil {
		return Projection{}, fmt.Errorf("selected-contract readiness admission is required")
	}
	var out Projection
	decoder := json.NewDecoder(bytes.NewReader(a.sealed.projection))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return Projection{}, fmt.Errorf("read admitted selected-contract readiness: %w", err)
	}
	return out, nil
}

func (a Admission) ValidateAgainst(binding Binding) error {
	if a.sealed == nil {
		return fmt.Errorf("selected-contract readiness admission is required")
	}
	plan, frontier, err := validateBinding(binding)
	if err != nil {
		return err
	}
	if plan != a.sealed.planBinding || frontier != a.sealed.frontierBinding ||
		binding.ContractSelection != a.sealed.selection || !binding.SourceArtifactFact.Matches(a.sealed.sourceFact) ||
		!binding.EffectiveSourceIdentity.Equal(a.sealed.effectiveSource) {
		return fmt.Errorf("selected-contract readiness admission disagrees with fixed plan/source/frontier")
	}
	var planning runfork.RunForkSelectedContractRecipientPlanning
	if err := json.Unmarshal(a.sealed.planning, &planning); err != nil {
		return err
	}
	equal, err := runfork.EqualSelectedContractRecipientPlanning(planning, binding.RecipientPlanning)
	if err != nil || !equal {
		return fmt.Errorf("selected-contract readiness admission disagrees with complete recipient planning: %v", err)
	}
	var modes map[string]executionmode.Mode
	if err := json.Unmarshal(a.sealed.modes, &modes); err != nil {
		return err
	}
	if !maps.Equal(modes, binding.SourceModes) {
		return fmt.Errorf("selected-contract readiness admission disagrees with complete source event modes")
	}
	return nil
}

func validateBinding(binding Binding) (string, string, error) {
	if err := binding.SourceArtifactFact.Validate(); err != nil {
		return "", "", fmt.Errorf("selected-contract readiness source artifact: %w", err)
	}
	if err := binding.EffectiveSourceIdentity.Validate(); err != nil {
		return "", "", fmt.Errorf("selected-contract readiness effective source: %w", err)
	}
	if !binding.EffectiveSourceIdentity.SourceArtifactFact().Matches(binding.SourceArtifactFact) {
		return "", "", fmt.Errorf("selected-contract readiness effective source disagrees with admitted source artifact")
	}
	plan := binding.Plan
	historical, ok := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !ok || strings.TrimSpace(plan.SourceRunID) == "" || plan.ForkPoint.Revision <= 0 {
		return "", "", fmt.Errorf("selected-contract readiness requires exact fixed-revision plan")
	}
	count, ids, frontier, err := runfork.RunForkContractFrontierEvidenceBinding(binding.FrontierAdmission)
	if err != nil {
		return "", "", err
	}
	if binding.FrontierAdmission.Owner != runfork.RunForkContractFrontierAdmissionOwner || !binding.FrontierAdmission.NonMutating ||
		binding.FrontierAdmission.ContractSelection != binding.ContractSelection ||
		binding.RecipientPlanning.Owner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		len(binding.RecipientPlanning.RecipientPlanEvents) != count || len(binding.SourceModes) != count {
		return "", "", fmt.Errorf("selected-contract readiness requires complete canonical frontier/planning/modes")
	}
	if binding.ContractSelection.Mode == runfork.RunForkContractSelectionModeBundleHash && binding.ContractSelection.BundleHash != binding.SourceArtifactFact.BundleHash() {
		return "", "", fmt.Errorf("selected-contract readiness artifact contradicts selected bundle")
	}
	seen := make(map[string]struct{}, count)
	for _, event := range binding.RecipientPlanning.RecipientPlanEvents {
		id := strings.TrimSpace(event.SourceEventID)
		if _, duplicate := seen[id]; duplicate || id == "" {
			return "", "", fmt.Errorf("selected-contract readiness has duplicate or blank event association")
		}
		seen[id] = struct{}{}
		var expected []forkrecipient.Evidence
		matched := false
		for _, frontierEvent := range binding.FrontierAdmission.FrontierEvents {
			if frontierEvent.SourceEventID == id && frontierEvent.EventName == event.EventName {
				expected, matched = frontierEvent.DerivedRecipients, true
				break
			}
		}
		if !matched {
			return "", "", fmt.Errorf("selected-contract readiness planning event disagrees with frontier")
		}
		left, err := forkrecipient.CanonicalSet(expected)
		if err != nil {
			return "", "", err
		}
		right, err := forkrecipient.CanonicalSet(event.Recipients)
		if err != nil {
			return "", "", err
		}
		if len(left) != len(right) {
			return "", "", fmt.Errorf("selected-contract readiness planning omits or adds recipients")
		}
		for i := range left {
			equal, err := forkrecipient.Equal(left[i], right[i])
			if err != nil || !equal {
				return "", "", fmt.Errorf("selected-contract readiness recipient differs from canonical frontier: %v", err)
			}
		}
	}
	for _, id := range ids {
		if _, ok := seen[id]; !ok || !binding.SourceModes[id].Valid() {
			return "", "", fmt.Errorf("selected-contract readiness is missing frontier event %s", id)
		}
	}
	// Current source status and post-R advancement are not historical authority.
	key, err := canonicaljson.Hash(struct {
		RunID      string
		ForkPoint  runfork.RunForkPoint
		Entities   []runfork.RunForkEntityState
		Pending    []runfork.RunForkPendingWork
		Historical []string
	}{plan.SourceRunID, plan.ForkPoint, plan.Entities, plan.PendingWork, historical})
	return key, frontier, err
}
