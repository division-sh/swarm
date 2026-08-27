package channelonboarding

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
)

type ActivationSource string

const (
	ActivationSourceDeclared ActivationSource = "declared"
	ActivationSourceLearned  ActivationSource = "learned"
)

type CompiledActivation struct {
	Source                ActivationSource
	OnboardingOperationID string
	Coordinate            ChannelRuntimeContextCoordinate
	ActivationRevision    int64
	Plan                  packs.OutboundBindingPlan
	CredentialAdmissions  []CredentialAdmission
}

func (a CompiledActivation) Validate() error {
	if a.Source != ActivationSourceDeclared && a.Source != ActivationSourceLearned {
		return fmt.Errorf("channel activation source is invalid")
	}
	if err := a.Coordinate.ValidateContext(); err != nil {
		return err
	}
	if a.Plan.RegistrationTarget() != "" && a.Coordinate.TargetGeneration == 0 {
		return fmt.Errorf("registered channel activation requires an exact target generation")
	}
	generation, err := a.Plan.PlanGeneration()
	if err != nil {
		return err
	}
	if !generation.Equal(a.Coordinate.PlanGeneration) {
		return fmt.Errorf("channel activation plan generation contradicts its runtime coordinate")
	}
	if a.Source == ActivationSourceLearned && a.ActivationRevision < 1 {
		return fmt.Errorf("learned channel activation requires a positive revision")
	}
	if a.Source == ActivationSourceLearned && strings.TrimSpace(a.OnboardingOperationID) == "" {
		return fmt.Errorf("learned channel activation requires its exact onboarding operation")
	}
	if a.Source == ActivationSourceDeclared && strings.TrimSpace(a.OnboardingOperationID) != "" {
		return fmt.Errorf("declared channel activation cannot claim an onboarding operation")
	}
	credentialKeys := a.Plan.CredentialStoreKeys()
	admitted := make(map[string]string, len(a.CredentialAdmissions))
	for _, admission := range a.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			return err
		}
		role, key := strings.TrimSpace(admission.Role), strings.TrimSpace(admission.StoreKey)
		if expected, ok := credentialKeys[role]; !ok || strings.TrimSpace(expected) != key {
			return fmt.Errorf("channel activation credential admission %q does not match its executable plan", role)
		}
		if _, duplicate := admitted[role]; duplicate {
			return fmt.Errorf("channel activation has duplicate credential admission role %q", role)
		}
		admitted[role] = key
	}
	for role, key := range credentialKeys {
		if admitted[strings.TrimSpace(role)] != strings.TrimSpace(key) {
			return fmt.Errorf("channel activation is missing credential admission role %q", role)
		}
	}
	return nil
}

func LearnedBindingID(slotKey string) string {
	return "learned_" + operatorchannel.Hash("connected-channel-binding-v1", strings.TrimSpace(slotKey))
}

func CompileLearnedActivation(candidate Candidate, activation ConnectedChannelActivation) (CompiledActivation, error) {
	if err := candidate.Validate(); err != nil {
		return CompiledActivation{}, err
	}
	if activation.Status != ActivationCurrent || activation.SlotKey == "" || !activation.Coordinate.Matches(candidate.Coordinate) || activation.Interface.Normalized() != candidate.Interface.Normalized() || activation.Provider != candidate.Provider || activation.TargetSelector != candidate.Target.Selector || activation.Posture != candidate.Posture {
		return CompiledActivation{}, fmt.Errorf("current learned activation contradicts the exact candidate")
	}
	if activation.BindingRevision < 1 || strings.TrimSpace(activation.ConversationRef) == "" {
		return CompiledActivation{}, fmt.Errorf("learned activation has no exact bound conversation destination")
	}
	credentialKeys := make(map[string]string, len(activation.CredentialAdmissions))
	for _, admission := range activation.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			return CompiledActivation{}, err
		}
		role := strings.TrimSpace(admission.Role)
		if existing, duplicate := credentialKeys[role]; duplicate && existing != admission.StoreKey {
			return CompiledActivation{}, fmt.Errorf("learned activation has contradictory credential role %q", role)
		}
		credentialKeys[role] = strings.TrimSpace(admission.StoreKey)
	}
	for _, required := range []string{candidate.ProviderCredentialRole, candidate.SigningCredentialRole} {
		if required != "" && credentialKeys[required] == "" {
			return CompiledActivation{}, fmt.Errorf("learned activation is missing credential role %q", required)
		}
	}
	plan, err := packs.NewOutboundBindingPlanWithRegistration(
		LearnedBindingID(activation.SlotKey), candidate.Plan, activation.ConversationRef, nil, credentialKeys, candidate.Target.Selector,
	)
	if err != nil {
		return CompiledActivation{}, err
	}
	compiled := CompiledActivation{
		Source: ActivationSourceLearned, OnboardingOperationID: activation.OperationID, Coordinate: activation.Coordinate,
		ActivationRevision: activation.Revision, Plan: plan,
		CredentialAdmissions: append([]CredentialAdmission(nil), activation.CredentialAdmissions...),
	}
	return compiled, compiled.Validate()
}

func MergeCompiledActivations(declared, learned []CompiledActivation) ([]CompiledActivation, error) {
	out := make([]CompiledActivation, 0, len(declared)+len(learned))
	byBinding := map[string]CompiledActivation{}
	bySlot := map[string]CompiledActivation{}
	for _, activation := range append(append([]CompiledActivation(nil), declared...), learned...) {
		if err := activation.Validate(); err != nil {
			return nil, err
		}
		bindingID := activation.Plan.BindingID()
		if existing, collision := byBinding[bindingID]; collision {
			return nil, fmt.Errorf("channel activation binding identity collision between %s and %s", existing.Source, activation.Source)
		}
		byBinding[bindingID] = activation
		if target := activation.Plan.RegistrationTarget(); target != "" {
			slot := activation.Coordinate.BundleHash + "\x00" + target
			if existing, collision := bySlot[slot]; collision {
				return nil, fmt.Errorf("channel activation target collision between %s binding %q and %s binding %q", existing.Source, existing.Plan.BindingID(), activation.Source, bindingID)
			}
			bySlot[slot] = activation
		}
		out = append(out, activation)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Coordinate.BundleHash != out[j].Coordinate.BundleHash {
			return out[i].Coordinate.BundleHash < out[j].Coordinate.BundleHash
		}
		return out[i].Plan.BindingID() < out[j].Plan.BindingID()
	})
	return out, nil
}
