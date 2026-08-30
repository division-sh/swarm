package agentframe

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

type InspectionScope string

const (
	InspectionStatic    InspectionScope = "static"
	InspectionEffective InspectionScope = "effective"
)

type Presence[T any] struct {
	Status string `json:"status"`
	Value  *T     `json:"value,omitempty"`
}

func unresolved[T any]() Presence[T] { return Presence[T]{Status: "unresolved"} }

func present[T any](value T) Presence[T] { return Presence[T]{Status: "resolved", Value: &value} }

type InspectionSelector struct {
	BundleHash   string `json:"bundle_hash,omitempty"`
	Flow         string `json:"flow,omitempty"`
	AgentID      string `json:"agent_id"`
	Root         bool   `json:"root,omitempty"`
	FlowInstance string `json:"flow_instance,omitempty"`
}

type InspectionSession struct {
	BundleHash     string                           `json:"bundle_hash"`
	AgentID        string                           `json:"agent_id"`
	AuthoredFlow   string                           `json:"authored_flow"`
	AgentIdentity  Presence[agentidentity.Identity] `json:"agent_identity"`
	Role           string                           `json:"role,omitempty"`
	FlowID         string                           `json:"flow_id,omitempty"`
	Intent         Intent                           `json:"intent"`
	Criteria       Criteria                         `json:"criteria"`
	Provider       Presence[Provider]               `json:"provider"`
	ProviderPrompt string                           `json:"provider_prompt"`
}

type InspectionTurn struct {
	FrameID        Presence[string]         `json:"frame_id"`
	ContentHash    Presence[string]         `json:"content_hash"`
	Kind           Presence[TurnKind]       `json:"kind"`
	ParentFrame    Presence[string]         `json:"parent_frame_id"`
	Event          Presence[Event]          `json:"event"`
	Capability     Presence[CapabilityPlan] `json:"capability"`
	Directive      Presence[Directive]      `json:"directive"`
	Remediation    Presence[Remediation]    `json:"remediation"`
	Lifecycle      Lifecycle                `json:"lifecycle"`
	PackProvenance UnresolvedFact           `json:"pack_provenance"`
}

type Inspection struct {
	Version  string             `json:"version"`
	Scope    InspectionScope    `json:"scope"`
	Selector InspectionSelector `json:"selector"`
	Session  InspectionSession  `json:"session_contract"`
	Turn     InspectionTurn     `json:"turn_context"`
}

type PreviewSeed struct {
	BundleHash     string
	AgentID        string
	AuthoredFlow   string
	AgentIdentity  *agentidentity.Identity
	Role           string
	FlowID         string
	Intent         agentintent.Resolved
	Criteria       []string
	Provider       *Provider
	ProviderPrompt string
}

func InspectStatic(selector InspectionSelector, seed PreviewSeed) (Inspection, error) {
	if bundleidentity.ValidateCanonicalHash(selector.BundleHash) != nil || !exactInspectionPath(selector.Flow) || !exactInspectionScalar(selector.AgentID) || selector.Root || selector.FlowInstance != "" {
		return Inspection{}, fmt.Errorf("static frame inspection requires exact bundle_hash, flow, and agent_id only")
	}
	seed.BundleHash = selector.BundleHash
	seed.AgentID = selector.AgentID
	seed.AuthoredFlow = selector.Flow
	seed.AgentIdentity = nil
	return inspect(InspectionStatic, selector, seed)
}

func InspectEffective(selector InspectionSelector, seed PreviewSeed) (Inspection, error) {
	if !exactInspectionScalar(selector.AgentID) || selector.BundleHash != "" || selector.Flow != "" || selector.Root == (selector.FlowInstance != "") || (!selector.Root && !exactInspectionPath(selector.FlowInstance)) {
		return Inspection{}, fmt.Errorf("effective frame inspection requires agent_id and exactly one of root or flow_instance")
	}
	if seed.AgentIdentity == nil {
		return Inspection{}, fmt.Errorf("effective frame inspection requires concrete agent identity")
	}
	identity := seed.AgentIdentity.Normalize()
	if err := identity.Validate(); err != nil {
		return Inspection{}, err
	}
	if identity.AgentID() != selector.AgentID {
		return Inspection{}, fmt.Errorf("effective frame selector does not match concrete agent identity")
	}
	if selector.Root {
		if identity.Route.Presence != agentidentity.RouteRoot {
			return Inspection{}, fmt.Errorf("effective frame selector does not match concrete agent identity")
		}
	} else if identity.FlowInstance() != selector.FlowInstance {
		return Inspection{}, fmt.Errorf("effective frame selector does not match concrete agent identity")
	}
	return inspect(InspectionEffective, selector, seed)
}

func exactInspectionPath(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && value == strings.Trim(value, "/")
}

func exactInspectionScalar(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func inspect(scope InspectionScope, selector InspectionSelector, seed PreviewSeed) (Inspection, error) {
	if err := bundleidentity.ValidateCanonicalHash(seed.BundleHash); err != nil {
		return Inspection{}, err
	}
	if err := seed.Intent.Validate(); err != nil {
		return Inspection{}, err
	}
	if strings.TrimSpace(seed.AgentID) == "" || strings.TrimSpace(seed.ProviderPrompt) == "" {
		return Inspection{}, fmt.Errorf("frame inspection session contract is incomplete")
	}
	criteriaIdentity, err := CriteriaIdentity(seed.BundleHash, seed.Criteria)
	if err != nil {
		return Inspection{}, err
	}
	session := InspectionSession{
		BundleHash: seed.BundleHash, AgentID: strings.TrimSpace(seed.AgentID),
		AuthoredFlow: strings.Trim(strings.TrimSpace(seed.AuthoredFlow), "/"), Role: strings.TrimSpace(seed.Role), FlowID: strings.Trim(strings.TrimSpace(seed.FlowID), "/"),
		AgentIdentity: unresolved[agentidentity.Identity](),
		Intent:        Intent{Kind: string(seed.Intent.Kind), Coordinate: seed.Intent.Coordinate, Provenance: seed.Intent.Provenance, Content: seed.Intent.Content, ContentHash: seed.Intent.ContentHash, Identity: seed.Intent.Identity},
		Criteria:      Criteria{References: append([]string{}, seed.Criteria...), Identity: criteriaIdentity},
		Provider:      unresolved[Provider](), ProviderPrompt: seed.ProviderPrompt,
	}
	if seed.AgentIdentity != nil {
		identity := seed.AgentIdentity.Normalize()
		if err := identity.Validate(); err != nil {
			return Inspection{}, err
		}
		session.AgentIdentity = present(identity)
	}
	if seed.Provider != nil {
		provider := *seed.Provider
		if strings.TrimSpace(provider.RuntimeMode) == "" || strings.TrimSpace(provider.Provider) == "" || strings.TrimSpace(provider.Transport) == "" {
			return Inspection{}, fmt.Errorf("resolved provider contract is incomplete")
		}
		if err := validateProviderModelSelection(provider.ModelAlias, provider.Model); err != nil {
			return Inspection{}, fmt.Errorf("resolved provider selection: %w", err)
		}
		session.Provider = present(provider)
	}
	return Inspection{
		Version: Version, Scope: scope, Selector: selector, Session: session,
		Turn: InspectionTurn{
			FrameID: unresolved[string](), ContentHash: unresolved[string](), Kind: unresolved[TurnKind](), ParentFrame: unresolved[string](),
			Event: unresolved[Event](), Capability: unresolved[CapabilityPlan](), Directive: unresolved[Directive](), Remediation: unresolved[Remediation](),
			Lifecycle: Lifecycle{Stage: Unresolved(), LoopRevision: Unresolved()}, PackProvenance: Unresolved(),
		},
	}, nil
}
