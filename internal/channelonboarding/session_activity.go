package channelonboarding

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// CompiledSessionActivityTarget is a declaration projection, not execution
// permission. The original activation and SDK owners still fence its launch.
type CompiledSessionActivityTarget struct {
	authoredTool string
	operation    string
	activation   CompiledActivation
	private      packs.PrivateActivityTargetIdentity
	publication  ChannelActivationGeneration
}

func (p CompiledSessionActivityTarget) AuthoredToolID() string { return p.authoredTool }
func (p CompiledSessionActivityTarget) Operation() string      { return p.operation }
func (p CompiledSessionActivityTarget) Activation() CompiledActivation {
	value := p.activation
	value.CredentialAdmissions = append([]CredentialAdmission(nil), value.CredentialAdmissions...)
	return value
}
func (p CompiledSessionActivityTarget) PrivateTarget() packs.PrivateActivityTargetIdentity {
	return p.private
}
func (p CompiledSessionActivityTarget) PublicationGeneration() ChannelActivationGeneration {
	return p.publication
}

// Qualification checks declaration ownership only; it cannot select a session.
func QualifySessionActivityDeclaration(source semanticview.Source, site contracts.ActivitySite) error {
	_, _, _, err := compileSessionActivityDeclaration(source, site)
	return err
}

func ResolveSessionActivitySite(source semanticview.Source, node identity.ExecutableNode, handler, toolID, activityID string) (contracts.ActivitySite, error) {
	var matches []contracts.ActivitySite
	if source == nil || !node.Valid() {
		return contracts.ActivitySite{}, fmt.Errorf("session activity requires its exact declaration source")
	}
	for _, site := range contracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node)) {
		if site.HandlerEventKey == handler && site.Spec.Tool == toolID && contracts.ActivityResultEventsForSite(site).ActivityID == activityID {
			matches = append(matches, site)
		}
	}
	if len(matches) != 1 {
		return contracts.ActivitySite{}, fmt.Errorf("session activity requires exactly one authored site, found %d", len(matches))
	}
	return matches[0], nil
}

func CompileSessionActivityTarget(source semanticview.Source, bundleHash string, site contracts.ActivitySite,
	publication ChannelActivationPublication,
) (CompiledSessionActivityTarget, error) {
	var absent CompiledSessionActivityTarget
	if source == nil || bundleHash == "" || bundleHash != strings.TrimSpace(bundleHash) || !publication.Executable() {
		return absent, fmt.Errorf("session activity requires its exact source and executable publication")
	}
	if err := publication.Validate(); err != nil {
		return absent, err
	}
	tool, selector, packID, err := compileSessionActivityDeclaration(source, site)
	if err != nil {
		return absent, err
	}
	toolHash, err := tool.CanonicalHash()
	if err != nil {
		return absent, err
	}
	var matches []CompiledSessionActivityTarget
	for _, activation := range publication.Activations() {
		if activation.Coordinate.BundleHash != bundleHash || activation.Plan.RegistrationTarget() != selector || activation.Plan.Transport() != packs.ChannelTransportSession {
			continue
		}
		if packID != "" && activation.Plan.TriggerIdentity().ID() != packID {
			return absent, fmt.Errorf("session activity ingress pack contradicts its admitted trigger")
		}
		selected, found, err := matchSessionActivityConnector(activation, site.Spec.Tool, toolHash)
		if err != nil {
			return absent, err
		}
		if found {
			selected.publication = publication.Generation()
			matches = append(matches, selected)
		}
	}
	if len(matches) != 1 {
		return absent, fmt.Errorf("session activity requires exactly one admitted connector/ingress responsibility, found %d", len(matches))
	}
	return matches[0], nil
}

func matchSessionActivityConnector(activation CompiledActivation, toolID, toolHash string) (CompiledSessionActivityTarget, bool, error) {
	var selected CompiledSessionActivityTarget
	found := false
	for _, name := range activation.Plan.OperationNames() {
		id, connector, err := activation.Plan.ConnectorOperation(name)
		if err != nil {
			return selected, false, err
		}
		if id != toolID {
			continue
		}
		if found {
			return selected, false, fmt.Errorf("session activity connector has ambiguous operation ownership")
		}
		hash, err := connector.CanonicalHash()
		if err != nil {
			return selected, false, err
		}
		if hash != toolHash {
			return selected, false, fmt.Errorf("session activity connector contradicts its admitted declaration")
		}
		private, err := activation.Plan.RuntimeActivityTarget(name)
		if err != nil {
			return selected, false, err
		}
		selected = CompiledSessionActivityTarget{authoredTool: toolID, operation: name, activation: activation, private: private}
		found = true
	}
	return selected, found, nil
}

func compileSessionActivityDeclaration(source semanticview.Source, site contracts.ActivitySite) (contracts.ToolSchemaEntry, string, string, error) {
	var absent contracts.ToolSchemaEntry
	scope, err := semanticview.ResolveExecutableNodeSemanticScope(source, site.Node)
	if err != nil {
		return absent, "", "", err
	}
	if !sessionActivitySiteDeclared(site, scope.Declaration.Entry.EventHandlers) {
		return absent, "", "", fmt.Errorf("session activity has no exact authored declaration site")
	}
	flow, owned := scope.OwningFlow()
	declaration, declared := source.FlowScopeByID(scope.Declaration.Source.FlowPath)
	tool, found := declaration.Tools[site.Spec.Tool]
	if !owned || !declared || !found || tool.AgentExposable() {
		return absent, "", "", fmt.Errorf("session activity requires its scoped private connector")
	}
	target, native := tool.InProcess()
	if !native || target != contracts.ToolInProcessWhatsAppSendText || tool.Effect() != contracts.ActivityEffectClassNonIdempotentWrite {
		return absent, "", "", fmt.Errorf("session activity has no supported private write contract")
	}
	schema, declared := source.FlowSchemaByID(flow.ID)
	if !declared || schema.Ingress == nil {
		return absent, "", "", fmt.Errorf("session activity requires explicit ingress in its owning flow")
	}
	providers := 0
	var provider contracts.ProjectFlowIngressProvider
	for _, ingress := range schema.Ingress.Providers {
		if ingress.Provider == target.Provider() {
			providers++
			provider = ingress
		}
	}
	if providers != 1 {
		return absent, "", "", fmt.Errorf("session activity requires exactly one explicit provider ingress declaration, found %d", providers)
	}
	if provider.Admission.Kind != "" && provider.Admission.Kind != "pack" {
		return absent, "", "", fmt.Errorf("session activity requires pack-admitted ingress, not raw or unknown admission")
	}
	selector := "ingress:" + flow.ID + ":" + target.Provider()
	if _, err := packs.ParseChannelRegistrationTarget(selector); err != nil {
		return absent, "", "", err
	}
	packID := ""
	if provider.Admission.Pack != nil {
		packID = provider.Admission.Pack.ID
		if packID == "" || packID != strings.TrimSpace(packID) {
			return absent, "", "", fmt.Errorf("session activity has a non-canonical explicit trigger pack")
		}
	}
	return tool, selector, packID, nil
}

func sessionActivitySiteDeclared(site contracts.ActivitySite, handlers map[string]contracts.SystemNodeEventHandler) bool {
	for _, declared := range contracts.ActivitySitesForNode(site.Node, handlers) {
		if declared.HandlerEventKey == site.HandlerEventKey && declared.Source == site.Source && declared.RuleIndex == site.RuleIndex &&
			declared.RuleRef == site.RuleRef && declared.Spec.Tool == site.Spec.Tool &&
			contracts.ActivityResultEventsForSite(declared).ActivityID == contracts.ActivityResultEventsForSite(site).ActivityID {
			return true
		}
	}
	return false
}
