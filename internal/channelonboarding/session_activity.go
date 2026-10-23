package channelonboarding

import (
	"fmt"
	"path"
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
func QualifySessionActivityDeclaration(source semanticview.Source, site contracts.ActivitySite, trigger packs.TriggerPackDescriptor) error {
	_, _, _, err := compileSessionActivityDeclaration(source, site, trigger)
	return err
}

func ResolveSessionActivitySite(source semanticview.Source, node identity.ExecutableNode, handler, toolID, activityID string) (contracts.ActivitySite, error) {
	var matches []contracts.ActivitySite
	if source == nil || !node.Valid() {
		return contracts.ActivitySite{}, fmt.Errorf("session activity requires its exact declaration source")
	}
	for _, site := range contracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node)) {
		if site.HandlerEventKey != handler || site.Spec.Tool != toolID {
			continue
		}
		events, err := contracts.ActivityResultEventsForSite(site)
		if err != nil {
			return contracts.ActivitySite{}, fmt.Errorf("session activity declaration identity: %w", err)
		}
		if events.ActivityID == activityID {
			matches = append(matches, site)
		}
	}
	if len(matches) != 1 {
		return contracts.ActivitySite{}, fmt.Errorf("session activity requires exactly one authored site, found %d", len(matches))
	}
	return matches[0], nil
}

func CompileSessionActivityTarget(source semanticview.Source, bundleHash string, site contracts.ActivitySite,
	publication ChannelActivationPublication, trigger packs.TriggerPackDescriptor,
) (CompiledSessionActivityTarget, error) {
	var absent CompiledSessionActivityTarget
	if source == nil || bundleHash == "" || bundleHash != strings.TrimSpace(bundleHash) || !publication.Executable() {
		return absent, fmt.Errorf("session activity requires its exact source and executable publication")
	}
	if err := publication.Validate(); err != nil {
		return absent, err
	}
	tool, selector, packID, err := compileSessionActivityDeclaration(source, site, trigger)
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
		id, connector, err := activation.Plan.ConnectorDeclaration(name)
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

func compileSessionActivityDeclaration(source semanticview.Source, site contracts.ActivitySite, trigger packs.TriggerPackDescriptor) (contracts.ToolSchemaEntry, string, string, error) {
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
	if !native || target != contracts.ToolInProcessWhatsAppSendText {
		return absent, "", "", fmt.Errorf("session activity has no supported private write contract")
	}
	if trigger.Provider != target.Provider() || trigger.Identity.ID() != target.Provider() ||
		trigger.Transport != packs.ChannelTransportSession || !trigger.Generation.Valid() ||
		trigger.Identity.Source().Path() != path.Join(flow.ID, target.Provider(), "trigger.yaml") {
		return absent, "", "", fmt.Errorf("session activity requires its exact owning-flow session trigger declaration")
	}
	selector := "ingress:" + flow.ID + ":" + target.Provider()
	if _, err := packs.ParseChannelRegistrationTarget(selector); err != nil {
		return absent, "", "", err
	}
	return tool, selector, trigger.Identity.ID(), nil
}

func sessionActivitySiteDeclared(site contracts.ActivitySite, handlers map[string]contracts.SystemNodeEventHandler) bool {
	wanted, err := contracts.ActivityResultEventsForSite(site)
	if err != nil {
		return false
	}
	for _, declared := range contracts.ActivitySitesForNode(site.Node, handlers) {
		if declared.HandlerEventKey != site.HandlerEventKey || declared.Kind != site.Kind || declared.Source != site.Source ||
			declared.RuleIndex != site.RuleIndex || declared.RuleRef != site.RuleRef || declared.Spec.Tool != site.Spec.Tool {
			continue
		}
		events, err := contracts.ActivityResultEventsForSite(declared)
		if err == nil && events.ActivityID == wanted.ActivityID {
			return true
		}
	}
	return false
}
