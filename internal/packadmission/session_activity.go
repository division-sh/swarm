package packadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// SessionActivityTrigger selects declaration evidence only. Publication and
// execution authority remain with the channel and native activity owners.
func SessionActivityTrigger(source semanticview.Source, site contracts.ActivitySite) (packs.TriggerPackDescriptor, error) {
	var absent packs.TriggerPackDescriptor
	declaration, err := semanticview.ResolveExecutableNodeSemanticScope(source, site.Node)
	if err != nil {
		return absent, err
	}
	flow, owned := declaration.OwningFlow()
	scope, declared := source.FlowScopeByID(declaration.Declaration.Source.FlowPath)
	tool, found := scope.Tools[site.Spec.Tool]
	target, native := tool.InProcess()
	if !owned || !declared || !found || !native {
		return absent, fmt.Errorf("session activity requires its scoped connector declaration")
	}
	bundle, backed := semanticview.Bundle(source)
	if !backed || bundle == nil {
		return absent, fmt.Errorf("session activity requires its captured source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return absent, err
	}
	providers, err := AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return absent, err
	}
	providerScope, err := providers.Scope(flow.ID)
	if err != nil {
		return absent, err
	}
	var inputs []string
	for _, pin := range source.FlowInputEventPins(flow.ID) {
		inputs = append(inputs, pin.EventType())
	}
	selected := absent
	for _, definition := range providerScope.ConsumedTriggerProviders(inputs) {
		if definition.Provider().Key() != target.Provider() {
			continue
		}
		catalog, _ := definition.TriggerCatalog()
		descriptor, found := catalog.PackDescriptorForProvider(target.Provider())
		if !found || descriptor.Transport != packs.ChannelTransportSession || selected.Identity.ID() != "" {
			return absent, fmt.Errorf("session activity requires one exact consumed session trigger")
		}
		selected = descriptor
	}
	if selected.Identity.ID() == "" {
		return absent, fmt.Errorf("session activity requires its owning flow's consumed session trigger")
	}
	return selected, nil
}
