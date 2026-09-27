package semanticviewtest

import (
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type providerIngressSource struct {
	semanticview.Source
	capabilities semanticview.Capabilities
}

func (s providerIngressSource) SemanticCapabilities() semanticview.Capabilities {
	return s.capabilities
}

// WithProviderIngress supplies admitted declaration facts to isolated consumer
// tests. Source-loading and gateway proofs must compile real ingress plans.
func WithProviderIngress(base semanticview.Source, declarations map[string][]string) semanticview.Source {
	generation := triggergeneration.FromCanonicalBytes([]byte("isolated-provider-ingress-consumer-test"))
	return providerIngressSource{Source: base, capabilities: base.SemanticCapabilities().
		WithProviderTriggerEvents(base, generation, nil).
		WithProviderIngressEvents(declarations)}
}
