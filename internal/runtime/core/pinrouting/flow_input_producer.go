package pinrouting

import (
	"slices"
	"sort"
	"strings"
	"sync"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// FlowInputProducerResolver shares compiled evidence only within one topology
// operation whose source remains unchanged. Do not retain it on a route table
// or bus, or reuse it after the operation/source changes. Only static producer
// evidence is memoized; returned evidence is caller-private. Mutable routing
// state is never captured here.
type FlowInputProducerResolver struct {
	graph       CompiledConnectGraph
	census      semanticview.AuthoredEventEndpointCensus
	preparation *flowInputProducerPreparation
}

type flowInputProducerQuery struct {
	flowID    string
	eventType string
	options   runtimecontracts.FlowInputProducerResolutionOptions
}

type flowInputProducerPreparation struct {
	mu      sync.Mutex
	results map[flowInputProducerQuery]runtimecontracts.FlowInputProducerResolution
}

// CompileConnectGraphWithInputProducerResolver binds graph and census to the
// same compilation, so composed callers cannot pair evidence from two sources.
func CompileConnectGraphWithInputProducerResolver(source semanticview.Source) (CompiledConnectGraph, FlowInputProducerResolver) {
	graph, census := compileConnectGraphWithCensus(source)
	return graph, FlowInputProducerResolver{graph: graph, census: census, preparation: &flowInputProducerPreparation{}}
}

func (r FlowInputProducerResolver) Resolve(flowID, eventType string) runtimecontracts.FlowInputProducerResolution {
	return r.ResolveWithOptions(flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{})
}

// ResolveFlowInputProducer combines non-connect producer evidence with the
// canonical compiled graph. No semanticview consumer can inspect authored
// connect rows directly.
func ResolveFlowInputProducer(source semanticview.Source, flowID, eventType string) runtimecontracts.FlowInputProducerResolution {
	return ResolveFlowInputProducerWithOptions(source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{})
}

func ResolveFlowInputProducerWithOptions(source semanticview.Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) runtimecontracts.FlowInputProducerResolution {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return semanticview.ResolveNonConnectFlowInputProducerWithOptions(source, flowID, eventType, opts)
	}
	graph, census := compileConnectGraphWithCensus(source)
	resolver := FlowInputProducerResolver{graph: graph, census: census}
	return resolver.ResolveWithOptions(flowID, eventType, opts)
}

func (r FlowInputProducerResolver) ResolveWithOptions(flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) runtimecontracts.FlowInputProducerResolution {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if r.preparation == nil {
		return r.resolveInputProducer(flowID, eventType, opts)
	}
	// Resolver copies share only this operation's immutable query results.
	// Serialize misses too, so concurrent consumers prepare a query once.
	r.preparation.mu.Lock()
	defer r.preparation.mu.Unlock()
	key := flowInputProducerQuery{flowID: flowID, eventType: eventType, options: opts}
	out, found := r.preparation.results[key]
	if !found {
		out = r.resolveInputProducer(flowID, eventType, opts)
		if r.preparation.results == nil {
			r.preparation.results = make(map[flowInputProducerQuery]runtimecontracts.FlowInputProducerResolution)
		}
		r.preparation.results[key] = out
	}
	out.Evidence = slices.Clone(out.Evidence)
	return out
}

func (r FlowInputProducerResolver) resolveInputProducer(flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) runtimecontracts.FlowInputProducerResolution {
	out := r.census.ResolveNonConnectFlowInputProducer(flowID, eventType, opts)
	if eventType == "" {
		return out
	}
	connected := false
	for _, plan := range r.graph.plans {
		if plan.providerOutputAuthorization != nil || plan.receiver.IsRoot() != (flowID == ".") || plan.receiver.flowID.value != flowID {
			continue
		}
		if string(plan.receiver.event.value) != eventType && string(plan.receiver.resolvedEvent.value) != eventType {
			continue
		}
		connected = true
		evidence := runtimecontracts.FlowInputProducerEvidence{
			Kind: runtimecontracts.FlowInputProducerBoundaryParentConnect, FlowID: plan.source.flowID.value,
			EventType: eventType, Pin: plan.receiver.pin.value, Detail: "compiled parent connect",
		}
		if !flowInputProducerEvidenceContains(out.Evidence, evidence) {
			out.Evidence = append(out.Evidence, evidence)
		}
	}
	if connected && flowID == "." && !opts.AllowNonInputEvent {
		filtered := out.Evidence[:0]
		for _, evidence := range out.Evidence {
			if evidence.Kind != runtimecontracts.FlowInputProducerBoundaryExternalIngress {
				filtered = append(filtered, evidence)
			}
		}
		out.Evidence = filtered
	}
	sort.SliceStable(out.Evidence, func(i, j int) bool {
		return flowInputProducerEvidenceLess(out.Evidence[i], out.Evidence[j])
	})
	return out
}

func flowInputProducerEvidenceContains(existing []runtimecontracts.FlowInputProducerEvidence, candidate runtimecontracts.FlowInputProducerEvidence) bool {
	for _, evidence := range existing {
		if flowInputProducerEvidenceKey(evidence) == flowInputProducerEvidenceKey(candidate) {
			return true
		}
	}
	return false
}

type flowInputProducerEvidenceIdentity struct {
	kind      string
	flowID    string
	eventType string
	pin       string
	pattern   string
	detail    string
}

func flowInputProducerEvidenceKey(e runtimecontracts.FlowInputProducerEvidence) flowInputProducerEvidenceIdentity {
	return flowInputProducerEvidenceIdentity{
		kind: e.Kind, flowID: e.FlowID, eventType: e.EventType,
		pin: e.Pin, pattern: e.Pattern, detail: e.Detail,
	}
}

func flowInputProducerEvidenceLess(left, right runtimecontracts.FlowInputProducerEvidence) bool {
	l, r := flowInputProducerEvidenceKey(left), flowInputProducerEvidenceKey(right)
	if l.kind != r.kind {
		return l.kind < r.kind
	}
	if l.flowID != r.flowID {
		return l.flowID < r.flowID
	}
	if l.eventType != r.eventType {
		return l.eventType < r.eventType
	}
	if l.pin != r.pin {
		return l.pin < r.pin
	}
	if l.pattern != r.pattern {
		return l.pattern < r.pattern
	}
	return l.detail < r.detail
}
