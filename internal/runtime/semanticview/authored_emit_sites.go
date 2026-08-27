package semanticview

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

type AuthoredEmitSiteSourceKind string

const (
	AuthoredEmitSiteSourceProject AuthoredEmitSiteSourceKind = "project"
	AuthoredEmitSiteSourceFlow    AuthoredEmitSiteSourceKind = "flow"
)

type AuthoredEmitSite struct {
	ID             string
	SourceKind     AuthoredEmitSiteSourceKind
	SourceScopeKey string
	FlowPath       string
	Node           runtimeidentity.ExecutableNode
	HandlerEvent   string
	Site           string
	SiteKey        string
	RuleID         string
	RuleRef        contractelementidentity.ContractElementRef
	Spec           runtimecontracts.EmitSpec
	Handler        runtimecontracts.SystemNodeEventHandler
}

func (s AuthoredEmitSite) FlowID() string     { return s.Node.FlowID() }
func (s AuthoredEmitSite) PackageKey() string { return s.Node.PackageKey() }
func (s AuthoredEmitSite) NodeID() string     { return s.Node.NodeID() }

func AuthoredEmitSites(source Source) []AuthoredEmitSite {
	if source == nil {
		return nil
	}
	builder := authoredEmitSiteBuilder{
		seen: map[string]struct{}{},
	}
	if bundle, ok := Bundle(source); ok {
		builder.bundle = bundle
	}
	records := append([]runtimecontracts.ScopedNodeRecord(nil), source.ExecutableNodeRecords()...)
	sort.Slice(records, func(i, j int) bool {
		left, _ := records[i].Identity()
		right, _ := records[j].Identity()
		return left.Key() < right.Key()
	})
	for _, record := range records {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		kind := AuthoredEmitSiteSourceProject
		if node.FlowID() != "" {
			kind = AuthoredEmitSiteSourceFlow
		}
		flowPath := executableNodeFlowPath(source, node)
		for _, handlerEvent := range sortedAuthoredHandlerEvents(record.Entry.EventHandlers) {
			handler := runtimecontracts.DefaultSystemNodeHandlerSourceEvent(record.Entry.EventHandlers[handlerEvent], handlerEvent)
			handler, _ = runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
			builder.appendHandlerSites(kind, node.PackageKey(), flowPath, node, handlerEvent, handler)
		}
	}
	sort.SliceStable(builder.sites, func(i, j int) bool {
		return builder.sites[i].ID < builder.sites[j].ID
	})
	return builder.sites
}

type authoredEmitSiteBuilder struct {
	bundle *runtimecontracts.WorkflowContractBundle
	seen   map[string]struct{}
	sites  []AuthoredEmitSite
}

func (b *authoredEmitSiteBuilder) appendHandlerSites(kind AuthoredEmitSiteSourceKind, scopeKey, flowPath string, node runtimeidentity.ExecutableNode, handlerEvent string, handler runtimecontracts.SystemNodeEventHandler) {
	add := func(site, siteKey, ruleID string, ruleRef contractelementidentity.ContractElementRef, spec runtimecontracts.EmitSpec) {
		if spec.Empty() {
			return
		}
		if b.bundle != nil {
			if lowered, err := b.bundle.LowerEmitSpecFields(runtimecontracts.EmitFieldLoweringContext{
				Node:             node,
				TriggerEventType: handlerEvent,
				Site:             siteKey,
			}, spec); err == nil {
				spec = lowered
			}
		}
		id := authoredEmitSiteIdentity(node.FlowID(), scopeKey, node.Key(), handlerEvent, siteKey)
		if _, ok := b.seen[id]; ok {
			return
		}
		b.seen[id] = struct{}{}
		b.sites = append(b.sites, AuthoredEmitSite{
			ID:             id,
			SourceKind:     kind,
			SourceScopeKey: scopeKey,
			FlowPath:       flowPath,
			Node:           node,
			HandlerEvent:   strings.TrimSpace(handlerEvent),
			Site:           site,
			SiteKey:        siteKey,
			RuleID:         strings.TrimSpace(ruleID),
			RuleRef:        ruleRef,
			Spec:           spec,
			Handler:        handler,
		})
	}
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler) {
		add(site.Source, site.SiteKey, site.RuleID, site.RuleRef, site.Spec)
	}
	if handler.Guard != nil {
		if emitSpec := authoredGuardEscalationEmitSpec(handler.Guard); !emitSpec.Empty() {
			add("handler.guard.on_fail.escalate", "handler.guard.on_fail.escalate", handler.Guard.ID, contractelementidentity.ContractElementRef{}, emitSpec)
		}
	}
}

func executableNodeFlowPath(source Source, node runtimeidentity.ExecutableNode) string {
	semanticScope, err := ResolveExecutableNodeSemanticScope(source, node)
	if err == nil {
		if scope, ok := semanticScope.OwningFlow(); ok {
			return strings.Trim(strings.TrimSpace(scope.Path), "/")
		}
	}
	return ""
}

func sortedAuthoredHandlerEvents(handlers map[string]runtimecontracts.SystemNodeEventHandler) []string {
	keys := make([]string, 0, len(handlers))
	for key := range handlers {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func authoredEmitSiteIdentity(flowID, scopeKey, nodeKey, handlerEvent, siteKey string) string {
	return strings.Join([]string{
		strings.TrimSpace(flowID),
		strings.TrimSpace(scopeKey),
		strings.TrimSpace(nodeKey),
		strings.TrimSpace(handlerEvent),
		strings.TrimSpace(siteKey),
	}, "\x1f")
}

func authoredGuardEscalationEmitSpec(guard *runtimecontracts.GuardSpec) runtimecontracts.EmitSpec {
	if guard == nil {
		return runtimecontracts.EmitSpec{}
	}
	failureSpec, err := guard.FailureSpec()
	if err != nil || failureSpec.Action != runtimecontracts.GuardFailureActionEscalate {
		return runtimecontracts.EmitSpec{}
	}
	return failureSpec.EscalationEmitSpec()
}
