package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const policySheetLookupCheckID = "policy_sheet_lookup_value_rows"

func checkPolicySheetLookupValueRows(c *checkerContext) []Finding {
	findings := make([]Finding, 0)
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		nodeID := node.Key()
		for eventType, handler := range record.Entry.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			findings = append(findings, policySheetLookupDuplicateComputedBindings(nodeID, eventType, handler)...)
			for idx, rule := range handler.Rules {
				if !policySheetRuleIsLookupValueRow(rule) {
					continue
				}
				ref := policySheetLookupRef{
					Node:      node,
					EventType: eventType,
					RuleIndex: idx,
					RuleID:    strings.TrimSpace(rule.ID),
					RuleRef:   policyRuleElementRef(rule),
				}
				findings = append(findings, validatePolicySheetLookupValueRow(c.source, ref, handler, rule)...)
			}
		}
	}
	return findings
}

type policySheetLookupRef struct {
	Node      runtimeidentity.ExecutableNode
	EventType string
	RuleIndex int
	RuleID    string
	RuleRef   runtimeidentity.DeclarationIdentity
}

func policySheetLookupFinding(ref policySheetLookupRef, detail string) Finding {
	return Finding{
		CheckID:  policySheetLookupCheckID,
		Severity: SeverityHardInvalidity,
		Message:  fmt.Sprintf("flow %s node %s handler %s lookup row %s: %s", defaultFlowLabel(ref.Node.FlowPath()), ref.Node.Key(), ref.EventType, ref.RowLabel(), detail),
		Location: ref.Node.Key(),
	}
}

func (r policySheetLookupRef) RowLabel() string {
	if id := strings.TrimSpace(r.RuleID); id != "" {
		return id
	}
	return fmt.Sprintf("#%d", r.RuleIndex)
}

func policySheetRuleIsLookupValueRow(rule runtimecontracts.HandlerRuleEntry) bool {
	if rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindLookup {
		return true
	}
	return rule.Compute != nil && rule.Compute.Operation == runtimecontracts.ComputeOpLookup
}

func validatePolicySheetLookupValueRow(source semanticview.Source, ref policySheetLookupRef, handler runtimecontracts.SystemNodeEventHandler, rule runtimecontracts.HandlerRuleEntry) []Finding {
	findings := make([]Finding, 0)
	if rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindLookup {
		findings = append(findings, policySheetLookupFinding(ref, "lookup compute must originate from a policy-sheet lookup row"))
	}
	if rule.Compute == nil || rule.Compute.Operation != runtimecontracts.ComputeOpLookup {
		findings = append(findings, policySheetLookupFinding(ref, "lookup row must lower to compute-owned lookup operation"))
		return findings
	}
	spec := rule.Compute.Lookup
	if spec == nil {
		findings = append(findings, policySheetLookupFinding(ref, "lookup row has no canonical lookup plan"))
		return findings
	}
	storePath := runtimepaths.Parse(rule.Compute.StoreAs)
	if storePath.Root != runtimepaths.RootComputed || len(storePath.Segments) == 0 {
		findings = append(findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.into must target computed.*, got %q", strings.TrimSpace(rule.Compute.StoreAs))))
	} else if !policySheetLookupPathIsSimple(storePath) {
		findings = append(findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.into %q must be a simple computed.* path", strings.TrimSpace(rule.Compute.StoreAs))))
	}
	if !spec.DefaultDeclared {
		if !policySheetLookupDomainsClosedAndExhaustive(source, ref, spec, &findings) {
			findings = append(findings, policySheetLookupFinding(ref, "lookup.default: fail is required because at least one lookup.on root has an open or unknown domain or the closed-domain table is not exhaustive"))
		}
	} else if !spec.DefaultFail {
		findings = append(findings, policySheetLookupFinding(ref, "lookup.default currently supports only fail"))
	}
	findings = append(findings, validatePolicySheetLookupKeyTypes(source, ref, spec)...)
	if !policySheetLookupBindingConsumed(source, ref.Node, ref.EventType, handler, ref.RuleIndex, strings.TrimSpace(rule.Compute.StoreAs)) {
		findings = append(findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.into %q is not consumed by a supported downstream condition, emit field, activity input, fan_out, or expression", strings.TrimSpace(rule.Compute.StoreAs))))
	}
	return findings
}

func policySheetLookupPathIsSimple(path runtimepaths.Path) bool {
	if path.Root == runtimepaths.RootUnknown || len(path.Segments) == 0 {
		return false
	}
	for _, segment := range path.Segments {
		if strings.TrimSpace(segment) == "" {
			return false
		}
	}
	return true
}

func policySheetLookupDomainsClosedAndExhaustive(source semanticview.Source, ref policySheetLookupRef, spec *runtimecontracts.ComputeLookupSpec, findings *[]Finding) bool {
	domains := make([][]string, 0, len(spec.OnPaths))
	for _, path := range spec.OnPaths {
		kind, closed := policySheetLookupPathDomain(source, ref.Node.FlowPath(), ref.EventType, path)
		if kind == "" {
			kind = "unknown"
		}
		if !closed {
			return false
		}
		switch kind {
		case "bool":
			domains = append(domains, []string{"bool:false", "bool:true"})
		default:
			if findings != nil {
				*findings = append(*findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.on %q resolves to unsupported closed domain kind %s", path.String(), kind)))
			}
			return false
		}
	}
	required := policySheetLookupCartesianKeys(domains)
	seen := map[string]struct{}{}
	for _, entry := range spec.Entries {
		seen[policySheetLookupEntryKey(entry.Key)] = struct{}{}
	}
	for _, key := range required {
		if _, ok := seen[key]; !ok {
			return false
		}
	}
	return true
}

func policySheetLookupCartesianKeys(domains [][]string) []string {
	if len(domains) == 0 {
		return nil
	}
	out := []string{""}
	for _, domain := range domains {
		next := make([]string, 0, len(out)*len(domain))
		for _, prefix := range out {
			for _, value := range domain {
				if prefix == "" {
					next = append(next, value)
					continue
				}
				next = append(next, prefix+"\x00"+value)
			}
		}
		out = next
	}
	return out
}

func policySheetLookupEntryKey(key []runtimecontracts.ComputeLookupLiteral) string {
	parts := make([]string, 0, len(key))
	for _, literal := range key {
		parts = append(parts, strings.TrimSpace(literal.Canonical))
	}
	return strings.Join(parts, "\x00")
}

func validatePolicySheetLookupKeyTypes(source semanticview.Source, ref policySheetLookupRef, spec *runtimecontracts.ComputeLookupSpec) []Finding {
	findings := make([]Finding, 0)
	kinds := make([]string, 0, len(spec.OnPaths))
	for _, path := range spec.OnPaths {
		kind, _ := policySheetLookupPathDomain(source, ref.Node.FlowPath(), ref.EventType, path)
		kinds = append(kinds, kind)
	}
	for entryIdx, entry := range spec.Entries {
		if len(entry.Key) != len(kinds) {
			findings = append(findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.entries[%d] key width %d does not match lookup.on width %d", entryIdx, len(entry.Key), len(kinds))))
			continue
		}
		for keyIdx, literal := range entry.Key {
			want := kinds[keyIdx]
			if want == "" {
				continue
			}
			if !policySheetLookupLiteralMatchesKind(literal.Kind, want) {
				findings = append(findings, policySheetLookupFinding(ref, fmt.Sprintf("lookup.entries[%d].key[%d] literal %s has type %s but lookup.on %q is %s", entryIdx, keyIdx, literal.Summary, literal.Kind, spec.On[keyIdx], want)))
			}
		}
	}
	return findings
}

func policySheetLookupLiteralMatchesKind(got, want string) bool {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	switch want {
	case "":
		return true
	case "number":
		return got == "number"
	default:
		return got == want
	}
}

func policySheetLookupPathDomain(source semanticview.Source, flowID, eventType string, path runtimepaths.Path) (kind string, closed bool) {
	if path.Root != runtimepaths.RootPayload || len(path.Segments) != 1 {
		return "", false
	}
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema {
		return "", false
	}
	field, ok := proof.Entry.Payload.Properties[strings.TrimSpace(path.Segments[0])]
	if !ok {
		return "", false
	}
	switch strings.TrimSpace(strings.ToLower(field.Type)) {
	case "bool", "boolean":
		return "bool", true
	case "int", "integer":
		return "int", false
	case "number", "numeric", "float", "double":
		return "number", false
	case "string", "text", "uuid", "timestamp", "datetime", "date":
		return "string", false
	default:
		return "", false
	}
}

func policySheetLookupBindingConsumed(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler, currentRuleIndex int, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, expr := range handlerExecutableReaderExpressionsForSource(source, node, eventType, handler) {
		if !policySheetBindingReaderExecutesAfter(expr, currentRuleIndex) {
			continue
		}
		if policySheetLookupExpressionConsumes(expr.Expression, target) {
			return true
		}
	}
	return false
}

func policySheetBindingReaderExecutesAfter(expr expressionReference, currentRuleIndex int) bool {
	if expr.HasRuleIndex && expr.RuleCollection == "rules" && expr.RuleField == "Compute" {
		return expr.RuleIndex > currentRuleIndex
	}
	switch expr.HandlerField {
	case "SelectEntity", "SelectOrCreateEntity", "Loop", "Query", "Guard", "Join", "Accumulate", "Filter", "GroupBy", "Reduce", "Count", "Compute":
		return false
	}
	switch expr.Phase {
	case runtimepipeline.WorkflowEntityFieldLifecycleFanOut,
		runtimepipeline.WorkflowEntityFieldLifecycleOnComplete,
		runtimepipeline.WorkflowEntityFieldLifecycleRule,
		runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation,
		runtimepipeline.WorkflowEntityFieldLifecycleEmitFields:
		return true
	default:
		return false
	}
}

func policySheetLookupExpressionConsumes(expression, target string) bool {
	expression = strings.TrimSpace(expression)
	target = strings.TrimSpace(target)
	if expression == "" || target == "" {
		return false
	}
	if expression == target {
		return true
	}
	return strings.Contains(expression, target+".") ||
		strings.Contains(expression, target+" ") ||
		strings.Contains(expression, target+"=") ||
		strings.Contains(expression, target+")") ||
		strings.Contains(expression, target+"]") ||
		strings.Contains(expression, target+",") ||
		strings.Contains(expression, target+"!") ||
		strings.HasSuffix(expression, target)
}

func policySheetLookupDuplicateComputedBindings(nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) []Finding {
	type binding struct {
		Target string
		Scope  string
	}
	bindings := make([]binding, 0)
	appendCompute := func(scope string, spec *runtimecontracts.ComputeSpec) {
		if spec == nil {
			return
		}
		target := strings.TrimSpace(spec.StoreAs)
		if target == "" {
			return
		}
		if runtimepaths.Parse(target).Root == runtimepaths.RootComputed {
			bindings = append(bindings, binding{Target: target, Scope: scope})
		}
	}
	appendCompute("handler.compute", handler.Compute)
	for idx, rule := range handler.Rules {
		scope := fmt.Sprintf("handler.rules[%d].compute", idx)
		if id := strings.TrimSpace(rule.ID); id != "" {
			scope = "handler.rules[" + id + "].compute"
		}
		appendCompute(scope, rule.Compute)
	}
	seen := map[string]binding{}
	findings := make([]Finding, 0)
	for _, item := range bindings {
		if prev, ok := seen[item.Target]; ok {
			findings = append(findings, Finding{
				CheckID:  policySheetLookupCheckID,
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("node %s handler %s computed binding %q is written by both %s and %s", nodeID, eventType, item.Target, prev.Scope, item.Scope),
				Location: nodeID,
			})
			continue
		}
		seen[item.Target] = item
	}
	return findings
}
