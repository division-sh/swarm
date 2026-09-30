package contracts

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func validatePolicySheetComputeModuleInto(into string) error {
	parsed := paths.Parse(into)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) == 0 {
		return fmt.Errorf("POLICY-SHEET-ROW: compute_module.into %q must target computed.*", strings.TrimSpace(into))
	}
	for _, segment := range parsed.Segments {
		if !isPolicySheetPathSegment(segment) {
			return fmt.Errorf("POLICY-SHEET-ROW: compute_module.into %q must be a simple computed.* path", strings.TrimSpace(into))
		}
	}
	return nil
}

func validatePolicySheetRows(rules []HandlerRuleEntry, context handlerRuleDecodeContext) error {
	hasPolicyRows := false
	for _, rule := range rules {
		if rule.PolicyRow.Kind != "" {
			hasPolicyRows = true
			break
		}
	}
	if !hasPolicyRows {
		return nil
	}
	if context != handlerRuleDecodeContextRules {
		return fmt.Errorf("POLICY-SHEET-ROW: typed policy-sheet rows are only supported under handler.rules")
	}
	hasSelectionRow := false
	hasDefault := false
	caseKeys := map[string]int{}
	rangesByValue := map[string][]policySheetRangeForValidation{}
	for idx, rule := range rules {
		if rule.PolicyRow.Kind == PolicySheetRowKindDefault {
			hasDefault = true
		}
		if rule.PolicyRow.Kind == PolicySheetRowKindWhen || rule.PolicyRow.Kind == PolicySheetRowKindCase || rule.PolicyRow.Kind == PolicySheetRowKindRange {
			hasSelectionRow = true
		}
		switch rule.PolicyRow.Kind {
		case PolicySheetRowKindCase:
			key := strings.Join(rule.PolicyRow.Selectors, "\x00") + "\x01" + strings.Join(rule.PolicyRow.CaseValues, "\x00")
			if prev, ok := caseKeys[key]; ok {
				return fmt.Errorf("POLICY-SHEET-ROW: duplicate case key at rules[%d] and rules[%d]", prev, idx)
			}
			caseKeys[key] = idx
		case PolicySheetRowKindRange:
			metadata := rule.PolicyRow
			rangesByValue[metadata.RangeValue] = append(rangesByValue[metadata.RangeValue], policySheetRangeForValidation{
				Index:        idx,
				Lower:        metadata.RangeLower,
				Upper:        metadata.RangeUpper,
				Monotonicity: metadata.Monotonicity,
			})
		}
	}
	if hasSelectionRow && !hasDefault {
		return fmt.Errorf("POLICY-SHEET-ROW: rules with typed policy-sheet rows require an else/default row")
	}
	for value, ranges := range rangesByValue {
		if err := validatePolicySheetRanges(value, ranges); err != nil {
			return err
		}
	}
	return nil
}

type policySheetRangeForValidation struct {
	Index        int
	Lower        PolicySheetRangeBound
	Upper        PolicySheetRangeBound
	Monotonicity []string
}

func validatePolicySheetValidateInto(into string) error {
	parsed := paths.Parse(into)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) < 2 || parsed.Segments[0] != "validation" {
		return fmt.Errorf("POLICY-SHEET-ROW: validate.into %q must target computed.validation.*", strings.TrimSpace(into))
	}
	for _, segment := range parsed.Segments {
		if !isPolicySheetPathSegment(segment) {
			return fmt.Errorf("POLICY-SHEET-ROW: validate.into %q must be a simple computed.validation.* path", strings.TrimSpace(into))
		}
	}
	return nil
}

func validatePolicySheetLookupInto(into string) error {
	parsed := paths.Parse(into)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) == 0 {
		return fmt.Errorf("POLICY-SHEET-ROW: lookup.into %q must target computed.*", strings.TrimSpace(into))
	}
	for _, segment := range parsed.Segments {
		if !isPolicySheetPathSegment(segment) {
			return fmt.Errorf("POLICY-SHEET-ROW: lookup.into %q must be a simple computed.* path", strings.TrimSpace(into))
		}
	}
	return nil
}

func canonicalPolicySheetLookupKey(key []ComputeLookupLiteral) string {
	parts := make([]string, 0, len(key))
	for _, literal := range key {
		parts = append(parts, literal.Canonical)
	}
	return strings.Join(parts, "\x00")
}

func CanonicalizeComputeLookupValue(value any) (kind, summary, canonical string, ok bool) {
	switch typed := value.(type) {
	case string:
		kind, summary = "string", strconv.Quote(typed)
	case bool:
		kind, summary = "bool", strconv.FormatBool(typed)
	case int:
		kind, summary = "int", strconv.FormatInt(int64(typed), 10)
	case int8:
		kind, summary = "int", strconv.FormatInt(int64(typed), 10)
	case int16:
		kind, summary = "int", strconv.FormatInt(int64(typed), 10)
	case int32:
		kind, summary = "int", strconv.FormatInt(int64(typed), 10)
	case int64:
		kind, summary = "int", strconv.FormatInt(typed, 10)
	case uint:
		kind, summary = "int", strconv.FormatUint(uint64(typed), 10)
	case uint8:
		kind, summary = "int", strconv.FormatUint(uint64(typed), 10)
	case uint16:
		kind, summary = "int", strconv.FormatUint(uint64(typed), 10)
	case uint32:
		kind, summary = "int", strconv.FormatUint(uint64(typed), 10)
	case uint64:
		kind, summary = "int", strconv.FormatUint(typed, 10)
	case float32:
		kind, summary = "number", strconv.FormatFloat(float64(typed), 'g', -1, 64)
	case float64:
		kind, summary = "number", strconv.FormatFloat(typed, 'g', -1, 64)
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			kind, summary = "int", strconv.FormatInt(i, 10)
		} else if f, err := typed.Float64(); err == nil {
			kind, summary = "number", strconv.FormatFloat(f, 'g', -1, 64)
		} else {
			return "", "", "", false
		}
	default:
		return "", "", "", false
	}
	return kind, summary, kind + ":" + summary, true
}

func validatePolicySheetRanges(value string, ranges []policySheetRangeForValidation) error {
	graph := newPolicySheetMonotonicityGraph()
	for _, row := range ranges {
		for _, constraint := range row.Monotonicity {
			if err := graph.addConstraint(constraint); err != nil {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range.monotonicity: %w", row.Index, err)
			}
		}
	}
	for _, row := range ranges {
		if row.Lower.Kind == "policy" || row.Upper.Kind == "policy" {
			if len(row.Monotonicity) == 0 {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range with policy-constant bounds requires monotonicity", row.Index)
			}
		}
		if row.Lower.Value != "" && row.Upper.Value != "" {
			if err := graph.proveOrdered(row.Lower.Value, row.Upper.Value); err != nil {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range lower/upper bounds for %s: %w", row.Index, value, err)
			}
		}
	}
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			if err := validatePolicySheetRangePairDoesNotOverlap(value, graph, ranges[i], ranges[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validatePolicySheetRangePairDoesNotOverlap(value string, graph *policySheetMonotonicityGraph, a, b policySheetRangeForValidation) error {
	if aNumeric := policySheetRangeUsesOnlyLiteralBounds(a); aNumeric && policySheetRangeUsesOnlyLiteralBounds(b) {
		if policySheetNumericRangesOverlap(a, b) {
			return fmt.Errorf("POLICY-SHEET-ROW: overlapping literal ranges for %s at rules[%d] and rules[%d]", value, a.Index, b.Index)
		}
		return nil
	}
	if policySheetRangesAreStructurallyDisjoint(graph, a, b) {
		return nil
	}
	return fmt.Errorf("POLICY-SHEET-ROW: overlapping ranges for %s at rules[%d] and rules[%d]", value, a.Index, b.Index)
}

func policySheetRangeUsesOnlyLiteralBounds(row policySheetRangeForValidation) bool {
	if row.Lower.Value != "" && row.Lower.Kind != "literal" {
		return false
	}
	if row.Upper.Value != "" && row.Upper.Kind != "literal" {
		return false
	}
	return true
}

func policySheetRangesAreStructurallyDisjoint(graph *policySheetMonotonicityGraph, a, b policySheetRangeForValidation) bool {
	return policySheetUpperBeforeLower(graph, a.Upper, b.Lower) || policySheetUpperBeforeLower(graph, b.Upper, a.Lower)
}

func policySheetUpperBeforeLower(graph *policySheetMonotonicityGraph, upper, lower PolicySheetRangeBound) bool {
	if upper.Value == "" || lower.Value == "" {
		return false
	}
	if err := graph.proveOrdered(upper.Value, lower.Value); err != nil {
		return false
	}
	return upper.Operator == "<" || lower.Operator == ">"
}

func policySheetNumericRangesOverlap(a, b policySheetRangeForValidation) bool {
	aMin, aMax, aMinClosed, aMaxClosed, okA := policySheetNumericInterval(a)
	bMin, bMax, bMinClosed, bMaxClosed, okB := policySheetNumericInterval(b)
	if !okA || !okB {
		return false
	}
	if aMax < bMin || bMax < aMin {
		return false
	}
	if aMax == bMin {
		return aMaxClosed && bMinClosed
	}
	if bMax == aMin {
		return bMaxClosed && aMinClosed
	}
	return true
}

func policySheetNumericInterval(row policySheetRangeForValidation) (float64, float64, bool, bool, bool) {
	min := math.Inf(-1)
	max := math.Inf(1)
	minClosed := false
	maxClosed := false
	if row.Lower.Value != "" {
		if row.Lower.Kind != "literal" {
			return 0, 0, false, false, false
		}
		parsed, err := strconv.ParseFloat(row.Lower.Value, 64)
		if err != nil {
			return 0, 0, false, false, false
		}
		min = parsed
		minClosed = row.Lower.Operator == ">="
	}
	if row.Upper.Value != "" {
		if row.Upper.Kind != "literal" {
			return 0, 0, false, false, false
		}
		parsed, err := strconv.ParseFloat(row.Upper.Value, 64)
		if err != nil {
			return 0, 0, false, false, false
		}
		max = parsed
		maxClosed = row.Upper.Operator == "<="
	}
	return min, max, minClosed, maxClosed, true
}

type policySheetMonotonicityGraph struct {
	edges map[string]map[string]struct{}
}

func newPolicySheetMonotonicityGraph() *policySheetMonotonicityGraph {
	return &policySheetMonotonicityGraph{edges: map[string]map[string]struct{}{}}
}

func (g *policySheetMonotonicityGraph) addConstraint(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("constraint must be non-empty")
	}
	parts := strings.Split(raw, "<=")
	if len(parts) < 2 {
		return fmt.Errorf("constraint %q must use <= monotonicity", raw)
	}
	for i := range parts {
		parts[i] = canonicalPolicySheetTerm(parts[i])
		if err := validatePolicySheetMonotonicityTerm(parts[i]); err != nil {
			return err
		}
	}
	for i := 0; i+1 < len(parts); i++ {
		from, to := parts[i], parts[i+1]
		if g.edges[from] == nil {
			g.edges[from] = map[string]struct{}{}
		}
		g.edges[from][to] = struct{}{}
	}
	return nil
}

func (g *policySheetMonotonicityGraph) proveOrdered(lower, upper string) error {
	lower = canonicalPolicySheetTerm(lower)
	upper = canonicalPolicySheetTerm(upper)
	if lower == "" || upper == "" || lower == upper {
		return nil
	}
	if l, errL := strconv.ParseFloat(lower, 64); errL == nil {
		if u, errU := strconv.ParseFloat(upper, 64); errU == nil {
			if l <= u {
				return nil
			}
			return fmt.Errorf("%s is greater than %s", lower, upper)
		}
	}
	seen := map[string]struct{}{lower: {}}
	queue := []string{lower}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for next := range g.edges[current] {
			if next == upper {
				return nil
			}
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return fmt.Errorf("monotonicity does not prove %s <= %s", lower, upper)
}

func validatePolicySheetMonotonicityTerm(term string) error {
	term = canonicalPolicySheetTerm(term)
	if term == "" {
		return fmt.Errorf("monotonicity term must be non-empty")
	}
	if _, err := strconv.ParseFloat(term, 64); err == nil {
		return nil
	}
	if isPolicySheetPolicyConstantExpression(term) {
		return nil
	}
	return fmt.Errorf("POLICY-SHEET-ROW: range.monotonicity term %q must be a numeric literal or policy-constant expression", term)
}

func validatePolicySheetSelector(selector, label string) error {
	return validatePolicySheetPath(selector, label, []string{"payload", "entity", "policy", "event"})
}

func validatePolicySheetPath(expr, label string, allowedRoots []string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return fmt.Errorf("POLICY-SHEET-ROW: %s must be non-empty", label)
	}
	root := policySheetRoot(expr)
	if root == "" {
		return fmt.Errorf("POLICY-SHEET-ROW: %s %q must be a dotted path rooted in %s", label, expr, strings.Join(allowedRoots, ", "))
	}
	for _, allowed := range allowedRoots {
		if root == allowed {
			parts := strings.Split(expr, ".")
			for idx, part := range parts {
				if !isPolicySheetPathSegment(part) {
					return fmt.Errorf("POLICY-SHEET-ROW: %s %q must be a simple dotted path", label, expr)
				}
				if idx == 0 && part != allowed {
					return fmt.Errorf("POLICY-SHEET-ROW: %s %q uses unsupported root %q", label, expr, root)
				}
			}
			return nil
		}
	}
	return fmt.Errorf("POLICY-SHEET-ROW: %s %q uses unsupported root %q", label, expr, root)
}

func isPolicySheetPathSegment(segment string) bool {
	if segment == "" {
		return false
	}
	for idx, r := range segment {
		switch {
		case r == '_':
			continue
		case r >= 'a' && r <= 'z':
			continue
		case r >= 'A' && r <= 'Z':
			continue
		case idx > 0 && r >= '0' && r <= '9':
			continue
		default:
			return false
		}
	}
	return true
}

func policySheetRoot(expr string) string {
	expr = strings.TrimSpace(expr)
	if idx := strings.Index(expr, "."); idx > 0 {
		return expr[:idx]
	}
	return ""
}

func canonicalPolicySheetTerm(term string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(term)), "")
}

func isPolicySheetPolicyConstantExpression(expr string) bool {
	expr = canonicalPolicySheetTerm(expr)
	if expr == "" {
		return false
	}
	hasPolicy := false
	tokens := strings.FieldsFunc(expr, func(r rune) bool {
		switch r {
		case '+', '-', '*', '/', '(', ')':
			return true
		default:
			return false
		}
	})
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if _, err := strconv.ParseFloat(token, 64); err == nil {
			continue
		}
		if strings.HasPrefix(token, "policy.") {
			if err := validatePolicySheetPath(token, "policy-constant bound", []string{"policy"}); err != nil {
				return false
			}
			hasPolicy = true
			continue
		}
		return false
	}
	for _, r := range expr {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '+' || r == '-' || r == '*' || r == '/' || r == '(' || r == ')' {
			continue
		}
		return false
	}
	return hasPolicy
}
