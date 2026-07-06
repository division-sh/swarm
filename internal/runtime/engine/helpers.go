package engine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/cel-go/cel"
)

const handlerAccumulatorBucketKey = "handler_accumulators"

var (
	executionConditionEnvOnce sync.Once
	executionConditionEnvRef  *cel.Env
	executionConditionEnvErr  error
)

type Accumulator struct {
	Expected       []string         `json:"expected,omitempty"`
	ExpectedCount  int              `json:"expected_count,omitempty"`
	Received       map[string]bool  `json:"received,omitempty"`
	Items          []map[string]any `json:"items,omitempty"`
	StartedAt      string           `json:"started_at,omitempty"`
	LastEventID    string           `json:"last_event_id,omitempty"`
	LastEventType  string           `json:"last_event_type,omitempty"`
	LastSource     string           `json:"last_source,omitempty"`
	LastReceivedAt string           `json:"last_received_at,omitempty"`
}

func arrivalIdentifier(evt events.Event, payload map[string]any) string {
	candidates := []string{
		strings.TrimSpace(evt.ID()),
		strings.TrimSpace(asString(payload["event_id"])),
		strings.TrimSpace(asString(payload["id"])),
		strings.TrimSpace(asString(payload["item_id"])),
		strings.TrimSpace(asString(payload["source"])),
		strings.TrimSpace(asString(payload["from"])),
		strings.TrimSpace(asString(payload["agent_id"])),
		strings.TrimSpace(asString(payload["node_id"])),
		strings.TrimSpace(evt.SourceAgent()),
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func dedupIdentifier(base BaseContext, state ExecutionState, evt events.Event, spec *runtimecontracts.AccumulateSpec) string {
	if spec != nil {
		if value, ok := resolveContractPath(base, state, spec.DedupPath, spec.DedupBy); ok {
			if key := stringifyDedupValue(value); key != "" {
				return key
			}
		} else if ref := strings.TrimSpace(spec.DedupBy); ref != "" {
			if value := resolveRef(base, state, ref); value != nil {
				if key := stringifyDedupValue(value); key != "" {
					return key
				}
			}
		}
	}
	return arrivalIdentifier(evt, base.Payload.Raw())
}

func lookupPath(source map[string]any, path string) (any, bool) {
	source = cloneStringAnyMap(source)
	return lookupParsedPath(source, paths.Parse(path))
}

func lookupParsedPath(source map[string]any, path paths.Path) (any, bool) {
	if source == nil || path.IsZero() {
		return nil, false
	}
	current := any(source)
	for _, segment := range path.Segments {
		object, ok := asObject(current)
		if !ok {
			return nil, false
		}
		current = object[segment]
	}
	return current, current != nil
}

func resolveParsedRef(base BaseContext, state ExecutionState, ref paths.Path) (any, bool) {
	if ref.IsZero() {
		return nil, false
	}
	if ref.HasExplicitRoot() {
		if ref.Root == paths.RootAccumulated {
			return state.AccumulatedBucket().Lookup(ref)
		}
		if ref.Root == paths.RootFanOut {
			return state.FanOutBucket().Lookup(ref)
		}
		if ref.Root == paths.RootComputed {
			return state.ComputedBucket().Lookup(ref)
		}
		if ref.Root == paths.RootGates {
			return base.Gates.Lookup(ref)
		}
		return base.Lookup(ref)
	}
	return nil, false
}

func resolveContractPath(base BaseContext, state ExecutionState, parsed paths.Path, raw string) (any, bool) {
	if parsed.IsZero() {
		parsed = paths.Parse(strings.TrimSpace(raw))
	}
	if parsed.HasExplicitRoot() {
		return resolveParsedRef(base, state, parsed)
	}
	return nil, false
}

func resolveRef(base BaseContext, state ExecutionState, ref string) any {
	parsed := paths.Parse(strings.TrimSpace(ref))
	if !parsed.HasExplicitRoot() {
		return nil
	}
	value, _ := resolveParsedRef(base, state, parsed)
	return value
}

func evalExpressionValue(base BaseContext, state ExecutionState, expr runtimecontracts.ExpressionValue, opts workflowexpr.ValueExpressionOptions) (any, bool, error) {
	if expr.IsZero() {
		return nil, false, nil
	}
	switch expr.Kind {
	case runtimecontracts.ExpressionKindLiteral:
		return expr.Literal, true, nil
	case runtimecontracts.ExpressionKindRef:
		if expr.RefPath.Root == paths.RootEvent {
			if err := events.ValidateEventContextReference(strings.Join(expr.RefPath.Segments, ".")); err != nil {
				return nil, false, err
			}
		}
		if value, ok := resolveParsedRef(base, state, expr.RefPath); ok {
			return value, true, nil
		}
		return nil, false, nil
	case runtimecontracts.ExpressionKindCEL:
		value, err := evalWorkflowValueExpression(base, state, expr.CEL, opts)
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported expression kind %q", expr.Kind)
	}
}

func setValuePath(target map[string]any, path string, value any) {
	setParsedValuePath(target, paths.Parse(path), value)
}

func setParsedValuePath(target map[string]any, path paths.Path, value any) {
	values.Wrap(target).SetPath(path, value)
}

func normalizeStateField(field string) string {
	field = strings.TrimSpace(field)
	switch {
	case strings.HasPrefix(field, "entity."):
		return strings.TrimSpace(strings.TrimPrefix(field, "entity."))
	case strings.HasPrefix(field, "metadata."):
		return strings.TrimSpace(strings.TrimPrefix(field, "metadata."))
	default:
		return field
	}
}

func applyDataAccumulationToState(base BaseContext, state ExecutionState, snapshot *StateSnapshot, spec runtimecontracts.WorkflowDataAccumulation) error {
	if snapshot == nil || len(spec.Writes) == 0 {
		return nil
	}
	if snapshot.StateCarrier.Metadata == nil {
		snapshot.StateCarrier.Metadata = map[string]any{}
	}
	for _, write := range spec.Writes {
		if write.IsContainedOperation() {
			return fmt.Errorf("data_accumulation target %s: contained operations require semantic source validation", strings.TrimSpace(write.Target()))
		}
		target := strings.TrimSpace(write.Target())
		if target == "" {
			continue
		}
		parsed := paths.Parse(target)
		switch parsed.Root {
		case paths.RootEntity, paths.RootMetadata:
			parsed = paths.Path{Segments: parsed.Segments}
		case paths.RootUnknown:
		default:
			return fmt.Errorf("data_accumulation target %s: unsupported target scope", strings.TrimSpace(write.Target()))
		}
		switch {
		case write.Value.HasLiteralValue():
			setParsedValuePath(snapshot.StateCarrier.Metadata, parsed, write.Value.Literal)
		case write.Value.HasCELValue():
			value, err := evalWorkflowValueExpression(base, state, write.Value.CEL, workflowexpr.ValueExpressionOptions{})
			if err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", strings.TrimSpace(write.Target()), err)
			}
			setParsedValuePath(snapshot.StateCarrier.Metadata, parsed, value)
		default:
			source := strings.TrimSpace(write.Source())
			if source == "" {
				continue
			}
			if value, ok := lookupPath(cloneStringAnyMap(base.Payload.Raw()), source); ok {
				setParsedValuePath(snapshot.StateCarrier.Metadata, parsed, value)
			}
		}
	}
	if sourceEvent := strings.TrimSpace(spec.SourceEvent); sourceEvent != "" {
		snapshot.SetMetadata("last_data_accumulation_source", sourceEvent)
	}
	return nil
}

func evalWorkflowValueExpression(base BaseContext, state ExecutionState, expression string, opts workflowexpr.ValueExpressionOptions) (any, error) {
	return workflowexpr.EvalValueExpressionWithOptions(expression, workflowexpr.ValueContext{
		Entity:         base.Entity.Raw(),
		PlatformEntity: base.PlatformEntity.Raw(),
		Event:          base.Event.Raw(),
		Payload:        base.Payload.Raw(),
		Policy:         base.Policy.Raw(),
		Computed:       base.Computed.Raw(),
		FanOut:         state.FanOut,
	}, opts)
}

func normalizeCELValue(value any) any {
	return workflowexpr.NormalizeCELValue(value)
}

func normalizedCELInputMap(source map[string]any) map[string]any {
	return workflowexpr.NormalizeCELInputMap(source)
}

func emitFieldsPayload(base BaseContext, state ExecutionState, spec runtimecontracts.EmitSpec, opts workflowexpr.ValueExpressionOptions) (map[string]any, error) {
	if len(spec.Fields) == 0 {
		return nil, nil
	}
	payload := map[string]any{}
	for target, valueSpec := range spec.Fields {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		value, ok, err := evalExpressionValue(base, state, valueSpec, opts)
		if err != nil {
			return nil, fmt.Errorf("emit field %s: %w", target, err)
		}
		if !ok {
			continue
		}
		setParsedValuePath(payload, paths.Parse(target), value)
	}
	return payload, nil
}

func nextChainDepth(current, max int) (int, error) {
	if max <= 0 {
		max = DefaultMaxChainDepth
	}
	next := current + 1
	if next > max {
		return next, ErrChainDepthExceeded
	}
	return next, nil
}

func accumulatorBucketRef(nodeID identity.NodeID, eventType events.EventType) timeridentity.AccumulatorBucketRef {
	return timeridentity.NewAccumulatorBucketRef(nodeID.String(), string(eventType))
}

func handlerAccumulatorEventType(req ExecutionRequest) events.EventType {
	eventType := strings.TrimSpace(req.HandlerEventKey)
	if eventType == "" {
		eventType = strings.TrimSpace(string(req.Event.Type()))
	}
	return events.EventType(eventType)
}

func handlerAccumulatorBucketRef(req ExecutionRequest) timeridentity.AccumulatorBucketRef {
	return accumulatorBucketRef(req.NodeID, handlerAccumulatorEventType(req))
}

func loadAccumulator(state StateSnapshot, nodeID identity.NodeID, eventType events.EventType) (*Accumulator, bool) {
	return loadAccumulatorForBucket(state, accumulatorBucketRef(nodeID, eventType))
}

func loadAccumulatorForBucket(state StateSnapshot, bucketRef timeridentity.AccumulatorBucketRef) (*Accumulator, bool) {
	bucketRef = bucketRef.Normalize()
	if !bucketRef.Valid() {
		return nil, false
	}
	bucket, ok := state.StateBucket(bucketRef.NodeID)
	if !ok {
		return nil, false
	}
	rawAccumulators, ok := bucket.Map(handlerAccumulatorBucketKey)
	if !ok {
		return nil, false
	}
	raw, ok := rawAccumulators.Map(bucketRef.Key())
	if !ok {
		return nil, false
	}
	acc := &Accumulator{
		Expected:       normalizeStrings(stringSliceFromAny(raw.Raw()["expected"])),
		ExpectedCount:  raw.Int("expected_count"),
		Received:       map[string]bool{},
		Items:          sliceOfMapsFromAny(raw.Raw()["items"]),
		StartedAt:      raw.String("started_at"),
		LastEventID:    raw.String("last_event_id"),
		LastEventType:  raw.String("last_event_type"),
		LastSource:     raw.String("last_source"),
		LastReceivedAt: raw.String("last_received_at"),
	}
	if received, ok := raw.Map("received"); ok {
		for _, key := range received.Keys() {
			acc.Received[strings.TrimSpace(key)] = received.Bool(key)
		}
	}
	return acc, true
}

func storeAccumulator(state *StateSnapshot, nodeID identity.NodeID, eventType events.EventType, acc *Accumulator) {
	storeAccumulatorForBucket(state, accumulatorBucketRef(nodeID, eventType), acc)
}

func storeAccumulatorForBucket(state *StateSnapshot, bucketRef timeridentity.AccumulatorBucketRef, acc *Accumulator) {
	bucketRef = bucketRef.Normalize()
	if state == nil || acc == nil || !bucketRef.Valid() {
		return
	}
	bucket := state.EnsureStateBucket(bucketRef.NodeID)
	accumulators := bucket.EnsureMap(handlerAccumulatorBucketKey)
	received := map[string]any{}
	keys := make([]string, 0, len(acc.Received))
	for key := range acc.Received {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		received[key] = acc.Received[key]
	}
	items := make([]map[string]any, 0, len(acc.Items))
	for _, item := range acc.Items {
		items = append(items, cloneStringAnyMap(item))
	}
	accumulators.Set(bucketRef.Key(), map[string]any{
		"expected":         append([]string{}, acc.Expected...),
		"expected_count":   acc.ExpectedCount,
		"received":         received,
		"items":            items,
		"started_at":       acc.StartedAt,
		"last_event_id":    acc.LastEventID,
		"last_event_type":  acc.LastEventType,
		"last_source":      acc.LastSource,
		"last_received_at": acc.LastReceivedAt,
	})
}

func isAccumulationTimeoutEvent(eventType events.EventType) bool {
	eventName := strings.TrimSpace(string(eventType))
	return strings.HasSuffix(eventName, ".timeout") || strings.EqualFold(eventName, "accumulate.timeout")
}

func accumulationTimeoutBucketRefFromPayload(payload map[string]any) (timeridentity.AccumulatorBucketRef, bool) {
	return timeridentity.ParseAccumulatorBucketRef(payload)
}

func expectedAccumulatorTargets(base BaseContext, state ExecutionState, parsed paths.Path, raw string) ([]string, int) {
	value, ok := resolveContractPath(base, state, parsed, raw)
	if !ok {
		value = nil
	}
	switch typed := value.(type) {
	case []string:
		return normalizeStrings(typed), len(typed)
	case []any:
		targets := stringSliceFromAny(typed)
		if len(targets) > 0 {
			return normalizeStrings(targets), len(targets)
		}
		return nil, len(typed)
	case int:
		return nil, typed
	case int64:
		return nil, int(typed)
	case float64:
		return nil, int(typed)
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil, 0
		}
		if n, err := strconv.Atoi(text); err == nil {
			return nil, n
		}
		return []string{text}, 1
	default:
		return nil, asInt(value)
	}
}

func accumulatorComplete(
	acc *Accumulator,
	spec *runtimecontracts.AccumulateSpec,
	evalBool func(expression string, extraVars map[string]any) (bool, error),
) (bool, error) {
	if acc == nil {
		return true, nil
	}
	completion := ""
	var completionSpec runtimecontracts.AccumulateCompletion
	if spec != nil {
		completionSpec = spec.Completion
		completion = completionSpec.String()
	}
	receivedCount := len(acc.Received)
	if completionSpec.Mode == runtimecontracts.AccumulateModeDefault ||
		completionSpec.Mode == runtimecontracts.AccumulateModeAll ||
		completionSpec.Mode == runtimecontracts.AccumulateModeThreshold {
		switch {
		case completionSpec.Mode == runtimecontracts.AccumulateModeThreshold && spec != nil && spec.Threshold > 0:
			return receivedCount >= spec.Threshold, nil
		case len(acc.Expected) > 0:
			for _, expected := range acc.Expected {
				if !acc.Received[strings.TrimSpace(expected)] {
					return false, nil
				}
			}
			return true, nil
		case acc.ExpectedCount > 0:
			return receivedCount >= acc.ExpectedCount, nil
		default:
			return receivedCount > 0, nil
		}
	}
	if completionSpec.Mode == runtimecontracts.AccumulateModeTimeout {
		if strings.TrimSpace(acc.StartedAt) == "" {
			return false, nil
		}
		if strings.HasSuffix(strings.TrimSpace(acc.LastEventType), ".timeout") || strings.EqualFold(strings.TrimSpace(acc.LastEventType), "accumulate.timeout") {
			return true, nil
		}
		return false, nil
	}
	if evalBool == nil {
		return false, nil
	}
	return evalBool(completion, map[string]any{
		"accumulation": map[string]any{
			"expected_count": acc.ExpectedCount,
			"received_count": receivedCount,
		},
	})
}

func accumulatorExpressionValue(acc *Accumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	items := make([]any, 0, len(acc.Items))
	for _, item := range acc.Items {
		items = append(items, cloneStringAnyMap(item))
	}
	expected := make([]any, 0, len(acc.Expected))
	for _, item := range acc.Expected {
		expected = append(expected, item)
	}
	return map[string]any{
		"items":          items,
		"expected":       expected,
		"expected_count": acc.ExpectedCount,
		"received_count": len(acc.Received),
		"started_at":     acc.StartedAt,
	}
}

func accumulatorItemFields(item map[string]any) map[string]any {
	if len(item) == 0 {
		return map[string]any{}
	}
	if payload, ok := asObject(item["payload"]); ok && len(payload) > 0 {
		return payload
	}
	return item
}

func executionItems(value any) []any {
	return sliceFromAny(value)
}

type executionScope struct {
	Item           any
	Payload        map[string]any
	Event          map[string]any
	Entity         map[string]any
	PlatformEntity map[string]any
	Policy         map[string]any
}

type executionOperandDefaultScope string

const (
	executionOperandDefaultNone executionOperandDefaultScope = ""
	executionOperandDefaultItem executionOperandDefaultScope = "item"
)

type compiledExecutionCondition struct {
	expression string
	program    cel.Program
}

func newExecutionScope(item any, payload, event, entity, platformEntity, policy map[string]any) executionScope {
	return executionScope{
		Item:           normalizeCELValue(item),
		Payload:        normalizedCELInputMap(payload),
		Event:          normalizedCELInputMap(event),
		Entity:         normalizedCELInputMap(entity),
		PlatformEntity: normalizedCELInputMap(platformEntity),
		Policy:         normalizedCELInputMap(policy),
	}
}

func (s executionScope) activation() map[string]any {
	return map[string]any{
		"item":    s.Item,
		"payload": s.Payload,
		"event":   s.Event,
		"entity":  s.Entity,
		"_entity": s.PlatformEntity,
		"policy":  s.Policy,
	}
}

func executionConditionEnv() (*cel.Env, error) {
	executionConditionEnvOnce.Do(func() {
		executionConditionEnvRef, executionConditionEnvErr = cel.NewEnv(
			cel.Variable("item", cel.DynType),
			cel.Variable("payload", cel.DynType),
			cel.Variable("event", cel.DynType),
			cel.Variable("entity", cel.DynType),
			cel.Variable("_entity", cel.DynType),
			cel.Variable("policy", cel.DynType),
		)
	})
	return executionConditionEnvRef, executionConditionEnvErr
}

func compileExecutionCondition(expr string) (*compiledExecutionCondition, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if err := workflowexpr.ValidateEventReferences(expr); err != nil {
		return nil, err
	}
	env, err := executionConditionEnv()
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &compiledExecutionCondition{
		expression: expr,
		program:    program,
	}, nil
}

func executionEvalCondition(expr string, scope executionScope) (bool, error) {
	compiled, err := compileExecutionCondition(expr)
	if err != nil {
		return false, err
	}
	if compiled == nil {
		return true, nil
	}
	return compiled.Eval(scope)
}

func (c *compiledExecutionCondition) Eval(scope executionScope) (bool, error) {
	if c == nil {
		return true, nil
	}
	out, _, err := c.program.Eval(scope.activation())
	if err != nil {
		return false, err
	}
	value := normalizeCELValue(out)
	boolean, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("condition %q did not evaluate to bool", c.expression)
	}
	return boolean, nil
}

func (s executionScope) resolveOperand(expr string, defaultScope executionOperandDefaultScope) (any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if strings.EqualFold(expr, "true") {
		return true, nil
	}
	if strings.EqualFold(expr, "false") {
		return false, nil
	}
	if strings.EqualFold(expr, "null") {
		return nil, nil
	}
	if (strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) || (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) {
		return strings.Trim(expr, "\"'"), nil
	}
	if n, err := strconv.Atoi(expr); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, nil
	}
	if strings.HasPrefix(expr, "item.") {
		value, ok := lookupExecutionOperandPath(s.Item, strings.Split(strings.TrimPrefix(expr, "item."), "."))
		if !ok {
			return nil, fmt.Errorf("operand %q is unavailable in item scope", expr)
		}
		return value, nil
	}
	parsed := paths.Parse(expr)
	if parsed.HasExplicitRoot() {
		if parsed.Root == paths.RootEvent {
			if err := events.ValidateEventContextReference(strings.Join(parsed.Segments, ".")); err != nil {
				return nil, err
			}
		}
		value, ok := s.lookupExplicitRoot(parsed.Root, parsed.Segments)
		if !ok {
			return nil, fmt.Errorf("operand %q is unavailable in %s scope", expr, parsed.Root.String())
		}
		return value, nil
	}
	if defaultScope == executionOperandDefaultItem {
		value, ok := lookupExecutionOperandPath(s.Item, parsed.Segments)
		if !ok {
			return nil, fmt.Errorf("operand %q is unavailable in item scope", expr)
		}
		return value, nil
	}
	return nil, fmt.Errorf("operand %q requires explicit scope", expr)
}

func (s executionScope) lookupExplicitRoot(root paths.PathRoot, segments []string) (any, bool) {
	switch root {
	case paths.RootPayload:
		return lookupExecutionOperandPath(s.Payload, segments)
	case paths.RootEvent:
		return lookupExecutionOperandPath(s.Event, segments)
	case paths.RootEntity:
		return lookupExecutionOperandPath(s.Entity, segments)
	case paths.RootPlatformEntity:
		return lookupExecutionOperandPath(s.PlatformEntity, segments)
	case paths.RootPolicy:
		return lookupExecutionOperandPath(s.Policy, segments)
	default:
		return nil, false
	}
}

func lookupExecutionOperandPath(root any, segments []string) (any, bool) {
	current := root
	if len(segments) == 0 {
		return current, current != nil
	}
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		object, ok := asObject(current)
		if !ok {
			return nil, false
		}
		next, ok := object[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func executionReduceValue(items []any, operation string) any {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case "sum":
		total := 0.0
		for _, item := range items {
			if value, ok := executionNumericValue(item); ok {
				total += value
			}
		}
		return executionNormalizeNumber(total)
	case "min":
		var (
			best float64
			ok   bool
		)
		for _, item := range items {
			value, has := executionNumericValue(item)
			if !has {
				continue
			}
			if !ok || value < best {
				best = value
				ok = true
			}
		}
		return executionNormalizeNumber(best)
	case "max":
		var (
			best float64
			ok   bool
		)
		for _, item := range items {
			value, has := executionNumericValue(item)
			if !has {
				continue
			}
			if !ok || value > best {
				best = value
				ok = true
			}
		}
		return executionNormalizeNumber(best)
	case "count":
		return len(items)
	case "weighted_average":
		total := 0.0
		weights := 0.0
		for _, item := range items {
			object, ok := asObject(item)
			if !ok {
				continue
			}
			score, okScore := asFloat(object["score"])
			weight, okWeight := asFloat(object["weight"])
			if !okScore || !okWeight || weight == 0 {
				continue
			}
			total += score * weight
			weights += weight
		}
		if weights == 0 {
			return 0
		}
		return executionNormalizeNumber(total / weights)
	case "pick_or_average":
		best := 0.0
		ok := false
		for _, item := range items {
			object, isObject := asObject(item)
			if !isObject {
				continue
			}
			score, has := asFloat(object["score"])
			if !has {
				continue
			}
			if !ok || score > best {
				best = score
				ok = true
			}
		}
		return executionNormalizeNumber(best)
	default:
		return nil
	}
}

func executionNumericValue(item any) (float64, bool) {
	if value, ok := asFloat(item); ok {
		return value, true
	}
	if object, ok := asObject(item); ok {
		if value, ok := asFloat(object["value"]); ok {
			return value, true
		}
		if value, ok := asFloat(object["score"]); ok {
			return value, true
		}
	}
	return 0, false
}

func executionNormalizeNumber(value float64) any {
	if float64(int(value)) == value {
		return int(value)
	}
	return value
}

func executionDeletePath(root map[string]any, segments []string) {
	if len(root) == 0 || len(segments) == 0 {
		return
	}
	current := root
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[strings.TrimSpace(segment)].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, strings.TrimSpace(segments[len(segments)-1]))
}

func computeValue(acc *Accumulator, payload map[string]any, spec *runtimecontracts.ComputeSpec) (any, error) {
	if spec == nil {
		return nil, nil
	}
	switch spec.Operation {
	case runtimecontracts.ComputeOpWeightedAverage:
		return computeWeightedAverage(acc, spec), nil
	case runtimecontracts.ComputeOpPickOrAverage:
		return aggregateAccumulatorNumbers(acc, spec.Keys, func(current, next float64, idx int) float64 {
			if idx == 0 || next > current {
				return next
			}
			return current
		}), nil
	case runtimecontracts.ComputeOpSum:
		return aggregateAccumulatorNumbers(acc, spec.Keys, func(current, next float64, idx int) float64 {
			return current + next
		}), nil
	case runtimecontracts.ComputeOpMin:
		return aggregateAccumulatorNumbers(acc, spec.Keys, func(current, next float64, idx int) float64 {
			if idx == 0 || next < current {
				return next
			}
			return current
		}), nil
	case runtimecontracts.ComputeOpMax:
		return aggregateAccumulatorNumbers(acc, spec.Keys, func(current, next float64, idx int) float64 {
			if idx == 0 || next > current {
				return next
			}
			return current
		}), nil
	case runtimecontracts.ComputeOpCount:
		return len(acc.Items), nil
	case runtimecontracts.ComputeOpLookup:
		return nil, fmt.Errorf("lookup compute requires handler context")
	default:
		return nil, ErrNotImplemented
	}
}

func computeLookupValue(ctx BaseContext, spec *runtimecontracts.ComputeSpec) (any, error) {
	if spec == nil || spec.Lookup == nil {
		return nil, ErrNotImplemented
	}
	lookup := spec.Lookup
	key := make([]runtimecontracts.ComputeLookupLiteral, 0, len(lookup.OnPaths))
	unmatched := make([]string, 0, len(lookup.OnPaths))
	for idx, path := range lookup.OnPaths {
		value, ok := ctx.Lookup(path)
		if !ok {
			unmatched = append(unmatched, strings.TrimSpace(lookup.On[idx])+"=<missing>")
			key = append(key, runtimecontracts.ComputeLookupLiteral{Canonical: "<missing>"})
			continue
		}
		kind, summary, canonical, ok := canonicalizeComputeLookupRuntimeValue(value, lookup, idx)
		if !ok {
			unmatched = append(unmatched, strings.TrimSpace(lookup.On[idx])+"=<unsupported>")
			key = append(key, runtimecontracts.ComputeLookupLiteral{Canonical: "<unsupported>"})
			continue
		}
		unmatched = append(unmatched, strings.TrimSpace(lookup.On[idx])+"="+summary)
		key = append(key, runtimecontracts.ComputeLookupLiteral{
			Value:     value,
			Kind:      kind,
			Canonical: canonical,
			Summary:   summary,
		})
	}
	canonical := computeLookupCanonicalKey(key)
	for _, entry := range lookup.Entries {
		if computeLookupCanonicalKey(entry.Key) == canonical {
			return entry.Value, nil
		}
	}
	rowID := strings.TrimSpace(lookup.RowID)
	if rowID == "" {
		rowID = strings.TrimSpace(spec.StoreAs)
	}
	return nil, fmt.Errorf("lookup_miss_no_retry: row %s unmatched tuple [%s]", rowID, strings.Join(unmatched, ", "))
}

func canonicalizeComputeLookupRuntimeValue(value any, lookup *runtimecontracts.ComputeLookupSpec, column int) (kind, summary, canonical string, ok bool) {
	kind, summary, canonical, ok = runtimecontracts.CanonicalizeComputeLookupValue(value)
	if !ok || kind != "number" || computeLookupColumnKind(lookup, column) != "int" {
		return kind, summary, canonical, ok
	}
	integer, ok := integralLookupFloatSummary(value)
	if !ok {
		return kind, summary, canonical, true
	}
	return "int", integer, "int:" + integer, true
}

func computeLookupColumnKind(lookup *runtimecontracts.ComputeLookupSpec, column int) string {
	if lookup == nil || column < 0 {
		return ""
	}
	kind := ""
	for _, entry := range lookup.Entries {
		if column >= len(entry.Key) {
			return ""
		}
		entryKind := strings.TrimSpace(entry.Key[column].Kind)
		if entryKind == "" {
			return ""
		}
		if kind == "" {
			kind = entryKind
			continue
		}
		if kind != entryKind {
			return ""
		}
	}
	return kind
}

func integralLookupFloatSummary(value any) (string, bool) {
	var f float64
	switch typed := value.(type) {
	case float32:
		f = float64(typed)
	case float64:
		f = typed
	default:
		return "", false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f {
		return "", false
	}
	summary := strconv.FormatFloat(f, 'f', 0, 64)
	if _, err := strconv.ParseInt(summary, 10, 64); err != nil {
		return "", false
	}
	return summary, true
}

func computeLookupCanonicalKey(key []runtimecontracts.ComputeLookupLiteral) string {
	parts := make([]string, 0, len(key))
	for _, literal := range key {
		parts = append(parts, literal.Canonical)
	}
	return strings.Join(parts, "\x00")
}

func computeWeightedAverage(acc *Accumulator, spec *runtimecontracts.ComputeSpec) float64 {
	if acc == nil || len(acc.Items) == 0 || spec == nil {
		return 0
	}
	tiers := spec.Tiers
	keys := spec.Keys
	if len(tiers) == 0 {
		return computeWeightedAverageFromItems(acc, spec)
	}
	dimensionKey := strings.TrimSpace(keys.DimensionKey)
	if dimensionKey == "" {
		return 0
	}
	scoreKeys := normalizeStrings(keys.ScoreKeys)
	if len(scoreKeys) == 0 {
		return 0
	}
	dimensionScores := map[string]float64{}
	for _, item := range acc.Items {
		payload := accumulatorItemFields(item)
		dimension := strings.TrimSpace(asString(payload[dimensionKey]))
		scoreValues := make([]any, 0, len(scoreKeys))
		for _, key := range scoreKeys {
			scoreValues = append(scoreValues, payload[strings.TrimSpace(key)])
		}
		score := firstNumeric(scoreValues...)
		if dimension == "" || math.IsNaN(score) {
			continue
		}
		dimensionScores[dimension] = score
	}
	totalWeight := 0.0
	total := 0.0
	for _, tier := range tiers {
		sum := 0.0
		count := 0
		for _, dimension := range tier.Dimensions {
			score, ok := dimensionScores[strings.TrimSpace(dimension)]
			if !ok {
				continue
			}
			sum += score
			count++
		}
		if count == 0 {
			continue
		}
		weight := tier.Weight
		if weight <= 0 {
			weight = 1
		}
		total += (sum / float64(count)) * weight
		totalWeight += weight
	}
	if totalWeight == 0 {
		return 0
	}
	return total / totalWeight
}

func computeWeightedAverageFromItems(acc *Accumulator, spec *runtimecontracts.ComputeSpec) float64 {
	if acc == nil || spec == nil || len(acc.Items) == 0 {
		return 0
	}
	valueField := strings.TrimSpace(spec.ValueField)
	weightField := strings.TrimSpace(spec.WeightField)
	if valueField == "" || weightField == "" {
		return 0
	}
	total := 0.0
	weightSum := 0.0
	for _, item := range acc.Items {
		payload := accumulatorItemFields(item)
		score, okScore := asFloat(payload[valueField])
		weight, okWeight := asFloat(payload[weightField])
		if !okScore || !okWeight || weight == 0 {
			continue
		}
		total += score * weight
		weightSum += weight
	}
	if weightSum == 0 {
		return 0
	}
	return total / weightSum
}

func computeWeightedPayload(payload map[string]any, tiers []runtimecontracts.ComputeTier) float64 {
	if len(payload) == 0 || len(tiers) == 0 {
		return 0
	}
	total := 0.0
	for _, tier := range tiers {
		sum := 0.0
		count := 0
		for _, dimension := range tier.Dimensions {
			var value any
			if resolved, ok := lookupPath(payload, strings.TrimPrefix(strings.TrimSpace(dimension), "payload.")); ok {
				value = resolved
			}
			score := firstNumeric(value)
			if math.IsNaN(score) {
				continue
			}
			sum += score
			count++
		}
		if count == 0 {
			continue
		}
		weight := tier.Weight
		if weight <= 0 {
			weight = 1
		}
		total += (sum / float64(count)) * weight
	}
	return total
}

func aggregateAccumulatorNumbers(acc *Accumulator, keys runtimecontracts.ComputeKeyConfig, combine func(current, next float64, idx int) float64) float64 {
	if acc == nil {
		return 0
	}
	numericKeys := normalizeStrings(keys.NumericKeys)
	if len(numericKeys) == 0 {
		return 0
	}
	current := 0.0
	idx := 0
	for _, item := range acc.Items {
		payload := accumulatorItemFields(item)
		values := make([]any, 0, len(numericKeys))
		for _, key := range numericKeys {
			values = append(values, payload[strings.TrimSpace(key)])
		}
		value := firstNumeric(values...)
		if math.IsNaN(value) {
			continue
		}
		current = combine(current, value, idx)
		idx++
	}
	if idx == 0 {
		return 0
	}
	return current
}

func firstNumeric(values ...any) float64 {
	for _, value := range values {
		switch typed := value.(type) {
		case int:
			return float64(typed)
		case int64:
			return float64(typed)
		case float64:
			return typed
		case float32:
			return float64(typed)
		case string:
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
				return parsed
			}
		}
	}
	return math.NaN()
}

func sliceFromAny(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func sliceOfMapsFromAny(raw any) []map[string]any {
	switch typed := raw.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneStringAnyMap(item))
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			m, ok := asObject(item)
			if ok {
				out = append(out, cloneStringAnyMap(m))
			}
		}
		return out
	default:
		return nil
	}
}

func stringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if v := strings.TrimSpace(asString(item)); v != "" {
				out = append(out, v)
			}
		}
		return out
	default:
		return nil
	}
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func uniqueOrderedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func truthy(raw any) bool {
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func stringifyDedupValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		var n int
		_, _ = fmtSscanfInt(t, &n)
		return n
	default:
		return 0
	}
}

func asFloat(v any) (float64, bool) {
	value := firstNumeric(v)
	if math.IsNaN(value) {
		return 0, false
	}
	return value, true
}

func asObject(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return ""
	}
}

func fmtSscanfInt(text string, target *int) (int, error) {
	return fmt.Sscanf(text, "%d", target)
}
