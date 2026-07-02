package engine

import (
	"fmt"
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type validatedBatchAgentRow struct {
	key        string
	sourceItem any
	result     map[string]any
}

func validateHandlerBatchAgentSpecs(handler runtimecontracts.SystemNodeEventHandler) error {
	validate := func(context string, spec *runtimecontracts.BatchAgentSpec) error {
		if spec == nil {
			return nil
		}
		if strings.TrimSpace(spec.Agent) == "" {
			return fmt.Errorf("%s.agent is required", context)
		}
		if strings.TrimSpace(spec.ItemsFrom) == "" {
			return fmt.Errorf("%s.items_from is required", context)
		}
		if strings.TrimSpace(spec.Result.ItemsFrom) == "" {
			return fmt.Errorf("%s.result.items_from is required", context)
		}
		if strings.TrimSpace(spec.Result.CorrelationKey) == "" {
			return fmt.Errorf("%s.result.correlation_key is required", context)
		}
		if spec.Emit.EventType() == "" {
			return fmt.Errorf("%s.emit.event is required", context)
		}
		return nil
	}
	validateRule := func(context string, rule runtimecontracts.HandlerRuleEntry) error {
		if rule.BatchAgent == nil {
			return nil
		}
		if !rule.Emit.Empty() {
			return fmt.Errorf("%s declares both emit and batch_agent; batch_agent.emit owns the scatter payload", context)
		}
		if rule.FanOut != nil {
			return fmt.Errorf("%s declares both fan_out and batch_agent", context)
		}
		return validate(context+".batch_agent", rule.BatchAgent)
	}
	if err := validate("handler.batch_agent", handler.BatchAgent); err != nil {
		return err
	}
	if handler.BatchAgent != nil && handler.FanOut != nil {
		return fmt.Errorf("handler declares both fan_out and batch_agent")
	}
	for idx, rule := range handler.Rules {
		if err := validateRule(handlerRuleContext("handler.rules", idx, rule.ID), rule); err != nil {
			return err
		}
	}
	for idx, rule := range handler.OnComplete {
		if err := validateRule(handlerRuleContext("handler.on_complete", idx, rule.ID), rule); err != nil {
			return err
		}
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			if err := validateRule(handlerRuleContext("handler.accumulate.on_complete", idx, rule.ID), rule); err != nil {
				return err
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			if err := validateRule(handlerRuleContext("handler.accumulate.on_timeout", 0, handler.Accumulate.OnTimeout.ID), *handler.Accumulate.OnTimeout); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Executor) stepBatchAgent(frame *executionFrame) (bool, error) {
	spec := e.selectedBatchAgent(frame)
	if spec == nil {
		return false, nil
	}
	current := e.currentContext(frame)
	itemsValue, _ := resolveContractPath(current, frame.state, spec.ItemsPath, spec.ItemsFrom)
	items := sliceFromAny(itemsValue)
	frame.result.BatchAgentCount = len(items)
	frame.state.BatchAgent = map[string]any{}
	frame.state.SetBatchAgent("agent", strings.TrimSpace(spec.Agent))
	frame.state.SetBatchAgent("count", len(items))
	if len(items) == 0 {
		return false, nil
	}
	if e.deps.BatchAgentRunner == nil {
		return false, fmt.Errorf("batch_agent runner is required")
	}
	input, err := e.batchAgentInput(frame, spec)
	if err != nil {
		return false, err
	}
	if _, err := expectedBatchAgentKeys(items, spec.Result.CorrelationKey); err != nil {
		return false, err
	}
	resp, err := e.deps.BatchAgentRunner.InvokeBatchAgent(frame.tx.Context(), BatchAgentRequest{
		FlowID:          strings.TrimSpace(frame.req.FlowID.String()),
		NodeID:          strings.TrimSpace(frame.req.NodeID.String()),
		Agent:           strings.TrimSpace(spec.Agent),
		Items:           cloneAnySlice(items),
		Input:           cloneStringAnyMap(input),
		ResultItemsFrom: strings.TrimSpace(spec.Result.ItemsFrom),
		CorrelationKey:  strings.TrimSpace(spec.Result.CorrelationKey),
		RequiredFields:  append([]string(nil), spec.Result.RequiredFields...),
	})
	if err != nil {
		return false, err
	}
	rows, err := batchAgentRowsFromOutput(resp.Output, spec.Result)
	if err != nil {
		return false, err
	}
	ordered, err := validateBatchAgentRows(items, rows, spec.Result)
	if err != nil {
		return false, err
	}
	frame.result.BatchAgentCount = len(ordered)
	frame.state.SetBatchAgent("count", len(ordered))
	for _, row := range ordered {
		if err := e.emitBatchAgentRow(frame, spec, row); err != nil {
			return false, err
		}
	}
	if len(frame.result.EmitIntents) == 0 && len(frame.result.DeadLetterIntents) == 0 {
		return false, nil
	}
	if err := e.stepAdvancesTo(frame); err != nil {
		return false, err
	}
	frame.result.Status = OutcomeFannedOut
	return true, nil
}

func (e *Executor) batchAgentInput(frame *executionFrame, spec *runtimecontracts.BatchAgentSpec) (map[string]any, error) {
	out := map[string]any{}
	base := e.currentContext(frame)
	for key, expr := range spec.Input {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value, ok, err := evalExpressionValue(base, frame.state, expr, workflowexpr.ValueExpressionOptions{})
		if err != nil {
			return nil, fmt.Errorf("batch_agent.input.%s: %w", key, err)
		}
		if !ok {
			continue
		}
		setParsedValuePath(out, paths.Parse(key), value)
	}
	return out, nil
}

func (e *Executor) emitBatchAgentRow(frame *executionFrame, spec *runtimecontracts.BatchAgentSpec, row validatedBatchAgentRow) error {
	frame.state.SetBatchAgent("source_item", cloneBatchAgentValue(row.sourceItem))
	frame.state.SetBatchAgent("result", cloneStringAnyMap(row.result))
	frame.state.SetBatchAgent("key", row.key)
	eventType := spec.Emit.EventType()
	if eventType == "" {
		return nil
	}
	eventType = e.resolveDeclarativeEmitEventType(frame, eventType)
	emitSpec := spec.Emit
	emitSpec.Event = eventType
	payload := map[string]any{}
	transformed, err := emitFieldsPayload(e.currentContext(frame), frame.state, emitSpec, workflowexpr.ValueExpressionOptions{})
	if err != nil {
		return err
	}
	if len(transformed) > 0 {
		payload = transformed
	}
	shaped, err := e.shapeEmitPayload(frame, eventType, payload)
	if err != nil {
		return err
	}
	_, err = e.queueEmitIntentForSpec(frame, emitSpec, eventType, shaped)
	return err
}

func batchAgentRowsFromOutput(output any, spec runtimecontracts.BatchAgentResultSpec) ([]any, error) {
	rowsPath := spec.ItemsPath
	if rowsPath.IsZero() {
		rowsPath = paths.Parse(spec.ItemsFrom)
	}
	if rowsPath.IsZero() {
		return nil, fmt.Errorf("batch_agent.result.items_from is required")
	}
	if rowsPath.HasExplicitRoot() {
		return nil, fmt.Errorf("batch_agent.result.items_from must address the agent output object, got %q", rowsPath.String())
	}
	object, ok := asObject(output)
	if !ok {
		return nil, fmt.Errorf("batch_agent output must be an object containing %q", rowsPath.String())
	}
	rowsValue, ok := lookupParsedPath(object, rowsPath)
	if !ok {
		return nil, fmt.Errorf("batch_agent output missing result rows at %q", rowsPath.String())
	}
	rows := sliceFromAny(rowsValue)
	if rows == nil {
		return nil, fmt.Errorf("batch_agent result rows at %q must be a list", rowsPath.String())
	}
	return rows, nil
}

func validateBatchAgentRows(items []any, rows []any, spec runtimecontracts.BatchAgentResultSpec) ([]validatedBatchAgentRow, error) {
	expected, err := expectedBatchAgentKeys(items, spec.CorrelationKey)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 && len(expected.order) > 0 {
		return nil, fmt.Errorf("batch_agent result missing rows for %d input item(s)", len(expected.order))
	}
	byKey := make(map[string]map[string]any, len(rows))
	for idx, row := range rows {
		rowObj, ok := asObject(row)
		if !ok {
			return nil, fmt.Errorf("batch_agent result row %d is malformed: row must be an object", idx)
		}
		key, err := batchAgentObjectKey(rowObj, spec.CorrelationKey)
		if err != nil {
			return nil, fmt.Errorf("batch_agent result row %d: %w", idx, err)
		}
		if _, ok := expected.byKey[key]; !ok {
			return nil, fmt.Errorf("batch_agent result row %d has unknown correlation key %q", idx, key)
		}
		if _, exists := byKey[key]; exists {
			return nil, fmt.Errorf("batch_agent result has duplicate correlation key %q", key)
		}
		for _, field := range spec.RequiredFields {
			if _, ok := batchAgentLookup(rowObj, field); !ok {
				return nil, fmt.Errorf("batch_agent result row %d for key %q missing required field %q", idx, key, strings.TrimSpace(field))
			}
		}
		byKey[key] = cloneStringAnyMap(rowObj)
	}
	out := make([]validatedBatchAgentRow, 0, len(expected.order))
	for _, key := range expected.order {
		row, ok := byKey[key]
		if !ok {
			return nil, fmt.Errorf("batch_agent result missing row for correlation key %q", key)
		}
		out = append(out, validatedBatchAgentRow{
			key:        key,
			sourceItem: expected.byKey[key],
			result:     row,
		})
	}
	return out, nil
}

type expectedBatchAgentKeySet struct {
	order []string
	byKey map[string]any
}

func expectedBatchAgentKeys(items []any, keyPath string) (expectedBatchAgentKeySet, error) {
	out := expectedBatchAgentKeySet{
		order: make([]string, 0, len(items)),
		byKey: make(map[string]any, len(items)),
	}
	for idx, item := range items {
		itemObj, ok := asObject(item)
		if !ok {
			return expectedBatchAgentKeySet{}, fmt.Errorf("batch_agent input item %d is malformed: item must be an object", idx)
		}
		key, err := batchAgentObjectKey(itemObj, keyPath)
		if err != nil {
			return expectedBatchAgentKeySet{}, fmt.Errorf("batch_agent input item %d: %w", idx, err)
		}
		if _, exists := out.byKey[key]; exists {
			return expectedBatchAgentKeySet{}, fmt.Errorf("batch_agent input has duplicate correlation key %q", key)
		}
		out.order = append(out.order, key)
		out.byKey[key] = cloneStringAnyMap(itemObj)
	}
	return out, nil
}

func batchAgentObjectKey(object map[string]any, keyPath string) (string, error) {
	value, ok := batchAgentLookup(object, keyPath)
	if !ok {
		return "", fmt.Errorf("missing correlation key %q", strings.TrimSpace(keyPath))
	}
	key := strings.TrimSpace(asString(value))
	if key == "" {
		key = strings.TrimSpace(fmt.Sprint(value))
	}
	if key == "" || key == "<nil>" {
		return "", fmt.Errorf("correlation key %q is empty", strings.TrimSpace(keyPath))
	}
	return key, nil
}

func batchAgentLookup(object map[string]any, rawPath string) (any, bool) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return nil, false
	}
	parsed := paths.Parse(rawPath)
	if parsed.HasExplicitRoot() {
		parsed = paths.Path{Segments: parsed.Segments, Raw: strings.Join(parsed.Segments, ".")}
	}
	return lookupParsedPath(object, parsed)
}

func cloneAnySlice(in []any) []any {
	if len(in) == 0 {
		return nil
	}
	out := make([]any, 0, len(in))
	for _, item := range in {
		out = append(out, cloneBatchAgentValue(item))
	}
	return out
}

func cloneBatchAgentValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneStringAnyMap(typed)
	case []any:
		return cloneAnySlice(typed)
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneStringAnyMap(item))
		}
		return out
	default:
		if reflect.TypeOf(value) == nil {
			return nil
		}
		return value
	}
}
