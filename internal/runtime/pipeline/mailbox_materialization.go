package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/core/values"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/workflowexpr"
)

type MailboxWriteMaterialization struct {
	ItemID        string
	EntityID      string
	FlowInstance  string
	Scope         string
	ItemType      string
	SourceEventID string
	FromAgent     string
	Severity      string
	Summary       string
	Payload       json.RawMessage
}

type MailboxWriteMaterializationStore interface {
	MaterializeMailboxWrite(context.Context, MailboxWriteMaterialization) error
}

func (pc *PipelineCoordinator) materializeMailboxItem(ctx context.Context, action runtimecontracts.ActionSpec, execCtx runtimeengine.ExecutionContext) error {
	if pc == nil || pc.mailboxMaterializer == nil {
		return fmt.Errorf("mailbox_write requires mailbox materialization store")
	}
	spec := action.Mailbox
	if spec == nil {
		return fmt.Errorf("mailbox_write requires mailbox declaration")
	}
	sourceEventID := strings.TrimSpace(execCtx.Request.Event.ID)
	if sourceEventID == "" {
		return fmt.Errorf("mailbox_write requires triggering event id")
	}
	nodeID := strings.TrimSpace(execCtx.Request.NodeID.String())
	if nodeID == "" {
		return fmt.Errorf("mailbox_write requires node id")
	}
	itemType, err := requiredMailboxString(execCtx.Base, spec.ItemType, "mailbox.item_type")
	if err != nil {
		return err
	}
	normalizedType, err := normalizeMailboxWriteItemType(itemType)
	if err != nil {
		return fmt.Errorf("mailbox.item_type: %w", err)
	}
	summary, err := requiredMailboxString(execCtx.Base, spec.Summary, "mailbox.summary")
	if err != nil {
		return err
	}
	severity := "normal"
	if !spec.Severity.IsZero() {
		severity, err = requiredMailboxString(execCtx.Base, spec.Severity, "mailbox.severity")
		if err != nil {
			return err
		}
		severity, err = normalizeMailboxWriteSeverity(severity)
		if err != nil {
			return err
		}
	}
	entityID := strings.TrimSpace(execCtx.Request.Event.EntityID())
	if entityID == "" {
		entityID = strings.TrimSpace(execCtx.Request.EntityID.String())
	}
	if !spec.EntityID.IsZero() {
		entityID, err = requiredMailboxString(execCtx.Base, spec.EntityID, "mailbox.entity_id")
		if err != nil {
			return err
		}
	}
	flowInstance := strings.Trim(strings.TrimSpace(execCtx.Request.Event.FlowInstance()), "/")
	if !spec.FlowInstance.IsZero() {
		flowInstance, err = requiredMailboxString(execCtx.Base, spec.FlowInstance, "mailbox.flow_instance")
		if err != nil {
			return err
		}
		flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	}
	payload, err := mailboxWritePayload(execCtx.Base, spec.Payload)
	if err != nil {
		return err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("mailbox.payload: %w", err)
	}
	if len(payloadJSON) == 0 || string(payloadJSON) == "null" {
		payloadJSON = []byte("{}")
	}
	scope := "global"
	switch {
	case strings.TrimSpace(entityID) != "":
		scope = "entity"
	case strings.TrimSpace(flowInstance) != "":
		scope = "flow"
	}
	itemID := deterministicMailboxItemID(sourceEventID, nodeID)
	record := MailboxWriteMaterialization{
		ItemID:        itemID,
		EntityID:      strings.TrimSpace(entityID),
		FlowInstance:  flowInstance,
		Scope:         scope,
		ItemType:      normalizedType,
		SourceEventID: sourceEventID,
		FromAgent:     "system_node:" + nodeID,
		Severity:      severity,
		Summary:       summary,
		Payload:       json.RawMessage(payloadJSON),
	}
	if err := pc.mailboxMaterializer.MaterializeMailboxWrite(ctx, record); err != nil {
		return fmt.Errorf("mailbox_write materialize: %w", err)
	}
	return nil
}

func deterministicMailboxItemID(sourceEventID, nodeID string) string {
	key := strings.Join([]string{
		"swarm",
		"mailbox_write",
		strings.TrimSpace(sourceEventID),
		strings.TrimSpace(nodeID),
	}, ":")
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(key)).String()
}

func requiredMailboxString(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue, field string) (string, error) {
	value, ok, err := evalMailboxExpressionValue(base, expr)
	if err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	if !ok {
		return "", fmt.Errorf("%s resolved empty", field)
	}
	out, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s resolved non-string %T", field, value)
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return "", fmt.Errorf("%s resolved empty", field)
	}
	return out, nil
}

func evalMailboxExpressionValue(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue) (any, bool, error) {
	if expr.IsZero() {
		return nil, false, nil
	}
	switch expr.Kind {
	case runtimecontracts.ExpressionKindLiteral:
		return expr.Literal, true, nil
	case runtimecontracts.ExpressionKindRef:
		value, ok := base.Lookup(expr.RefPath)
		return value, ok, nil
	case runtimecontracts.ExpressionKindCEL:
		value, err := workflowexpr.EvalValueExpressionWithOptions(expr.CEL, workflowexpr.ValueContext{
			Entity:  base.Entity.Raw(),
			Event:   base.Event.Raw(),
			Payload: base.Payload.Raw(),
			Policy:  base.Policy.Raw(),
			FanOut:  base.FanOut.Raw(),
		}, workflowexpr.ValueExpressionOptions{})
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported expression kind %q", expr.Kind)
	}
}

func mailboxWritePayload(base runtimeengine.BaseContext, fields map[string]runtimecontracts.ExpressionValue) (map[string]any, error) {
	out := map[string]any{}
	for target, expr := range fields {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		value, ok, err := evalMailboxExpressionValue(base, expr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", target, err)
		}
		if !ok {
			continue
		}
		values.Wrap(out).SetPath(paths.Parse(target), value)
	}
	return out, nil
}

func normalizeMailboxWriteSeverity(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "normal":
		return "normal", nil
	case "urgent":
		return "urgent", nil
	case "critical":
		return "critical", nil
	default:
		return "", fmt.Errorf("invalid mailbox severity %q", raw)
	}
}

func normalizeMailboxWriteItemType(raw string) (string, error) {
	itemType := strings.ToLower(strings.TrimSpace(raw))
	itemType = strings.ReplaceAll(itemType, "-", "_")
	itemType = strings.ReplaceAll(itemType, ".", "_")
	if itemType == "" {
		return "", fmt.Errorf("invalid mailbox item_type %q", raw)
	}
	return itemType, nil
}
