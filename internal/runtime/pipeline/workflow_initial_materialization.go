package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const FlowConstructionReceiptVersion = 3

const workflowInitialMaterializationProjectionVersion = FlowConstructionReceiptVersion

func validateWorkflowInitialEntry(source semanticview.Source, instance WorkflowInstance, initialStage string) error {
	graph, found := semanticview.WorkflowStageTopology(source, instance.WorkflowName)
	if !found || graph.FlowID != instance.WorkflowName {
		return fmt.Errorf("workflow initial entry requires the exact compiled flow %q", instance.WorkflowName)
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		return fmt.Errorf("workflow initial entry: %w", err)
	}
	want := initial.ID()
	if want == "" || initialStage != want || instance.CurrentState != want {
		return fmt.Errorf("workflow initial entry must match canonical initial stage %q and prepared state %q, got %q", want, instance.CurrentState, initialStage)
	}
	return nil
}

// ValidateFlowConstructionPublication consumes the immutable constructor
// receipt. Neither handler settlement nor current attachment progress is proof
// that this publication constructed its exact receiver.
func ValidateFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID, eventID string) error {
	receipt, err := decodeFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return err
	}
	return matchFlowConstructionPublication(receipt.CreatingInput, eventID)
}

type FlowConstructionPublicationEvidence struct {
	Identity      runtimeflowidentity.Instance
	CreatingInput FlowConstructionInput
	Fields        map[string]any
}

type FlowConstructionPublicationReader interface {
	LoadFlowConstructionPublication(context.Context, runtimeflowidentity.RunScopedFlowInstance, string) (FlowConstructionPublicationEvidence, error)
}

// ProjectFlowConstructionPublication discovers the immutable creating input
// independently of the incoming delivery or its projected target kind.
func ProjectFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (FlowConstructionPublicationEvidence, error) {
	receipt, err := decodeFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return FlowConstructionPublicationEvidence{}, err
	}
	evidence := FlowConstructionPublicationEvidence{Identity: receipt.Identity, CreatingInput: receipt.CreatingInput}
	if receipt.Persisted.Fields == nil {
		return evidence, nil
	}
	fields, err := canonicaljson.CloneRuntimeValue(receipt.Persisted.Fields)
	if err != nil {
		return FlowConstructionPublicationEvidence{}, err
	}
	evidence.Fields = fields.(map[string]any)
	return evidence, nil
}

// FlowConstructionPublicationFields projects immutable initial state through
// the same receipt admission used by execution, never from current state.
func FlowConstructionPublicationFields(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	evidence, err := ProjectFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return nil, err
	}
	if err := matchFlowConstructionPublication(evidence.CreatingInput, eventID); err != nil {
		return nil, err
	}
	return evidence.Fields, nil
}

func matchFlowConstructionPublication(input FlowConstructionInput, eventID string) error {
	if eventID == "" || input.EventID != eventID {
		return fmt.Errorf("flow construction receipt contradicts exact receiver or creating publication")
	}
	return nil
}

func decodeFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (workflowInitialMaterializationProjection, error) {
	return DecodeFlowConstructionReceipt(raw, owner, entityID)
}

// DecodeStoredFlowConstructionReceipt binds a fixed header to the receipt's
// retained route. The header need not independently reconstruct an instance ID.
func DecodeStoredFlowConstructionReceipt(raw []byte, runID, entityID, instancePath, workflowName string) (FlowConstructionReceipt, error) {
	if runID == "" || entityID == "" || instancePath == "" || workflowName == "" {
		return FlowConstructionReceipt{}, fmt.Errorf("stored flow construction receipt requires its exact header")
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt: %w", err)
	}
	fields, object := value.ObjectMap()
	if !object {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt requires one object")
	}
	retained, object := fields["identity"].ObjectMap()
	if !object {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt requires identity")
	}
	var identity runtimeflowidentity.Instance
	if err := canonicaljson.ValueInto(fields["identity"], &identity); err != nil {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt identity: %w", err)
	}
	for _, name := range []string{"ScopeKey", "InstanceID", "InstancePath"} {
		if coordinate, ok := retained[name].String(); !ok || coordinate == "" {
			return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt identity requires recorded %s", name)
		}
	}
	if identity.InstancePath != instancePath || identity.TemplateID != workflowName || identity.EntityID != entityID {
		return FlowConstructionReceipt{}, fmt.Errorf("stored flow construction receipt identity contradicts fixed header")
	}
	route := identity.Route()
	if route.ScopeKey != identity.ScopeKey || route.InstanceID != identity.InstanceID || route.InstancePath != identity.InstancePath {
		return FlowConstructionReceipt{}, fmt.Errorf("stored flow construction receipt cannot infer or normalize route coordinates")
	}
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	if owner.RunID != runID {
		return FlowConstructionReceipt{}, fmt.Errorf("stored flow construction receipt requires exact run identity")
	}
	return DecodeFlowConstructionReceipt(raw, owner, entityID)
}

// DecodeFlowConstructionReceipt admits immutable construction, independently
// of the operational readiness plan and current-at-cut state.
func DecodeFlowConstructionReceipt(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (FlowConstructionReceipt, error) {
	if err := owner.Validate(); err != nil {
		return FlowConstructionReceipt{}, err
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt: %w", err)
	}
	fields, object := value.ObjectMap()
	if !object {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt requires one object")
	}
	if err := validateFlowConstructionReceiptFields(fields); err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := validateFlowConstructionIdentityFields(fields["identity"]); err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := validateFlowConstructionInputFields(fields["creating_input"]); err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := validateFlowConstructionInitialProjectionFields(fields["persisted"]); err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := validateFlowConstructionOutgoingOccurrenceFields(fields["creation_event"]); err != nil {
		return FlowConstructionReceipt{}, err
	}
	var wire struct {
		FlowConstructionReceipt
		CreatingInput *FlowConstructionInput `json:"creating_input"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&wire); err != nil {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt requires one object")
	}
	if wire.CreatingInput == nil {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction receipt requires creating_input")
	}
	receipt := wire.FlowConstructionReceipt
	receipt.CreatingInput = *wire.CreatingInput
	if err := validateFlowConstructionReceipt(receipt, owner, entityID); err != nil {
		return FlowConstructionReceipt{}, err
	}
	receipt.Persisted, err = cloneFlowConstructionInitialProjection(receipt.Persisted)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	return receipt, nil
}

func validateFlowConstructionReceiptFields(fields map[string]semanticvalue.Value) error {
	for name := range fields {
		switch name {
		case "version", "run_id", "entity_id", "flow_instance", "workflow_name", "workflow_version",
			"initial_state", "occurred_at", "persisted", "identity", "bundle_hash", "execution_mode", "creation_event", "creating_input":
		default:
			return fmt.Errorf("flow construction receipt has undeclared field %q", name)
		}
	}
	for _, name := range []string{
		"version", "run_id", "entity_id", "flow_instance", "workflow_name", "workflow_version",
		"initial_state", "occurred_at", "persisted", "identity", "bundle_hash", "execution_mode", "creation_event", "creating_input",
	} {
		if _, present := fields[name]; !present {
			return fmt.Errorf("flow construction receipt requires %s", name)
		}
	}
	return nil
}

func validateFlowConstructionIdentityFields(identity semanticvalue.Value) error {
	// The typed identity's native field spellings are already its wire contract.
	identityFields, object := identity.ObjectMap()
	if !object {
		return fmt.Errorf("flow construction receipt requires identity")
	}
	for _, name := range []string{"TemplateID", "ScopeKey", "InstanceID", "InstancePath", "EntityID", "ParentEntityID", "ParentRoute", "HasStoredPath"} {
		if _, present := identityFields[name]; !present {
			return fmt.Errorf("flow construction receipt identity requires %s", name)
		}
	}
	for name, value := range identityFields {
		switch name {
		case "TemplateID", "ScopeKey", "InstanceID", "InstancePath", "EntityID", "ParentEntityID":
			if _, ok := value.String(); !ok {
				return fmt.Errorf("flow construction receipt identity %s requires a string", name)
			}
		case "HasStoredPath", "ParentRoute":
		default:
			return fmt.Errorf("flow construction receipt identity has undeclared field %q", name)
		}
	}
	parentFields, object := identityFields["ParentRoute"].ObjectMap()
	if !object || len(parentFields) != 3 {
		return fmt.Errorf("flow construction receipt requires explicit parent coordinates")
	}
	for _, name := range []string{"FlowID", "FlowInstance", "EntityID"} {
		if _, ok := parentFields[name].String(); !ok {
			return fmt.Errorf("flow construction receipt parent requires %s", name)
		}
	}
	return nil
}

func validateFlowConstructionInputFields(input semanticvalue.Value) error {
	inputFields, object := input.ObjectMap()
	if !object || len(inputFields) != 2 {
		return fmt.Errorf("flow construction receipt requires explicit creating_input")
	}
	for _, name := range []string{"event_id", "input"} {
		if _, ok := inputFields[name].String(); !ok {
			return fmt.Errorf("flow construction receipt creating_input requires %s", name)
		}
	}
	return nil
}

func validateFlowConstructionInitialProjectionFields(persisted semanticvalue.Value) error {
	persistedFields, object := persisted.ObjectMap()
	if !object || len(persistedFields) != 5 {
		return fmt.Errorf("flow construction receipt requires its complete initial projection")
	}
	for _, name := range []string{"fields", "bookkeeping", "gates", "accumulator", "control"} {
		if _, present := persistedFields[name]; !present {
			return fmt.Errorf("flow construction initial projection requires %s", name)
		}
	}
	return validateFlowConstructionInitialControlFields(persistedFields["control"])
}

func validateFlowConstructionInitialControlFields(control semanticvalue.Value) error {
	controlFields, object := control.ObjectMap()
	if !object {
		return fmt.Errorf("flow construction receipt requires initial control")
	}
	for name := range controlFields {
		switch name {
		case "workflow_version", "storage_ref", "entity_id", "slug", "name", "entity_type", "instance_id", "flow_path",
			"instance_kind", "template_version", "status", "parent_flow_id", "parent_flow_instance", "parent_entity_id", "transition_history":
		default:
			return fmt.Errorf("flow construction initial control has undeclared field %q", name)
		}
	}
	for _, name := range []string{"workflow_version", "storage_ref", "entity_id", "slug", "name", "entity_type", "instance_id", "flow_path",
		"instance_kind", "template_version", "status", "parent_flow_id", "parent_flow_instance", "parent_entity_id"} {
		if _, ok := controlFields[name].String(); !ok {
			return fmt.Errorf("flow construction initial control requires %s", name)
		}
	}
	if _, present := controlFields["transition_history"]; !present {
		return fmt.Errorf("flow construction initial control requires transition_history")
	}
	return nil
}

func validateFlowConstructionOutgoingOccurrenceFields(occurrence semanticvalue.Value) error {
	receiptEvent, object := occurrence.ObjectMap()
	if !object {
		return nil
	}
	for name := range receiptEvent {
		switch name {
		case "event_id", "event_type", "run_id", "parent_event_id", "execution_mode", "payload", "created_at", "delivery_context":
		default:
			return fmt.Errorf("flow construction outgoing occurrence has undeclared field %q", name)
		}
	}
	for _, name := range []string{"event_id", "event_type", "run_id", "parent_event_id", "execution_mode", "payload", "created_at"} {
		if _, exists := receiptEvent[name]; !exists {
			return fmt.Errorf("flow construction outgoing occurrence requires %s", name)
		}
	}
	if context, exists := receiptEvent["delivery_context"]; exists {
		return validateFlowConstructionOutgoingContextFields(context)
	}
	return nil
}

func validateFlowConstructionOutgoingContextFields(context semanticvalue.Value) error {
	contextFields, object := context.ObjectMap()
	if !object {
		return fmt.Errorf("flow construction outgoing context must be an object")
	}
	for name := range contextFields {
		if name != "reply" && name != "joins" {
			return fmt.Errorf("flow construction outgoing context has undeclared field %q", name)
		}
	}
	return nil
}

func cloneFlowConstructionInitialProjection(projection workflowInstancePersistedProjection) (workflowInstancePersistedProjection, error) {
	for _, target := range []*map[string]any{&projection.Fields, &projection.Bookkeeping, &projection.Accumulator} {
		if *target == nil {
			continue
		}
		cloned, err := canonicaljson.CloneRuntimeValue(*target)
		if err != nil {
			return workflowInstancePersistedProjection{}, err
		}
		*target = cloned.(map[string]any)
	}
	return projection, nil
}

func validateFlowConstructionReceipt(receipt FlowConstructionReceipt, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) error {
	if err := receipt.CreatingInput.Validate(); err != nil {
		return err
	}
	if err := validateFlowConstructionOwnerBinding(receipt, owner, entityID); err != nil {
		return err
	}
	if err := validateFlowConstructionIdentity(receipt, owner, entityID); err != nil {
		return err
	}
	if err := validateFlowConstructionInitialProjection(receipt); err != nil {
		return err
	}
	if err := validateFlowConstructionSemanticOwner(receipt); err != nil {
		return err
	}
	return validateFlowConstructionOutgoingOccurrence(receipt)
}

func validateFlowConstructionOwnerBinding(receipt FlowConstructionReceipt, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) error {
	if receipt.Version != workflowInitialMaterializationProjectionVersion ||
		receipt.RunID != owner.RunID || receipt.FlowInstance != owner.Route.InstancePath || receipt.EntityID != entityID ||
		receipt.WorkflowName != owner.Route.ScopeKey || receipt.WorkflowVersion == "" || receipt.InitialState == "" || receipt.OccurredAt.IsZero() ||
		receipt.Persisted.Control.StorageRef != owner.Route.InstancePath || receipt.Persisted.Control.EntityID != entityID {
		return fmt.Errorf("flow construction receipt contradicts exact receiver or creating publication")
	}
	return nil
}

func validateFlowConstructionIdentity(receipt FlowConstructionReceipt, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) error {
	identity := receipt.Identity
	if !identity.HasStoredPath || identity.TemplateID == "" || identity.ScopeKey == "" || identity.InstanceID == "" || identity.InstancePath == "" ||
		identity.Route() != owner.Route || identity.EntityID != entityID || identity.TemplateID != receipt.WorkflowName {
		return fmt.Errorf("flow construction receipt identity contradicts receiver")
	}
	return nil
}

func validateFlowConstructionInitialProjection(receipt FlowConstructionReceipt) error {
	identity := receipt.Identity
	parent := identity.ParentRoute
	control := receipt.Persisted.Control
	if parent != parent.Normalized() || parent.Empty() != (identity.ParentEntityID == "") ||
		(!parent.Empty() && (!parent.Complete() || parent.EntityID != identity.ParentEntityID)) ||
		control.InstanceID != identity.InstanceID || control.FlowPath != identity.InstancePath ||
		control.ParentFlowID != parent.FlowID || control.ParentFlowInstance != parent.FlowInstance || control.ParentEntityID != identity.ParentEntityID {
		return fmt.Errorf("flow construction receipt control contradicts exact identity or ancestry")
	}
	return nil
}

func validateFlowConstructionSemanticOwner(receipt FlowConstructionReceipt) error {
	// Reuse the existing semantic identity/source/mode/creation-event checks,
	// without importing its actor census, process phase, attempt or plan hash.
	admitted, err := (DynamicFlowRuntimeReadinessPlan{
		Identity: receipt.Identity, RunID: receipt.RunID, BundleHash: receipt.BundleHash,
		WorkflowVersion: receipt.WorkflowVersion, ExecutionMode: receipt.ExecutionMode, CreationEvent: receipt.CreationEvent,
	}).Normalized()
	if err != nil {
		return fmt.Errorf("flow construction receipt semantic identity: %w", err)
	}
	if admitted.Identity != receipt.Identity || admitted.RunID != receipt.RunID || admitted.BundleHash != receipt.BundleHash || admitted.WorkflowVersion != receipt.WorkflowVersion {
		return fmt.Errorf("flow construction receipt requires exact semantic identity")
	}
	return nil
}

func validateFlowConstructionOutgoingOccurrence(receipt FlowConstructionReceipt) error {
	if receipt.CreationEvent == nil {
		return nil
	}
	if receipt.CreationEvent.EventID == receipt.CreatingInput.EventID {
		return fmt.Errorf("flow construction incoming publication cannot be its outgoing occurrence")
	}
	if err := receipt.CreationEvent.DeliveryContext.Validate(); err != nil {
		return fmt.Errorf("flow construction outgoing context: %w", err)
	}
	if _, err := canonicaljson.Decode(receipt.CreationEvent.Payload); err != nil {
		return fmt.Errorf("flow construction outgoing payload: %w", err)
	}
	return nil
}

// FlowConstructionReceipt is immutable initialization and publication evidence.
// Readiness and current workflow progress are separate projections.
type FlowConstructionReceipt struct {
	Version         int                                  `json:"version"`
	RunID           string                               `json:"run_id"`
	EntityID        string                               `json:"entity_id"`
	FlowInstance    string                               `json:"flow_instance"`
	WorkflowName    string                               `json:"workflow_name"`
	WorkflowVersion string                               `json:"workflow_version"`
	InitialState    string                               `json:"initial_state"`
	OccurredAt      time.Time                            `json:"occurred_at"`
	Persisted       workflowInstancePersistedProjection  `json:"persisted"`
	Identity        runtimeflowidentity.Instance         `json:"identity"`
	BundleHash      string                               `json:"bundle_hash"`
	ExecutionMode   executionmode.Mode                   `json:"execution_mode"`
	CreationEvent   *DynamicFlowRuntimeCreationEventPlan `json:"creation_event"`
	CreatingInput   FlowConstructionInput                `json:"creating_input"`
}

type workflowInitialMaterializationProjection = FlowConstructionReceipt

func EncodeFlowConstructionReceipt(receipt FlowConstructionReceipt) ([]byte, error) {
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(receipt.RunID, receipt.Identity.Route())
	if err != nil {
		return nil, err
	}
	if err := validateFlowConstructionReceipt(receipt, owner, receipt.EntityID); err != nil {
		return nil, err
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeFlowConstructionReceipt(raw, owner, receipt.EntityID); err != nil {
		return nil, err
	}
	return raw, nil
}

// ProjectFlowConstructionReceipt consumes caller-admitted correspondence only.
// It never re-runs construction or derives parent or publication identities.
func ProjectFlowConstructionReceipt(receipt FlowConstructionReceipt, childOwner runtimeflowidentity.RunScopedFlowInstance, childIdentity runtimeflowidentity.Instance, creatingInput FlowConstructionInput, creationEvent *DynamicFlowRuntimeCreationEventPlan) (FlowConstructionReceipt, error) {
	raw, err := EncodeFlowConstructionReceipt(receipt)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	sourceOwner, err := runtimeflowidentity.NewRunScopedFlowInstance(receipt.RunID, receipt.Identity.Route())
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	projected, err := DecodeFlowConstructionReceipt(raw, sourceOwner, receipt.EntityID)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := childOwner.Validate(); err != nil {
		return FlowConstructionReceipt{}, err
	}
	if err := validateFlowConstructionChildCorrespondence(receipt, childOwner, childIdentity, creatingInput, creationEvent); err != nil {
		return FlowConstructionReceipt{}, err
	}
	projected.RunID, projected.EntityID, projected.FlowInstance = childOwner.RunID, childIdentity.EntityID, childIdentity.InstancePath
	projected.Identity, projected.CreatingInput = childIdentity, creatingInput
	control := &projected.Persisted.Control
	control.StorageRef, control.FlowPath, control.InstanceID, control.EntityID = childIdentity.InstancePath, childIdentity.InstancePath, childIdentity.InstanceID, childIdentity.EntityID
	control.ParentFlowID, control.ParentFlowInstance, control.ParentEntityID = childIdentity.ParentRoute.FlowID, childIdentity.ParentRoute.FlowInstance, childIdentity.ParentEntityID
	if creationEvent != nil {
		if err := validateFlowConstructionChildOutgoingOccurrence(receipt, childOwner, creatingInput, creationEvent); err != nil {
			return FlowConstructionReceipt{}, err
		}
		projected.CreationEvent = creationEvent
	}
	if err := validateFlowConstructionReceipt(projected, childOwner, childIdentity.EntityID); err != nil {
		return FlowConstructionReceipt{}, err
	}
	// Strict encode/decode isolates all nested maps, slices and occurrence bytes.
	raw, err = EncodeFlowConstructionReceipt(projected)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	projected, err = DecodeFlowConstructionReceipt(raw, childOwner, childIdentity.EntityID)
	if err != nil {
		return FlowConstructionReceipt{}, err
	}
	if creationEvent != nil {
		projected.CreationEvent.Payload = bytes.Clone(creationEvent.Payload)
	}
	if projected.Identity != childIdentity {
		return FlowConstructionReceipt{}, fmt.Errorf("flow construction child identity was normalized instead of retained")
	}
	return projected, nil
}

func validateFlowConstructionChildCorrespondence(receipt FlowConstructionReceipt, childOwner runtimeflowidentity.RunScopedFlowInstance, childIdentity runtimeflowidentity.Instance, creatingInput FlowConstructionInput, creationEvent *DynamicFlowRuntimeCreationEventPlan) error {
	if childIdentity.TemplateID != receipt.Identity.TemplateID || childIdentity.ScopeKey != receipt.Identity.ScopeKey ||
		childIdentity.ParentRoute.Empty() != receipt.Identity.ParentRoute.Empty() || childIdentity.ParentRoute.FlowID != receipt.Identity.ParentRoute.FlowID ||
		creatingInput.Input != receipt.CreatingInput.Input || (creatingInput.EventID == "") != (receipt.CreatingInput.EventID == "") ||
		(creationEvent == nil) != (receipt.CreationEvent == nil) {
		return fmt.Errorf("flow construction child projection changes immutable owner or publication shape")
	}
	if childOwner.RunID != receipt.RunID && creatingInput.EventID != "" && creatingInput.EventID == receipt.CreatingInput.EventID {
		return fmt.Errorf("flow construction child projection requires mapped incoming publication")
	}
	return nil
}

func validateFlowConstructionChildOutgoingOccurrence(receipt FlowConstructionReceipt, childOwner runtimeflowidentity.RunScopedFlowInstance, creatingInput FlowConstructionInput, creationEvent *DynamicFlowRuntimeCreationEventPlan) error {
	original := receipt.CreationEvent
	if childOwner.RunID != receipt.RunID && creationEvent.EventID == original.EventID {
		return fmt.Errorf("flow construction child projection requires mapped outgoing occurrence")
	}
	if creationEvent.EventType != original.EventType || creationEvent.ExecutionMode != original.ExecutionMode ||
		!creationEvent.CreatedAt.Equal(original.CreatedAt) || !bytes.Equal(creationEvent.Payload, original.Payload) ||
		(original.ParentEventID == receipt.CreatingInput.EventID && creationEvent.ParentEventID != creatingInput.EventID) ||
		(original.DeliveryContext.Reply == nil) != (creationEvent.DeliveryContext.Reply == nil) || len(original.DeliveryContext.Joins) != len(creationEvent.DeliveryContext.Joins) {
		return fmt.Errorf("flow construction child projection changes immutable outgoing occurrence")
	}
	return validateFlowConstructionChildReturnAdmissions(original, creationEvent, childOwner.RunID)
}

func validateFlowConstructionChildReturnAdmissions(original, creationEvent *DynamicFlowRuntimeCreationEventPlan, childRunID string) error {
	for index, originalJoin := range original.DeliveryContext.Joins {
		mappedJoin := creationEvent.DeliveryContext.Joins[index]
		if originalJoin.Disposition != mappedJoin.Disposition || !originalJoin.Ref.Declaration().Equal(mappedJoin.Ref.Declaration()) {
			return fmt.Errorf("flow construction child projection changes outgoing return admission")
		}
		if !mappedJoin.Ref.StageEntry().Empty() && mappedJoin.Ref.StageEntry().RunID != childRunID {
			return fmt.Errorf("flow construction child projection has foreign outgoing return admission")
		}
	}
	return nil
}
