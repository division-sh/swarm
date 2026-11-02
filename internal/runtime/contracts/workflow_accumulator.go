package contracts

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

func (b *WorkflowContractBundle) ValidateAccumulator(node runtimeidentity.ExecutableNode, event string, spec *AccumulateSpec) error {
	if spec == nil {
		return nil
	}
	if spec.Key == "" || spec.Key != strings.TrimSpace(spec.Key) {
		return fmt.Errorf("accumulate.key %q must name an exact required scalar item field", spec.Key)
	}
	primary, err := b.ResolveFlowPrimaryEntity(node.FlowPath())
	if err != nil {
		return err
	}
	decl, present := primary.Contract.Fields[spec.Target]
	if !present {
		return fmt.Errorf("accumulate target %q must name a declared state list field", spec.Target)
	}
	resolved, err := (CatalogTypeReference{Type: decl.Type, Catalog: primary.Types}).Resolve()
	if err != nil {
		return err
	}
	if resolved.Kind != CatalogTypeList || resolved.Element == nil || resolved.Element.Kind != CatalogTypeObject {
		return fmt.Errorf("accumulate target %s: %s must be a list of typed payload objects", spec.Target, decl.Type)
	}
	compiled, found, err := b.ResolveEffectiveCompiledFlowEventSchema(node.FlowPath(), event)
	if err != nil {
		return err
	}
	expected, err := canonicaljson.Bytes(eventschema.CanonicalAcceptanceSchema(eventSchemaForCatalogType(*resolved.Element, primary.Types)))
	if err != nil {
		return err
	}
	if !found || !bytes.Equal(expected, compiled.CanonicalAcceptanceSchema()) {
		return fmt.Errorf("accumulate target %s: %s but %s payload has a different type; declare _embed: [%s] on the event or change the target's element type", spec.Target, decl.Type, event, resolved.Element.Name)
	}
	key, found := resolved.Element.Field(spec.Key)
	if !found || key.IsOptional {
		return fmt.Errorf("accumulate.key %q must name a required item field; write a bare field name, not a payload path", spec.Key)
	}
	switch key.Type.Kind {
	case CatalogTypeText, CatalogTypeInteger, CatalogTypeNumber, CatalogTypeBoolean:
	default:
		return fmt.Errorf("accumulate.key %q must be a required scalar item field", spec.Key)
	}
	for _, connect := range b.Semantics.CompositionConnects {
		input, found, err := b.ConnectionInput(connect)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		receiver, found := input.ReceiverEventSchema()
		if !found || receiver.FlowPath() != node.FlowPath() || receiver.EventName() != b.ResolveExecutableNodeEventReference(node, event) {
			continue
		}
		source := input.SourceEvidence().Source
		if source.Kind == FlowInputInstanceSourcePayload && source.Path == "payload."+spec.Key {
			return fmt.Errorf("accumulate.key %q selects this keyed instance through connect at %s:%d; choose the field that distinguishes one arrival", spec.Key, connect.SourceFile, connect.SourceLine)
		}
	}
	return nil
}

func (b *WorkflowContractBundle) validateAccumulatorWriters() error {
	owners := map[string]string{}
	location := func(record ScopedNodeRecord, event, slot string) string {
		path := fmt.Sprintf("[%q][%q].%s", record.LogicalID, event, slot)
		if at, found := b.IntroductionLocation(record.Source.File, path, false); found {
			return at.String()
		}
		return record.Source.File + ":" + path
	}
	for _, record := range b.ScopedNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for _, event := range sortedContractKeys(record.Entry.EventHandlers) {
			handler := record.Entry.EventHandlers[event]
			if handler.Accumulate == nil {
				continue
			}
			at := location(record, event, "accumulate")
			if err := ValidateAccumulateHandlerIsolation(handler); err != nil {
				return fmt.Errorf("%s: %w", at, err)
			}
			if err := b.ValidateAccumulator(node, event, handler.Accumulate); err != nil {
				return fmt.Errorf("%s: %w", at, err)
			}
			key := node.FlowPath() + "\x00" + handler.Accumulate.Target
			if previous, exists := owners[key]; exists {
				return fmt.Errorf("accumulate target %q has two writers at %s and %s", handler.Accumulate.Target, previous, at)
			}
			owners[key] = at
		}
	}
	for _, record := range b.ScopedNodeRecords() {
		for _, event := range sortedContractKeys(record.Entry.EventHandlers) {
			handler := record.Entry.EventHandlers[event]
			check := func(target, slot string) error {
				path := paths.Parse(target)
				if path.Root != paths.RootEntity && path.Root != paths.RootUnknown || len(path.Segments) == 0 {
					return nil
				}
				if owner := owners[record.Source.FlowPath+"\x00"+path.Segments[0]]; owner != "" {
					return fmt.Errorf("accumulate target %q has its sole writer at %s; conflicting %s at %s", path.Segments[0], owner, slot, location(record, event, slot))
				}
				return nil
			}
			writes := append([]WorkflowDataWrite(nil), handler.DataAccumulation.Writes...)
			computes := []*ComputeSpec{handler.Compute}
			for _, rule := range HandlerRuleEntries(handler) {
				writes = append(writes, rule.DataAccumulation.Writes...)
				computes = append(computes, rule.Compute)
			}
			for _, write := range writes {
				if err := check(write.Target(), "data_accumulation"); err != nil {
					return err
				}
			}
			for _, compute := range computes {
				if compute != nil {
					if err := check(compute.StoreAs, "compute"); err != nil {
						return err
					}
				}
			}
			if handler.Query != nil {
				if err := check(handler.Query.StoreAs, "query"); err != nil {
					return err
				}
			}
			stores := map[string]string{}
			if handler.Filter != nil {
				stores["filter"] = handler.Filter.StoreAs
			}
			if handler.GroupBy != nil {
				stores["group_by"] = handler.GroupBy.StoreAs
			}
			if handler.Reduce != nil {
				stores["reduce"] = handler.Reduce.StoreAs
			}
			if handler.Count != nil {
				stores["count"] = handler.Count.StoreAs
			}
			for _, slot := range sortedContractKeys(stores) {
				if err := check(stores[slot], slot); err != nil {
					return err
				}
			}
			if handler.Clear != nil {
				for _, target := range handler.Clear.Targets {
					if err := check(target, "clear"); err != nil {
						return err
					}
				}
			}
		}
	}
	for _, agent := range b.AgentDeclarationRecords() {
		for key, owner := range owners {
			flow, field, _ := strings.Cut(key, "\x00")
			if flow != agent.OwnerFlowID {
				continue
			}
			granted := agent.Entry.Writes.All
			for _, candidate := range agent.Entry.Writes.Fields {
				granted = granted || candidate == field
			}
			if granted {
				return fmt.Errorf("accumulate target %q has its sole writer at %s; conflicting agent write grant at %s:%s.writes", field, owner, agent.Source.File, agent.LogicalID)
			}
		}
	}
	return nil
}
