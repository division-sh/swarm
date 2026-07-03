package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/platformcontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type wave1WriteTarget struct {
	FlowID    string
	NodeID    string
	EventType string
	Kind      string
	Target    string
	Field     string
	Nested    bool
	Entity    bool
}

type wave1ScopedAgentRecord struct {
	LogicalID string
	Entry     runtimecontracts.AgentRegistryEntry
	Source    runtimecontracts.ContractItemSource
}

func wave1SpecialClearTarget(target string) bool {
	switch strings.TrimSpace(target) {
	case "accumulator_state", "cycle_counters", "pending_dedup":
		return true
	default:
		return false
	}
}

func wave1WriteTargetContract(source semanticview.Source, target wave1WriteTarget) (wave1EntityContractView, bool) {
	if !target.Entity {
		return wave1EntityContractView{}, false
	}
	if _, ownerFlowID, _, err := wave1ResolveWriteTargetPath(source, target); err == nil {
		view := wave1EntityContractForFlow(source, ownerFlowID)
		if view.Defined {
			return view, true
		}
	}
	view := wave1EntityContractForFlow(source, target.FlowID)
	if view.Defined {
		if _, ok := view.Contract.Fields[target.Field]; ok {
			return view, true
		}
	}
	if target.FlowID != "" && wave1FlowWritesRootField(source, target.FlowID, target.Field) {
		if root, ok := wave1RootFieldContract(source, target.Field); ok {
			return root, true
		}
	}
	return wave1EntityContractView{}, false
}

func checkEntityWriterCoverage(c *checkerContext) []Finding { return c.entityWriterCoverage() }
func checkEntityWriteTargetCompliance(c *checkerContext) []Finding {
	return c.entityWriteTargetCompliance()
}
func checkEntityReaderCoverage(c *checkerContext) []Finding { return c.entityReaderCoverage() }

func (c *checkerContext) entityWriterCoverage() []Finding {
	if c.entityWriterCoverageLoaded {
		return c.entityWriterCoverageFindings
	}
	c.entityWriterCoverageLoaded = true

	writers := wave1EntityWriterCoverageByFlow(c.source)
	for _, contract := range wave1DeclaredEntityContracts(c.source) {
		for fieldName, fieldDecl := range contract.Contract.Fields {
			fieldName = strings.TrimSpace(fieldName)
			if fieldName == "" {
				continue
			}
			if fieldDecl.Initial != nil {
				continue
			}
			if strings.TrimSpace(fieldDecl.MaterializeFrom) != "" {
				continue
			}
			if strings.TrimSpace(fieldDecl.UnusedReason) != "" {
				continue
			}
			if _, ok := writers[contract.FlowID][fieldName]; ok {
				continue
			}
			c.entityWriterCoverageFindings = append(c.entityWriterCoverageFindings, Finding{
				CheckID:  "entity_writer_coverage",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("flow %s entity_type %s declares field %s without authored writer coverage, initial, or _unused_reason", defaultFlowLabel(contract.FlowID), contract.EntityType, fieldName),
				Location: defaultFlowLabel(contract.FlowID),
			})
		}
	}
	c.entityWriterCoverageFindings = append(c.entityWriterCoverageFindings, wave1PromptEntityWriteAuthorizationFindings(c.source)...)
	return c.entityWriterCoverageFindings
}

func (c *checkerContext) entityWriteTargetCompliance() []Finding {
	if c.entityWriteTargetComplianceLoaded {
		return c.entityWriteTargetComplianceFindings
	}
	c.entityWriteTargetComplianceLoaded = true
	seen := map[string]struct{}{}
	for _, target := range wave1AllEntityWriteTargets(c.source) {
		if !target.Entity || strings.TrimSpace(target.Field) == "" {
			continue
		}
		key := strings.Join([]string{target.FlowID, target.NodeID, target.EventType, target.Kind, target.Target}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		if target.Kind == "handler.clear" && wave1SpecialClearTarget(target.Field) {
			continue
		}
		resolved, ownerFlowID, rootField, err := wave1ResolveWriteTargetPath(c.source, target)
		if err != nil {
			c.entityWriteTargetComplianceFindings = append(c.entityWriteTargetComplianceFindings, Finding{
				CheckID:  "entity_write_target_compliance",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("flow %s node %s handler %s %s target %q is invalid: %v", defaultFlowLabel(target.FlowID), target.NodeID, target.EventType, target.Kind, target.Target, err),
				Location: target.NodeID,
			})
			continue
		}
		_ = resolved
		contract, ok := wave1WriteTargetContract(c.source, target)
		if !ok {
			c.entityWriteTargetComplianceFindings = append(c.entityWriteTargetComplianceFindings, Finding{
				CheckID:  "entity_write_target_compliance",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("flow %s node %s handler %s %s writes %q missing from declared Wave 1 entity contract", defaultFlowLabel(target.FlowID), target.NodeID, target.EventType, target.Kind, target.Field),
				Location: target.NodeID,
			})
			continue
		}
		if ownerFlowID != "" {
			contract.FlowID = ownerFlowID
		}
		if _, ok := contract.Contract.Fields[rootField]; !ok {
			c.entityWriteTargetComplianceFindings = append(c.entityWriteTargetComplianceFindings, Finding{
				CheckID:  "entity_write_target_compliance",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("flow %s node %s handler %s %s writes undeclared entity field %q on entity_type %s", defaultFlowLabel(target.FlowID), target.NodeID, target.EventType, target.Kind, rootField, contract.EntityType),
				Location: target.NodeID,
			})
		}
	}
	return c.entityWriteTargetComplianceFindings
}

func (c *checkerContext) entityReaderCoverage() []Finding {
	if c.entityReaderCoverageLoaded {
		return c.entityReaderCoverageFindings
	}
	c.entityReaderCoverageLoaded = true

	readers := wave1EntityReaderCoverageByFlow(c.source)
	for _, contract := range wave1DeclaredEntityContracts(c.source) {
		for fieldName, fieldDecl := range contract.Contract.Fields {
			fieldName = strings.TrimSpace(fieldName)
			if fieldName == "" {
				continue
			}
			if strings.TrimSpace(fieldDecl.UnusedReaderReason) != "" {
				continue
			}
			if _, ok := readers[contract.FlowID][fieldName]; ok {
				continue
			}
			c.entityReaderCoverageFindings = append(c.entityReaderCoverageFindings, Finding{
				CheckID:  "entity_reader_coverage",
				Severity: SeverityLintEvidence,
				Message:  fmt.Sprintf("flow %s entity_type %s declares field %s with no detected internal reader coverage", defaultFlowLabel(contract.FlowID), contract.EntityType, fieldName),
				Location: defaultFlowLabel(contract.FlowID),
			})
		}
	}
	return c.entityReaderCoverageFindings
}

func wave1DeclaredEntityContracts(source semanticview.Source) []wave1EntityContractView {
	contracts := make([]wave1EntityContractView, 0)
	root := wave1EntityContractForFlow(source, "")
	if root.Defined {
		contracts = append(contracts, root)
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		view := wave1EntityContractForFlow(source, flowID)
		if view.Defined {
			contracts = append(contracts, view)
		}
	}
	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].FlowID == contracts[j].FlowID {
			return contracts[i].EntityType < contracts[j].EntityType
		}
		return contracts[i].FlowID < contracts[j].FlowID
	})
	return contracts
}

func wave1EntityWriterCoverageByFlow(source semanticview.Source) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, target := range wave1AllEntityWriteTargets(source) {
		if !target.Entity || wave1SpecialClearTarget(target.Field) {
			continue
		}
		_, ownerFlowID, rootField, err := wave1ResolveWriteTargetPath(source, target)
		if err != nil {
			continue
		}
		if out[ownerFlowID] == nil {
			out[ownerFlowID] = map[string]struct{}{}
		}
		out[ownerFlowID][rootField] = struct{}{}
	}
	for flowID, fields := range wave1AgentExplicitEntityWriteCoverageByFlow(source) {
		if out[flowID] == nil {
			out[flowID] = map[string]struct{}{}
		}
		for field := range fields {
			out[flowID][field] = struct{}{}
		}
	}
	return out
}

func wave1AgentExplicitEntityWriteCoverageByFlow(source semanticview.Source) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return out
	}
	for _, record := range wave1ScopedAgentRecords(bundle) {
		if len(record.Entry.EntityWrites) == 0 {
			continue
		}
		for entityType, decl := range record.Entry.EntityWrites {
			contract, ok := wave1ResolveEntityWriteContract(source, record.Source, entityType)
			if !ok {
				continue
			}
			if out[contract.FlowID] == nil {
				out[contract.FlowID] = map[string]struct{}{}
			}
			for _, field := range decl.Create.Fields {
				if _, declared := contract.Contract.Fields[field]; declared {
					out[contract.FlowID][field] = struct{}{}
				}
			}
			for _, field := range decl.Save.Fields {
				if _, declared := contract.Contract.Fields[field]; declared {
					out[contract.FlowID][field] = struct{}{}
				}
			}
		}
	}
	for _, ref := range wave1ContainedStateOperations(source) {
		contract, ok := entityruntime.ResolveForFlow(source, ref.FlowID)
		if !ok {
			continue
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, ref.Write.Target(), string(ref.Write.Operation), !ref.Write.Key.IsZero(), !ref.Write.Index.IsZero())
		if err != nil {
			continue
		}
		ownerFlowID := strings.TrimSpace(ref.FlowID)
		if out[ownerFlowID] == nil {
			out[ownerFlowID] = map[string]struct{}{}
		}
		out[ownerFlowID][target.RootField] = struct{}{}
	}
	return out
}

func wave1PromptEntityWriteAuthorizationFindings(source semanticview.Source) []Finding {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	evidence, err := runtimecontracts.DerivePromptEntityWriteEvidence(bundle)
	if err != nil {
		return []Finding{{
			CheckID:  "entity_writer_coverage",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("prompt entity write evidence derivation failed: %v", err),
			Location: defaultFlowLabel(""),
		}}
	}
	findings := make([]Finding, 0)
	for _, item := range evidence {
		contract, ok := wave1AgentPromptEntityContract(source, item.Source)
		if !ok {
			continue
		}
		writeDecl, ok := wave1PromptEntityWriteDecl(item.Entry, contract)
		if item.CreateEntity && (!ok || !writeDecl.Create.Declared()) {
			findings = append(findings, Finding{
				CheckID:  "entity_writer_coverage",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("agent %s prompt declares create_entity for flow %s entity_type %s without matching agents.yaml entity_writes.%s.create authorization", item.AgentID, defaultFlowLabel(contract.FlowID), contract.EntityType, contract.EntityType),
				Location: item.PromptFile,
			})
		}
		if item.SaveEntity && (!ok || !writeDecl.Save.Declared()) {
			findings = append(findings, Finding{
				CheckID:  "entity_writer_coverage",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("agent %s prompt declares save_entity_field for flow %s entity_type %s without matching agents.yaml entity_writes.%s.save authorization", item.AgentID, defaultFlowLabel(contract.FlowID), contract.EntityType, contract.EntityType),
				Location: item.PromptFile,
			})
			continue
		}
		if !item.SaveEntity {
			continue
		}
		for _, field := range item.SaveFields {
			if err := wave1PromptSaveEntityFieldPathValid(contract, field); err != nil {
				findings = append(findings, Finding{
					CheckID:  "entity_writer_coverage",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("agent %s prompt declares save_entity_field for undeclared field path %s on flow %s entity_type %s: %v", item.AgentID, field, defaultFlowLabel(contract.FlowID), contract.EntityType, err),
					Location: item.PromptFile,
				})
				continue
			}
			if writeDecl.Save.All {
				continue
			}
			if !wave1PromptSaveEntityFieldAuthorized(writeDecl.Save, field) {
				findings = append(findings, Finding{
					CheckID:  "entity_writer_coverage",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("agent %s prompt declares save_entity_field for field %s on flow %s entity_type %s without matching agents.yaml entity_writes.%s.save authorization", item.AgentID, field, defaultFlowLabel(contract.FlowID), contract.EntityType, contract.EntityType),
					Location: item.PromptFile,
				})
			}
		}
	}
	return findings
}

func wave1PromptSaveEntityFieldPathValid(contract wave1EntityContractView, field string) error {
	field = strings.TrimSpace(field)
	if field == "" {
		return fmt.Errorf("field is required")
	}
	resolved, err := entityruntime.ResolveFieldPath(entityruntime.Contract{
		FlowID:     contract.FlowID,
		EntityType: contract.EntityType,
		Entity:     contract.Contract,
		Types:      contract.Types,
	}, field)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resolved.FieldDecl.MaterializeFrom) != "" {
		return fmt.Errorf("field %s is materialized by runtime accumulator projection and is not agent-writable", field)
	}
	return nil
}

func wave1PromptSaveEntityFieldAuthorized(rule runtimecontracts.AgentEntityWriteRule, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	if rule.All {
		return true
	}
	root, _, _ := strings.Cut(field, ".")
	root = strings.TrimSpace(root)
	for _, candidate := range rule.Fields {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if candidate == field || candidate == root {
			return true
		}
	}
	return false
}

func wave1PromptEntityWriteDecl(entry runtimecontracts.AgentRegistryEntry, contract wave1EntityContractView) (runtimecontracts.AgentEntityWriteDecl, bool) {
	if len(entry.EntityWrites) == 0 {
		return runtimecontracts.AgentEntityWriteDecl{}, false
	}
	if contract.FlowID != "" {
		if value, ok := entry.EntityWrites[contract.FlowID+"."+contract.EntityType]; ok {
			return value, true
		}
	}
	if value, ok := entry.EntityWrites[contract.EntityType]; ok {
		return value, true
	}
	return runtimecontracts.AgentEntityWriteDecl{}, false
}

func wave1ResolveEntityWriteContract(source semanticview.Source, agentSource runtimecontracts.ContractItemSource, entityType string) (wave1EntityContractView, bool) {
	entityType = strings.TrimSpace(entityType)
	if entityType == "" {
		return wave1EntityContractView{}, false
	}
	if flowID := strings.TrimSpace(agentSource.FlowID); flowID != "" {
		view := wave1EntityContractForFlow(source, flowID)
		if view.Defined {
			if entityType == view.EntityType || entityType == flowID+"."+view.EntityType {
				return view, true
			}
		}
	}
	for _, contract := range wave1DeclaredEntityContracts(source) {
		if entityType == contract.EntityType {
			return contract, true
		}
		if contract.FlowID != "" && entityType == contract.FlowID+"."+contract.EntityType {
			return contract, true
		}
	}
	return wave1EntityContractView{}, false
}

func wave1AgentPromptEntityContract(source semanticview.Source, agentSource runtimecontracts.ContractItemSource) (wave1EntityContractView, bool) {
	if flowID := strings.TrimSpace(agentSource.FlowID); flowID != "" {
		view := wave1EntityContractForFlow(source, flowID)
		if view.Defined {
			return view, true
		}
	}
	root := wave1EntityContractForFlow(source, "")
	if root.Defined {
		return root, true
	}
	return wave1EntityContractView{}, false
}

func wave1ScopedAgentRecords(bundle *runtimecontracts.WorkflowContractBundle) []wave1ScopedAgentRecord {
	if bundle == nil {
		return nil
	}
	out := make([]wave1ScopedAgentRecord, 0)
	for _, view := range bundle.ProjectViews() {
		agentIDs := make([]string, 0, len(view.Agents))
		for logicalID := range view.Agents {
			logicalID = strings.TrimSpace(logicalID)
			if logicalID != "" {
				agentIDs = append(agentIDs, logicalID)
			}
		}
		sort.Strings(agentIDs)
		for _, logicalID := range agentIDs {
			out = append(out, wave1ScopedAgentRecord{
				LogicalID: logicalID,
				Entry:     view.Agents[logicalID],
				Source: runtimecontracts.ContractItemSource{
					PackageKey: strings.TrimSpace(view.Paths.Key),
					Layer:      "project",
				},
			})
		}
	}
	for _, view := range bundle.FlowViews() {
		agentIDs := make([]string, 0, len(view.Agents))
		for logicalID := range view.Agents {
			logicalID = strings.TrimSpace(logicalID)
			if logicalID != "" {
				agentIDs = append(agentIDs, logicalID)
			}
		}
		sort.Strings(agentIDs)
		for _, logicalID := range agentIDs {
			out = append(out, wave1ScopedAgentRecord{
				LogicalID: logicalID,
				Entry:     view.Agents[logicalID],
				Source: runtimecontracts.ContractItemSource{
					PackageKey: strings.TrimSpace(view.Paths.PackageKey),
					FlowID:     strings.TrimSpace(view.Paths.ID),
					Layer:      "flow",
				},
			})
		}
	}
	return out
}

func wave1EntityReaderCoverageByFlow(source semanticview.Source) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, nodeID := range sortedNodeIDs(source) {
		flowID := ""
		if sourceRef, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		for eventType, handler := range source.NodeEventHandlers(nodeID) {
			eventType = strings.TrimSpace(eventType)
			for _, expr := range handlerEntityExpressions(handler) {
				for _, ref := range wave1ResolvedExpressionRefs(source, flowID, nodeID, eventType, expr) {
					ownerFlowID := strings.TrimSpace(ref.OwnerFlowID)
					if out[ownerFlowID] == nil {
						out[ownerFlowID] = map[string]struct{}{}
					}
					out[ownerFlowID][ref.Field] = struct{}{}
				}
			}
		}
	}
	return out
}

func wave1AllEntityWriteTargets(source semanticview.Source) []wave1WriteTarget {
	out := make([]wave1WriteTarget, 0)
	nodes := source.NodeEntries()
	for _, nodeID := range sortedNodeIDs(source) {
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		flowID := ""
		if sourceRef, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, target := range wave1HandlerWriteTargets(flowID, strings.TrimSpace(nodeID), eventType, handler) {
				out = append(out, target)
			}
		}
	}
	return out
}

func wave1HandlerWriteTargets(flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) []wave1WriteTarget {
	out := make([]wave1WriteTarget, 0)
	add := func(kind, target string) {
		write := wave1ParseWriteTarget(flowID, nodeID, eventType, kind, target)
		if strings.TrimSpace(write.Target) == "" {
			return
		}
		out = append(out, write)
	}
	addRuleTargets := func(scope string, rule runtimecontracts.HandlerRuleEntry) {
		for _, write := range rule.DataAccumulation.Writes {
			if write.IsContainedOperation() {
				continue
			}
			add(scope+".data_accumulation", write.Target())
		}
		if rule.Compute != nil {
			add(scope+".compute", rule.Compute.StoreAs)
		}
	}
	var addQueryTargets func(scope string, query *runtimecontracts.QuerySpec)
	addQueryTargets = func(scope string, query *runtimecontracts.QuerySpec) {
		if query == nil {
			return
		}
		add(scope+".query", query.StoreAs)
		for i := range query.Queries {
			addQueryTargets(scope+".query", &query.Queries[i])
		}
	}

	addQueryTargets("handler", handler.Query)
	for _, write := range handler.DataAccumulation.Writes {
		if write.IsContainedOperation() {
			continue
		}
		add("handler.data_accumulation", write.Target())
	}
	if handler.Compute != nil {
		add("handler.compute", handler.Compute.StoreAs)
	}
	if handler.Filter != nil {
		add("handler.filter", handler.Filter.StoreAs)
	}
	if handler.GroupBy != nil {
		add("handler.group_by", handler.GroupBy.StoreAs)
	}
	if handler.Reduce != nil {
		add("handler.reduce", handler.Reduce.StoreAs)
	}
	if handler.Count != nil {
		add("handler.count", handler.Count.StoreAs)
	}
	if handler.Clear != nil {
		for _, target := range handler.Clear.Targets {
			add("handler.clear", target)
		}
	}
	for idx, rule := range handler.Rules {
		scope := fmt.Sprintf("handler.rules[%d]", idx)
		if id := strings.TrimSpace(rule.ID); id != "" {
			scope = "handler.rules[" + id + "]"
		}
		addRuleTargets(scope, rule)
	}
	for idx, rule := range handler.OnComplete {
		scope := fmt.Sprintf("handler.on_complete[%d]", idx)
		if id := strings.TrimSpace(rule.ID); id != "" {
			scope = "handler.on_complete[" + id + "]"
		}
		addRuleTargets(scope, rule)
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			scope := fmt.Sprintf("handler.accumulate.on_complete[%d]", idx)
			if id := strings.TrimSpace(rule.ID); id != "" {
				scope = "handler.accumulate.on_complete[" + id + "]"
			}
			addRuleTargets(scope, rule)
		}
		if handler.Accumulate.OnTimeout != nil {
			scope := "handler.accumulate.on_timeout"
			if id := strings.TrimSpace(handler.Accumulate.OnTimeout.ID); id != "" {
				scope = "handler.accumulate.on_timeout[" + id + "]"
			}
			addRuleTargets(scope, *handler.Accumulate.OnTimeout)
		}
	}
	for idx, branch := range handler.Branch {
		if branch.Then != nil {
			addRuleTargets(fmt.Sprintf("handler.branch[%d].then", idx), *branch.Then)
		}
		if branch.Else != nil {
			addRuleTargets(fmt.Sprintf("handler.branch[%d].else", idx), *branch.Else)
		}
	}
	return out
}

func wave1ParseWriteTarget(flowID, nodeID, eventType, kind, target string) wave1WriteTarget {
	write := wave1WriteTarget{
		FlowID:    strings.TrimSpace(flowID),
		NodeID:    strings.TrimSpace(nodeID),
		EventType: strings.TrimSpace(eventType),
		Kind:      strings.TrimSpace(kind),
		Target:    strings.TrimSpace(target),
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return write
	}
	if strings.HasPrefix(target, "metadata.") {
		return write
	}
	if target == platformcontext.EntityRoot || strings.HasPrefix(target, platformcontext.EntityRoot+".") {
		write.Entity = true
		write.Field = platformcontext.EntityRoot
		return write
	}
	if strings.HasPrefix(target, "gates.") || target == "gates" {
		write.Field = "gates"
		write.Entity = false
		return write
	}
	if strings.HasPrefix(target, "entity.") {
		write.Entity = true
		target = strings.TrimSpace(strings.TrimPrefix(target, "entity."))
	} else if !strings.Contains(target, ".") {
		write.Entity = true
	}
	if !write.Entity {
		return write
	}
	if idx := strings.IndexByte(target, '.'); idx >= 0 {
		write.Field = strings.TrimSpace(target[:idx])
		write.Nested = true
		return write
	}
	write.Field = strings.TrimSpace(target)
	return write
}

func wave1ResolveWriteTargetPath(source semanticview.Source, target wave1WriteTarget) (wave1ResolvedType, string, string, error) {
	path, entityTarget, err := entityruntime.EntityWritePath(target.Target)
	if err != nil {
		return wave1ResolvedType{}, "", "", err
	}
	if !entityTarget {
		return wave1ResolvedType{}, "", "", fmt.Errorf("target %q is not an entity write target", target.Target)
	}
	rootField, _, _ := strings.Cut(path, ".")
	if !strings.Contains(path, ".") {
		if view := wave1EntityContractForFlow(source, target.FlowID); view.Defined {
			if _, ok := view.Contract.Fields[strings.TrimSpace(rootField)]; ok {
				return wave1ResolvedType{}, strings.TrimSpace(view.FlowID), strings.TrimSpace(rootField), nil
			}
		}
		if target.FlowID != "" && wave1FlowWritesRootField(source, target.FlowID, rootField) {
			if root, ok := wave1RootFieldContract(source, rootField); ok {
				return wave1ResolvedType{}, strings.TrimSpace(root.FlowID), strings.TrimSpace(rootField), nil
			}
		}
		if platformcontext.LegacyEntityMetadataField(rootField) {
			return wave1ResolvedType{}, "", "", fmt.Errorf("%s", legacyEntityMetadataDiagnostic(rootField))
		}
		return wave1ResolvedType{}, strings.TrimSpace(target.FlowID), strings.TrimSpace(rootField), nil
	}
	leaf, ownerFlowID, err := wave1ResolveEntityPathWithOwner(source, target.FlowID, "entity."+path)
	if err != nil {
		return wave1ResolvedType{}, "", "", err
	}
	return leaf, ownerFlowID, strings.TrimSpace(rootField), nil
}

type wave1ResolvedExpressionRef struct {
	Ref         string
	Field       string
	OwnerFlowID string
	Leaf        wave1ResolvedType
}

func wave1ResolvedExpressionRefs(source semanticview.Source, flowID, nodeID, eventType string, expr expressionReference) []wave1ResolvedExpressionRef {
	refs := runtimepipeline.WorkflowEntityReferences(expr.Expression)
	out := make([]wave1ResolvedExpressionRef, 0, len(refs))
	for _, ref := range refs {
		leaf, ownerFlowID, err := wave1ResolveEntityPathWithOwner(source, flowID, ref)
		if err != nil {
			continue
		}
		out = append(out, wave1ResolvedExpressionRef{
			Ref:         ref,
			Field:       runtimepipeline.WorkflowEntityReferenceField(ref),
			OwnerFlowID: strings.TrimSpace(ownerFlowID),
			Leaf:        leaf,
		})
	}
	return out
}

func sortedNodeIDs(source semanticview.Source) []string {
	nodes := source.NodeEntries()
	out := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID != "" {
			out = append(out, nodeID)
		}
	}
	sort.Strings(out)
	return out
}
