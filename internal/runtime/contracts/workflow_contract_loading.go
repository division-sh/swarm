package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func rootWorkflowPolicy(bundle *WorkflowContractBundle) PolicyDocument {
	if bundle == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	for _, view := range bundle.RootProjectViews() {
		return clonePolicyDocument(view.Policy)
	}
	if bundle.FlowTree.Root != nil {
		return clonePolicyDocument(bundle.FlowTree.Root.Policy)
	}
	return PolicyDocument{Values: map[string]PolicyValue{}}
}
func LoadWorkflowContractBundle(repoRoot string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPaths(repoRoot))
}
func LoadWorkflowContractBundleWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride))
}
func loadWorkflowContractBundleForPaths(paths ContractPaths) (*WorkflowContractBundle, error) {
	bundle := &WorkflowContractBundle{
		Paths:                 paths,
		projectContracts:      map[string]ProjectContractView{},
		flowTypes:             map[string]TypeCatalogDocument{},
		flowEntities:          map[string]EntityContractsDocument{},
		scopedNodes:           map[string]SystemNodeContract{},
		scopedEvents:          map[string]EventCatalogEntry{},
		scopedAgents:          map[string]AgentRegistryEntry{},
		scopedTools:           map[string]ToolSchemaEntry{},
		scopedNodeSources:     map[string]ContractItemSource{},
		scopedEventSources:    map[string]ContractItemSource{},
		scopedAgentSources:    map[string]ContractItemSource{},
		scopedToolSources:     map[string]ContractItemSource{},
		nodeSources:           map[string]ContractItemSource{},
		eventSources:          map[string]ContractItemSource{},
		agentSources:          map[string]ContractItemSource{},
		toolSources:           map[string]ContractItemSource{},
		ambiguousNodeAliases:  map[string]struct{}{},
		ambiguousEventAliases: map[string]struct{}{},
		ambiguousAgentAliases: map[string]struct{}{},
		ambiguousToolAliases:  map[string]struct{}{},
		Nodes:                 map[string]SystemNodeContract{},
		Events:                map[string]EventCatalogEntry{},
		Agents:                map[string]AgentRegistryEntry{},
		Tools:                 map[string]ToolSchemaEntry{},
		FlowSchemas:           map[string]FlowSchemaDocument{},
	}
	flowViewsByID := map[string]FlowContractView{}
	if paths.ProjectPackageFile != "" {
		if strings.TrimSpace(paths.RootSchemaFile) != "" {
			var rootSchema FlowSchemaDocument
			if err := loadYAMLFile(paths.RootSchemaFile, &rootSchema); err != nil {
				return nil, err
			}
			bundle.RootSchema = &rootSchema
		}
		if err := loadOptionalYAMLMap(paths.RootTypesFile, &bundle.RootTypes); err != nil {
			return nil, err
		}
		if err := loadOptionalYAMLMap(paths.RootEntitiesFile, &bundle.RootEntities); err != nil {
			return nil, err
		}
		for i, pkgPaths := range paths.ProjectPackages {
			var manifest ProjectPackageDocument
			if err := loadYAMLFile(pkgPaths.PackageFile, &manifest); err != nil {
				return nil, err
			}
			if i == 0 {
				bundle.Package = manifest
			}
			bundle.PackageTree = append(bundle.PackageTree, LoadedProjectPackage{
				Key:       pkgPaths.Key,
				ParentKey: pkgPaths.ParentKey,
				Depth:     pkgPaths.Depth,
				Paths:     pkgPaths,
				Manifest:  manifest,
			})
			projectView, err := loadProjectContractView(pkgPaths, manifest)
			if err != nil {
				return nil, err
			}
			bundle.projectContracts[pkgPaths.Key] = projectView
		}
		if err := validateDiscoveredPackageTree(bundle.PackageTree); err != nil {
			return nil, err
		}
		for _, flow := range paths.Flows {
			if strings.TrimSpace(flow.ID) == "" || strings.TrimSpace(flow.SchemaFile) == "" {
				continue
			}
			if _, exists := bundle.FlowSchemas[flow.ID]; exists {
				return nil, fmt.Errorf("duplicate flow id %q discovered in package tree", flow.ID)
			}
			var schema FlowSchemaDocument
			if err := loadYAMLFile(flow.SchemaFile, &schema); err != nil {
				return nil, err
			}
			if schema.Mode == "" {
				schema.Mode = strings.TrimSpace(flow.Mode)
			}
			bundle.FlowSchemas[flow.ID] = schema
			var flowTypes TypeCatalogDocument
			if err := loadOptionalYAMLMap(flow.TypesFile, &flowTypes); err != nil {
				return nil, err
			}
			if len(flowTypes.Scalars) > 0 || len(flowTypes.Enums) > 0 || len(flowTypes.Types) > 0 {
				bundle.flowTypes[flow.ID] = flowTypes
			}
			var flowEntities EntityContractsDocument
			if err := loadOptionalYAMLMap(flow.EntitiesFile, &flowEntities); err != nil {
				return nil, err
			}
			if len(flowEntities) > 0 {
				bundle.flowEntities[flow.ID] = flowEntities
			}
			flowView, err := loadFlowContractView(flow, schema)
			if err != nil {
				return nil, err
			}
			flowViewsByID[flow.ID] = flowView
		}
		if err := validateWave1ContractsLoadBoundary(bundle); err != nil {
			return nil, err
		}
		if err := buildFlowTree(bundle, flowViewsByID); err != nil {
			return nil, err
		}
		if err := populateMergedPackageViews(bundle, flowViewsByID); err != nil {
			return nil, err
		}
	}
	bundle.Policy = rootWorkflowPolicy(bundle)
	if err := loadYAMLFile(paths.PlatformSpecFile, &bundle.Platform); err != nil {
		return nil, err
	}
	populateWorkflowSemantics(bundle)
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}
func validateWorkflowContractBundleLoadConstraints(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	errs := make([]error, 0, 8)
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if authoredID := strings.TrimSpace(node.ID); !SystemNodeIDMatchesKey(nodeID, authoredID) {
			errs = append(errs, fmt.Errorf("%w: node %s id %q must match map key", ErrInvalidField, nodeID, authoredID))
		}
		if strings.TrimSpace(node.ExecutionType) != "" {
			if err := ValidateSystemNodeExecutionType(node.ExecutionType); err != nil {
				errs = append(errs, fmt.Errorf("%w: node %s %v", ErrInvalidField, nodeID, err))
			}
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if workflowHandlerDeclaresConflictingCompletion(handler) {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s declares both on_complete and rules", ErrConflictingCompletion, nodeID, eventType))
			}
			if usesDeprecatedGuardFallback(handler.Guard) {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s uses deprecated id-only guard; migrate to check:", ErrDeprecatedGuardFallback, nodeID, eventType))
			}
			if strings.TrimSpace(handler.Action.ID) != "" && !IsSupportedHandlerActionID(handler.Action.ID) {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s action %s is not in platform spec", ErrInvalidField, nodeID, eventType, strings.TrimSpace(handler.Action.ID)))
			}
		}
	}
	for eventType, owners := range bundle.Semantics.EventOwners {
		if len(normalizeStrings(owners)) > 1 {
			errs = append(errs, fmt.Errorf("%w: event %s has multiple authoritative system node owners: %s", ErrMultipleAuthoritativeOwners, strings.TrimSpace(eventType), strings.Join(normalizeStrings(owners), ", ")))
		}
	}
	errs = append(errs, validateWorkflowSchemaRefinements(bundle)...)
	errs = append(errs, validateWorkflowCriteriaContracts(bundle)...)
	errs = append(errs, validateWorkflowPolicyValidationContracts(bundle)...)
	errs = append(errs, validateWorkflowComputeModuleContracts(bundle)...)
	if len(errs) > 0 {
		sort.Slice(errs, func(i, j int) bool {
			return strings.TrimSpace(errs[i].Error()) < strings.TrimSpace(errs[j].Error())
		})
		return &LoadValidationError{Items: errs}
	}
	return nil
}
func workflowHandlerDeclaresConflictingCompletion(handler SystemNodeEventHandler) bool {
	return len(handler.Rules) > 0 && workflowHandlerHasOnComplete(handler)
}
func workflowHandlerHasOnComplete(handler SystemNodeEventHandler) bool {
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
}
func usesDeprecatedGuardFallback(spec *GuardSpec) bool {
	if spec == nil {
		return false
	}
	if strings.TrimSpace(spec.Check) != "" {
		return false
	}
	for _, check := range spec.Checks {
		if strings.TrimSpace(check.Check) != "" {
			return false
		}
	}
	return strings.TrimSpace(spec.ID) != ""
}
