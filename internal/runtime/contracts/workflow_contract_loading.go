package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
)

type WorkflowContractLoadOptions struct {
	PlatformPackBase   *packartifact.PlatformPackInventory
	PlatformPackBases  packartifact.PlatformPackBaseResolver
	AdmitPackInventory func(*packartifact.EffectivePackInventory, PlatformSpecDocument) (PackAdmissionProjection, error)
}

type PackAdmissionProjection interface {
	EffectivePackInventoryDigest() string
}

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
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPaths(repoRoot), WorkflowContractLoadOptions{})
}
func LoadWorkflowContractBundleWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride), WorkflowContractLoadOptions{})
}
func LoadWorkflowContractBundleWithOptions(repoRoot, workflowDirOverride, platformSpecFileOverride string, options WorkflowContractLoadOptions) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride), options)
}
func loadWorkflowContractBundleForPaths(paths ContractPaths, options WorkflowContractLoadOptions) (*WorkflowContractBundle, error) {
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
			projectView, err := loadProjectContractView(paths.ContractsRoot, pkgPaths, manifest)
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
			effectiveMode, err := ResolveEffectiveFlowMode(flow.ID, flow.Mode, schema.Mode)
			if err != nil {
				return nil, err
			}
			schema.Mode = effectiveMode
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
			flowView, err := loadFlowContractView(paths.ContractsRoot, flow, schema)
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
	projectPacks, err := packartifact.LoadProjectPackSet(paths.ContractsRoot)
	if err != nil {
		return nil, err
	}
	receiptPath := ""
	if strings.TrimSpace(paths.ContractsRoot) != "" {
		receiptPath = filepath.Join(paths.ContractsRoot, filepath.FromSlash(packartifact.PackSelectionRelativePath))
	}
	persistedReceipt, receipt, err := loadWorkflowPackSelectionReceipt(receiptPath)
	if err != nil {
		return nil, err
	}
	base, err := resolveWorkflowPlatformPackBase(options, strings.TrimSpace(bundle.Platform.Platform.Version), receipt)
	if err != nil {
		return nil, err
	}
	effective, err := packartifact.NewEffectivePackInventory(base, projectPacks.Sources)
	if err != nil {
		return nil, fmt.Errorf("resolve effective pack inventory: %w", err)
	}
	receiptBody, err := effective.SelectionReceiptBody()
	if err != nil {
		return nil, err
	}
	if receipt != nil {
		if !receipt.Matches(base, effective) {
			return nil, fmt.Errorf(
				"pack selection receipt requires %s base %s and effective inventory %s but reconstruction selected %s base %s and effective inventory %s",
				receipt.BaseMode, receipt.BaseDigest, receipt.EffectiveDigest,
				base.SelectionMode(), base.Digest(), effective.Digest(),
			)
		}
		bundle.PackSelectionPath = receiptPath
		receiptBody = persistedReceipt
	}
	if options.AdmitPackInventory != nil {
		admission, err := options.AdmitPackInventory(effective, bundle.Platform)
		if err != nil {
			return nil, fmt.Errorf("admit effective pack inventory: %w", err)
		}
		if admission == nil || strings.TrimSpace(admission.EffectivePackInventoryDigest()) != effective.Digest() {
			return nil, fmt.Errorf("admitted pack projection does not own effective inventory %s", effective.Digest())
		}
		bundle.PackAdmission = admission
	} else if base.SelectionMode() == packartifact.SelectionDevelopmentOverride || len(projectPacks.Sources) > 0 {
		return nil, fmt.Errorf("body-specific pack admission is required for development or project pack inventory")
	}
	bundle.PackSelectionBody = receiptBody
	bundle.ProjectPacks = projectPacks
	bundle.PackInventory = effective
	populateWorkflowSemantics(bundle)
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}

func loadWorkflowPackSelectionReceipt(receiptPath string) ([]byte, *packartifact.PackSelectionReceipt, error) {
	if strings.TrimSpace(receiptPath) == "" {
		return nil, nil, nil
	}
	info, err := os.Lstat(receiptPath)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("inspect pack selection receipt: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("pack selection receipt %s must be a regular file", packartifact.PackSelectionRelativePath)
	}
	body, err := os.ReadFile(receiptPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read pack selection receipt: %w", err)
	}
	receipt, err := packartifact.ParsePackSelectionReceipt(body)
	if err != nil {
		return nil, nil, err
	}
	return body, &receipt, nil
}

func resolveWorkflowPlatformPackBase(options WorkflowContractLoadOptions, runningVersion string, receipt *packartifact.PackSelectionReceipt) (*packartifact.PlatformPackInventory, error) {
	if options.PlatformPackBase != nil && options.PlatformPackBases != nil {
		return nil, fmt.Errorf("workflow contract load must not provide competing platform pack base owners")
	}
	if options.PlatformPackBase != nil {
		if receipt != nil && !receipt.MatchesBase(options.PlatformPackBase) {
			return nil, fmt.Errorf("pack selection receipt requires %s base %s but selected base is %s base %s", receipt.BaseMode, receipt.BaseDigest, options.PlatformPackBase.SelectionMode(), options.PlatformPackBase.Digest())
		}
		return options.PlatformPackBase, nil
	}
	if options.PlatformPackBases != nil {
		if receipt != nil {
			return options.PlatformPackBases.ResolvePlatformPackBase(*receipt)
		}
		return options.PlatformPackBases.CurrentPlatformPackBase()
	}
	base, err := packartifact.LoadEmbeddedPlatformPackInventory(runningVersion)
	if err != nil {
		return nil, fmt.Errorf("load embedded platform pack inventory: %w", err)
	}
	if receipt != nil && !receipt.MatchesBase(base) {
		return nil, fmt.Errorf("pack selection receipt requires %s base %s but default embedded selection is %s", receipt.BaseMode, receipt.BaseDigest, base.Digest())
	}
	return base, nil
}
func validateWorkflowContractBundleLoadConstraints(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	errs := make([]error, 0, 8)
	elementOwners := map[string]string{}
	for _, record := range bundle.ScopedNodeRecords() {
		node, identityErr := record.Identity()
		if identityErr != nil {
			errs = append(errs, identityErr)
			continue
		}
		nodeID := node.Key()
		for eventType, handler := range record.Entry.EventHandlers {
			qualified, qualifyErr := QualifySystemNodeHandlerRuleRefs(node, handler)
			if qualifyErr != nil {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s: %v", ErrInvalidField, nodeID, strings.TrimSpace(eventType), qualifyErr))
				continue
			}
			for _, rule := range HandlerRuleEntries(qualified) {
				ref, ok := rule.ContractElementRef()
				if !ok {
					continue
				}
				key := ref.PackageKey().String() + "|" + ref.ElementID().String()
				owner := nodeID + ":" + strings.TrimSpace(eventType)
				if previous, exists := elementOwners[key]; exists {
					errs = append(errs, fmt.Errorf("%w: contract element_id %s is duplicated in package %s by %s and %s", ErrInvalidField, ref.ElementID().String(), ref.PackageKey().String(), previous, owner))
				} else {
					elementOwners[key] = owner
				}
			}
		}
		if _, scopeErr := bundle.ExecutableNodeSemanticScope(node); scopeErr != nil {
			errs = append(errs, fmt.Errorf("%w: node %s semantic scope: %v", ErrInvalidField, nodeID, scopeErr))
			continue
		}
		if authoredID := strings.TrimSpace(record.Entry.ID); !SystemNodeIDMatchesKey(node.NodeID(), authoredID) {
			errs = append(errs, fmt.Errorf("%w: node %s id %q must match map key", ErrInvalidField, nodeID, authoredID))
		}
		if strings.TrimSpace(record.Entry.ExecutionType) != "" {
			if err := ValidateSystemNodeExecutionType(record.Entry.ExecutionType); err != nil {
				errs = append(errs, fmt.Errorf("%w: node %s %v", ErrInvalidField, nodeID, err))
			}
		}
		for eventType, handler := range record.Entry.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if err := ValidateAccumulateHandlerIsolation(handler); err != nil {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s: %v", ErrInvalidField, nodeID, eventType, err))
			}
			for _, site := range HandlerFanOutSites(handler) {
				if _, err := bundle.ResolveFanOutEffectiveSemantics(node, eventType, *site.Spec); err != nil {
					errs = append(errs, fmt.Errorf("%w: node %s handler %s %s: %v", ErrInvalidField, nodeID, eventType, site.Source, err))
				}
			}
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
	errs = append(errs, validateScopedAgentIntentCoordinates(bundle)...)
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
	return len(handler.OnComplete) > 0
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
