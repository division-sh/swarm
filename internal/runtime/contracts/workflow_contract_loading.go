package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/sourceartifact"
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
	if bundle.FlowTree.Root != nil {
		return clonePolicyDocument(bundle.FlowTree.Root.Policy)
	}
	return PolicyDocument{Values: map[string]PolicyValue{}}
}
func LoadWorkflowContractBundle(repoRoot string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleFromSource(repoRoot, repoRoot, DefaultPlatformSpecFile(repoRoot), WorkflowContractLoadOptions{})
}
func LoadWorkflowContractBundleWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleFromSource(repoRoot, workflowDirOverride, platformSpecFileOverride, WorkflowContractLoadOptions{})
}
func LoadWorkflowContractBundleWithOptions(repoRoot, workflowDirOverride, platformSpecFileOverride string, options WorkflowContractLoadOptions) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleFromSource(repoRoot, workflowDirOverride, platformSpecFileOverride, options)
}
func loadWorkflowContractBundleFromSource(repoRoot, sourceRoot, platformSpecFile string, options WorkflowContractLoadOptions) (*WorkflowContractBundle, error) {
	sourceRoot = strings.TrimSpace(sourceRoot)
	if sourceRoot == "" {
		sourceRoot = strings.TrimSpace(repoRoot)
	}
	if strings.TrimSpace(platformSpecFile) == "" {
		platformSpecFile = DefaultPlatformSpecFile(repoRoot)
	}
	artifact, err := sourceartifact.AdmitDirectory(sourceRoot)
	if err != nil {
		return nil, err
	}
	return LoadWorkflowContractBundleFromArtifact(repoRoot, artifact, platformSpecFile, options)
}

// LoadWorkflowContractBundleFromArtifact compiles an already-admitted source
// snapshot. It never receives or reconstructs the source's ambient host path.
func LoadWorkflowContractBundleFromArtifact(repoRoot string, artifact *sourceartifact.AdmittedSourceArtifact, platformSpecFile string, options WorkflowContractLoadOptions) (*WorkflowContractBundle, error) {
	if artifact == nil {
		return nil, fmt.Errorf("admitted source artifact is required")
	}
	if strings.TrimSpace(platformSpecFile) == "" {
		platformSpecFile = DefaultPlatformSpecFile(repoRoot)
	}
	flowSources, err := indexFlowSources(artifact)
	if err != nil {
		return nil, err
	}
	paths := ContractPaths{PlatformSpecFile: platformSpecFile}
	bundle := &WorkflowContractBundle{
		SourceArtifact:        artifact,
		FlowSources:           flowSources,
		Paths:                 paths,
		flowTypes:             map[string]TypeCatalogDocument{},
		flowEntities:          map[string]EntityContractsDocument{},
		dataDeclarations:      map[string]DurableDataDeclaration{},
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
	for _, source := range sortedFlowSources(flowSources) {
		schema := FlowSchemaDocument{}
		if source.Schema != "" {
			if err := artifact.DecodeYAML(source.Schema, &schema); err != nil {
				return nil, fmt.Errorf("decode %s: %w", source.Schema, err)
			}
		}
		view, err := loadFlowContractViewFromSource(artifact, source, schema)
		if err != nil {
			return nil, err
		}
		flowViewsByID[source.FlowPath] = view
		flowTypes, err := loadOptionalTypeDeclarationsFromSource(artifact, source.Types)
		if err != nil {
			return nil, err
		}
		flowEntities, err := loadOptionalEntityDeclarationsFromSource(artifact, source.Entities)
		if err != nil {
			return nil, err
		}
		if source.FlowPath == "." {
			if source.Schema != "" {
				root := schema
				bundle.RootSchema = &root
			}
			bundle.RootTypes = flowTypes
			bundle.RootEntities = flowEntities
		} else {
			if source.Schema != "" {
				bundle.FlowSchemas[source.FlowPath] = schema
			}
			if len(flowTypes.Scalars) > 0 || len(flowTypes.Enums) > 0 || len(flowTypes.Types) > 0 {
				bundle.flowTypes[source.FlowPath] = flowTypes
			}
			if len(flowEntities) > 0 {
				bundle.flowEntities[source.FlowPath] = flowEntities
			}
		}
	}
	if err := buildFilesystemFlowTree(bundle, flowViewsByID); err != nil {
		return nil, err
	}
	if err := populateMergedFlowViews(bundle); err != nil {
		return nil, err
	}
	if err := validateWave1ContractsLoadBoundary(bundle); err != nil {
		return nil, err
	}
	bundle.Policy = rootWorkflowPolicy(bundle)
	if err := loadYAMLFile(platformSpecFile, &bundle.Platform); err != nil {
		return nil, err
	}
	projectPacks, err := packartifact.LoadProjectPackSetFS(artifact.FS())
	if err != nil {
		return nil, err
	}
	base, err := resolveWorkflowPlatformPackBase(options, strings.TrimSpace(bundle.Platform.Platform.Version))
	if err != nil {
		return nil, err
	}
	effective, err := packartifact.NewEffectivePackInventory(base, projectPacks.Sources)
	if err != nil {
		return nil, fmt.Errorf("resolve effective pack inventory: %w", err)
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
	bundle.ProjectPacks = projectPacks
	bundle.PackInventory = effective
	if err := populateWorkflowSemantics(bundle); err != nil {
		return nil, fmt.Errorf("compile workflow semantics: %w", err)
	}
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err != nil {
		return nil, err
	}
	populateEffectiveEventProvenance(bundle)
	if err := loadDurableDataDeclarations(bundle); err != nil {
		return nil, err
	}
	if _, err := BuildDurableDataCatalog(bundle); err != nil {
		return nil, fmt.Errorf("compile durable data catalog: %w", err)
	}
	return bundle, nil
}
func resolveWorkflowPlatformPackBase(options WorkflowContractLoadOptions, runningVersion string) (*packartifact.PlatformPackInventory, error) {
	if options.PlatformPackBase != nil && options.PlatformPackBases != nil {
		return nil, fmt.Errorf("workflow contract load must not provide competing platform pack base owners")
	}
	if options.PlatformPackBase != nil {
		return options.PlatformPackBase, nil
	}
	if options.PlatformPackBases != nil {
		return options.PlatformPackBases.CurrentPlatformPackBase()
	}
	base, err := packartifact.LoadEmbeddedPlatformPackInventory(runningVersion)
	if err != nil {
		return nil, fmt.Errorf("load embedded platform pack inventory: %w", err)
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
			qualified, qualifyErr := QualifySystemNodeHandlerRuleRefsForEvent(node, eventType, handler)
			if qualifyErr != nil {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s: %v", ErrInvalidField, nodeID, strings.TrimSpace(eventType), qualifyErr))
				continue
			}
			handler = qualified
			for _, rule := range HandlerRuleEntries(handler) {
				ref, ok := rule.DeclarationIdentity()
				if !ok {
					continue
				}
				key := ref.Key()
				owner := nodeID + ":" + strings.TrimSpace(eventType)
				if previous, exists := elementOwners[key]; exists {
					errs = append(errs, fmt.Errorf("%w: declaration identity %s is duplicated by %s and %s", ErrInvalidField, key, previous, owner))
				} else {
					elementOwners[key] = owner
				}
			}
			for _, site := range HandlerFanOutSites(handler) {
				ref, ok := site.Spec.DeclarationIdentity()
				if !ok {
					errs = append(errs, fmt.Errorf("%w: node %s handler %s %s requires canonical declaration identity", ErrInvalidField, nodeID, strings.TrimSpace(eventType), site.Source))
					continue
				}
				key := ref.Key()
				owner := nodeID + ":" + strings.TrimSpace(eventType) + ":" + site.Source
				if previous, exists := elementOwners[key]; exists {
					errs = append(errs, fmt.Errorf("%w: declaration identity %s is duplicated by %s and %s", ErrInvalidField, key, previous, owner))
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
			handler, _ = QualifySystemNodeHandlerRuleRefsForEvent(node, eventType, handler)
			eventType = strings.TrimSpace(eventType)
			if err := ValidateAccumulateHandlerIsolation(handler); err != nil {
				errs = append(errs, fmt.Errorf("%w: node %s handler %s: %v", ErrInvalidField, nodeID, eventType, err))
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
	for _, failure := range bundle.PrepareFanOutPlans() {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidField, failure.Error()))
	}
	for eventType, owners := range bundle.Semantics.EventOwners {
		if len(normalizeStrings(owners)) > 1 {
			errs = append(errs, fmt.Errorf("%w: event %s has multiple authoritative system node owners: %s", ErrMultipleAuthoritativeOwners, strings.TrimSpace(eventType), strings.Join(normalizeStrings(owners), ", ")))
		}
	}
	errs = append(errs, validateWorkflowSchemaRefinements(bundle)...)
	errs = append(errs, validateIntraPackageEventSchemaOwnership(bundle)...)
	errs = append(errs, validateEventBusinessKeys(bundle)...)
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

func validateEventBusinessKeys(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	var errs []error
	for _, record := range bundle.canonicalCurrentEventDeclarationRecords() {
		if strings.TrimSpace(record.entry.BusinessKeyField) == "" {
			continue
		}
		if _, _, err := bundle.compileCurrentEventDeclaration(
			record.flowPath,
			record.layer,
			record.sourceFile,
			record.localName,
			record.qualifiedName,
			record.entry,
			record.types,
		); err != nil {
			errs = append(errs, fmt.Errorf("%w: %v", ErrInvalidField, err))
		}
	}
	return errs
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
