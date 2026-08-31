package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

type FanOutEffectiveSemantics struct {
	PlanRef          FanOutPlanRef
	ItemsFrom        string
	ItemsPath        paths.Path
	CollectionType   CatalogTypeReference
	ItemType         ResolvedCatalogType
	ItemAlias        string
	Identity         string
	IdentityDerived  bool
	MaxItems         int
	AuthoredMaxItems int
	MaxItemsSet      bool
}

type FanOutPlanRef struct {
	BundleHash     string           `json:"bundle_hash"`
	ElementRef     FanOutElementRef `json:"element_ref"`
	SemanticDigest string           `json:"semantic_digest"`
}

type FanOutElementRef struct {
	PackageKey string `json:"package_key"`
	ElementID  string `json:"element_id"`
}

func FanOutElementRefFrom(ref contractelementidentity.ContractElementRef) FanOutElementRef {
	if !ref.Valid() {
		return FanOutElementRef{}
	}
	return FanOutElementRef{PackageKey: ref.PackageKey().String(), ElementID: ref.ElementID().String()}
}

func (r FanOutElementRef) ContractElementRef() (contractelementidentity.ContractElementRef, error) {
	return contractelementidentity.ParseContractElementRef(strings.TrimSpace(r.PackageKey), strings.TrimSpace(r.ElementID))
}

type FanOutCompiledPlan struct {
	Site              FanOutSiteRef        `json:"site"`
	Ref               FanOutPlanRef        `json:"ref"`
	ItemsFrom         string               `json:"items_from"`
	ItemsPath         paths.Path           `json:"items_path"`
	CollectionType    CatalogTypeReference `json:"collection_type"`
	ItemType          ResolvedCatalogType  `json:"item_type"`
	ItemAlias         string               `json:"item_alias"`
	Identity          string               `json:"identity"`
	IdentityDerived   bool                 `json:"identity_derived"`
	MaxItems          int                  `json:"max_items"`
	AuthoredMaxItems  int                  `json:"authored_max_items"`
	MaxItemsSet       bool                 `json:"max_items_set"`
	SourceAfterWrites bool                 `json:"source_after_writes"`
	Writes            []WorkflowDataWrite  `json:"-"`
	Emit              EmitSpec             `json:"emit"`
}

type FanOutSiteKind string

const (
	FanOutSiteHandler    FanOutSiteKind = "handler"
	FanOutSiteRule       FanOutSiteKind = "rule"
	FanOutSiteOnComplete FanOutSiteKind = "on_complete"
)

type FanOutSiteRef struct {
	Node      runtimeidentity.ExecutableNode `json:"node"`
	EventType string                         `json:"event_type"`
	Kind      FanOutSiteKind                 `json:"kind"`
	Index     int                            `json:"index"`
}

func NewFanOutSiteRef(node runtimeidentity.ExecutableNode, eventType string, kind FanOutSiteKind, index int) (FanOutSiteRef, error) {
	ref := FanOutSiteRef{Node: node, EventType: strings.TrimSpace(eventType), Kind: kind, Index: index}
	if err := ref.Validate(); err != nil {
		return FanOutSiteRef{}, err
	}
	return ref, nil
}

func (r FanOutSiteRef) Validate() error {
	if !r.Node.Valid() || strings.TrimSpace(r.EventType) == "" {
		return fmt.Errorf("fan_out site requires exact executable node and handler event")
	}
	switch r.Kind {
	case FanOutSiteHandler:
		if r.Index != -1 {
			return fmt.Errorf("handler fan_out site index must be -1")
		}
	case FanOutSiteRule, FanOutSiteOnComplete:
		if r.Index < 0 {
			return fmt.Errorf("%s fan_out site index must be non-negative", r.Kind)
		}
	default:
		return fmt.Errorf("fan_out site kind %q is unsupported", r.Kind)
	}
	return nil
}

type WorkflowFanOutSite struct {
	Source string
	Kind   FanOutSiteKind
	Index  int
	Spec   *FanOutSpec
	Writes []WorkflowDataWrite
}

type FanOutPlanFailure struct {
	Node      runtimeidentity.ExecutableNode
	EventType string
	Source    string
	Detail    string
}

func (f FanOutPlanFailure) Error() string {
	return fmt.Sprintf("node %s handler %s %s: %s", f.Node.Key(), strings.TrimSpace(f.EventType), strings.TrimSpace(f.Source), strings.TrimSpace(f.Detail))
}

func (b *WorkflowContractBundle) CompileFanOutPlan(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler, site WorkflowFanOutSite) (FanOutCompiledPlan, error) {
	if site.Spec == nil {
		return FanOutCompiledPlan{}, fmt.Errorf("fan_out compiled plan requires an authored site")
	}
	spec := *site.Spec
	siteRef, err := NewFanOutSiteRef(node, eventType, site.Kind, site.Index)
	if err != nil {
		return FanOutCompiledPlan{}, err
	}
	effective, err := b.ResolveFanOutEffectiveSemantics(node, eventType, spec)
	if err != nil {
		return FanOutCompiledPlan{}, err
	}
	ref, ok := spec.ContractElementRef()
	if !ok {
		return FanOutCompiledPlan{}, fmt.Errorf("fan_out requires canonical package-qualified element_id; run `swarm mint-element-ids --contracts <path>`")
	}
	bundleHash, err := BundleHash(b)
	if err != nil {
		return FanOutCompiledPlan{}, fmt.Errorf("fan_out bundle identity: %w", err)
	}
	emit, err := b.LowerEmitSpecFields(EmitFieldLoweringContext{
		Node: node, TriggerEventType: strings.TrimSpace(eventType), Site: "fan_out.emit",
	}, spec.Emit)
	if err != nil {
		return FanOutCompiledPlan{}, err
	}
	plan := FanOutCompiledPlan{
		Site:      siteRef,
		ItemsFrom: effective.ItemsFrom, ItemsPath: effective.ItemsPath,
		CollectionType: effective.CollectionType, ItemType: effective.ItemType,
		ItemAlias: effective.ItemAlias, Identity: effective.Identity,
		IdentityDerived: effective.IdentityDerived, MaxItems: effective.MaxItems,
		AuthoredMaxItems: effective.AuthoredMaxItems, MaxItemsSet: effective.MaxItemsSet,
		SourceAfterWrites: b.fanOutSourceAfterWrites(node, handler, spec, effective.ItemsPath),
		Writes:            cloneFanOutWrites(site.Writes),
		Emit:              emit,
	}
	digest, err := canonicaljson.Hash(struct {
		ElementRef        FanOutElementRef     `json:"element_ref"`
		ItemsFrom         string               `json:"items_from"`
		CollectionType    CatalogTypeReference `json:"collection_type"`
		ItemType          ResolvedCatalogType  `json:"item_type"`
		ItemAlias         string               `json:"item_alias"`
		Identity          string               `json:"identity"`
		IdentityDerived   bool                 `json:"identity_derived"`
		MaxItems          int                  `json:"max_items"`
		SourceAfterWrites bool                 `json:"source_after_writes"`
		Emit              EmitSpec             `json:"emit"`
	}{
		ElementRef: FanOutElementRefFrom(ref), ItemsFrom: plan.ItemsFrom,
		CollectionType: plan.CollectionType, ItemType: plan.ItemType,
		ItemAlias: plan.ItemAlias, Identity: plan.Identity, IdentityDerived: plan.IdentityDerived,
		MaxItems: plan.MaxItems, SourceAfterWrites: plan.SourceAfterWrites, Emit: plan.Emit,
	})
	if err != nil {
		return FanOutCompiledPlan{}, fmt.Errorf("fan_out semantic plan digest: %w", err)
	}
	plan.Ref = FanOutPlanRef{BundleHash: bundleHash, ElementRef: FanOutElementRefFrom(ref), SemanticDigest: digest}
	return plan, nil
}

// CompileFanOutHandlerPlans admits every fan-out site in one handler into the
// bundle-owned immutable runtime projection. It is called by strict loading;
// test fixtures may call it after constructing an in-memory bundle.
func (b *WorkflowContractBundle) CompileFanOutHandlerPlans(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler) error {
	if b == nil {
		return fmt.Errorf("fan_out plans require a loaded contract bundle")
	}
	qualified, err := QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		return err
	}
	plans := make([]FanOutCompiledPlan, 0, 5)
	for _, site := range HandlerFanOutSites(qualified) {
		plan, err := b.CompileFanOutPlan(node, eventType, qualified, site)
		if err != nil {
			return fmt.Errorf("compile %s: %w", site.Source, err)
		}
		plans = append(plans, plan)
	}
	for _, plan := range plans {
		if err := b.storeFanOutCompiledPlan(plan); err != nil {
			return err
		}
	}
	b.fanOutPlansPrepared = true
	return nil
}

// PrepareFanOutPlans compiles every authored site once at semantic-source
// admission. Invalid sites remain typed compiler failures; downstream
// consumers never receive a partial raw-spec fallback.
func (b *WorkflowContractBundle) PrepareFanOutPlans() []FanOutPlanFailure {
	if b == nil {
		return nil
	}
	b.resetFanOutPlans()
	for _, record := range b.ScopedNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			b.fanOutPlanFailures = append(b.fanOutPlanFailures, FanOutPlanFailure{Detail: err.Error()})
			continue
		}
		for eventType, handler := range record.Entry.EventHandlers {
			qualified, err := QualifySystemNodeHandlerRuleRefs(node, handler)
			if err != nil {
				b.fanOutPlanFailures = append(b.fanOutPlanFailures, FanOutPlanFailure{Node: node, EventType: eventType, Source: "handler", Detail: err.Error()})
				continue
			}
			for _, site := range HandlerFanOutSites(qualified) {
				plan, err := b.CompileFanOutPlan(node, eventType, qualified, site)
				if err != nil {
					b.fanOutPlanFailures = append(b.fanOutPlanFailures, FanOutPlanFailure{Node: node, EventType: eventType, Source: site.Source, Detail: err.Error()})
					continue
				}
				if err := b.storeFanOutCompiledPlan(plan); err != nil {
					b.fanOutPlanFailures = append(b.fanOutPlanFailures, FanOutPlanFailure{Node: node, EventType: eventType, Source: site.Source, Detail: err.Error()})
				}
			}
		}
	}
	b.fanOutPlansPrepared = true
	return b.FanOutPlanFailures()
}

func (b *WorkflowContractBundle) storeFanOutCompiledPlan(plan FanOutCompiledPlan) error {
	if b.fanOutPlansBySite == nil {
		b.fanOutPlansBySite = make(map[FanOutSiteRef]FanOutCompiledPlan)
	}
	if b.fanOutPlansByElement == nil {
		b.fanOutPlansByElement = make(map[FanOutElementRef]FanOutCompiledPlan)
	}
	if prior, exists := b.fanOutPlansByElement[plan.Ref.ElementRef]; exists && prior.Site != plan.Site {
		return fmt.Errorf("fan_out element %s/%s has multiple compiled sites", plan.Ref.ElementRef.PackageKey, plan.Ref.ElementRef.ElementID)
	}
	b.fanOutPlansBySite[plan.Site] = cloneFanOutCompiledPlan(plan)
	b.fanOutPlansByElement[plan.Ref.ElementRef] = cloneFanOutCompiledPlan(plan)
	return nil
}

func (b *WorkflowContractBundle) FanOutPlansArePrepared() bool {
	return b != nil && b.fanOutPlansPrepared
}

func (b *WorkflowContractBundle) FanOutPlanFailures() []FanOutPlanFailure {
	if b == nil {
		return nil
	}
	return append([]FanOutPlanFailure(nil), b.fanOutPlanFailures...)
}

func (b *WorkflowContractBundle) FanOutPlanForSite(site FanOutSiteRef) (FanOutCompiledPlan, bool) {
	if b == nil || site.Validate() != nil {
		return FanOutCompiledPlan{}, false
	}
	plan, ok := b.fanOutPlansBySite[site]
	return cloneFanOutCompiledPlan(plan), ok
}

func (b *WorkflowContractBundle) FanOutPlanForElement(ref FanOutElementRef) (FanOutCompiledPlan, bool) {
	if b == nil {
		return FanOutCompiledPlan{}, false
	}
	plan, ok := b.fanOutPlansByElement[ref]
	return cloneFanOutCompiledPlan(plan), ok
}

func (b *WorkflowContractBundle) FanOutPlans() []FanOutCompiledPlan {
	if b == nil {
		return nil
	}
	out := make([]FanOutCompiledPlan, 0, len(b.fanOutPlansBySite))
	for _, plan := range b.fanOutPlansBySite {
		out = append(out, cloneFanOutCompiledPlan(plan))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Site.Node.Key() != out[j].Site.Node.Key() {
			return out[i].Site.Node.Key() < out[j].Site.Node.Key()
		}
		if out[i].Site.EventType != out[j].Site.EventType {
			return out[i].Site.EventType < out[j].Site.EventType
		}
		if out[i].Site.Kind != out[j].Site.Kind {
			return out[i].Site.Kind < out[j].Site.Kind
		}
		return out[i].Site.Index < out[j].Site.Index
	})
	return out
}

func (b *WorkflowContractBundle) FanOutPlansForHandler(node runtimeidentity.ExecutableNode, eventType string) []FanOutCompiledPlan {
	eventType = strings.TrimSpace(eventType)
	all := b.FanOutPlans()
	out := make([]FanOutCompiledPlan, 0, 3)
	for _, plan := range all {
		if plan.Site.Node.Equal(node) && plan.Site.EventType == eventType {
			out = append(out, plan)
		}
	}
	return out
}

func (b *WorkflowContractBundle) resetFanOutPlans() {
	b.fanOutPlansBySite = nil
	b.fanOutPlansByElement = nil
	b.fanOutPlanFailures = nil
	b.fanOutPlansPrepared = false
}

func cloneFanOutCompiledPlan(plan FanOutCompiledPlan) FanOutCompiledPlan {
	plan.ItemsPath.Segments = append([]string(nil), plan.ItemsPath.Segments...)
	plan.Writes = cloneFanOutWrites(plan.Writes)
	plan.Emit = cloneEmitSpec(plan.Emit)
	return plan
}

func cloneFanOutWrites(writes []WorkflowDataWrite) []WorkflowDataWrite {
	out := append([]WorkflowDataWrite(nil), writes...)
	for i := range out {
		out[i].SourcePath.Segments = append([]string(nil), out[i].SourcePath.Segments...)
		out[i].TargetPath.Segments = append([]string(nil), out[i].TargetPath.Segments...)
		out[i].Value.RefPath.Segments = append([]string(nil), out[i].Value.RefPath.Segments...)
		out[i].Key.RefPath.Segments = append([]string(nil), out[i].Key.RefPath.Segments...)
		out[i].Index.RefPath.Segments = append([]string(nil), out[i].Index.RefPath.Segments...)
	}
	return out
}

func HandlerFanOutSites(handler SystemNodeEventHandler) []WorkflowFanOutSite {
	out := make([]WorkflowFanOutSite, 0, 5)
	topLevelWrites := append([]WorkflowDataWrite(nil), handler.DataAccumulation.Writes...)
	add := func(source string, kind FanOutSiteKind, index int, spec *FanOutSpec, writes []WorkflowDataWrite) {
		if spec != nil {
			out = append(out, WorkflowFanOutSite{Source: strings.TrimSpace(source), Kind: kind, Index: index, Spec: spec, Writes: append([]WorkflowDataWrite(nil), writes...)})
		}
	}
	add("handler.fan_out", FanOutSiteHandler, -1, handler.FanOut, topLevelWrites)
	for idx := range handler.Rules {
		writes := append([]WorkflowDataWrite(nil), handler.Rules[idx].DataAccumulation.Writes...)
		writes = append(writes, topLevelWrites...)
		add(indexedFanOutSiteSource("handler.rules", idx, handler.Rules[idx].ID), FanOutSiteRule, idx, handler.Rules[idx].FanOut, writes)
	}
	for idx := range handler.OnComplete {
		writes := append([]WorkflowDataWrite(nil), handler.OnComplete[idx].DataAccumulation.Writes...)
		writes = append(writes, topLevelWrites...)
		add(indexedFanOutSiteSource("handler.on_complete", idx, handler.OnComplete[idx].ID), FanOutSiteOnComplete, idx, handler.OnComplete[idx].FanOut, writes)
	}
	return out
}

func (b *WorkflowContractBundle) fanOutSourceAfterWrites(node runtimeidentity.ExecutableNode, handler SystemNodeEventHandler, spec FanOutSpec, source paths.Path) bool {
	if source.Root != paths.RootEntity || len(source.Segments) == 0 {
		return false
	}
	sourceField := strings.TrimSpace(source.Segments[0])
	writes := fanOutSiteWrites(handler, spec)
	for _, write := range writes {
		target := paths.Parse(write.Target())
		if target.Root == paths.RootEntity && len(target.Segments) > 0 && strings.TrimSpace(target.Segments[0]) == sourceField {
			return true
		}
		if !target.HasExplicitRoot() && len(target.Segments) > 0 && strings.TrimSpace(target.Segments[0]) == sourceField {
			return true
		}
	}
	if handler.Accumulate == nil || strings.TrimSpace(handler.Accumulate.Into) == "" {
		return false
	}
	primary, err := b.ResolveFlowPrimaryEntity(node.FlowID())
	if err != nil {
		return false
	}
	decl, ok := primary.Contract.Fields[sourceField]
	if !ok {
		return false
	}
	want := strings.TrimSpace(node.NodeID()) + "." + strings.TrimSpace(handler.Accumulate.Into)
	return strings.TrimSpace(decl.MaterializeFrom) == want
}

func fanOutSiteWrites(handler SystemNodeEventHandler, spec FanOutSpec) []WorkflowDataWrite {
	want, ok := spec.ContractElementRef()
	if !ok {
		return nil
	}
	for _, site := range HandlerFanOutSites(handler) {
		got, present := site.Spec.ContractElementRef()
		if present && got == want {
			return append([]WorkflowDataWrite(nil), site.Writes...)
		}
	}
	return nil
}

func indexedFanOutSiteSource(scope string, index int, id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return fmt.Sprintf("%s[%s].fan_out", scope, id)
	}
	return fmt.Sprintf("%s[%d].fan_out", scope, index)
}

func (b *WorkflowContractBundle) ResolveFanOutEffectiveSemantics(node runtimeidentity.ExecutableNode, eventType string, spec FanOutSpec) (FanOutEffectiveSemantics, error) {
	if !node.Valid() {
		return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out requires an exact executable node owner")
	}
	itemsPath, err := ValidateFanOutItemsSource(spec)
	if err != nil {
		return FanOutEffectiveSemantics{}, err
	}
	if err := ValidateFanOutAlias(spec.As); err != nil {
		return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.%w", err)
	}
	if err := ValidateFanOutMaxItems(spec); err != nil {
		return FanOutEffectiveSemantics{}, err
	}

	collectionType, err := b.resolveWorkflowCollectionType(node, eventType, itemsPath)
	if err != nil {
		return FanOutEffectiveSemantics{}, err
	}
	itemType, err := resolveWorkflowCollectionItemType(collectionType, itemsPath.Segments[1:])
	if err != nil {
		return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.items_from %q %w", strings.TrimSpace(spec.ItemsFrom), err)
	}

	identity := strings.TrimSpace(spec.Identity)
	derived := false
	if identity == "" {
		if !fanOutScalarItemKind(itemType.Kind) {
			kind := strings.TrimSpace(string(itemType.Kind))
			if kind == "" {
				kind = string(CatalogTypeDynamic)
			}
			return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.identity is required because items_from %q has non-scalar or unresolved item type %s", strings.TrimSpace(spec.ItemsFrom), kind)
		}
		identity = strings.TrimSpace(spec.As)
		derived = true
	}

	return FanOutEffectiveSemantics{
		ItemsFrom:        strings.TrimSpace(spec.ItemsFrom),
		ItemsPath:        itemsPath,
		CollectionType:   collectionType,
		ItemType:         itemType,
		ItemAlias:        strings.TrimSpace(spec.As),
		Identity:         identity,
		IdentityDerived:  derived,
		MaxItems:         EffectiveFanOutMaxItems(spec),
		AuthoredMaxItems: spec.MaxItems,
		MaxItemsSet:      spec.MaxItemsSet,
	}, nil
}

func (b *WorkflowContractBundle) resolveWorkflowCollectionType(node runtimeidentity.ExecutableNode, eventType string, path paths.Path) (CatalogTypeReference, error) {
	if b == nil {
		return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from requires a loaded contract bundle")
	}
	flowID := node.FlowID()
	field := strings.TrimSpace(path.Segments[0])
	switch path.Root {
	case paths.RootPayload:
		ref, ok := ResolveExecutableNodeEventFieldType(b, node, eventType, field)
		if !ok {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references undeclared payload field %s for event %s at executable node %s", field, defaultFanOutEventLabel(eventType), node.Key())
		}
		return ref, nil
	case paths.RootEntity:
		primary, err := b.ResolveFlowPrimaryEntity(flowID)
		if err != nil {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references entity but flow %s has no primary entity contract: %w", defaultPrimaryEntityFlowLabel(flowID), err)
		}
		decl, ok := primary.Contract.Fields[field]
		if !ok {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references undeclared entity field %s", field)
		}
		return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(primary.Types)}, nil
	default:
		return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from must use payload or entity scope")
	}
}

// WorkflowCollectionItemResolution is the immutable type authority for one
// executable handler collection source. Boot verification and runtime both
// consume this result instead of reconstructing intermediate ancestry.
type WorkflowCollectionItemResolution struct {
	Source   string
	Origin   string
	ItemType ResolvedCatalogType
}

// ResolveHandlerCollectionItemType binds direct and handler-produced
// collection sources to one recursive catalog item type.
func (b *WorkflowContractBundle) ResolveHandlerCollectionItemType(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler, itemsFrom string) (WorkflowCollectionItemResolution, error) {
	authored := strings.TrimSpace(itemsFrom)
	if authored == "" {
		return WorkflowCollectionItemResolution{}, fmt.Errorf("collection source is required")
	}
	item, origin, err := b.resolveHandlerCollectionItemType(node, eventType, handler, paths.Parse(authored), map[string]struct{}{})
	if err != nil {
		return WorkflowCollectionItemResolution{}, err
	}
	if item.Kind == CatalogTypeDynamic {
		return WorkflowCollectionItemResolution{}, fmt.Errorf("collection source %q has dynamic item type", authored)
	}
	return WorkflowCollectionItemResolution{Source: authored, Origin: origin, ItemType: item}, nil
}

func (b *WorkflowContractBundle) resolveHandlerCollectionItemType(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler, source paths.Path, seen map[string]struct{}) (ResolvedCatalogType, string, error) {
	if source.IsZero() {
		return ResolvedCatalogType{}, "", fmt.Errorf("collection source is required")
	}
	key := source.String()
	if _, duplicate := seen[key]; duplicate {
		return ResolvedCatalogType{}, "", fmt.Errorf("collection source %q has cyclic handler ancestry", key)
	}
	seen[key] = struct{}{}

	if handler.Query != nil && sameWorkflowCollectionPath(source, handler.Query.StorePath, handler.Query.StoreAs) {
		if handler.Query.Count || strings.TrimSpace(handler.Query.GroupBy) != "" {
			return ResolvedCatalogType{}, "", fmt.Errorf("query output %q is an aggregate, not a collection", key)
		}
		item, origin, err := b.resolveQueryCollectionItemType(node, eventType, handler, *handler.Query, seen)
		if err != nil {
			return ResolvedCatalogType{}, "", err
		}
		if len(handler.Query.Select) > 0 {
			item, err = projectQueryCollectionItemType(item, handler.Query.Select)
			if err != nil {
				return ResolvedCatalogType{}, "", err
			}
		}
		return item, origin, nil
	}
	if handler.Filter != nil && sameWorkflowCollectionPath(source, handler.Filter.StorePath, handler.Filter.StoreAs) {
		upstream := firstWorkflowCollectionSource(handler.Filter.ItemsFrom, handler.Filter.Source)
		return b.resolveHandlerCollectionItemType(node, eventType, handler, paths.Parse(upstream), seen)
	}

	if source.Root != paths.RootPayload && source.Root != paths.RootEntity {
		return ResolvedCatalogType{}, "", fmt.Errorf("collection source %q is neither an exact payload/entity collection nor a supported query/filter intermediate", key)
	}
	collection, err := b.resolveWorkflowCollectionType(node, eventType, source)
	if err != nil {
		return ResolvedCatalogType{}, "", err
	}
	item, err := resolveWorkflowCollectionItemType(collection, source.Segments[1:])
	if err != nil {
		return ResolvedCatalogType{}, "", err
	}
	return item, source.String(), nil
}

func (b *WorkflowContractBundle) resolveQueryCollectionItemType(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler, query QuerySpec, seen map[string]struct{}) (ResolvedCatalogType, string, error) {
	if source := strings.TrimSpace(query.Source); source != "" {
		path := query.SourcePath
		if path.IsZero() {
			path = paths.Parse(source)
		}
		return b.resolveHandlerCollectionItemType(node, eventType, handler, path, seen)
	}
	entities := strings.TrimSpace(query.Entities)
	entitiesPath := query.EntitiesPath
	if entitiesPath.IsZero() {
		entitiesPath = paths.Parse(entities)
	}
	if entitiesPath.HasExplicitRoot() {
		return b.resolveHandlerCollectionItemType(node, eventType, handler, entitiesPath, seen)
	}
	if entities == "" {
		return ResolvedCatalogType{}, "", fmt.Errorf("query collection source is required")
	}
	primary, err := b.ResolveFlowPrimaryEntity(node.FlowID())
	if err != nil {
		return ResolvedCatalogType{}, "", fmt.Errorf("query entities %q has no exact entity contract: %w", entities, err)
	}
	if entities != primary.EntityType {
		return ResolvedCatalogType{}, "", fmt.Errorf("query entities %q does not match flow %s primary entity %q", entities, defaultPrimaryEntityFlowLabel(node.FlowID()), primary.EntityType)
	}
	fields := make([]ResolvedCatalogField, 0, len(primary.Contract.Fields))
	for _, name := range sortedEntityFieldKeys(primary.Contract.Fields) {
		decl := primary.Contract.Fields[name]
		resolved, resolveErr := (CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(primary.Types)}).Resolve()
		if resolveErr != nil {
			return ResolvedCatalogType{}, "", fmt.Errorf("query entities %q field %s: %w", entities, name, resolveErr)
		}
		fields = append(fields, ResolvedCatalogField{Name: name, Type: resolved})
	}
	return ResolvedCatalogType{Kind: CatalogTypeObject, Name: primary.EntityType, Fields: fields}, "entities." + primary.EntityType, nil
}

func projectQueryCollectionItemType(item ResolvedCatalogType, selected []string) (ResolvedCatalogType, error) {
	if item.Kind != CatalogTypeObject {
		return ResolvedCatalogType{}, fmt.Errorf("query select requires an object collection item, got %s", item.Kind)
	}
	fields := make([]ResolvedCatalogField, 0, len(selected))
	seen := map[string]struct{}{}
	for _, raw := range selected {
		name := strings.TrimSpace(raw)
		if name == "" {
			return ResolvedCatalogType{}, fmt.Errorf("query select contains an empty field")
		}
		if _, duplicate := seen[name]; duplicate {
			return ResolvedCatalogType{}, fmt.Errorf("query select repeats field %s", name)
		}
		seen[name] = struct{}{}
		field, ok := item.Field(name)
		if !ok {
			return ResolvedCatalogType{}, fmt.Errorf("query select references undeclared item field %s", name)
		}
		fields = append(fields, field)
	}
	return ResolvedCatalogType{Kind: CatalogTypeObject, Name: item.Name + "QuerySelection", Fields: fields}, nil
}

func sameWorkflowCollectionPath(left, parsed paths.Path, authored string) bool {
	if parsed.IsZero() {
		parsed = paths.Parse(strings.TrimSpace(authored))
	}
	if left.Root != parsed.Root || len(left.Segments) != len(parsed.Segments) {
		return false
	}
	for index := range left.Segments {
		if left.Segments[index] != parsed.Segments[index] {
			return false
		}
	}
	return true
}

func firstWorkflowCollectionSource(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func defaultFanOutEventLabel(eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return "<unknown>"
	}
	return eventType
}

func resolveWorkflowCollectionItemType(ref CatalogTypeReference, nested []string) (ResolvedCatalogType, error) {
	resolved, err := ref.Resolve()
	if err == nil {
		for _, segment := range nested {
			if resolved.Kind != CatalogTypeObject {
				return ResolvedCatalogType{}, fmt.Errorf("collection path segment %q traverses non-object type %s", segment, resolved.Kind)
			}
			field, ok := resolved.Field(segment)
			if !ok {
				return ResolvedCatalogType{}, fmt.Errorf("collection path references undeclared field %s", segment)
			}
			resolved = field.Type.Clone()
		}
	}
	if err == nil && resolved.Kind == CatalogTypeList && resolved.Element != nil {
		return *resolved.Element, nil
	}
	raw := strings.TrimSpace(ref.Type)
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "array<") && strings.HasSuffix(lower, ">") {
		inner := strings.TrimSpace(raw[len("array<") : len(raw)-1])
		if inner == "" {
			return ResolvedCatalogType{}, fmt.Errorf("must reference a collection with a declared item type")
		}
		return ref.ResolveReference(inner)
	}
	if lower == "array" || strings.HasPrefix(lower, "array ") || strings.HasPrefix(lower, "array(") {
		return ResolvedCatalogType{}, fmt.Errorf("must declare an exact collection item type; untyped array is not executable item authority")
	}
	if err != nil {
		return ResolvedCatalogType{}, fmt.Errorf("must reference a collection field: %v", err)
	}
	return ResolvedCatalogType{}, fmt.Errorf("must reference a list/array collection field; field has type %q", raw)
}

func fanOutScalarItemKind(kind CatalogTypeKind) bool {
	switch kind {
	case CatalogTypeText, CatalogTypeInteger, CatalogTypeNumber, CatalogTypeBoolean:
		return true
	default:
		return false
	}
}
