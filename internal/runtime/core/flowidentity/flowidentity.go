package flowidentity

import (
	"fmt"
	"path"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

var flowInstanceEntityNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("flow-instance-entity"))
var standingServiceNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("standing-flow-service"))
var standingRunNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("standing-flow-generation"))

type Instance struct {
	TemplateID     string
	ScopeKey       string
	InstanceID     string
	InstancePath   string
	EntityID       string
	ParentEntityID string
	ParentRoute    ParentRoute
	HasStoredPath  bool
}

type ParentRoute struct {
	FlowInstance string
	EntityID     string
	FlowID       string
}

func (r ParentRoute) Normalized() ParentRoute {
	return ParentRoute{
		FlowInstance: normalizeRef(r.FlowInstance),
		EntityID:     strings.TrimSpace(r.EntityID),
		FlowID:       strings.TrimSpace(r.FlowID),
	}
}

func (r ParentRoute) Empty() bool {
	r = r.Normalized()
	return r.FlowInstance == "" && r.EntityID == "" && r.FlowID == ""
}

func (r ParentRoute) Complete() bool {
	r = r.Normalized()
	return r.FlowInstance != "" && r.EntityID != "" && r.FlowID != ""
}

func ParentRouteFromMetadata(metadata map[string]any) ParentRoute {
	if len(metadata) == 0 {
		return ParentRoute{}
	}
	return ParentRoute{
		FlowID:       metadataString(metadata, "parent_flow_id"),
		FlowInstance: metadataString(metadata, "parent_flow_instance"),
		EntityID:     metadataString(metadata, "parent_entity_id"),
	}.Normalized()
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

type Persisted struct {
	Instance
	StorageRef string
}

type Route struct {
	ScopeKey     string
	InstanceID   string
	InstancePath string
}

func ScopeKey(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	if source != nil {
		if flowPath := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"); flowPath != "" {
			return flowPath
		}
	}
	return flowID
}

func InstancePath(source semanticview.Source, flowID, instanceID string) string {
	basePath := strings.Trim(strings.TrimSpace(ScopeKey(source, flowID)), "/")
	instanceID = strings.Trim(strings.TrimSpace(instanceID), "/")
	switch {
	case basePath == "":
		return instanceID
	case instanceID == "":
		return basePath
	default:
		return basePath + "/" + instanceID
	}
}

func routePath(scopeKey, instanceID string) string {
	scopeKey = normalizeRef(scopeKey)
	instanceID = strings.Trim(strings.TrimSpace(instanceID), "/")
	switch {
	case scopeKey == "":
		return instanceID
	case instanceID == "":
		return scopeKey
	default:
		return scopeKey + "/" + instanceID
	}
}

func normalizeRef(ref string) string {
	return strings.Trim(strings.TrimSpace(ref), "/")
}

func EntityID(ref string) string {
	ref = normalizeRef(ref)
	if ref == "" {
		return ""
	}
	if parsed, err := uuid.Parse(ref); err == nil {
		return parsed.String()
	}
	return uuid.NewSHA1(flowInstanceEntityNamespace, []byte(ref)).String()
}

func LookupKeys(ref string) []string {
	ref = normalizeRef(ref)
	if ref == "" {
		return nil
	}
	keys := make([]string, 0, 2)
	if parsed, err := uuid.Parse(ref); err == nil {
		keys = append(keys, parsed.String())
	}
	if entityID := EntityID(ref); entityID != "" && !contains(keys, entityID) {
		keys = append(keys, entityID)
	}
	return keys
}

func LogicalInstanceID(instancePath string) string {
	instancePath = normalizeRef(instancePath)
	if instancePath == "" {
		return ""
	}
	return strings.TrimSpace(path.Base(instancePath))
}

func Derive(source semanticview.Source, flowID, instanceID string) Instance {
	scopeKey := normalizeRef(ScopeKey(source, flowID))
	instancePath := normalizeRef(InstancePath(source, flowID, instanceID))
	instanceID = strings.TrimSpace(instanceID)
	entityID := strings.TrimSpace(instanceID)
	if instancePath != "" {
		entityID = EntityID(instancePath)
	}
	return Instance{
		TemplateID:    strings.TrimSpace(flowID),
		ScopeKey:      scopeKey,
		InstanceID:    instanceID,
		InstancePath:  instancePath,
		EntityID:      entityID,
		HasStoredPath: instancePath != "",
	}
}

func StandingServiceID(packageKey, flowID string) string {
	material := strings.Join([]string{
		strings.TrimSpace(packageKey),
		strings.TrimSpace(flowID),
	}, "\x00")
	return uuid.NewSHA1(standingServiceNamespace, []byte(material)).String()
}

func StandingForService(source semanticview.Source, flowID, serviceID string) Instance {
	return Derive(source, strings.TrimSpace(flowID), strings.TrimSpace(serviceID))
}

func StandingGenerationRunID(serviceID string, generation int64) string {
	material := strings.Join([]string{
		strings.TrimSpace(serviceID),
		fmt.Sprintf("%d", generation),
	}, "\x00")
	return uuid.NewSHA1(standingRunNamespace, []byte(material)).String()
}

func DeriveRoute(scopeKey, instanceID string) Route {
	return StoredRoute(scopeKey, instanceID, "")
}

func StoredRoute(scopeKey, instanceID, instancePath string) Route {
	scopeKey = normalizeRef(scopeKey)
	instancePath = normalizeRef(instancePath)
	instanceID = strings.TrimSpace(instanceID)
	if scopeKey == "" && instancePath != "" {
		scopeKey = SemanticScope(instancePath)
	}
	if instanceID == "" && instancePath != "" {
		instanceID = LogicalInstanceID(instancePath)
	}
	if instancePath == "" {
		instancePath = routePath(scopeKey, instanceID)
	}
	return Route{
		ScopeKey:     scopeKey,
		InstanceID:   instanceID,
		InstancePath: instancePath,
	}
}

func (i Instance) Route() Route {
	return StoredRoute(i.ScopeKey, i.InstanceID, i.InstancePath)
}

func (r Route) Valid() bool {
	return strings.TrimSpace(r.ScopeKey) != "" &&
		strings.TrimSpace(r.InstanceID) != "" &&
		strings.TrimSpace(r.InstancePath) != ""
}

func SemanticScopeFromInstancePath(instancePath string) string {
	instancePath = normalizeRef(instancePath)
	if instancePath == "" {
		return ""
	}
	idx := strings.LastIndex(instancePath, "/")
	if idx <= 0 {
		return ""
	}
	return strings.TrimSpace(instancePath[:idx])
}

func SemanticScope(instancePath string) string {
	instancePath = normalizeRef(instancePath)
	if instancePath == "" {
		return ""
	}
	if scopeKey := SemanticScopeFromInstancePath(instancePath); scopeKey != "" {
		return scopeKey
	}
	return instancePath
}

func SemanticScopeFromFlowInstanceRef(flowInstance string) string {
	flowInstance = normalizeRef(flowInstance)
	if flowInstance == "" {
		return ""
	}
	if scopeKey := SemanticScopeFromInstancePath(flowInstance); scopeKey != "" {
		return scopeKey
	}
	if _, err := uuid.Parse(flowInstance); err == nil {
		return ""
	}
	return flowInstance
}

func Stored(
	source semanticview.Source,
	workflowName,
	materializedPath,
	instanceID,
	entityID,
	parentEntityID string,
) Instance {
	workflowName = strings.TrimSpace(workflowName)
	materializedPath = normalizeRef(materializedPath)
	instancePath := materializedPath
	if instancePath == "" {
		instancePath = normalizeRef(ScopeKey(source, workflowName))
	}
	if strings.TrimSpace(instanceID) == "" && materializedPath != "" {
		instanceID = LogicalInstanceID(materializedPath)
	}
	if strings.TrimSpace(entityID) == "" && instancePath != "" {
		entityID = EntityID(instancePath)
	}
	return Instance{
		TemplateID:     workflowName,
		ScopeKey:       normalizeRef(storedScopeKey(source, workflowName, instancePath)),
		InstanceID:     strings.TrimSpace(instanceID),
		InstancePath:   instancePath,
		EntityID:       strings.TrimSpace(entityID),
		ParentEntityID: strings.TrimSpace(parentEntityID),
		ParentRoute: ParentRoute{
			EntityID: strings.TrimSpace(parentEntityID),
		},
		HasStoredPath: materializedPath != "",
	}
}

func StoredPersisted(
	source semanticview.Source,
	workflowName,
	storageRef,
	instancePath,
	instanceID,
	entityID,
	parentEntityID string,
) (Persisted, error) {
	instance := Stored(source, workflowName, instancePath, instanceID, entityID, parentEntityID)
	storageRef = normalizeRef(storageRef)
	if instance.InstancePath != "" {
		if logical := LogicalInstanceID(instance.InstancePath); logical != "" && SemanticScopeFromInstancePath(instance.InstancePath) != "" {
			if strings.TrimSpace(instance.InstanceID) != "" && strings.TrimSpace(instance.InstanceID) != logical {
				return Persisted{}, fmt.Errorf("flow identity instance_id %q disagrees with flow_instance_path %q", instance.InstanceID, instance.InstancePath)
			}
		}
	}
	if storageRef == "" {
		switch {
		case instance.HasStoredPath && instance.InstancePath != "":
			storageRef = instance.InstancePath
		case strings.TrimSpace(entityID) != "":
			storageRef = strings.TrimSpace(entityID)
		default:
			storageRef = strings.TrimSpace(instance.InstanceID)
		}
	}
	return Persisted{
		Instance:   instance,
		StorageRef: storageRef,
	}, nil
}

func (p Persisted) RowID() string {
	return EntityID(p.StorageRef)
}

func (p Persisted) LookupKeys() []string {
	return LookupKeys(p.StorageRef)
}

func IsDescendant(scopeKey, instancePath string) bool {
	scopeKey = normalizeRef(scopeKey)
	instancePath = normalizeRef(instancePath)
	if scopeKey == "" || instancePath == "" || instancePath == scopeKey {
		return false
	}
	if !strings.HasPrefix(instancePath, scopeKey+"/") {
		return false
	}
	remainder := strings.Trim(strings.TrimPrefix(instancePath, scopeKey+"/"), "/")
	if remainder == "" {
		return false
	}
	return strings.Contains(remainder, "/")
}

func OwnedByFlow(source semanticview.Source, ownerFlowID, targetInstancePath string) bool {
	ownerScope := strings.TrimSpace(ScopeKey(source, ownerFlowID))
	return OwnedByScope(ownerScope, targetInstancePath)
}

func OwnedByScope(ownerScope, targetInstancePath string) bool {
	ownerScope = normalizeRef(ownerScope)
	targetScope := SemanticScope(targetInstancePath)
	if ownerScope == "" || strings.TrimSpace(targetInstancePath) == "" {
		return true
	}
	return ownerScope == targetScope
}

func storedScopeKey(source semanticview.Source, workflowName, instancePath string) string {
	instancePath = normalizeRef(instancePath)
	if instancePath != "" {
		return SemanticScope(instancePath)
	}
	return normalizeRef(ScopeKey(source, workflowName))
}

func contains(items []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}
