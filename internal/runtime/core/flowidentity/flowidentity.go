package flowidentity

import (
	"path"
	"strings"

	"github.com/google/uuid"
	"swarm/internal/runtime/semanticview"
)

var flowInstanceEntityNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("flow-instance-entity"))

type Identity struct {
	TemplateID   string
	ScopeKey     string
	InstanceID   string
	InstancePath string
	EntityID     string
}

type Coordinates struct {
	ScopeKey     string
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

func Derive(source semanticview.Source, flowID, instanceID string) Identity {
	flowID = strings.TrimSpace(flowID)
	instanceID = strings.TrimSpace(instanceID)
	coordinates := DerivedCoordinates(source, flowID, instanceID)
	instancePath := coordinates.InstancePath
	entityID := strings.TrimSpace(instanceID)
	if instancePath != "" {
		entityID = EntityID(instancePath)
	}
	return Identity{
		TemplateID:   flowID,
		ScopeKey:     coordinates.ScopeKey,
		InstanceID:   instanceID,
		InstancePath: instancePath,
		EntityID:     entityID,
	}
}

func DerivedCoordinates(source semanticview.Source, flowID, instanceID string) Coordinates {
	scopeKey := normalizeRef(ScopeKey(source, flowID))
	instancePath := normalizeRef(InstancePath(source, flowID, instanceID))
	return Coordinates{
		ScopeKey:     scopeKey,
		InstancePath: instancePath,
	}
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

func StoredCoordinates(source semanticview.Source, workflowName, materializedPath string) Coordinates {
	instancePath := normalizeRef(materializedPath)
	if instancePath == "" {
		instancePath = normalizeRef(ScopeKey(source, workflowName))
	}
	return Coordinates{
		ScopeKey:     normalizeRef(storedScopeKey(source, workflowName, instancePath)),
		InstancePath: instancePath,
	}
}

func StoredScopeKey(source semanticview.Source, workflowName, materializedPath string) string {
	return StoredCoordinates(source, workflowName, materializedPath).ScopeKey
}

func StoredInstancePath(source semanticview.Source, workflowName, materializedPath string) string {
	return StoredCoordinates(source, workflowName, materializedPath).InstancePath
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
