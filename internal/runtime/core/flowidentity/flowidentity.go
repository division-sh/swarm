package flowidentity

import (
	"path"
	"strings"

	"github.com/google/uuid"
	"swarm/internal/runtime/semanticview"
)

type Identity struct {
	TemplateID   string
	ScopeKey     string
	InstanceID   string
	InstancePath string
	EntityID     string
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

func EntityID(instancePath string) string {
	ref := strings.Trim(strings.TrimSpace(instancePath), "/")
	if ref == "" {
		return ""
	}
	namespace := uuid.NewSHA1(uuid.NameSpaceOID, []byte("flow-instance-entity"))
	return uuid.NewSHA1(namespace, []byte(ref)).String()
}

func LogicalInstanceID(instancePath string) string {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if instancePath == "" {
		return ""
	}
	return strings.TrimSpace(path.Base(instancePath))
}

func Derive(source semanticview.Source, flowID, instanceID string) Identity {
	flowID = strings.TrimSpace(flowID)
	instanceID = strings.TrimSpace(instanceID)
	instancePath := InstancePath(source, flowID, instanceID)
	entityID := strings.TrimSpace(instanceID)
	if instancePath != "" {
		entityID = EntityID(instancePath)
	}
	return Identity{
		TemplateID:   flowID,
		ScopeKey:     ScopeKey(source, flowID),
		InstanceID:   instanceID,
		InstancePath: instancePath,
		EntityID:     entityID,
	}
}

func SemanticScopeFromInstancePath(instancePath string) string {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if instancePath == "" {
		return ""
	}
	idx := strings.LastIndex(instancePath, "/")
	if idx <= 0 {
		return ""
	}
	return strings.TrimSpace(instancePath[:idx])
}

func InstanceScopeKey(source semanticview.Source, workflowName, flowPath, fallbackFlowID string) string {
	if workflowName = strings.TrimSpace(workflowName); workflowName != "" {
		if scopeKey := strings.TrimSpace(ScopeKey(source, workflowName)); scopeKey != "" {
			return scopeKey
		}
	}
	if scopeKey := strings.TrimSpace(SemanticScopeFromInstancePath(flowPath)); scopeKey != "" {
		return scopeKey
	}
	return strings.TrimSpace(ScopeKey(source, fallbackFlowID))
}

func IsDescendant(scopeKey, instancePath string) bool {
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
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
	ownerScope = strings.TrimSpace(ownerScope)
	targetScope := strings.TrimSpace(SemanticScopeFromInstancePath(targetInstancePath))
	if ownerScope == "" || strings.TrimSpace(targetInstancePath) == "" {
		return true
	}
	return ownerScope == targetScope
}
