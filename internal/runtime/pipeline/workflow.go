package pipeline

import (
	"path/filepath"
	"runtime"
	"strings"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type WorkflowStateID string

type WorkflowState struct {
	EntityID string                     `json:"entity_id,omitempty"`
	Stage    WorkflowStateID            `json:"stage"`
	Status   string                     `json:"status,omitempty"`
	Metadata map[string]any             `json:"metadata,omitempty"`
	Control  runtimeengine.StateControl `json:"-"`
}

const workflowStateBucketEntityProjection = "entity_projection"

func NormalizeWorkflowStateID(raw string) WorkflowStateID {
	return WorkflowStateID(strings.TrimSpace(raw))
}

func workflowStateBucketObject(instance WorkflowInstance, key string) (map[string]any, bool) {
	if instance.StateBuckets == nil {
		return nil, false
	}
	bucket, ok := instance.StateBuckets[strings.TrimSpace(key)]
	if !ok {
		return nil, false
	}
	out, ok := bucket.(map[string]any)
	return out, ok
}

func workflowMutableStateBucket(instance *WorkflowInstance, key string) map[string]any {
	if instance == nil {
		return map[string]any{}
	}
	if instance.StateBuckets == nil {
		instance.StateBuckets = map[string]any{}
	}
	key = strings.TrimSpace(key)
	bucket, _ := instance.StateBuckets[key].(map[string]any)
	if bucket == nil {
		bucket = map[string]any{}
		instance.StateBuckets[key] = bucket
	}
	return bucket
}

func workflowSetStateBucket(instance *WorkflowInstance, key string, value map[string]any) {
	if instance == nil {
		return
	}
	if instance.StateBuckets == nil {
		instance.StateBuckets = map[string]any{}
	}
	instance.StateBuckets[strings.TrimSpace(key)] = cloneMap(value)
}

func WorkflowRepoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}
