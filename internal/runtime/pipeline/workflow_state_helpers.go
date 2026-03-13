package pipeline

import (
	"strings"
	"time"
)

const (
	workflowStateBucketEntityProjection       = "entity_projection"
	workflowStateBucketValidationOrchestrator = "validation_state"
)

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

func workflowDeleteStateBucket(instance *WorkflowInstance, key string) {
	if instance == nil || instance.StateBuckets == nil {
		return
	}
	delete(instance.StateBuckets, strings.TrimSpace(key))
}

func workflowMutableMetadata(instance *WorkflowInstance) map[string]any {
	if instance == nil {
		return map[string]any{}
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	return instance.Metadata
}

func truthyMetadataFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "on":
			return true
		}
	}
	return asInt(v) > 0
}

func parseWorkflowTime(v any) time.Time {
	raw := strings.TrimSpace(asString(v))
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func workflowMetadataSnapshot(instance WorkflowInstance) map[string]any {
	return cloneStringAnyMap(instance.Metadata)
}

func workflowValidationProjectionBucket(instance WorkflowInstance) (map[string]any, bool) {
	return workflowStateBucketObject(instance, workflowStateBucketValidationOrchestrator)
}
