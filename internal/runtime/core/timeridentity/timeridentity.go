package timeridentity

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TriggerKind string

const (
	TriggerKindState TriggerKind = "state"
	TriggerKindEvent TriggerKind = "event"
	TriggerKindBoot  TriggerKind = "boot"
)

type Trigger struct {
	Kind TriggerKind
	Name string
}

type TimerHandleKind string

const (
	TimerHandleWorkflowTimer       TimerHandleKind = "workflow_timer"
	TimerHandleAccumulationTimeout TimerHandleKind = "accumulation_timeout"
	timerHandlePayloadKey                          = "timer_handle"
	accumulationTimeoutTaskPrefix                  = "accumulate_timeout:"
)

type TimerHandle struct {
	Kind    TimerHandleKind
	TimerID string
	Bucket  AccumulatorBucketRef
}

type AccumulatorBucketRef struct {
	NodeID    string
	EventType string
	Window    string
}

func ParseStartTrigger(raw string) (Trigger, error) {
	return parseTrigger(raw, true)
}

func ParseCancelTrigger(raw string) (Trigger, error) {
	return parseTrigger(raw, false)
}

func ParseDelayDuration(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if duration, err := time.ParseDuration(raw); err == nil && duration > 0 {
		return duration, true
	}
	if !strings.HasSuffix(raw, "d") {
		return 0, false
	}
	daysRaw := strings.TrimSpace(strings.TrimSuffix(raw, "d"))
	days, err := strconv.ParseInt(daysRaw, 10, 64)
	if err != nil || days <= 0 {
		return 0, false
	}
	const day = 24 * time.Hour
	const maxDuration = time.Duration(1<<63 - 1)
	if days > int64(maxDuration/day) {
		return 0, false
	}
	return time.Duration(days) * day, true
}

func parseTrigger(raw string, allowBoot bool) (Trigger, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Trigger{}, nil
	}
	if raw == string(TriggerKindBoot) {
		if !allowBoot {
			return Trigger{}, fmt.Errorf("boot is not valid here")
		}
		return Trigger{Kind: TriggerKindBoot}, nil
	}
	prefix, value, ok := strings.Cut(raw, ":")
	if !ok {
		return Trigger{}, fmt.Errorf("trigger %q must use state:<name>, event:<name>, or boot", raw)
	}
	prefix = strings.TrimSpace(prefix)
	value = strings.TrimSpace(value)
	if value == "" {
		return Trigger{}, fmt.Errorf("trigger %q is missing a target name", raw)
	}
	switch TriggerKind(prefix) {
	case TriggerKindState:
		return Trigger{Kind: TriggerKindState, Name: value}, nil
	case TriggerKindEvent:
		return Trigger{Kind: TriggerKindEvent, Name: value}, nil
	default:
		return Trigger{}, fmt.Errorf("trigger %q uses unsupported prefix %q", raw, prefix)
	}
}

func (t Trigger) Valid() bool {
	switch t.Kind {
	case TriggerKindState, TriggerKindEvent:
		return strings.TrimSpace(t.Name) != ""
	case TriggerKindBoot:
		return true
	default:
		return false
	}
}

func (t Trigger) IsBoot() bool {
	return t.Kind == TriggerKindBoot
}

func (t Trigger) MatchesStage(stage string) bool {
	return t.Kind == TriggerKindState && strings.TrimSpace(t.Name) == strings.TrimSpace(stage)
}

func (t Trigger) MatchesEvent(eventType string) bool {
	return t.Kind == TriggerKindEvent && strings.TrimSpace(t.Name) == strings.TrimSpace(eventType)
}

func (t Trigger) String() string {
	switch t.Kind {
	case TriggerKindState, TriggerKindEvent:
		if name := strings.TrimSpace(t.Name); name != "" {
			return string(t.Kind) + ":" + name
		}
	case TriggerKindBoot:
		return string(TriggerKindBoot)
	}
	return ""
}

func WorkflowTimerHandle(timerID string) TimerHandle {
	return TimerHandle{
		Kind:    TimerHandleWorkflowTimer,
		TimerID: strings.TrimSpace(timerID),
	}
}

func AccumulationTimeoutHandle(bucket AccumulatorBucketRef) TimerHandle {
	return TimerHandle{
		Kind:   TimerHandleAccumulationTimeout,
		Bucket: bucket.Normalize(),
	}
}

func (h TimerHandle) Valid() bool {
	switch h.Kind {
	case TimerHandleWorkflowTimer:
		return strings.TrimSpace(h.TimerID) != ""
	case TimerHandleAccumulationTimeout:
		return h.Bucket.Valid()
	default:
		return false
	}
}

func (h TimerHandle) TaskID() string {
	switch h.Kind {
	case TimerHandleWorkflowTimer:
		return strings.TrimSpace(h.TimerID)
	case TimerHandleAccumulationTimeout:
		if !h.Bucket.Valid() {
			return ""
		}
		return accumulationTimeoutTaskPrefix + h.Bucket.Key()
	default:
		return ""
	}
}

func (h TimerHandle) PayloadMetadata() map[string]any {
	if !h.Valid() {
		return nil
	}
	handle := map[string]any{
		"kind": string(h.Kind),
	}
	switch h.Kind {
	case TimerHandleWorkflowTimer:
		handle["timer_id"] = strings.TrimSpace(h.TimerID)
	case TimerHandleAccumulationTimeout:
		handle["bucket"] = h.Bucket.PayloadValue()
	}
	return map[string]any{
		timerHandlePayloadKey: handle,
	}
}

func ParseTimerHandle(payload map[string]any) (TimerHandle, bool) {
	handleMap, ok := stringAnyMap(payload[timerHandlePayloadKey])
	if !ok {
		return TimerHandle{}, false
	}
	switch TimerHandleKind(strings.TrimSpace(asString(handleMap["kind"]))) {
	case TimerHandleWorkflowTimer:
		handle := WorkflowTimerHandle(asString(handleMap["timer_id"]))
		return handle, handle.Valid()
	case TimerHandleAccumulationTimeout:
		bucket, ok := bucketFromAny(handleMap["bucket"])
		if !ok {
			return TimerHandle{}, false
		}
		handle := AccumulationTimeoutHandle(bucket)
		return handle, handle.Valid()
	default:
		return TimerHandle{}, false
	}
}

func NewAccumulatorBucketRef(nodeID, eventType string) AccumulatorBucketRef {
	return AccumulatorBucketRef{
		NodeID:    strings.TrimSpace(nodeID),
		EventType: strings.TrimSpace(eventType),
	}
}

func NewAccumulatorWindowBucketRef(nodeID, eventType, window string) AccumulatorBucketRef {
	ref := NewAccumulatorBucketRef(nodeID, eventType)
	ref.Window = strings.TrimSpace(window)
	return ref
}

func ParseAccumulatorBucketRef(payload map[string]any) (AccumulatorBucketRef, bool) {
	handle, ok := ParseTimerHandle(payload)
	if !ok || handle.Kind != TimerHandleAccumulationTimeout {
		return AccumulatorBucketRef{}, false
	}
	return handle.Bucket, handle.Bucket.Valid()
}

func ParseAccumulatorBucketKey(key string) (AccumulatorBucketRef, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return AccumulatorBucketRef{}, false
	}
	window := ""
	if base, encoded, ok := strings.Cut(key, "@window="); ok {
		key = strings.TrimSpace(base)
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return AccumulatorBucketRef{}, false
		}
		window = string(decoded)
	}
	nodeID, eventType, ok := strings.Cut(key, ":")
	if !ok {
		return AccumulatorBucketRef{}, false
	}
	bucket := NewAccumulatorWindowBucketRef(nodeID, eventType, window)
	return bucket, bucket.Valid()
}

func (r AccumulatorBucketRef) Normalize() AccumulatorBucketRef {
	return NewAccumulatorWindowBucketRef(r.NodeID, r.EventType, r.Window)
}

func (r AccumulatorBucketRef) Valid() bool {
	return strings.TrimSpace(r.NodeID) != "" && strings.TrimSpace(r.EventType) != ""
}

func (r AccumulatorBucketRef) Key() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	key := r.NodeID + ":" + r.EventType
	if r.Window == "" {
		return key
	}
	return key + "@window=" + base64.RawURLEncoding.EncodeToString([]byte(r.Window))
}

func (r AccumulatorBucketRef) PayloadValue() map[string]any {
	r = r.Normalize()
	if !r.Valid() {
		return nil
	}
	payload := map[string]any{
		"node_id":    r.NodeID,
		"event_type": r.EventType,
	}
	if r.Window != "" {
		payload["window"] = r.Window
	}
	return payload
}

func stringAnyMap(value any) (map[string]any, bool) {
	typed, ok := value.(map[string]any)
	if !ok || typed == nil {
		return nil, false
	}
	return typed, true
}

func bucketFromAny(value any) (AccumulatorBucketRef, bool) {
	payload, ok := stringAnyMap(value)
	if !ok {
		return AccumulatorBucketRef{}, false
	}
	bucket := NewAccumulatorWindowBucketRef(asString(payload["node_id"]), asString(payload["event_type"]), asString(payload["window"]))
	return bucket, bucket.Valid()
}

func asString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
