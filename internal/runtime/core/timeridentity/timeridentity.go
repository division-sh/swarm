package timeridentity

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
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
	TimerHandleWorkflowTimer TimerHandleKind = "workflow_timer"
	TimerHandleJoinTimeout   TimerHandleKind = "join_timeout"
	TimerHandleJoinComplete  TimerHandleKind = "join_complete"
	timerHandlePayloadKey                    = "timer_handle"
	joinTimeoutTaskPrefix                    = "join_timeout:"
	joinCompleteTaskPrefix                   = "join_complete:"
)

type TimerHandle struct {
	Kind       TimerHandleKind
	TimerID    string
	Join       JoinRef
	Generation attemptgeneration.Generation
}

type JoinRef struct {
	NodeID       string
	HandlerEvent string
	Stage        string
	JoinID       string
	Window       string
	Generation   attemptgeneration.Generation
}

type AccumulatorBucketRef struct {
	NodeID     string
	EventType  string
	Window     string
	Generation attemptgeneration.Generation
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

func JoinTimeoutHandle(ref JoinRef) TimerHandle {
	return TimerHandle{Kind: TimerHandleJoinTimeout, Join: ref.Normalize(), Generation: ref.Generation.Normalize()}
}

func JoinCompleteHandle(ref JoinRef) TimerHandle {
	return TimerHandle{Kind: TimerHandleJoinComplete, Join: ref.Normalize(), Generation: ref.Generation.Normalize()}
}

func (h TimerHandle) Valid() bool {
	switch h.Kind {
	case TimerHandleWorkflowTimer:
		return strings.TrimSpace(h.TimerID) != ""
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		return h.Join.Valid()
	default:
		return false
	}
}

func (h TimerHandle) TaskID() string {
	generationSuffix := h.Generation.Normalize().KeySuffix()
	appendGeneration := func(value string) string {
		if generationSuffix == "" {
			return value
		}
		return value + ":generation:" + generationSuffix
	}
	switch h.Kind {
	case TimerHandleWorkflowTimer:
		return appendGeneration(strings.TrimSpace(h.TimerID))
	case TimerHandleJoinTimeout:
		return joinTimeoutTaskPrefix + h.Join.Key()
	case TimerHandleJoinComplete:
		return joinCompleteTaskPrefix + h.Join.Key()
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
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		handle["join"] = h.Join.PayloadValue()
	}
	if generation := h.Generation.Normalize(); generation.Valid() {
		handle[attemptgeneration.PayloadKey] = generation.PayloadValue()
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
	generation, _ := attemptgeneration.FromPayload(map[string]any{attemptgeneration.PayloadKey: handleMap[attemptgeneration.PayloadKey]})
	switch TimerHandleKind(strings.TrimSpace(asString(handleMap["kind"]))) {
	case TimerHandleWorkflowTimer:
		handle := WorkflowTimerHandle(asString(handleMap["timer_id"]))
		handle.Generation = generation
		return handle, handle.Valid()
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		ref, ok := joinRefFromAny(handleMap["join"])
		if !ok {
			return TimerHandle{}, false
		}
		handle := JoinTimeoutHandle(ref)
		if TimerHandleKind(strings.TrimSpace(asString(handleMap["kind"]))) == TimerHandleJoinComplete {
			handle = JoinCompleteHandle(ref)
		}
		handle.Generation = generation
		return handle, handle.Valid()
	default:
		return TimerHandle{}, false
	}
}

func NewJoinRef(nodeID, handlerEvent, stage, joinID, window string) JoinRef {
	return JoinRef{
		NodeID:       strings.TrimSpace(nodeID),
		HandlerEvent: strings.TrimSpace(handlerEvent),
		Stage:        strings.TrimSpace(stage),
		JoinID:       strings.TrimSpace(joinID),
		Window:       strings.TrimSpace(window),
	}
}

func NewJoinRefForGeneration(nodeID, handlerEvent, stage, joinID, window string, generation attemptgeneration.Generation) JoinRef {
	ref := NewJoinRef(nodeID, handlerEvent, stage, joinID, window)
	ref.Generation = generation.Normalize()
	return ref
}

func (r JoinRef) Normalize() JoinRef {
	return NewJoinRefForGeneration(r.NodeID, r.HandlerEvent, r.Stage, r.JoinID, r.Window, r.Generation)
}

func (r JoinRef) Valid() bool {
	r = r.Normalize()
	return r.NodeID != "" && r.HandlerEvent != "" && r.Stage != "" && r.JoinID != ""
}

func (r JoinRef) Key() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	parts := []string{r.NodeID, r.HandlerEvent, r.Stage, r.JoinID, r.Window}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	key := strings.Join(parts, ".")
	if suffix := r.Generation.KeySuffix(); suffix != "" {
		key += ".generation." + suffix
	}
	return key
}

func (r JoinRef) PayloadValue() map[string]any {
	r = r.Normalize()
	if !r.Valid() {
		return nil
	}
	payload := map[string]any{
		"node_id":       r.NodeID,
		"handler_event": r.HandlerEvent,
		"stage":         r.Stage,
		"join_id":       r.JoinID,
		"window":        r.Window,
	}
	if generation := r.Generation.Normalize(); generation.Valid() {
		payload[attemptgeneration.PayloadKey] = generation.PayloadValue()
	}
	return payload
}

func ParseJoinRef(payload map[string]any) (JoinRef, TimerHandleKind, bool) {
	handle, ok := ParseTimerHandle(payload)
	if !ok || (handle.Kind != TimerHandleJoinTimeout && handle.Kind != TimerHandleJoinComplete) {
		return JoinRef{}, "", false
	}
	return handle.Join, handle.Kind, handle.Join.Valid()
}

func joinRefFromAny(value any) (JoinRef, bool) {
	raw, ok := stringAnyMap(value)
	if !ok {
		return JoinRef{}, false
	}
	generation, _ := attemptgeneration.FromPayload(map[string]any{attemptgeneration.PayloadKey: raw[attemptgeneration.PayloadKey]})
	ref := NewJoinRefForGeneration(asString(raw["node_id"]), asString(raw["handler_event"]), asString(raw["stage"]), asString(raw["join_id"]), asString(raw["window"]), generation)
	return ref, ref.Valid()
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

func NewAccumulatorBucketRefForGeneration(nodeID, eventType, window string, generation attemptgeneration.Generation) AccumulatorBucketRef {
	ref := NewAccumulatorWindowBucketRef(nodeID, eventType, window)
	ref.Generation = generation.Normalize()
	return ref
}

func ParseAccumulatorBucketKey(key string) (AccumulatorBucketRef, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return AccumulatorBucketRef{}, false
	}
	generation := attemptgeneration.Generation{}
	if base, encoded, ok := strings.Cut(key, "@generation="); ok {
		key = strings.TrimSpace(base)
		generation, _ = attemptgeneration.ParseKeySuffix(strings.TrimSpace(encoded))
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
	bucket := NewAccumulatorBucketRefForGeneration(nodeID, eventType, window, generation)
	return bucket, bucket.Valid()
}

func (r AccumulatorBucketRef) Normalize() AccumulatorBucketRef {
	return NewAccumulatorBucketRefForGeneration(r.NodeID, r.EventType, r.Window, r.Generation)
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
		if suffix := r.Generation.KeySuffix(); suffix != "" {
			return key + "@generation=" + suffix
		}
		return key
	}
	key += "@window=" + base64.RawURLEncoding.EncodeToString([]byte(r.Window))
	if suffix := r.Generation.KeySuffix(); suffix != "" {
		key += "@generation=" + suffix
	}
	return key
}

func stringAnyMap(value any) (map[string]any, bool) {
	typed, ok := value.(map[string]any)
	if !ok || typed == nil {
		return nil, false
	}
	return typed, true
}

func asString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
