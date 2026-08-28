package timeridentity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
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
	TimerHandleWorkflowTimer          TimerHandleKind = "workflow_timer"
	TimerHandleJoinTimeout            TimerHandleKind = "join_timeout"
	TimerHandleJoinComplete           TimerHandleKind = "join_complete"
	timerHandlePayloadKey                             = "timer_handle"
	joinTimeoutTaskPrefix                             = "join_timeout:"
	joinCompleteTaskPrefix                            = "join_complete:"
	workflowTimerActivationTaskPrefix                 = "workflow_timer:v2:"
	workflowTimerOccurrenceTaskPrefix                 = "workflow_timer_occurrence:v2:"
)

var workflowTimerIdentityNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.workflow-timer.identity.v2"))

// WorkflowTimerActivationRef is the typed durable identity stored in
// timers.timer_name. It is deliberately independent from fire_payload.
type WorkflowTimerActivationRef struct {
	ActivationID        string                       `json:"activation_id"`
	DeclarationKey      string                       `json:"declaration_key"`
	DeclarationRevision string                       `json:"declaration_revision"`
	Cause               WorkflowTimerActivationCause `json:"cause"`
	Generation          attemptgeneration.Generation `json:"generation,omitempty"`
}

type WorkflowTimerActivationCause string

const (
	WorkflowTimerActivationCauseInitial    WorkflowTimerActivationCause = "initial_stage"
	WorkflowTimerActivationCauseEvent      WorkflowTimerActivationCause = "event"
	WorkflowTimerActivationCauseTransition WorkflowTimerActivationCause = "transition"
)

func (r WorkflowTimerActivationRef) Normalize() WorkflowTimerActivationRef {
	r.ActivationID = strings.TrimSpace(r.ActivationID)
	r.DeclarationKey = strings.TrimSpace(r.DeclarationKey)
	r.DeclarationRevision = strings.TrimSpace(r.DeclarationRevision)
	r.Cause = WorkflowTimerActivationCause(strings.TrimSpace(string(r.Cause)))
	r.Generation = r.Generation.Normalize()
	return r
}

func (r WorkflowTimerActivationRef) Valid() bool {
	r = r.Normalize()
	if r.ActivationID == "" || r.DeclarationKey == "" || r.DeclarationRevision == "" {
		return false
	}
	switch r.Cause {
	case WorkflowTimerActivationCauseInitial,
		WorkflowTimerActivationCauseEvent,
		WorkflowTimerActivationCauseTransition:
	default:
		return false
	}
	_, err := uuid.Parse(r.ActivationID)
	return err == nil
}

func (r WorkflowTimerActivationRef) TaskID() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return workflowTimerActivationTaskPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

func ParseWorkflowTimerActivationTaskID(raw string) (WorkflowTimerActivationRef, bool) {
	if raw != strings.TrimSpace(raw) {
		return WorkflowTimerActivationRef{}, false
	}
	if !strings.HasPrefix(raw, workflowTimerActivationTaskPrefix) {
		return WorkflowTimerActivationRef{}, false
	}
	encoded := strings.TrimPrefix(raw, workflowTimerActivationTaskPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return WorkflowTimerActivationRef{}, false
	}
	var ref WorkflowTimerActivationRef
	if err := decodeStrictJSON(payload, &ref); err != nil {
		return WorkflowTimerActivationRef{}, false
	}
	ref = ref.Normalize()
	return ref, ref.Valid() && ref.TaskID() == raw
}

func IsWorkflowTimerActivationTaskID(raw string) bool {
	_, ok := ParseWorkflowTimerActivationTaskID(raw)
	return ok
}

func WorkflowTimerActivationTaskPrefix() string {
	return workflowTimerActivationTaskPrefix
}

// WorkflowTimerOccurrenceRef identifies one persisted due coordinate. The
// same coordinate always yields the same event ID; advancing recurrence
// produces a different occurrence without changing activation identity.
type WorkflowTimerOccurrenceRef struct {
	Activation WorkflowTimerActivationRef `json:"activation"`
	DueAt      time.Time                  `json:"due_at"`
}

func (r WorkflowTimerOccurrenceRef) Normalize() WorkflowTimerOccurrenceRef {
	r.Activation = r.Activation.Normalize()
	if !r.DueAt.IsZero() {
		r.DueAt = r.DueAt.UTC()
	}
	return r
}

func (r WorkflowTimerOccurrenceRef) Valid() bool {
	r = r.Normalize()
	return r.Activation.Valid() && !r.DueAt.IsZero()
}

func (r WorkflowTimerOccurrenceRef) TaskID() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return workflowTimerOccurrenceTaskPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

func ParseWorkflowTimerOccurrenceTaskID(raw string) (WorkflowTimerOccurrenceRef, bool) {
	if raw != strings.TrimSpace(raw) {
		return WorkflowTimerOccurrenceRef{}, false
	}
	if !strings.HasPrefix(raw, workflowTimerOccurrenceTaskPrefix) {
		return WorkflowTimerOccurrenceRef{}, false
	}
	encoded := strings.TrimPrefix(raw, workflowTimerOccurrenceTaskPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return WorkflowTimerOccurrenceRef{}, false
	}
	var ref WorkflowTimerOccurrenceRef
	if err := decodeStrictJSON(payload, &ref); err != nil {
		return WorkflowTimerOccurrenceRef{}, false
	}
	ref = ref.Normalize()
	return ref, ref.Valid() && ref.TaskID() == raw
}

func WorkflowTimerActivationID(parts ...string) string {
	normalized := make([]string, len(parts))
	for i, part := range parts {
		normalized[i] = strings.TrimSpace(part)
	}
	return uuid.NewSHA1(workflowTimerIdentityNamespace, []byte("activation\x00"+strings.Join(normalized, "\x00"))).String()
}

func WorkflowTimerForkActivationID(sourceActivationID, forkRunID, forkEventID string) string {
	return uuid.NewSHA1(workflowTimerIdentityNamespace, []byte(strings.Join([]string{
		"fork",
		strings.TrimSpace(sourceActivationID),
		strings.TrimSpace(forkRunID),
		strings.TrimSpace(forkEventID),
	}, "\x00"))).String()
}

func WorkflowTimerOccurrenceEventID(ref WorkflowTimerOccurrenceRef) string {
	ref = ref.Normalize()
	if !ref.Valid() {
		return ""
	}
	return uuid.NewSHA1(workflowTimerIdentityNamespace, []byte(strings.Join([]string{
		"occurrence",
		ref.Activation.ActivationID,
		ref.DueAt.Format(time.RFC3339Nano),
	}, "\x00"))).String()
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

type TimerHandle struct {
	kind       TimerHandleKind
	timerID    string
	join       JoinRef
	generation attemptgeneration.Generation
}

type JoinRef struct {
	node         runtimeidentity.ExecutableNode
	handlerEvent string
	joinID       string
	mode         JoinRefMode
	arrival      JoinArrivalRef
	fanOut       FanOutDeliveryRef
	generation   attemptgeneration.Generation
}

type JoinRefMode string

const (
	JoinRefModeArrival        JoinRefMode = "arrival"
	JoinRefModeFanOutDelivery JoinRefMode = "fan_out_delivery"
)

type JoinArrivalRef struct {
	stage  string
	window string
}

// FanOutDeliveryRef is the immutable declaration/intent coordinate shared by
// the fan-out owner, barrier owner, and generic completion schedule.
type FanOutDeliveryRef struct {
	packageKey           runtimeidentity.PackageKey
	elementID            contractelementidentity.ContractElementID
	bundleHash           string
	semanticDigest       string
	triggeringDeliveryID string
}

var semanticDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type AccumulatorBucketRef struct {
	Node       runtimeidentity.ExecutableNode
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
		kind:    TimerHandleWorkflowTimer,
		timerID: strings.TrimSpace(timerID),
	}
}

func WorkflowTimerHandleForGeneration(timerID string, generation attemptgeneration.Generation) TimerHandle {
	handle := WorkflowTimerHandle(timerID)
	handle.generation = generation.Normalize()
	return handle
}

func JoinTimeoutHandle(ref JoinRef) (TimerHandle, error) {
	if ref.Normalize().mode != JoinRefModeArrival {
		return TimerHandle{}, fmt.Errorf("join timeout handle requires an arrival declaration")
	}
	return newJoinHandle(TimerHandleJoinTimeout, ref)
}

func JoinCompleteHandle(ref JoinRef) (TimerHandle, error) {
	ref = ref.Normalize()
	if ref.mode == JoinRefModeFanOutDelivery && ref.fanOut.triggeringDeliveryID == "" {
		return TimerHandle{}, fmt.Errorf("fan-out delivery completion requires an exact triggering delivery")
	}
	return newJoinHandle(TimerHandleJoinComplete, ref)

}

func newJoinHandle(kind TimerHandleKind, ref JoinRef) (TimerHandle, error) {
	ref = ref.Normalize()
	if !ref.Valid() {
		return TimerHandle{}, fmt.Errorf("join timer handle requires a complete declaration reference")
	}
	return TimerHandle{kind: kind, join: ref}, nil
}

func (h TimerHandle) Valid() bool {
	switch h.kind {
	case TimerHandleWorkflowTimer:
		return strings.TrimSpace(h.timerID) != ""
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		return h.join.Valid() && !h.generation.Valid()
	default:
		return false
	}
}

func (h TimerHandle) Kind() TimerHandleKind { return h.kind }

func (h TimerHandle) TimerID() string { return strings.TrimSpace(h.timerID) }

func (h TimerHandle) JoinRef() (JoinRef, bool) {
	if h.kind != TimerHandleJoinTimeout && h.kind != TimerHandleJoinComplete || !h.join.Valid() {
		return JoinRef{}, false
	}
	return h.join.Normalize(), true
}

func (h TimerHandle) Generation() attemptgeneration.Generation {
	if ref, ok := h.JoinRef(); ok {
		return ref.Generation()
	}
	return h.generation.Normalize()
}

func (h TimerHandle) EventType() string {
	switch h.kind {
	case TimerHandleJoinTimeout:
		return "platform.join_timeout"
	case TimerHandleJoinComplete:
		return "platform.join_complete"
	default:
		return ""
	}
}

func (h TimerHandle) TaskID() string {
	generationSuffix := h.generation.Normalize().KeySuffix()
	appendGeneration := func(value string) string {
		if generationSuffix == "" {
			return value
		}
		return value + ":generation:" + generationSuffix
	}
	switch h.kind {
	case TimerHandleWorkflowTimer:
		return appendGeneration(strings.TrimSpace(h.timerID))
	case TimerHandleJoinTimeout:
		return joinTimeoutTaskPrefix + h.join.Key()
	case TimerHandleJoinComplete:
		return joinCompleteTaskPrefix + h.join.Key()
	default:
		return ""
	}
}

func (h TimerHandle) PayloadMetadata() map[string]any {
	if !h.Valid() {
		return nil
	}
	handle := map[string]any{
		"kind": string(h.kind),
	}
	switch h.kind {
	case TimerHandleWorkflowTimer:
		handle["timer_id"] = strings.TrimSpace(h.timerID)
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		handle["join"] = h.join.PayloadValue()
	}
	if generation := h.generation.Normalize(); generation.Valid() {
		handle[attemptgeneration.PayloadKey] = generation.PayloadValue()
	}
	payload := map[string]any{
		timerHandlePayloadKey: handle,
	}
	if ref, ok := h.JoinRef(); ok {
		if generation := ref.Generation(); generation.Valid() {
			payload[generation.RevisionField] = generation.RevisionID
		}
	}
	return payload
}

func ParseTimerHandle(payload map[string]any) (TimerHandle, bool) {
	handleMap, ok := stringAnyMap(payload[timerHandlePayloadKey])
	if !ok {
		return TimerHandle{}, false
	}
	return parseTimerHandleMap(handleMap)
}

func parseTimerHandleMap(handleMap map[string]any) (TimerHandle, bool) {
	kind := TimerHandleKind(strings.TrimSpace(asString(handleMap["kind"])))
	switch kind {
	case TimerHandleWorkflowTimer:
		if !onlyKeys(handleMap, "kind", "timer_id", attemptgeneration.PayloadKey) {
			return TimerHandle{}, false
		}
		generation := attemptgeneration.Generation{}
		if _, present := handleMap[attemptgeneration.PayloadKey]; present {
			var ok bool
			generation, ok = attemptgeneration.FromPayload(map[string]any{attemptgeneration.PayloadKey: handleMap[attemptgeneration.PayloadKey]})
			if !ok {
				return TimerHandle{}, false
			}
		}
		handle := WorkflowTimerHandle(asString(handleMap["timer_id"]))
		handle.generation = generation
		return handle, handle.Valid()
	case TimerHandleJoinTimeout, TimerHandleJoinComplete:
		if !onlyKeys(handleMap, "kind", "join") {
			return TimerHandle{}, false
		}
		ref, ok := joinRefFromAny(handleMap["join"])
		if !ok {
			return TimerHandle{}, false
		}
		var handle TimerHandle
		var err error
		if kind == TimerHandleJoinComplete {
			handle, err = JoinCompleteHandle(ref)
		} else {
			handle, err = JoinTimeoutHandle(ref)
		}
		return handle, err == nil && handle.Valid()
	default:
		return TimerHandle{}, false
	}
}

func NewJoinRef(node runtimeidentity.ExecutableNode, handlerEvent, stage, joinID, window string) (JoinRef, error) {
	return NewJoinRefForGeneration(node, handlerEvent, stage, joinID, window, attemptgeneration.Generation{})
}

func NewJoinRefForGeneration(node runtimeidentity.ExecutableNode, handlerEvent, stage, joinID, window string, generation attemptgeneration.Generation) (JoinRef, error) {
	ref := JoinRef{
		node:         node,
		handlerEvent: strings.TrimSpace(handlerEvent),
		joinID:       strings.TrimSpace(joinID),
		mode:         JoinRefModeArrival,
		arrival: JoinArrivalRef{
			stage:  strings.TrimSpace(stage),
			window: strings.TrimSpace(window),
		},
		generation: generation.Normalize(),
	}
	if !ref.Valid() {
		return JoinRef{}, fmt.Errorf("join declaration reference requires node, handler event, stage, and join identity")
	}
	if generation != (attemptgeneration.Generation{}) && !ref.generation.Valid() {
		return JoinRef{}, fmt.Errorf("join declaration reference generation is invalid")
	}
	return ref, nil
}

// NewFanOutDeliveryJoinRef creates the declaration form. BindFanOutIntent
// turns it into the exact completion-occurrence identity after S1 assigns the
// triggering delivery and the execution generation is known.
func NewFanOutDeliveryJoinRef(
	node runtimeidentity.ExecutableNode,
	handlerEvent, joinID, packageKey, elementID, bundleHash, semanticDigest string,
) (JoinRef, error) {
	pkg, err := runtimeidentity.ParsePackageKey(packageKey)
	if err != nil {
		return JoinRef{}, fmt.Errorf("fan-out delivery join package: %w", err)
	}
	element, err := contractelementidentity.ParseContractElementID(elementID)
	if err != nil {
		return JoinRef{}, fmt.Errorf("fan-out delivery join element: %w", err)
	}
	ref := JoinRef{
		node:         node,
		handlerEvent: strings.TrimSpace(handlerEvent),
		joinID:       strings.TrimSpace(joinID),
		mode:         JoinRefModeFanOutDelivery,
		fanOut: FanOutDeliveryRef{
			packageKey:     pkg,
			elementID:      element,
			bundleHash:     strings.TrimSpace(bundleHash),
			semanticDigest: strings.TrimSpace(semanticDigest),
		},
	}
	if !ref.Valid() {
		return JoinRef{}, fmt.Errorf("fan-out delivery join reference requires exact node, handler, join, bundle, element, and semantic identity")
	}
	return ref, nil
}

func (r JoinRef) BindFanOutIntent(triggeringDeliveryID string, generation attemptgeneration.Generation) (JoinRef, error) {
	r = r.Normalize()
	if r.mode != JoinRefModeFanOutDelivery {
		return JoinRef{}, fmt.Errorf("only a fan-out delivery declaration can bind an intent")
	}
	r.fanOut.triggeringDeliveryID = strings.TrimSpace(triggeringDeliveryID)
	r.generation = generation.Normalize()
	if r.fanOut.triggeringDeliveryID == "" || generation != (attemptgeneration.Generation{}) && !r.generation.Valid() || !r.Valid() {
		return JoinRef{}, fmt.Errorf("fan-out delivery intent requires exact triggering delivery and valid generation")
	}
	return r, nil
}

func (r JoinRef) Normalize() JoinRef {
	r.handlerEvent = strings.TrimSpace(r.handlerEvent)
	r.joinID = strings.TrimSpace(r.joinID)
	r.mode = JoinRefMode(strings.TrimSpace(string(r.mode)))
	r.arrival.stage = strings.TrimSpace(r.arrival.stage)
	r.arrival.window = strings.TrimSpace(r.arrival.window)
	r.fanOut.bundleHash = strings.TrimSpace(r.fanOut.bundleHash)
	r.fanOut.semanticDigest = strings.TrimSpace(r.fanOut.semanticDigest)
	r.fanOut.triggeringDeliveryID = strings.TrimSpace(r.fanOut.triggeringDeliveryID)
	r.generation = r.generation.Normalize()
	return r
}

func (r JoinRef) Valid() bool {
	r = r.Normalize()
	if !r.node.Valid() || r.handlerEvent == "" || r.joinID == "" ||
		(r.generation != (attemptgeneration.Generation{}) && !r.generation.Valid()) {
		return false
	}
	switch r.mode {
	case JoinRefModeArrival:
		return r.arrival.stage != "" && r.fanOut == (FanOutDeliveryRef{})
	case JoinRefModeFanOutDelivery:
		return r.arrival == (JoinArrivalRef{}) && r.fanOut.Valid()
	default:
		return false
	}
}

func (r JoinRef) Node() runtimeidentity.ExecutableNode { return r.Normalize().node }
func (r JoinRef) PackageKey() string                   { return r.Node().PackageKey() }
func (r JoinRef) FlowID() string                       { return r.Node().FlowID() }
func (r JoinRef) NodeID() string                       { return r.Node().NodeID() }
func (r JoinRef) HandlerEvent() string                 { return r.Normalize().handlerEvent }
func (r JoinRef) Mode() JoinRefMode                    { return r.Normalize().mode }
func (r JoinRef) Stage() string                        { return r.Normalize().arrival.stage }
func (r JoinRef) JoinID() string                       { return r.Normalize().joinID }
func (r JoinRef) Window() string                       { return r.Normalize().arrival.window }
func (r JoinRef) Generation() attemptgeneration.Generation {
	return r.Normalize().generation
}

func (r JoinRef) FanOutDelivery() (FanOutDeliveryRef, bool) {
	r = r.Normalize()
	return r.fanOut, r.mode == JoinRefModeFanOutDelivery && r.fanOut.Valid()
}

func (r FanOutDeliveryRef) Valid() bool {
	r = r.Normalize()
	return r.packageKey.Valid() && r.elementID.Valid() &&
		bundleidentity.IsCanonicalHash(r.bundleHash) && semanticDigestPattern.MatchString(r.semanticDigest)
}

func (r FanOutDeliveryRef) Normalize() FanOutDeliveryRef {
	r.bundleHash = strings.TrimSpace(r.bundleHash)
	r.semanticDigest = strings.TrimSpace(r.semanticDigest)
	r.triggeringDeliveryID = strings.TrimSpace(r.triggeringDeliveryID)
	return r
}

func (r FanOutDeliveryRef) PackageKey() string           { return r.Normalize().packageKey.String() }
func (r FanOutDeliveryRef) ElementID() string            { return r.Normalize().elementID.String() }
func (r FanOutDeliveryRef) BundleHash() string           { return r.Normalize().bundleHash }
func (r FanOutDeliveryRef) SemanticDigest() string       { return r.Normalize().semanticDigest }
func (r FanOutDeliveryRef) TriggeringDeliveryID() string { return r.Normalize().triggeringDeliveryID }

func (r JoinRef) WithGeneration(generation attemptgeneration.Generation) (JoinRef, error) {
	r = r.Normalize()
	if r.mode == JoinRefModeArrival {
		return NewJoinRefForGeneration(r.Node(), r.HandlerEvent(), r.Stage(), r.JoinID(), r.Window(), generation)
	}
	if r.mode != JoinRefModeFanOutDelivery {
		return JoinRef{}, fmt.Errorf("join declaration mode is invalid")
	}
	r.generation = generation.Normalize()
	if generation != (attemptgeneration.Generation{}) && !r.generation.Valid() || !r.Valid() {
		return JoinRef{}, fmt.Errorf("join declaration generation is invalid")
	}
	return r, nil
}

func (r JoinRef) Declaration() JoinRef {
	r = r.Normalize()
	r.generation = attemptgeneration.Generation{}
	if r.mode == JoinRefModeArrival {
		r.arrival.window = ""
	} else if r.mode == JoinRefModeFanOutDelivery {
		r.fanOut.triggeringDeliveryID = ""
	}
	return r
}

func (r JoinRef) Equal(other JoinRef) bool { return r.Normalize() == other.Normalize() }

func (r JoinRef) Key() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	parts := []string{string(r.mode), r.node.Key(), r.handlerEvent, r.joinID}
	switch r.mode {
	case JoinRefModeArrival:
		parts = append(parts, r.arrival.stage, r.arrival.window)
	case JoinRefModeFanOutDelivery:
		parts = append(parts, r.fanOut.packageKey.String(), r.fanOut.elementID.String(), r.fanOut.bundleHash, r.fanOut.semanticDigest, r.fanOut.triggeringDeliveryID)
	}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	key := strings.Join(parts, ".")
	if suffix := r.generation.KeySuffix(); suffix != "" {
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
		"mode": string(r.mode),
		"node": map[string]any{
			"package_key": r.node.PackageKey(),
			"flow_id":     r.node.FlowID(),
			"node_id":     r.node.NodeID(),
		},
		"handler_event": r.handlerEvent,
		"join_id":       r.joinID,
	}
	switch r.mode {
	case JoinRefModeArrival:
		payload["stage"] = r.arrival.stage
		payload["window"] = r.arrival.window
	case JoinRefModeFanOutDelivery:
		payload["fan_out"] = map[string]any{
			"package_key":            r.fanOut.packageKey.String(),
			"element_id":             r.fanOut.elementID.String(),
			"bundle_hash":            r.fanOut.bundleHash,
			"semantic_digest":        r.fanOut.semanticDigest,
			"triggering_delivery_id": r.fanOut.triggeringDeliveryID,
		}
	}
	if generation := r.generation.Normalize(); generation.Valid() {
		payload[attemptgeneration.PayloadKey] = generation.PayloadValue()
	}
	return payload
}

func ParseJoinHandle(payload map[string]any) (TimerHandle, JoinRef, bool) {
	handle, ok := ParseTimerHandle(payload)
	if !ok || (handle.Kind() != TimerHandleJoinTimeout && handle.Kind() != TimerHandleJoinComplete) {
		return TimerHandle{}, JoinRef{}, false
	}
	joinRef, ok := handle.JoinRef()
	if !ok {
		return TimerHandle{}, JoinRef{}, false
	}
	allowed := []string{timerHandlePayloadKey}
	if joinRef.Mode() == JoinRefModeFanOutDelivery && handle.Kind() == TimerHandleJoinComplete {
		allowed = append(allowed, "join")
	}
	if generation := joinRef.Generation(); generation.Valid() {
		allowed = append(allowed, generation.RevisionField)
		if strings.TrimSpace(asString(payload[generation.RevisionField])) != generation.RevisionID {
			return TimerHandle{}, JoinRef{}, false
		}
	}
	if !onlyKeys(payload, allowed...) {
		return TimerHandle{}, JoinRef{}, false
	}
	return handle, joinRef, true
}

func ParseJoinRef(payload map[string]any) (JoinRef, TimerHandleKind, bool) {
	handle, ref, ok := ParseJoinHandle(payload)
	return ref, handle.Kind(), ok
}

func joinRefFromAny(value any) (JoinRef, bool) {
	raw, ok := stringAnyMap(value)
	if !ok {
		return JoinRef{}, false
	}
	nodeRaw, hasNode := stringAnyMap(raw["node"])
	if !hasNode || !exactKeys(nodeRaw, "package_key", "flow_id", "node_id") {
		return JoinRef{}, false
	}
	node, err := runtimeidentity.ParseExecutableNode(asExactString(nodeRaw["package_key"]), asExactString(nodeRaw["flow_id"]), asExactString(nodeRaw["node_id"]))
	if err != nil {
		return JoinRef{}, false
	}
	generation := attemptgeneration.Generation{}
	if _, present := raw[attemptgeneration.PayloadKey]; present {
		var generationOK bool
		generation, generationOK = attemptgeneration.FromPayload(map[string]any{attemptgeneration.PayloadKey: raw[attemptgeneration.PayloadKey]})
		if !generationOK {
			return JoinRef{}, false
		}
	}
	mode := JoinRefMode(asExactString(raw["mode"]))
	var ref JoinRef
	switch mode {
	case JoinRefModeArrival:
		if !onlyKeys(raw, "mode", "node", "handler_event", "stage", "join_id", "window", attemptgeneration.PayloadKey) {
			return JoinRef{}, false
		}
		if _, present := raw["fan_out"]; present {
			return JoinRef{}, false
		}
		ref, err = NewJoinRefForGeneration(node, asExactString(raw["handler_event"]), asExactString(raw["stage"]), asExactString(raw["join_id"]), asExactString(raw["window"]), generation)
	case JoinRefModeFanOutDelivery:
		if !onlyKeys(raw, "mode", "node", "handler_event", "join_id", "fan_out", attemptgeneration.PayloadKey) {
			return JoinRef{}, false
		}
		fanOut, ok := stringAnyMap(raw["fan_out"])
		if !ok || !exactKeys(fanOut, "package_key", "element_id", "bundle_hash", "semantic_digest", "triggering_delivery_id") {
			return JoinRef{}, false
		}
		ref, err = NewFanOutDeliveryJoinRef(
			node,
			asExactString(raw["handler_event"]),
			asExactString(raw["join_id"]),
			asExactString(fanOut["package_key"]),
			asExactString(fanOut["element_id"]),
			asExactString(fanOut["bundle_hash"]),
			asExactString(fanOut["semantic_digest"]),
		)
		if err == nil {
			ref, err = ref.BindFanOutIntent(asExactString(fanOut["triggering_delivery_id"]), generation)
		}
	default:
		return JoinRef{}, false
	}
	return ref, err == nil
}

func (h TimerHandle) MarshalJSON() ([]byte, error) {
	metadata := h.PayloadMetadata()
	if metadata == nil {
		return nil, fmt.Errorf("timer handle is invalid")
	}
	return json.Marshal(metadata[timerHandlePayloadKey])
}

func (h *TimerHandle) UnmarshalJSON(raw []byte) error {
	if h == nil {
		return fmt.Errorf("timer handle target is nil")
	}
	var value map[string]any
	if err := decodeStrictJSON(raw, &value); err != nil {
		return err
	}
	parsed, ok := parseTimerHandleMap(value)
	if !ok {
		return fmt.Errorf("timer handle is invalid")
	}
	*h = parsed
	return nil
}

func onlyKeys(values map[string]any, allowed ...string) bool {
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for key := range values {
		if _, ok := set[key]; !ok {
			return false
		}
	}
	return true
}

func exactKeys(values map[string]any, required ...string) bool {
	return len(values) == len(required) && onlyKeys(values, required...)
}

func NewAccumulatorBucketRef(node runtimeidentity.ExecutableNode, eventType string) AccumulatorBucketRef {
	return AccumulatorBucketRef{
		Node:      node,
		EventType: strings.TrimSpace(eventType),
	}
}

func NewAccumulatorWindowBucketRef(node runtimeidentity.ExecutableNode, eventType, window string) AccumulatorBucketRef {
	ref := NewAccumulatorBucketRef(node, eventType)
	ref.Window = strings.TrimSpace(window)
	return ref
}

func NewAccumulatorBucketRefForGeneration(node runtimeidentity.ExecutableNode, eventType, window string, generation attemptgeneration.Generation) AccumulatorBucketRef {
	ref := NewAccumulatorWindowBucketRef(node, eventType, window)
	ref.Generation = generation.Normalize()
	return ref
}

func ParseAccumulatorBucketKey(key string) (AccumulatorBucketRef, bool) {
	if key == "" || key != strings.TrimSpace(key) {
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
	nodeKey, eventType, ok := strings.Cut(key, ":")
	if !ok {
		return AccumulatorBucketRef{}, false
	}
	node, err := runtimeidentity.ParseExecutableNodeKey(nodeKey)
	if err != nil {
		return AccumulatorBucketRef{}, false
	}
	bucket := NewAccumulatorBucketRefForGeneration(node, eventType, window, generation)
	return bucket, bucket.Valid()
}

func (r AccumulatorBucketRef) Normalize() AccumulatorBucketRef {
	return NewAccumulatorBucketRefForGeneration(r.Node, r.EventType, r.Window, r.Generation)
}

func (r AccumulatorBucketRef) Valid() bool {
	return r.Node.Valid() && strings.TrimSpace(r.EventType) != ""
}

func (r AccumulatorBucketRef) Key() string {
	r = r.Normalize()
	if !r.Valid() {
		return ""
	}
	key := r.Node.Key() + ":" + r.EventType
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

func asExactString(value any) string {
	text, _ := value.(string)
	return text
}
