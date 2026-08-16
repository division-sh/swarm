package joinruntime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

const bucketKey = "handler_joins"

type Status string

const (
	StatusOpen   Status = "open"
	StatusClosed Status = "closed"
)

type CloseReason string

const (
	CloseReasonComplete  CloseReason = "complete"
	CloseReasonTimeout   CloseReason = "timeout"
	CloseReasonStageExit CloseReason = "stage_exit"
)

type MemberOutput struct {
	Hash  string `json:"hash"`
	Value any    `json:"value"`
}

type Activation struct {
	handle          timeridentity.TimerHandle
	Members         []string                `json:"members"`
	Outputs         map[string]MemberOutput `json:"outputs"`
	Status          Status                  `json:"status"`
	CloseReason     CloseReason             `json:"close_reason,omitempty"`
	ArmedAt         time.Time               `json:"armed_at"`
	FireAt          time.Time               `json:"fire_at"`
	TimerCancelled  bool                    `json:"timer_cancelled,omitempty"`
	OutcomePending  bool                    `json:"outcome_pending,omitempty"`
	OutcomeFired    bool                    `json:"outcome_fired,omitempty"`
	CompletionEvent string                  `json:"completion_event,omitempty"`
}

type activationJSON struct {
	Handle          timeridentity.TimerHandle `json:"timer_handle"`
	Members         []string                  `json:"members"`
	Outputs         map[string]MemberOutput   `json:"outputs"`
	Status          Status                    `json:"status"`
	CloseReason     CloseReason               `json:"close_reason,omitempty"`
	ArmedAt         time.Time                 `json:"armed_at"`
	FireAt          time.Time                 `json:"fire_at"`
	TimerCancelled  bool                      `json:"timer_cancelled,omitempty"`
	OutcomePending  bool                      `json:"outcome_pending,omitempty"`
	OutcomeFired    bool                      `json:"outcome_fired,omitempty"`
	CompletionEvent string                    `json:"completion_event,omitempty"`
}

type AddDisposition string

const (
	AddAccepted             AddDisposition = "accepted"
	AddExactDuplicate       AddDisposition = "exact_duplicate"
	AddConflictingDuplicate AddDisposition = "conflicting_duplicate"
	AddUnexpected           AddDisposition = "unexpected"
)

func NewActivation(handle timeridentity.TimerHandle, members []string, armedAt, fireAt time.Time) (Activation, error) {
	normalizedMembers, err := normalizeMembers(members)
	if err != nil {
		return Activation{}, err
	}
	activation := Activation{
		handle:  handle,
		Members: normalizedMembers,
		Outputs: map[string]MemberOutput{},
		Status:  StatusOpen,
		ArmedAt: armedAt.UTC(),
		FireAt:  fireAt.UTC(),
	}
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	return activation, nil
}

func (a Activation) Validate() error {
	if !a.handle.Valid() || a.handle.Kind() != timeridentity.TimerHandleJoinTimeout && a.handle.Kind() != timeridentity.TimerHandleJoinComplete {
		return fmt.Errorf("join activation requires one valid typed timer handle")
	}
	if a.Status != StatusOpen && a.Status != StatusClosed {
		return fmt.Errorf("join activation status %q is invalid", a.Status)
	}
	if _, err := normalizeMembers(a.Members); err != nil {
		return err
	}
	memberSet := make(map[string]struct{}, len(a.Members))
	for _, member := range a.Members {
		memberSet[member] = struct{}{}
	}
	for member, output := range a.Outputs {
		if _, ok := memberSet[strings.TrimSpace(member)]; !ok {
			return fmt.Errorf("join output member %q is not declared", member)
		}
		if strings.TrimSpace(output.Hash) == "" {
			return fmt.Errorf("join output member %q has empty canonical hash", member)
		}
	}
	if a.Status == StatusOpen && a.CloseReason != "" {
		return fmt.Errorf("open join activation has close reason %q", a.CloseReason)
	}
	if a.Status == StatusOpen && a.TimerCancelled {
		return fmt.Errorf("open join activation cannot have a cancelled timer")
	}
	if a.Status == StatusClosed && a.CloseReason == "" {
		return fmt.Errorf("closed join activation is missing close reason")
	}
	return nil
}

func (a Activation) Key() string {
	ref, _ := a.handle.JoinRef()
	return ActivationKeyForGeneration(ref.Stage(), ref.JoinID(), ref.Window(), ref.Generation())
}

func ReplaceGeneration(buckets map[string]map[string]any, activation Activation, generation attemptgeneration.Generation) error {
	oldKey := activation.Key()
	oldNode := activation.JoinRef().Node()
	ref, ok := activation.handle.JoinRef()
	if !ok {
		return fmt.Errorf("join activation is missing its typed declaration handle")
	}
	ref, err := ref.WithGeneration(generation)
	if err != nil {
		return err
	}
	activation.handle, err = joinHandleForKind(activation.handle.Kind(), ref)
	if err != nil {
		return err
	}
	if err := Store(buckets, activation); err != nil {
		return err
	}
	if oldKey != activation.Key() {
		if nodeBucket := buckets[joinNodeBucketKey(oldNode)]; nodeBucket != nil {
			if joins, ok := nodeBucket[bucketKey].(map[string]any); ok {
				delete(joins, oldKey)
			}
		}
	}
	return nil
}

func (a Activation) TimerHandle() timeridentity.TimerHandle { return a.handle }

func (a Activation) JoinRef() timeridentity.JoinRef {
	ref, _ := a.handle.JoinRef()
	return ref
}

func (a Activation) FlowID() string       { return a.JoinRef().FlowID() }
func (a Activation) NodeID() string       { return a.JoinRef().NodeID() }
func (a Activation) HandlerEvent() string { return a.JoinRef().HandlerEvent() }
func (a Activation) Stage() string        { return a.JoinRef().Stage() }
func (a Activation) JoinID() string       { return a.JoinRef().JoinID() }
func (a Activation) Window() string       { return a.JoinRef().Window() }
func (a Activation) Generation() attemptgeneration.Generation {
	return a.JoinRef().Generation()
}
func (a Activation) TimerTaskID() string    { return a.handle.TaskID() }
func (a Activation) TimerEventType() string { return a.handle.EventType() }

func (a Activation) WithTimerHandle(handle timeridentity.TimerHandle, fireAt time.Time) (Activation, error) {
	current, currentOK := a.handle.JoinRef()
	next, nextOK := handle.JoinRef()
	if !currentOK || !nextOK || !current.Equal(next) {
		return Activation{}, fmt.Errorf("join activation timer replacement must preserve declaration identity")
	}
	a.handle = handle
	a.FireAt = fireAt.UTC()
	if err := a.Validate(); err != nil {
		return Activation{}, err
	}
	return a, nil
}

func joinHandleForKind(kind timeridentity.TimerHandleKind, ref timeridentity.JoinRef) (timeridentity.TimerHandle, error) {
	switch kind {
	case timeridentity.TimerHandleJoinTimeout:
		return timeridentity.JoinTimeoutHandle(ref)
	case timeridentity.TimerHandleJoinComplete:
		return timeridentity.JoinCompleteHandle(ref)
	default:
		return timeridentity.TimerHandle{}, fmt.Errorf("join timer handle kind %q is invalid", kind)
	}
}

func ActivationKey(stage, joinID, window string) string {
	return ActivationKeyForGeneration(stage, joinID, window, attemptgeneration.Generation{})
}

func ActivationKeyForGeneration(stage, joinID, window string, generation attemptgeneration.Generation) string {
	stage = strings.TrimSpace(stage)
	joinID = strings.TrimSpace(joinID)
	window = strings.TrimSpace(window)
	if stage == "" || joinID == "" {
		return ""
	}
	parts := []string{stage, joinID}
	if window != "" {
		parts = append(parts, window)
	}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	key := strings.Join(parts, ".")
	if suffix := generation.Normalize().KeySuffix(); suffix != "" {
		key += ".generation." + suffix
	}
	return key
}

type CompletionEvaluator func(expression string, joinContext map[string]any) (bool, error)

func CompletionSatisfied(activation Activation, completeWhen string, evaluate CompletionEvaluator) (bool, error) {
	completeWhen = strings.TrimSpace(completeWhen)
	if completeWhen == "" {
		return activation.Completed() == activation.Expected(), nil
	}
	if evaluate == nil {
		return false, fmt.Errorf("join completion evaluator is required")
	}
	return evaluate(completeWhen, activation.Context())
}

func SupportedContextFields() []string {
	return []string{"expected", "completed", "missing", "results", "timed_out"}
}

func (a *Activation) Add(member string, value any) (AddDisposition, error) {
	if a == nil {
		return "", fmt.Errorf("join activation is nil")
	}
	member = strings.TrimSpace(member)
	if !a.HasMember(member) {
		return AddUnexpected, nil
	}
	hash, err := computemodule.CanonicalJSONHash(value)
	if err != nil {
		return "", fmt.Errorf("canonicalize join output: %w", err)
	}
	if existing, ok := a.Outputs[member]; ok {
		if existing.Hash == hash {
			return AddExactDuplicate, nil
		}
		return AddConflictingDuplicate, nil
	}
	if a.Outputs == nil {
		a.Outputs = map[string]MemberOutput{}
	}
	a.Outputs[member] = MemberOutput{Hash: hash, Value: cloneJSONValue(value)}
	return AddAccepted, nil
}

func (a Activation) HasMember(member string) bool {
	member = strings.TrimSpace(member)
	for _, candidate := range a.Members {
		if candidate == member {
			return true
		}
	}
	return false
}

func (a Activation) Completed() int { return len(a.Outputs) }
func (a Activation) Expected() int  { return len(a.Members) }

func (a Activation) Missing() []string {
	out := make([]string, 0, len(a.Members)-len(a.Outputs))
	for _, member := range a.Members {
		if _, ok := a.Outputs[member]; !ok {
			out = append(out, member)
		}
	}
	return out
}

func (a Activation) Results() []any {
	out := make([]any, 0, len(a.Outputs))
	for _, member := range a.Members {
		if output, ok := a.Outputs[member]; ok {
			out = append(out, cloneJSONValue(output.Value))
		}
	}
	return out
}

func (a Activation) Context() map[string]any {
	return map[string]any{
		"expected":  a.Expected(),
		"completed": a.Completed(),
		"missing":   a.Missing(),
		"results":   a.Results(),
		"timed_out": a.CloseReason == CloseReasonTimeout,
	}
}

func (a *Activation) Close(reason CloseReason, outcomePending, outcomeFired bool) bool {
	if a == nil || a.Status == StatusClosed {
		return false
	}
	a.Status = StatusClosed
	a.CloseReason = reason
	a.OutcomePending = outcomePending
	a.OutcomeFired = outcomeFired
	return true
}

func (a *Activation) CloseForStageExit() bool {
	if a == nil {
		return false
	}
	if a.Status == StatusOpen {
		a.Status = StatusClosed
		a.CloseReason = CloseReasonStageExit
		a.OutcomePending = false
		a.OutcomeFired = false
		return true
	}
	if a.Status != StatusClosed || a.CloseReason != CloseReasonComplete || !a.OutcomePending || a.OutcomeFired {
		return false
	}
	// A zero-member completion is closed before its internal event fires. Stage
	// exit supersedes that pending outcome so a delayed event cannot apply it.
	a.CloseReason = CloseReasonStageExit
	a.OutcomePending = false
	return true
}

func Load(stateBuckets map[string]map[string]any, nodeRef runtimeidentity.ExecutableNode, key string) (Activation, bool, error) {
	if !nodeRef.Valid() {
		return Activation{}, false, fmt.Errorf("join load requires exact executable node identity")
	}
	node := stateBuckets[joinNodeBucketKey(nodeRef)]
	joins, _ := node[bucketKey].(map[string]any)
	raw, ok := joins[strings.TrimSpace(key)]
	if !ok {
		return Activation{}, false, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return Activation{}, false, err
	}
	var activation Activation
	if err := decodeStrictJSON(encoded, &activation); err != nil {
		return Activation{}, false, err
	}
	if activation.Outputs == nil {
		activation.Outputs = map[string]MemberOutput{}
	}
	if err := activation.Validate(); err != nil {
		return Activation{}, false, err
	}
	return activation, true, nil
}

func Store(stateBuckets map[string]map[string]any, activation Activation) error {
	if err := activation.Validate(); err != nil {
		return err
	}
	nodeRef := activation.JoinRef().Node()
	if stateBuckets == nil {
		return fmt.Errorf("join state bucket set is nil")
	}
	nodeKey := joinNodeBucketKey(nodeRef)
	node := stateBuckets[nodeKey]
	if node == nil {
		node = map[string]any{}
		stateBuckets[nodeKey] = node
	}
	joins, _ := node[bucketKey].(map[string]any)
	if joins == nil {
		joins = map[string]any{}
		node[bucketKey] = joins
	}
	encoded, err := json.Marshal(activation)
	if err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		return err
	}
	joins[activation.Key()] = raw
	return nil
}

func (a Activation) MarshalJSON() ([]byte, error) {
	return json.Marshal(activationJSON{
		Handle: a.handle, Members: a.Members, Outputs: a.Outputs, Status: a.Status,
		CloseReason: a.CloseReason, ArmedAt: a.ArmedAt, FireAt: a.FireAt,
		TimerCancelled: a.TimerCancelled, OutcomePending: a.OutcomePending,
		OutcomeFired: a.OutcomeFired, CompletionEvent: a.CompletionEvent,
	})
}

func (a *Activation) UnmarshalJSON(raw []byte) error {
	if a == nil {
		return fmt.Errorf("join activation target is nil")
	}
	var persisted activationJSON
	if err := decodeStrictJSON(raw, &persisted); err != nil {
		return err
	}
	*a = Activation{
		handle: persisted.Handle, Members: persisted.Members, Outputs: persisted.Outputs,
		Status: persisted.Status, CloseReason: persisted.CloseReason,
		ArmedAt: persisted.ArmedAt, FireAt: persisted.FireAt,
		TimerCancelled: persisted.TimerCancelled, OutcomePending: persisted.OutcomePending,
		OutcomeFired: persisted.OutcomeFired, CompletionEvent: persisted.CompletionEvent,
	}
	return nil
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

func List(stateBuckets map[string]map[string]any) ([]Activation, error) {
	out := make([]Activation, 0)
	for nodeID, node := range stateBuckets {
		joins, _ := node[bucketKey].(map[string]any)
		for key := range joins {
			encoded, err := json.Marshal(joins[key])
			if err != nil {
				return nil, err
			}
			var activation Activation
			if err := decodeStrictJSON(encoded, &activation); err != nil {
				return nil, fmt.Errorf("decode join activation in bucket %s: %w", nodeID, err)
			}
			if activation.Outputs == nil {
				activation.Outputs = map[string]MemberOutput{}
			}
			if err := activation.Validate(); err != nil {
				return nil, err
			}
			if joinNodeBucketKey(activation.JoinRef().Node()) != nodeID {
				return nil, fmt.Errorf("join activation node identity contradicts its state bucket")
			}
			out = append(out, activation)
		}
	}
	return out, nil
}

func joinNodeBucketKey(node runtimeidentity.ExecutableNode) string {
	if !node.Valid() {
		return ""
	}
	return "handler_joins:" + node.Key()
}

func normalizeMembers(members []string) ([]string, error) {
	out := make([]string, 0, len(members))
	seen := map[string]struct{}{}
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			return nil, fmt.Errorf("join membership contains an empty identity")
		}
		if _, ok := seen[member]; ok {
			return nil, fmt.Errorf("join membership contains duplicate identity %q", member)
		}
		seen[member] = struct{}{}
		out = append(out, member)
	}
	return out, nil
}

func cloneJSONValue(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return value
	}
	return out
}
