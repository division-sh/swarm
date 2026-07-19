package eventrecord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

var (
	ErrMissing = errors.New("event record missing")
	ErrCorrupt = errors.New("event record corrupt")
)

type MissingError struct{ EventID string }

func (e *MissingError) Error() string {
	return fmt.Sprintf("%s: event_id=%s", ErrMissing, strings.TrimSpace(e.EventID))
}

func (e *MissingError) Unwrap() error { return ErrMissing }

type CorruptError struct {
	EventID string
	Err     error
}

func (e *CorruptError) Error() string {
	if e == nil || e.Err == nil {
		return ErrCorrupt.Error()
	}
	return fmt.Sprintf("%s: event_id=%s: %v", ErrCorrupt, strings.TrimSpace(e.EventID), e.Err)
}

func (e *CorruptError) Unwrap() error {
	if e == nil || e.Err == nil {
		return ErrCorrupt
	}
	return e.Err
}

func (e *CorruptError) Is(target error) bool {
	return target == ErrCorrupt || errors.Is(e.Err, target)
}

func Missing(eventID string) error { return &MissingError{EventID: eventID} }

func Corrupt(eventID string, err error) error {
	return &CorruptError{EventID: eventID, Err: err}
}

// Record is the complete immutable durable representation of one admitted
// event. SQL adapters bind and scan this value; they do not interpret it.
type Record struct {
	Class                      events.EventAdmissionClass
	EventID                    string
	RunID                      string
	EventName                  string
	TaskID                     string
	EntityID                   string
	FlowInstance               string
	Scope                      events.EventScope
	Payload                    []byte
	ExecutionMode              executionmode.Mode
	ChainDepth                 int
	ProducedBy                 string
	ProducedByType             events.EventProducerType
	SourceEventID              string
	CreatedAt                  time.Time
	RoutingSourceKind          events.RoutingSourceKind
	RoutingSourceAuthority     string
	SourceRoute                []byte
	TargetRoute                []byte
	TargetSet                  []byte
	OperatorReferencedEventID  string
	SelectedForkSourceRunID    string
	SelectedForkSourceEventID  string
	SelectedForkAuthorityStamp string
	SelectedForkLineageOwners  int
}

func FromAdmitted(admitted events.AdmittedEvent) (Record, error) {
	event := admitted.Event()
	if err := events.ValidatePersistentEvent(event); err != nil {
		return Record{}, fmt.Errorf("admitted event: %w", err)
	}
	envelope := event.NormalizedEnvelope()
	record := Record{
		Class:                  event.AdmissionClass(),
		EventID:                event.ID(),
		RunID:                  event.RunID(),
		EventName:              string(event.Type()),
		TaskID:                 event.TaskID(),
		EntityID:               envelope.EntityID,
		FlowInstance:           envelope.FlowInstance,
		Scope:                  envelope.Scope,
		Payload:                event.Payload(),
		ExecutionMode:          event.ExecutionMode(),
		ChainDepth:             event.ChainDepth(),
		ProducedBy:             event.Producer().ID(),
		ProducedByType:         event.Producer().Type(),
		SourceEventID:          event.ParentEventID(),
		CreatedAt:              event.CreatedAt().UTC().Truncate(time.Microsecond),
		RoutingSourceKind:      event.RoutingSource().Kind(),
		RoutingSourceAuthority: event.RoutingSource().Authority(),
		SourceRoute:            marshalRoute(event.RoutingSource().Route()),
		TargetRoute:            marshalRoute(envelope.Target),
		TargetSet:              marshalRouteSet(envelope.TargetSet),
	}
	if provenance, ok := event.OperatorReference(); ok {
		record.OperatorReferencedEventID = provenance.ReferencedEventID()
	}
	if lineage, ok := event.SelectedForkLineage(); ok {
		record.SelectedForkSourceRunID = lineage.SourceRunID()
		record.SelectedForkSourceEventID = lineage.SourceEventID()
		record.SelectedForkAuthorityStamp = lineage.AuthorityStamp()
		record.SelectedForkLineageOwners = 1
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	return record.Clone(), nil
}

func (r Record) Clone() Record {
	r.Payload = bytes.Clone(r.Payload)
	r.SourceRoute = bytes.Clone(r.SourceRoute)
	r.TargetRoute = bytes.Clone(r.TargetRoute)
	r.TargetSet = bytes.Clone(r.TargetSet)
	return r
}

func (r Record) Validate() error {
	for field, value := range map[string]string{
		"event_class": string(r.Class), "event_id": r.EventID, "run_id": r.RunID, "event_name": r.EventName,
		"task_id": r.TaskID, "entity_id": r.EntityID, "produced_by": r.ProducedBy,
		"produced_by_type": string(r.ProducedByType), "source_event_id": r.SourceEventID,
		"routing_source_kind": string(r.RoutingSourceKind), "routing_source_authority": r.RoutingSourceAuthority,
		"operator_reference_event_id":   r.OperatorReferencedEventID,
		"selected_fork_source_run_id":   r.SelectedForkSourceRunID,
		"selected_fork_source_event_id": r.SelectedForkSourceEventID,
		"selected_fork_authority_stamp": r.SelectedForkAuthorityStamp,
		"scope":                         string(r.Scope), "execution_mode": string(r.ExecutionMode),
	} {
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("event record %s is not canonical", field)
		}
	}
	if r.FlowInstance != strings.Trim(strings.TrimSpace(r.FlowInstance), "/") {
		return fmt.Errorf("event record flow_instance is not canonical")
	}
	switch r.Class {
	case events.EventAdmissionRootIngress,
		events.EventAdmissionOperatorInjected,
		events.EventAdmissionChild,
		events.EventAdmissionReplay,
		events.EventAdmissionSelectedForkReplay,
		events.EventAdmissionRuntimeControl,
		events.EventAdmissionRuntimeDiagnostic,
		events.EventAdmissionDiagnosticDirect:
	default:
		return fmt.Errorf("event record class %q is invalid", r.Class)
	}
	if strings.TrimSpace(r.EventID) == "" {
		return fmt.Errorf("event record event_id is required")
	}
	if strings.TrimSpace(r.EventName) == "" {
		return fmt.Errorf("event record event_name is required")
	}
	if r.CreatedAt.IsZero() {
		return fmt.Errorf("event record created_at is required")
	}
	_, offset := r.CreatedAt.Zone()
	if offset != 0 || r.CreatedAt.Nanosecond()%1000 != 0 {
		return fmt.Errorf("event record created_at must be canonical UTC microsecond precision")
	}
	if !json.Valid(r.Payload) {
		return fmt.Errorf("event record payload must be valid JSON")
	}
	if r.ChainDepth < 0 {
		return fmt.Errorf("event record chain_depth must be nonnegative")
	}
	if !r.ExecutionMode.Valid() {
		return fmt.Errorf("event record execution_mode must be live or mock")
	}
	producer, err := events.NewProducerIdentity(r.ProducedByType, r.ProducedBy)
	if err != nil {
		return fmt.Errorf("event record producer identity: %w", err)
	}
	if err := events.ValidateEventStructuralContract(r.Class, events.EventType(r.EventName), producer, r.RunID, r.Scope); err != nil {
		return fmt.Errorf("event record identity contract: %w", err)
	}
	if _, err := r.decodeEnvelope(); err != nil {
		return fmt.Errorf("event record envelope: %w", err)
	}
	if err := r.validateClassFacts(); err != nil {
		return err
	}
	return nil
}

func (r Record) validateClassFacts() error {
	runID := strings.TrimSpace(r.RunID)
	parentEventID := strings.TrimSpace(r.SourceEventID)
	operatorReferenceID := strings.TrimSpace(r.OperatorReferencedEventID)
	selectedSourceRunID := strings.TrimSpace(r.SelectedForkSourceRunID)
	selectedSourceEventID := strings.TrimSpace(r.SelectedForkSourceEventID)
	selectedAuthority := strings.TrimSpace(r.SelectedForkAuthorityStamp)

	switch r.Class {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected:
		if runID == "" {
			return fmt.Errorf("event record class %q requires run_id", r.Class)
		}
		if parentEventID != "" {
			return fmt.Errorf("event record class %q cannot carry source_event_id", r.Class)
		}
	case events.EventAdmissionChild, events.EventAdmissionReplay:
		if runID == "" || parentEventID == "" {
			return fmt.Errorf("event record class %q requires run_id and source_event_id", r.Class)
		}
	case events.EventAdmissionSelectedForkReplay:
		if runID == "" || selectedSourceRunID == "" || selectedSourceEventID == "" || selectedAuthority == "" {
			return fmt.Errorf("selected-fork event record requires destination run, source run, source event, and selection authority")
		}
		if parentEventID != "" {
			return fmt.Errorf("selected-fork event record cannot carry generic source_event_id")
		}
		if r.SelectedForkLineageOwners != 1 {
			return fmt.Errorf("selected-fork event record requires exactly one lineage owner; got %d", r.SelectedForkLineageOwners)
		}
	case events.EventAdmissionRuntimeControl, events.EventAdmissionRuntimeDiagnostic, events.EventAdmissionDiagnosticDirect:
		if parentEventID != "" && runID == "" {
			return fmt.Errorf("event record class %q with source_event_id requires run_id", r.Class)
		}
	}

	if r.Class != events.EventAdmissionOperatorInjected && operatorReferenceID != "" {
		return fmt.Errorf("event record class %q cannot carry operator provenance", r.Class)
	}
	if r.Class != events.EventAdmissionSelectedForkReplay &&
		(selectedSourceRunID != "" || selectedSourceEventID != "" || selectedAuthority != "" || r.SelectedForkLineageOwners != 0) {
		return fmt.Errorf("event record class %q cannot carry selected-fork lineage", r.Class)
	}
	return nil
}

func (r Record) Decode() (events.AdmittedEvent, error) {
	admitted, err := r.decode()
	if err != nil {
		return events.AdmittedEvent{}, Corrupt(r.EventID, err)
	}
	return admitted, nil
}

func (r Record) decode() (events.AdmittedEvent, error) {
	if err := r.Validate(); err != nil {
		return events.AdmittedEvent{}, err
	}
	envelope, err := r.decodeEnvelope()
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	routingSourceRoute, err := unmarshalRoute("source_route", r.SourceRoute)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	routingSource, err := events.RestoreRoutingSource(r.RoutingSourceKind, routingSourceRoute, r.RoutingSourceAuthority)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("event record routing source: %w", err)
	}
	facts := events.EventFacts{
		ID:            r.EventID,
		Type:          events.EventType(r.EventName),
		Producer:      events.ProducerClaim{Type: r.ProducedByType, ID: r.ProducedBy},
		TaskID:        r.TaskID,
		Payload:       bytes.Clone(r.Payload),
		ChainDepth:    r.ChainDepth,
		Envelope:      envelope,
		RoutingSource: routingSource,
		CreatedAt:     r.CreatedAt,
		ExecutionMode: r.ExecutionMode,
	}
	var operatorRef *events.OperatorReferenceProvenance
	if referenceID := strings.TrimSpace(r.OperatorReferencedEventID); referenceID != "" {
		value, err := events.NewOperatorReferenceProvenance(referenceID)
		if err != nil {
			return events.AdmittedEvent{}, fmt.Errorf("event record operator provenance: %w", err)
		}
		operatorRef = &value
	}
	var selectedFork *events.SelectedForkLineage
	if r.Class == events.EventAdmissionSelectedForkReplay {
		value, err := events.NewSelectedForkLineage(
			r.RunID,
			r.SelectedForkSourceRunID,
			r.SelectedForkSourceEventID,
			r.SelectedForkAuthorityStamp,
			r.TaskID,
			r.ExecutionMode,
		)
		if err != nil {
			return events.AdmittedEvent{}, fmt.Errorf("event record selected-fork lineage: %w", err)
		}
		selectedFork = &value
	}
	restored, err := events.RestoreAdmittedEvent(events.RestoredEventInput{
		Class:         r.Class,
		Facts:         facts,
		RunID:         r.RunID,
		ParentEventID: r.SourceEventID,
		OperatorRef:   operatorRef,
		SelectedFork:  selectedFork,
	})
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s: %w", strings.TrimSpace(r.EventID), err)
	}
	decoded, err := FromAdmitted(restored)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s: reconstruct durable record: %w", strings.TrimSpace(r.EventID), err)
	}
	if !r.Equal(decoded) {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s changed durable facts", strings.TrimSpace(r.EventID))
	}
	return restored, nil
}

func (r Record) Equal(other Record) bool {
	return r.Class == other.Class &&
		r.EventID == other.EventID &&
		r.RunID == other.RunID &&
		r.EventName == other.EventName &&
		r.TaskID == other.TaskID &&
		r.EntityID == other.EntityID &&
		r.FlowInstance == other.FlowInstance &&
		r.Scope == other.Scope &&
		jsonEqual(r.Payload, other.Payload) &&
		r.ExecutionMode == other.ExecutionMode &&
		r.ChainDepth == other.ChainDepth &&
		r.ProducedBy == other.ProducedBy &&
		r.ProducedByType == other.ProducedByType &&
		r.SourceEventID == other.SourceEventID &&
		r.CreatedAt.Equal(other.CreatedAt) &&
		r.RoutingSourceKind == other.RoutingSourceKind &&
		r.RoutingSourceAuthority == other.RoutingSourceAuthority &&
		jsonEqual(r.SourceRoute, other.SourceRoute) &&
		jsonEqual(r.TargetRoute, other.TargetRoute) &&
		jsonEqual(r.TargetSet, other.TargetSet) &&
		r.OperatorReferencedEventID == other.OperatorReferencedEventID &&
		r.SelectedForkSourceRunID == other.SelectedForkSourceRunID &&
		r.SelectedForkSourceEventID == other.SelectedForkSourceEventID &&
		r.SelectedForkAuthorityStamp == other.SelectedForkAuthorityStamp &&
		r.SelectedForkLineageOwners == other.SelectedForkLineageOwners
}

func (r Record) decodeEnvelope() (events.EventEnvelope, error) {
	source, err := unmarshalRoute("source_route", r.SourceRoute)
	if err != nil {
		return events.EventEnvelope{}, err
	}
	if r.RoutingSourceKind == events.RoutingSourceDeclaredIngress {
		source = events.RouteIdentity{}
	}
	target, err := unmarshalRoute("target_route", r.TargetRoute)
	if err != nil {
		return events.EventEnvelope{}, err
	}
	var targets []events.RouteIdentity
	if len(bytes.TrimSpace(r.TargetSet)) == 0 {
		return events.EventEnvelope{}, fmt.Errorf("target_set is required")
	}
	if err := json.Unmarshal(r.TargetSet, &targets); err != nil {
		return events.EventEnvelope{}, fmt.Errorf("decode target_set: %w", err)
	}
	envelope := events.EventEnvelope{
		EntityID: strings.TrimSpace(r.EntityID), FlowInstance: strings.Trim(strings.TrimSpace(r.FlowInstance), "/"),
		Scope: r.Scope, Source: source, Target: target, TargetSet: targets,
	}
	if err := events.ValidateEnvelope(envelope); err != nil {
		return events.EventEnvelope{}, err
	}
	return envelope.Normalized(), nil
}

func marshalRoute(route events.RouteIdentity) []byte {
	raw, _ := json.Marshal(route.Normalized())
	return raw
}

func marshalRouteSet(routes []events.RouteIdentity) []byte {
	if routes == nil {
		routes = []events.RouteIdentity{}
	}
	raw, _ := json.Marshal(routes)
	return raw
}

func unmarshalRoute(label string, raw []byte) (events.RouteIdentity, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return events.RouteIdentity{}, fmt.Errorf("%s is required", label)
	}
	var route events.RouteIdentity
	if err := json.Unmarshal(raw, &route); err != nil {
		return events.RouteIdentity{}, fmt.Errorf("decode %s: %w", label, err)
	}
	return route.Normalized(), nil
}

func jsonEqual(left, right []byte) bool {
	leftValue, err := decodeJSON(left)
	if err != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	rightValue, err := decodeJSON(right)
	if err != nil {
		return false
	}
	return equalJSONValue(leftValue, rightValue)
}

func decodeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func equalJSONValue(left, right any) bool {
	switch leftValue := left.(type) {
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := new(big.Rat).SetString(leftValue.String())
		rightNumber, rightOK := new(big.Rat).SetString(rightValue.String())
		return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for index := range leftValue {
			if !equalJSONValue(leftValue[index], rightValue[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, item := range leftValue {
			rightItem, exists := rightValue[key]
			if !exists || !equalJSONValue(item, rightItem) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}
