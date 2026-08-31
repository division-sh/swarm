package replayconformance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const (
	TranscriptVersion = "catalog-replay-transcript/v1"
	ProjectionVersion = "catalog-replay-projection/v1"
	InputRootIngress  = "existing_run_root_ingress"
)

type Identity struct {
	Version            string
	PlatformSpecDigest string
	BundleHash         string
	BundleSource       string
	RunID              string
}

func ValidateIdentity(actual, expected Identity) error {
	if actual.Version != TranscriptVersion {
		return fmt.Errorf("transcript version %q is unsupported", actual.Version)
	}
	if actual.PlatformSpecDigest != expected.PlatformSpecDigest {
		return fmt.Errorf("transcript platform spec digest %q does not match %q", actual.PlatformSpecDigest, expected.PlatformSpecDigest)
	}
	if actual.BundleHash != expected.BundleHash {
		return fmt.Errorf("transcript bundle hash %q does not match %q", actual.BundleHash, expected.BundleHash)
	}
	if actual.BundleSource != expected.BundleSource {
		return fmt.Errorf("transcript bundle source %q does not match %q", actual.BundleSource, expected.BundleSource)
	}
	if actual.RunID != expected.RunID || strings.TrimSpace(actual.RunID) == "" {
		return fmt.Errorf("transcript run_id %q does not match fixed run identity %q", actual.RunID, expected.RunID)
	}
	return nil
}

type RootInput struct {
	Kind        string
	EventID     string
	CreatedAt   time.Time
	EventType   string
	Payload     json.RawMessage
	SourceAgent string
	Accepted    bool
}

func ValidateRootInputs(inputs []RootInput) error {
	if len(inputs) == 0 {
		return fmt.Errorf("transcript root inputs are required")
	}
	seen := map[string]struct{}{}
	for index, input := range inputs {
		if input.Kind != InputRootIngress {
			return fmt.Errorf("transcript input %d kind %q is unsupported", index, input.Kind)
		}
		if strings.TrimSpace(input.EventType) == "" || strings.TrimSpace(input.SourceAgent) == "" || strings.TrimSpace(input.EventID) == "" || input.CreatedAt.IsZero() || len(input.Payload) == 0 {
			return fmt.Errorf("transcript input %d is partially constructed", index)
		}
		if _, err := uuid.Parse(input.EventID); err != nil {
			return fmt.Errorf("transcript input %d event_id is invalid: %w", index, err)
		}
		if _, duplicate := seen[input.EventID]; duplicate {
			return fmt.Errorf("transcript repeats root event %s", input.EventID)
		}
		seen[input.EventID] = struct{}{}
		if _, err := CanonicalJSON(input.Payload); err != nil {
			return fmt.Errorf("transcript input %d payload is invalid: %w", index, err)
		}
	}
	return nil
}

type EventLister interface {
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
}

func LoadOperatorEvents(ctx context.Context, lister EventLister, runID string) (map[string]operatorread.OperatorEventFull, error) {
	if lister == nil {
		return nil, fmt.Errorf("catalog operator event lister is required")
	}
	out := map[string]operatorread.OperatorEventFull{}
	cursor := ""
	seenCursors := map[string]struct{}{}
	for {
		page, err := lister.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: strings.TrimSpace(runID)},
			Limit:  100, Cursor: cursor, Order: "asc", ExcludeRuntimeLogs: true,
		})
		if err != nil {
			return nil, err
		}
		for _, event := range page.Events {
			if _, duplicate := out[event.EventID]; duplicate {
				return nil, fmt.Errorf("operator event pagination repeated %s", event.EventID)
			}
			out[event.EventID] = event
		}
		next := strings.TrimSpace(page.NextCursor)
		if next == "" {
			return out, nil
		}
		if _, cycle := seenCursors[next]; cycle {
			return nil, fmt.Errorf("operator event pagination cursor cycle")
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
}

type projectionEvent struct {
	Key             string                     `json:"key"`
	AdmissionClass  events.EventAdmissionClass `json:"admission_class"`
	EventID         string                     `json:"event_id,omitempty"`
	EventName       string                     `json:"event_name"`
	RunID           string                     `json:"run_id"`
	Parent          string                     `json:"parent,omitempty"`
	TaskID          string                     `json:"task_id,omitempty"`
	EntityID        string                     `json:"entity_id,omitempty"`
	FlowInstance    string                     `json:"flow_instance,omitempty"`
	Scope           events.EventScope          `json:"scope"`
	Payload         json.RawMessage            `json:"payload"`
	ExecutionMode   string                     `json:"execution_mode"`
	ChainDepth      int                        `json:"chain_depth"`
	Producer        string                     `json:"producer"`
	ProducerType    events.EventProducerType   `json:"producer_type"`
	RoutingSource   events.RoutingSource       `json:"routing_source"`
	SourceRoute     events.RouteIdentity       `json:"source_route"`
	TargetRoute     events.RouteIdentity       `json:"target_route"`
	TargetSet       []events.RouteIdentity     `json:"target_set"`
	CreatedAt       *time.Time                 `json:"created_at,omitempty"`
	DeliveryContext json.RawMessage            `json:"delivery_context"`
	Deliveries      []projectionDelivery       `json:"deliveries"`
	DeadLetters     []projectionDeadLetter     `json:"dead_letters"`
}

type projectionDelivery struct {
	Key            string                    `json:"key"`
	SubscriberType string                    `json:"subscriber_type"`
	SubscriberID   string                    `json:"subscriber_id"`
	Route          json.RawMessage           `json:"route"`
	Status         string                    `json:"status"`
	ReasonCode     string                    `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope `json:"failure,omitempty"`
	RetryCount     int                       `json:"retry_count"`
	RetryScheduled bool                      `json:"retry_scheduled"`
	Terminal       bool                      `json:"terminal"`
}

type projectionDeadLetter struct {
	DeliveryKey string                   `json:"delivery_key,omitempty"`
	Failure     runtimefailures.Envelope `json:"failure"`
	RetryCount  int                      `json:"retry_count"`
	ChainDepth  int                      `json:"chain_depth"`
	HandlerNode string                   `json:"handler_node,omitempty"`
}

func Project(eventsByID map[string]operatorread.OperatorEventFull, runID string, roots []RootInput) ([]byte, error) {
	if err := ValidateRootInputs(roots); err != nil {
		return nil, err
	}
	rootByID := make(map[string]RootInput, len(roots))
	included := map[string]struct{}{}
	for _, root := range roots {
		rootByID[root.EventID] = root
		full, exists := eventsByID[root.EventID]
		switch {
		case root.Accepted && !exists:
			return nil, fmt.Errorf("accepted transcript root event %s is missing", root.EventID)
		case !root.Accepted && exists:
			return nil, fmt.Errorf("rejected transcript root event %s was persisted", root.EventID)
		case exists:
			event, err := full.EventSnapshot()
			if err != nil {
				return nil, err
			}
			wantPayload, err := CanonicalJSON(root.Payload)
			if err != nil {
				return nil, err
			}
			gotPayload, err := CanonicalJSON(event.Payload())
			if err != nil {
				return nil, err
			}
			if event.AdmissionClass() != events.EventAdmissionRootIngress || event.ID() != root.EventID ||
				!event.CreatedAt().Equal(root.CreatedAt) || event.RunID() != strings.TrimSpace(runID) ||
				string(event.Type()) != root.EventType || event.SourceAgent() != root.SourceAgent ||
				!bytesEqual(gotPayload, wantPayload) {
				return nil, fmt.Errorf("persisted root event %s does not match the exact transcript input: class=%q/%q id=%q/%q created=%s/%s run=%q/%q type=%q/%q source=%q/%q payload=%s/%s",
					root.EventID, event.AdmissionClass(), events.EventAdmissionRootIngress, event.ID(), root.EventID,
					event.CreatedAt().Format(time.RFC3339Nano), root.CreatedAt.Format(time.RFC3339Nano), event.RunID(), strings.TrimSpace(runID),
					event.Type(), root.EventType, event.SourceAgent(), root.SourceAgent, gotPayload, wantPayload)
			}
			included[root.EventID] = struct{}{}
		}
	}
	for id, full := range eventsByID {
		if _, authored := rootByID[id]; authored || strings.TrimSpace(full.SourceEventID) != "" {
			continue
		}
		event, err := full.EventSnapshot()
		if err != nil {
			return nil, err
		}
		if event.AdmissionClass() == events.EventAdmissionRuntimeControl {
			included[id] = struct{}{}
		}
	}
	for changed := true; changed; {
		changed = false
		for id, event := range eventsByID {
			if _, ok := included[id]; ok {
				continue
			}
			if _, ok := included[strings.TrimSpace(event.SourceEventID)]; ok {
				included[id] = struct{}{}
				changed = true
			}
		}
	}
	for id, event := range eventsByID {
		if _, ok := included[id]; !ok {
			return nil, fmt.Errorf("event %s (%s) with causal parent %s and operator reference %s is outside the fixed transcript frontier", id, event.EventName, strings.TrimSpace(event.SourceEventID), strings.TrimSpace(event.OperatorReferenceEventID))
		}
	}

	keys := map[string]string{}
	identityKeys := map[string]string{}
	visiting := map[string]bool{}
	var eventKey func(string) (string, error)
	eventKey = func(id string) (string, error) {
		if key := keys[id]; key != "" {
			return key, nil
		}
		if visiting[id] {
			return "", fmt.Errorf("causal event cycle at %s", id)
		}
		full, ok := eventsByID[id]
		if !ok {
			return "", fmt.Errorf("causal event %s is missing", id)
		}
		visiting[id] = true
		parentKey := ""
		if parentID := strings.TrimSpace(full.SourceEventID); parentID != "" {
			if _, ok := included[parentID]; !ok {
				return "", fmt.Errorf("causal event %s has parent %s outside the fixed frontier", id, parentID)
			}
			var err error
			parentKey, err = eventKey(parentID)
			if err != nil {
				return "", err
			}
		}
		event, err := full.EventSnapshot()
		if err != nil {
			return "", err
		}
		_, exactRoot := rootByID[id]
		payload, err := canonicalEventPayload(event, exactRoot)
		if err != nil {
			return "", fmt.Errorf("canonicalize event %s identity payload: %w", id, err)
		}
		identity := struct {
			Class    events.EventAdmissionClass `json:"class"`
			Name     string                     `json:"name"`
			Parent   string                     `json:"parent,omitempty"`
			TaskID   string                     `json:"task_id,omitempty"`
			EntityID string                     `json:"entity_id,omitempty"`
			Flow     string                     `json:"flow,omitempty"`
			Payload  json.RawMessage            `json:"payload"`
			Producer string                     `json:"producer"`
			Chain    int                        `json:"chain"`
		}{event.AdmissionClass(), string(event.Type()), parentKey, event.TaskID(), event.EntityID(), event.FlowInstance(), payload, event.SourceAgent(), event.ChainDepth()}
		raw, err := json.Marshal(identity)
		if err != nil {
			return "", err
		}
		if exactRoot {
			keys[id] = "root:" + id
			identityKeys[id] = keys[id]
		} else {
			sum := sha256.Sum256(raw)
			identityKeys[id] = "generated:" + hex.EncodeToString(sum[:])
			keys[id] = identityKeys[id]
			peers := []string{}
			for peerID, peerFull := range eventsByID {
				if peerID == id || strings.TrimSpace(peerFull.SourceEventID) != strings.TrimSpace(full.SourceEventID) {
					continue
				}
				peerEvent, peerErr := peerFull.EventSnapshot()
				if peerErr != nil {
					return "", peerErr
				}
				peerPayload, peerErr := canonicalEventPayload(peerEvent, false)
				if peerErr != nil {
					return "", peerErr
				}
				peerIdentity := struct {
					Class    events.EventAdmissionClass `json:"class"`
					Name     string                     `json:"name"`
					Parent   string                     `json:"parent,omitempty"`
					TaskID   string                     `json:"task_id,omitempty"`
					EntityID string                     `json:"entity_id,omitempty"`
					Flow     string                     `json:"flow,omitempty"`
					Payload  json.RawMessage            `json:"payload"`
					Producer string                     `json:"producer"`
					Chain    int                        `json:"chain"`
				}{peerEvent.AdmissionClass(), string(peerEvent.Type()), parentKey, peerEvent.TaskID(), peerEvent.EntityID(), peerEvent.FlowInstance(), peerPayload, peerEvent.SourceAgent(), peerEvent.ChainDepth()}
				peerRaw, peerErr := json.Marshal(peerIdentity)
				if peerErr != nil {
					return "", peerErr
				}
				if bytesEqual(peerRaw, raw) {
					peers = append(peers, peerID)
				}
			}
			if len(peers) > 0 {
				peers = append(peers, id)
				sort.Slice(peers, func(i, j int) bool {
					left, right := eventsByID[peers[i]], eventsByID[peers[j]]
					if !left.CreatedAt.Equal(right.CreatedAt) {
						return left.CreatedAt.Before(right.CreatedAt)
					}
					return peers[i] < peers[j]
				})
				for index, peerID := range peers {
					if peerID == id {
						keys[id] += fmt.Sprintf(":%d", index+1)
						break
					}
				}
			}
		}
		visiting[id] = false
		return keys[id], nil
	}

	keyOwners := map[string]string{}
	generatedOutcomeFingerprints := map[string]string{}
	projection := make([]projectionEvent, 0, len(included))
	for id := range included {
		key, err := eventKey(id)
		if err != nil {
			return nil, err
		}
		if prior := keyOwners[key]; prior != "" && prior != id {
			return nil, fmt.Errorf("ambiguous causal event projection %s is shared by %s and %s", key, prior, id)
		}
		keyOwners[key] = id
		full := eventsByID[id]
		event, err := full.EventSnapshot()
		if err != nil {
			return nil, err
		}
		parentKey := ""
		if event.ParentEventID() != "" {
			parentKey, err = eventKey(event.ParentEventID())
			if err != nil {
				return nil, err
			}
		}
		deliveryContext, err := json.Marshal(event.DeliveryContext())
		if err != nil {
			return nil, err
		}
		_, exactRoot := rootByID[id]
		payload, err := canonicalEventPayload(event, exactRoot)
		if err != nil {
			return nil, fmt.Errorf("canonicalize event %s payload: %w", id, err)
		}
		item := projectionEvent{
			Key: key, AdmissionClass: event.AdmissionClass(), EventName: string(event.Type()), RunID: event.RunID(),
			Parent: parentKey, TaskID: event.TaskID(), EntityID: event.EntityID(), FlowInstance: event.FlowInstance(), Scope: event.Scope(),
			Payload: payload, ExecutionMode: string(event.ExecutionMode()), ChainDepth: event.ChainDepth(),
			Producer: event.SourceAgent(), ProducerType: event.ProducerType(), RoutingSource: event.RoutingSource(),
			SourceRoute: event.SourceRoute(), TargetRoute: event.TargetRoute(), TargetSet: event.TargetRoutes(),
			DeliveryContext: deliveryContext,
		}
		if exactRoot {
			createdAt := event.CreatedAt().UTC()
			item.EventID = id
			item.CreatedAt = &createdAt
		}
		deliveryKeys := map[string]string{}
		for _, delivery := range full.Deliveries {
			route, err := json.Marshal(delivery.Route)
			if err != nil {
				return nil, err
			}
			projected := projectionDelivery{
				SubscriberType: delivery.SubscriberType, SubscriberID: delivery.SubscriberID, Route: route,
				Status: delivery.Status, ReasonCode: delivery.ReasonCode, Failure: delivery.Failure,
				RetryCount: delivery.RetryCount, RetryScheduled: delivery.RetryScheduled, Terminal: delivery.Terminal,
			}
			raw, err := json.Marshal(projected)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(raw)
			projected.Key = "delivery:" + hex.EncodeToString(sum[:])
			if prior := deliveryKeys[delivery.DeliveryID]; prior != "" && prior != projected.Key {
				return nil, fmt.Errorf("delivery %s has conflicting canonical keys", delivery.DeliveryID)
			}
			deliveryKeys[delivery.DeliveryID] = projected.Key
			item.Deliveries = append(item.Deliveries, projected)
		}
		sort.Slice(item.Deliveries, func(i, j int) bool { return item.Deliveries[i].Key < item.Deliveries[j].Key })
		for _, deadLetter := range full.DeadLetters {
			deliveryKey := deliveryKeys[deadLetter.DeliveryID]
			if strings.TrimSpace(deadLetter.DeliveryID) != "" && deliveryKey == "" {
				return nil, fmt.Errorf("dead letter for event %s references unknown delivery %s", id, deadLetter.DeliveryID)
			}
			item.DeadLetters = append(item.DeadLetters, projectionDeadLetter{
				DeliveryKey: deliveryKey, Failure: deadLetter.Failure, RetryCount: deadLetter.RetryCount,
				ChainDepth: deadLetter.ChainDepth, HandlerNode: deadLetter.HandlerNode,
			})
		}
		sort.Slice(item.DeadLetters, func(i, j int) bool {
			left, _ := json.Marshal(item.DeadLetters[i])
			right, _ := json.Marshal(item.DeadLetters[j])
			return string(left) < string(right)
		})
		if strings.HasPrefix(identityKeys[id], "generated:") {
			fingerprintItem := item
			fingerprintItem.Key = ""
			fingerprintRaw, err := json.Marshal(fingerprintItem)
			if err != nil {
				return nil, err
			}
			fingerprint := string(fingerprintRaw)
			if prior := generatedOutcomeFingerprints[identityKeys[id]]; prior != "" && prior != fingerprint {
				return nil, fmt.Errorf("ambiguous non-isomorphic generated events share identity %s", identityKeys[id])
			}
			generatedOutcomeFingerprints[identityKeys[id]] = fingerprint
		}
		projection = append(projection, item)
	}
	sort.Slice(projection, func(i, j int) bool { return projection[i].Key < projection[j].Key })
	return json.Marshal(struct {
		Version string            `json:"version"`
		Events  []projectionEvent `json:"events"`
	}{Version: ProjectionVersion, Events: projection})
}

func CanonicalJSON(raw []byte) ([]byte, error) {
	var value any
	if len(raw) == 0 {
		raw = []byte("null")
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func canonicalEventPayload(event events.Event, exactRoot bool) ([]byte, error) {
	var value any
	raw := event.Payload()
	if len(raw) == 0 {
		raw = []byte("null")
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if !exactRoot && event.AdmissionClass() == events.EventAdmissionRuntimeDiagnostic {
		if object, ok := value.(map[string]any); ok {
			if _, generatedTime := object["timestamp"]; generatedTime {
				object["timestamp"] = "<generated-event-time>"
			}
		}
	}
	return json.Marshal(value)
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}
