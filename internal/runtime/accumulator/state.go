package accumulator

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type State struct {
	Received   map[string]string `json:"received,omitempty"`
	Deliveries map[string]string `json:"deliveries,omitempty"`
	Items      []map[string]any  `json:"items,omitempty"`
	loadError  error
}

func (s *State) Err() error {
	if s == nil {
		return nil
	}
	return s.loadError
}

// Admit checks every refusal before changing either receipts or business items.
// Delivery receipts are transport idempotency, not implicit business dedup keys.
func (s *State) Admit(spec *runtimecontracts.AccumulateSpec, payload map[string]any, deliveryID string) (duplicate bool, err error) {
	return s.admit(spec, payload, deliveryID, false)
}

// Preview evaluates a hypothetical arrival without minting transport evidence.
// The caller owns a private snapshot; this state must never be persisted.
func (s *State) Preview(spec *runtimecontracts.AccumulateSpec, payload map[string]any) (duplicate bool, err error) {
	return s.admit(spec, payload, "", true)
}

func (s *State) admit(spec *runtimecontracts.AccumulateSpec, payload map[string]any, deliveryID string, preview bool) (duplicate bool, err error) {
	if s.loadError != nil {
		return false, s.loadError
	}
	var key string
	keyed := spec != nil && spec.Key != ""
	if keyed {
		var err error
		key, err = s.admitBusinessKey(spec.Key, payload)
		if err != nil {
			return false, err
		}
	} else {
		if len(s.Received) != 0 {
			return false, invalidState("unkeyed receipt mode")
		}
		if deliveryID == "" && !preview {
			return false, failures.New(failures.ClassLifecycleConflict, "accumulator_delivery_required", "accumulator", "admit", nil)
		}
	}
	if payload == nil {
		return false, failures.New(failures.ClassSchemaInvalid, "accumulator_payload_invalid", "accumulator", "admit", nil)
	}
	canonical, err := canonicaljson.Bytes(payload)
	if err != nil {
		return false, failures.Wrap(failures.ClassSchemaInvalid, "accumulator_payload_invalid", "accumulator", "admit", nil, err)
	}
	if preview && !keyed {
		s.Items = append(s.Items, cloneObject(payload))
		return false, nil
	}
	hash := canonicaljson.HashBytes(canonical)
	identity, receipts := key, s.Received
	if !keyed {
		identity, receipts = deliveryID, s.Deliveries
	}
	if previous, exists := receipts[identity]; exists {
		if previous != hash {
			return false, failures.New(failures.ClassConflictingDuplicate, "accumulator_payload_conflict", "accumulator", "admit", nil)
		}
		return true, nil
	}
	if receipts == nil {
		receipts = map[string]string{}
		if keyed {
			s.Received = receipts
		} else {
			s.Deliveries = receipts
		}
	}
	receipts[identity] = hash
	s.Items = append(s.Items, cloneObject(payload))
	return false, nil
}

func (s *State) admitBusinessKey(selector string, payload map[string]any) (string, error) {
	path, err := keyPath(selector)
	if err != nil {
		return "", err
	}
	if len(s.Deliveries) != 0 {
		return "", invalidState("keyed receipt mode")
	}
	storedKeys := make(map[string]bool, len(s.Items))
	for _, item := range s.Items {
		storedKey, ok := payloadKey(item, path.Segments)
		if !ok || storedKeys[storedKey] || s.Received[storedKey] == "" {
			return "", invalidState("business key receipt")
		}
		canonical, err := canonicaljson.Bytes(item)
		if err != nil || s.Received[storedKey] != canonicaljson.HashBytes(canonical) {
			return "", invalidState("business key payload hash")
		}
		storedKeys[storedKey] = true
	}
	if len(storedKeys) != len(s.Received) {
		return "", invalidState("business key receipt count")
	}
	key, ok := payloadKey(payload, path.Segments)
	if !ok {
		return "", failures.New(failures.ClassSchemaInvalid, "accumulator_key_invalid", "accumulator", "admit", map[string]any{"key": selector})
	}
	return key, nil
}

func payloadKey(payload map[string]any, segments []string) (string, bool) {
	var value any = payload
	for _, segment := range segments {
		object, ok := value.(map[string]any)
		if !ok {
			return "", false
		}
		value = object[segment]
	}
	key, ok := value.(string)
	return key, ok && key != ""
}

// Load refuses old boolean receipts rather than inventing payload equality.
func Load(raw map[string]any) *State {
	s := &State{Received: map[string]string{}, Deliveries: map[string]string{}}
	for name, target := range map[string]map[string]string{"received": s.Received, "deliveries": s.Deliveries} {
		value, present := raw[name]
		if !present {
			continue
		}
		object, ok := value.(map[string]any)
		if !ok {
			s.loadError = invalidState(name)
			return s
		}
		for key, value := range object {
			hash, ok := value.(string)
			decoded, err := hex.DecodeString(strings.TrimPrefix(hash, "sha256:"))
			if !ok || key == "" || err != nil || len(decoded) != 32 || "sha256:"+hex.EncodeToString(decoded) != hash {
				s.loadError = invalidState(name)
				return s
			}
			target[key] = hash
		}
	}
	switch items := raw["items"].(type) {
	case nil:
	case []map[string]any:
		for _, item := range items {
			s.Items = append(s.Items, cloneObject(item))
		}
	case []any:
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok {
				s.loadError = invalidState("items")
				return s
			}
			s.Items = append(s.Items, cloneObject(object))
		}
	default:
		s.loadError = invalidState("items")
	}
	if s.loadError == nil {
		s.loadError = s.validateEvidence()
	}
	return s
}

func (s *State) validateEvidence() error {
	if len(s.Received)+len(s.Deliveries) != len(s.Items) {
		return invalidState("receipt count")
	}
	hashes := map[string]int{}
	for _, item := range s.Items {
		canonical, err := canonicaljson.Bytes(item)
		if err != nil {
			return invalidState("item payload")
		}
		hashes[canonicaljson.HashBytes(canonical)]++
	}
	for _, receipts := range []map[string]string{s.Received, s.Deliveries} {
		for _, hash := range receipts {
			if hashes[hash] == 0 {
				return invalidState("payload hash")
			}
			hashes[hash]--
		}
	}
	return nil
}

func invalidState(field string) error {
	return failures.Wrap(failures.ClassSchemaInvalid, "accumulator_state_invalid", "accumulator", "load", nil, fmt.Errorf("accumulator %s has no canonical payload evidence", field))
}

func cloneObject(object map[string]any) map[string]any {
	result := make(map[string]any, len(object))
	for key, value := range object {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneObject(value)
	case []any:
		result := make([]any, len(value))
		for index, item := range value {
			result[index] = cloneValue(item)
		}
		return result
	default:
		return value
	}
}
