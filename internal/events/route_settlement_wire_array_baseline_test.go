package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Benchmark-only snapshot of the qualified token-field codec immediately before
// streaming the plans array. The original two-pass oracle remains unchanged.
type settlementPlanWireBeforeArray settlementPlanWire

type settlementLedgerWireBeforeArray struct {
	Plans []settlementPlanWireBeforeArray `json:"plans"`
}

func (w *settlementLedgerWireBeforeArray) UnmarshalJSON(raw []byte) error {
	var decoded settlementLedgerWireBeforeArray
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("route settlement evaluation must be an object")
	}
	plansPresent := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || !strings.EqualFold(key, "plans") {
			return fmt.Errorf("json: unknown field %q", key)
		}
		if err := decoder.Decode(&decoded.Plans); err != nil {
			return err
		}
		if key == "plans" {
			plansPresent = decoded.Plans != nil
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if err := settlementWireBeforeEOF(decoder); err != nil {
		return err
	}
	if !plansPresent {
		return fmt.Errorf("route settlement evaluation plans are required")
	}
	*w = decoded
	return nil
}

func (w *settlementPlanWireBeforeArray) UnmarshalJSON(raw []byte) error {
	var decoded settlementPlanWireBeforeArray
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("route settlement plan must be an object")
	}
	targetsPresent, candidatesPresent := false, false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("route settlement plan field must be a string")
		}
		switch {
		case strings.EqualFold(key, "plan_sha256"):
			err = decoder.Decode(&decoded.PlanID)
		case strings.EqualFold(key, "resolution"):
			err = decoder.Decode(&decoded.Resolution)
		case strings.EqualFold(key, "targets"):
			err = decoder.Decode(&decoded.Targets)
			if key == "targets" {
				targetsPresent = decoded.Targets != nil
			}
		case strings.EqualFold(key, "candidates"):
			err = decoder.Decode(&decoded.Candidates)
			if key == "candidates" {
				candidatesPresent = decoded.Candidates != nil
			}
		default:
			return fmt.Errorf("json: unknown field %q", key)
		}
		if err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if err := settlementWireBeforeEOF(decoder); err != nil {
		return err
	}
	if !targetsPresent {
		return fmt.Errorf("route settlement plan targets are required")
	}
	if !candidatesPresent {
		return fmt.Errorf("route settlement plan candidates are required")
	}
	*w = decoded
	return nil
}
