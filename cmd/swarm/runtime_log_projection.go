package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type runtimeLogEntry struct {
	LogID     string          `json:"log_id"`
	TS        string          `json:"ts"`
	Level     string          `json:"level"`
	Component string          `json:"component"`
	Source    string          `json:"source"`
	RunID     string          `json:"run_id,omitempty"`
	EntityID  string          `json:"entity_id,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
	Failure   json.RawMessage `json:"failure,omitempty"`
	Message   *string         `json:"message"`
	Details   map[string]any  `json:"details,omitempty"`
}

func (e *runtimeLogEntry) UnmarshalJSON(raw []byte) error {
	type wire runtimeLogEntry
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded wire
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := runtimeLogRequireJSONEOF(decoder); err != nil {
		return err
	}
	*e = runtimeLogEntry(decoded)
	return nil
}

type runtimeLogSemanticProjection struct {
	Action          string
	Failure         string
	Message         string
	MessageVisible  bool
	ResidualDetails map[string]any
}

type runtimeLogFailureEvidenceError struct {
	prefix string
	logID  string
	raw    string
	cause  error
}

func (e *runtimeLogFailureEvidenceError) Error() string {
	if e == nil {
		return ""
	}
	subject := strings.TrimSpace(e.prefix)
	if subject == "" {
		subject = "runtime log"
	}
	if logID := strings.TrimSpace(e.logID); logID != "" {
		subject += " " + logID
	}
	return fmt.Sprintf("WARNING: %s contains malformed failure evidence: %s", subject, e.raw)
}

func (e *runtimeLogFailureEvidenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type runtimeLogMessageTuple struct {
	Component string
	Action    string
	Message   string
}

var runtimeLogRedundantMessageTuples = map[runtimeLogMessageTuple]struct{}{
	{
		Component: "eventbus",
		Action:    "published",
		Message:   "Event was published to the event bus",
	}: {},
}

func projectRuntimeLogEntries(prefix string, logs []runtimeLogEntry) ([]runtimeLogSemanticProjection, error) {
	projections := make([]runtimeLogSemanticProjection, 0, len(logs))
	for i, log := range logs {
		projection, err := projectRuntimeLogEntry(fmt.Sprintf("%s[%d]", prefix, i), log)
		if err != nil {
			return nil, err
		}
		projections = append(projections, projection)
	}
	return projections, nil
}

func projectRuntimeLogEntry(prefix string, log runtimeLogEntry) (runtimeLogSemanticProjection, error) {
	projection := runtimeLogSemanticProjection{
		Action:  runtimeLogAction(log),
		Message: runtimeLogMessage(log),
	}
	projection.MessageVisible = projection.Message != "" && !runtimeLogMessageIsRedundant(log)

	rawFailure := bytes.TrimSpace(log.Failure)
	if len(rawFailure) > 0 {
		failure, err := runtimefailures.UnmarshalEnvelope(rawFailure)
		if err != nil {
			return runtimeLogSemanticProjection{}, &runtimeLogFailureEvidenceError{
				prefix: prefix,
				logID:  log.LogID,
				raw:    runtimeLogCompactRawJSON(rawFailure),
				cause:  err,
			}
		}
		projection.Failure = runtimeLogFailureSummary(failure)
	}

	projection.ResidualDetails = runtimeLogResidualDetails(log, projection)
	return projection, nil
}

func runtimeLogMessageIsRedundant(log runtimeLogEntry) bool {
	if log.Message == nil || len(log.Details) == 0 {
		return false
	}
	rawAction, ok := log.Details["action"].(string)
	if !ok {
		return false
	}
	_, ok = runtimeLogRedundantMessageTuples[runtimeLogMessageTuple{
		Component: log.Component,
		Action:    rawAction,
		Message:   *log.Message,
	}]
	return ok
}

func runtimeLogFailureSummary(failure runtimefailures.Envelope) string {
	class := strings.TrimPrefix(strings.TrimSpace(string(failure.Class)), "platform.")
	return class + "/" + strings.TrimSpace(failure.Detail.Code)
}

func runtimeLogResidualDetails(log runtimeLogEntry, projection runtimeLogSemanticProjection) map[string]any {
	if len(log.Details) == 0 {
		return nil
	}
	residual := make(map[string]any, len(log.Details))
	for key, value := range log.Details {
		residual[key] = value
	}

	runtimeLogRemoveEquivalentString(residual, "component", log.Component)
	if projection.Action != "" {
		runtimeLogRemoveEquivalentString(residual, "action", projection.Action)
	}
	runtimeLogRemoveEquivalentString(residual, "agent_id", log.Source)
	runtimeLogRemoveEquivalentString(residual, "run_id", log.RunID)
	runtimeLogRemoveEquivalentString(residual, "entity_id", log.EntityID)
	runtimeLogRemoveEquivalentString(residual, "session_id", log.SessionID)

	rawFailure := bytes.TrimSpace(log.Failure)
	if len(rawFailure) > 0 {
		if detailFailure, ok := residual["failure"]; ok && runtimeLogJSONEquivalent(rawFailure, detailFailure) {
			delete(residual, "failure")
		}
	}
	if len(residual) == 0 {
		return nil
	}
	return residual
}

func runtimeLogRemoveEquivalentString(details map[string]any, key, expected string) {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return
	}
	value, ok := details[key].(string)
	if !ok || strings.TrimSpace(value) != expected {
		return
	}
	delete(details, key)
}

func runtimeLogJSONEquivalent(raw json.RawMessage, value any) bool {
	left, err := runtimeLogDecodeJSONValue(raw)
	if err != nil {
		return false
	}
	rightRaw, err := json.Marshal(value)
	if err != nil {
		return false
	}
	right, err := runtimeLogDecodeJSONValue(rightRaw)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func runtimeLogDecodeJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := runtimeLogRequireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return value, nil
}

func runtimeLogRequireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func runtimeLogCompactRawJSON(raw []byte) string {
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err == nil {
		return compact.String()
	}
	return strings.TrimSpace(string(raw))
}
