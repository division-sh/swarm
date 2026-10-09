package runforkpersistence

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type runForkHistoricalReplyFact struct {
	runForkRevisionedFact
	Record replycontext.Record
}

// decodeRunForkHistoricalReplyFact is read-only. The contextual snapshot owner
// appends the returned record and reserves its identity only after admission.
func decodeRunForkHistoricalReplyFact(snapshot *runForkRevisionSnapshot, context runForkHistoricalFactContext, raw []byte) (runForkHistoricalReplyFact, error) {
	record, err := decodeRunForkHistoricalReplyRecord(snapshot, context, raw)
	if err != nil {
		return runForkHistoricalReplyFact{}, fmt.Errorf("decode run fork historical reply context: %w", err)
	}
	return runForkHistoricalReplyFact{
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: context.FirstRevision, Revision: context.Revision},
		Record:                record,
	}, nil
}

func decodeRunForkHistoricalReplyRecord(snapshot *runForkRevisionSnapshot, context runForkHistoricalFactContext, raw []byte) (replycontext.Record, error) {
	if err := validateRunForkHistoricalReplyContext(snapshot, context); err != nil {
		return replycontext.Record{}, err
	}
	if err := validateRunForkHistoricalReplyFactKey(snapshot, context, raw); err != nil {
		return replycontext.Record{}, err
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return replycontext.Record{}, err
	}
	fields, object := value.ObjectMap()
	if !object {
		return replycontext.Record{}, fmt.Errorf("fact must be an object")
	}
	record, err := decodeRunForkHistoricalReplyStringFields(fields)
	if err != nil {
		return replycontext.Record{}, err
	}
	if err := validateRunForkHistoricalReplyIdentity(record, context); err != nil {
		return replycontext.Record{}, err
	}
	if err := decodeRunForkHistoricalReplyOrigin(fields, &record); err != nil {
		return replycontext.Record{}, err
	}
	if err := decodeRunForkHistoricalReplyTimes(fields, &record); err != nil {
		return replycontext.Record{}, err
	}
	if err := validateRunForkHistoricalReplyRecord(record); err != nil {
		return replycontext.Record{}, err
	}
	return record, nil
}

func validateRunForkHistoricalReplyContext(snapshot *runForkRevisionSnapshot, context runForkHistoricalFactContext) error {
	if snapshot == nil || snapshot.RunID == "" || snapshot.Revision <= 0 {
		return fmt.Errorf("selected run and revision are required")
	}
	if _, err := uuid.Parse(snapshot.RunID); err != nil {
		return fmt.Errorf("selected run identity: %w", err)
	}
	if context.Family != runforkrevision.FamilyReplyContexts || context.RunID != snapshot.RunID ||
		context.FirstRevision <= 0 || context.FirstRevision > context.Revision || context.Revision > snapshot.Revision {
		return fmt.Errorf("fact disagrees with selected run/family/revision context")
	}
	return nil
}

func validateRunForkHistoricalReplyFactKey(snapshot *runForkRevisionSnapshot, context runForkHistoricalFactContext, raw []byte) error {
	key, err := runforkrevision.FactKey(context.Family, raw)
	if err != nil {
		return err
	}
	if key != context.Key {
		return fmt.Errorf("ledger key %q disagrees with reply identity %q", context.Key, key)
	}
	if _, duplicate := snapshot.admittedFacts[runForkHistoricalFactKey{family: context.Family, key: key}]; duplicate {
		return fmt.Errorf("duplicate reply context %q", key)
	}
	return nil
}

func decodeRunForkHistoricalReplyStringFields(fields map[string]semanticvalue.Value) (replycontext.Record, error) {
	var record replycontext.Record
	var state string
	stringFields := map[string]*string{
		"reply_context_id": &record.ID, "run_id": &record.RunID,
		"request_event_id": &record.RequestEventID, "requester_flow_id": &record.RequesterFlowID,
		"request_output_pin": &record.RequestOutputPin, "reply_input_pin": &record.ReplyInputPin,
		"provider_flow_id": &record.ProviderFlowID, "provider_input_pin": &record.ProviderInputPin,
		"provider_output_pin": &record.ProviderOutputPin, "request_correlation_id": &record.RequestCorrelationID,
		"correlation_key": &record.CorrelationKey, "state": &state,
		"accepted_reply_event_id": &record.AcceptedReplyEventID,
	}
	for name := range fields {
		switch name {
		case "origin_route", "created_at", "updated_at", "terminal_at":
		default:
			if _, known := stringFields[name]; !known {
				return replycontext.Record{}, fmt.Errorf("undeclared field %q", name)
			}
		}
	}
	for name, target := range stringFields {
		text, ok := fields[name].String()
		if !ok {
			return replycontext.Record{}, fmt.Errorf("%s requires a projected string", name)
		}
		*target = text
	}
	record.State = replycontext.State(state)
	return record, nil
}

func validateRunForkHistoricalReplyIdentity(record replycontext.Record, context runForkHistoricalFactContext) error {
	if record.RunID != context.RunID {
		return fmt.Errorf("owning run disagrees with ledger run")
	}
	for name, id := range map[string]string{
		"request_event_id": record.RequestEventID, "accepted_reply_event_id": record.AcceptedReplyEventID,
	} {
		if name == "accepted_reply_event_id" && id == "" {
			continue
		}
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("%s identity: %w", name, err)
		}
	}
	return nil
}

func decodeRunForkHistoricalReplyOrigin(fields map[string]semanticvalue.Value, record *replycontext.Record) error {
	origin, err := canonicaljson.Encode(fields["origin_route"])
	if err != nil {
		return err
	}
	return record.DecodeOrigin(origin)
}

func decodeRunForkHistoricalReplyTimes(fields map[string]semanticvalue.Value, record *replycontext.Record) error {
	var terminalAt time.Time
	for name, target := range map[string]*time.Time{
		"created_at": &record.CreatedAt, "updated_at": &record.UpdatedAt, "terminal_at": &terminalAt,
	} {
		value, exists := fields[name]
		if !exists {
			return fmt.Errorf("%s is required", name)
		}
		if name == "terminal_at" && value.Kind() == semanticvalue.KindNull {
			continue
		}
		text, ok := value.String()
		if !ok {
			return fmt.Errorf("%s requires a projected timestamp", name)
		}
		decoded, present, err := sqliteTimeValue(text)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if !present || decoded.IsZero() {
			return fmt.Errorf("%s requires a nonzero timestamp", name)
		}
		*target = decoded
	}
	if !terminalAt.IsZero() {
		record.TerminalAt = &terminalAt
	}
	return nil
}

func validateRunForkHistoricalReplyRecord(record replycontext.Record) error {
	// Do not default an absent lifecycle to open through Record.Normalized.
	if record.State == "" {
		return fmt.Errorf("state is required")
	}
	return record.Validate()
}
