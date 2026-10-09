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
	fail := func(err error) (runForkHistoricalReplyFact, error) {
		return runForkHistoricalReplyFact{}, fmt.Errorf("decode run fork historical reply context: %w", err)
	}
	if snapshot == nil || snapshot.RunID == "" || snapshot.Revision <= 0 {
		return fail(fmt.Errorf("selected run and revision are required"))
	}
	if _, err := uuid.Parse(snapshot.RunID); err != nil {
		return fail(fmt.Errorf("selected run identity: %w", err))
	}
	if context.Family != runforkrevision.FamilyReplyContexts || context.RunID != snapshot.RunID ||
		context.FirstRevision <= 0 || context.FirstRevision > context.Revision || context.Revision > snapshot.Revision {
		return fail(fmt.Errorf("fact disagrees with selected run/family/revision context"))
	}
	key, err := runforkrevision.FactKey(context.Family, raw)
	if err != nil {
		return fail(err)
	}
	if key != context.Key {
		return fail(fmt.Errorf("ledger key %q disagrees with reply identity %q", context.Key, key))
	}
	if _, duplicate := snapshot.admittedFacts[runForkHistoricalFactKey{family: context.Family, key: key}]; duplicate {
		return fail(fmt.Errorf("duplicate reply context %q", key))
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return fail(err)
	}
	fields, object := value.ObjectMap()
	if !object {
		return fail(fmt.Errorf("fact must be an object"))
	}
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
				return fail(fmt.Errorf("undeclared field %q", name))
			}
		}
	}
	for name, target := range stringFields {
		text, ok := fields[name].String()
		if !ok {
			return fail(fmt.Errorf("%s requires a projected string", name))
		}
		*target = text
	}
	record.State = replycontext.State(state)
	if record.RunID != context.RunID {
		return fail(fmt.Errorf("owning run disagrees with ledger run"))
	}
	for name, id := range map[string]string{
		"request_event_id": record.RequestEventID, "accepted_reply_event_id": record.AcceptedReplyEventID,
	} {
		if name == "accepted_reply_event_id" && id == "" {
			continue
		}
		if _, err := uuid.Parse(id); err != nil {
			return fail(fmt.Errorf("%s identity: %w", name, err))
		}
	}
	origin, err := canonicaljson.Encode(fields["origin_route"])
	if err != nil {
		return fail(err)
	}
	if err := record.DecodeOrigin(origin); err != nil {
		return fail(err)
	}
	var terminalAt time.Time
	for name, target := range map[string]*time.Time{
		"created_at": &record.CreatedAt, "updated_at": &record.UpdatedAt, "terminal_at": &terminalAt,
	} {
		value, exists := fields[name]
		if !exists {
			return fail(fmt.Errorf("%s is required", name))
		}
		if name == "terminal_at" && value.Kind() == semanticvalue.KindNull {
			continue
		}
		text, ok := value.String()
		if !ok {
			return fail(fmt.Errorf("%s requires a projected timestamp", name))
		}
		decoded, present, err := sqliteTimeValue(text)
		if err != nil {
			return fail(fmt.Errorf("%s: %w", name, err))
		}
		if !present || decoded.IsZero() {
			return fail(fmt.Errorf("%s requires a nonzero timestamp", name))
		}
		*target = decoded
	}
	if !terminalAt.IsZero() {
		record.TerminalAt = &terminalAt
	}
	// Do not default an absent lifecycle to open through Record.Normalized.
	if record.State == "" {
		return fail(fmt.Errorf("state is required"))
	}
	if err := record.Validate(); err != nil {
		return fail(err)
	}
	return runForkHistoricalReplyFact{
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: context.FirstRevision, Revision: context.Revision},
		Record:                record,
	}, nil
}
