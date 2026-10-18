package pipeline

import (
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
)

func ActivityRequestResultPublications(request events.Event) ([]activityresult.Record, error) {
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return nil, err
	}
	var out []activityresult.Record
	for _, declaration := range []string{intent.SuccessEvent, intent.FailureEvent} {
		publication, err := admitActivityPublication(intent, declaration)
		if err != nil {
			return nil, err
		}
		out = append(out, activityresult.Record{EventID: publication.eventID, EventType: string(publication.eventType)})
	}
	return out, nil
}
