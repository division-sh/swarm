package replycontext

import (
	"reflect"
	"testing"
)

func TestIssue642InheritedRequestRetainsLogicalOccurrenceAndReturn(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		retained := a2ReplyOriginRecord(t).Normalized()
		retained.SourceRunID, retained.SourceRequestEventID = "source", "original"
		retained.EnclosingReplyContextID = "own-enclosing"
		if keyed {
			retained.CorrelationKey = "key"
		}
		draft := retained
		draft.ID, draft.RequestEventID = "physical-row", "physical-publication"
		draft.SourceRunID, draft.SourceRequestEventID = draft.RunID, draft.RequestEventID
		draft.EnclosingReplyContextID, draft.ReturnJoins = "", nil
		if !keyed {
			draft.RequestCorrelationID = draft.RequestEventID
		}
		beforeRetained, beforeDraft := retained.Normalized(), draft.Normalized()
		if err := retained.ValidateInheritedRequestPublication(draft); err != nil {
			t.Fatal(err)
		}
		for _, fault := range []string{"run", "origin", "provider", "return_pin", "enclosing", "settled", "correlation"} {
			if fault == "correlation" && !keyed {
				continue
			}
			t.Run(fault, func(t *testing.T) {
				bad := draft.Normalized()
				switch fault {
				case "run":
					bad.RunID = "foreign"
				case "origin":
					bad.Origin.EntityID = "sibling"
				case "provider":
					bad.ProviderFlowID = "other-provider"
				case "return_pin":
					bad.ReplyInputPin = "other-reply"
				case "enclosing":
					bad.EnclosingReplyContextID = "sibling-enclosing"
				case "settled":
					bad.State = StateTerminal
				case "correlation":
					bad.RequestCorrelationID = "wrong-key"
				}
				if err := retained.ValidateInheritedRequestPublication(bad); err == nil {
					t.Fatal("inexact inherited request accepted")
				}
			})
		}
		if !reflect.DeepEqual(beforeRetained, retained) || !reflect.DeepEqual(beforeDraft, draft) {
			t.Fatal("request admission mutated retained or physical evidence")
		}
	}
}
