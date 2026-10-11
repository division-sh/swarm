package replycontext

import (
	"reflect"
	"testing"
)

func TestIssue642EnclosingReplyIsAnImmutableOwnRowReference(t *testing.T) {
	parent := a2ReplyOriginRecord(t)
	child := parent
	child.ID, child.RequestEventID, child.SourceRequestEventID = "inner", "inner-request", "inner-request"
	child.RequesterFlowID, child.ProviderFlowID, child.ReturnJoins = parent.ProviderFlowID, "inner-provider", nil
	if err := child.BindEnclosing(parent); err != nil || child.EnclosingReplyContextID != parent.ID {
		t.Fatalf("retain exact publishing turn's obligation: %+v %v", child, err)
	}
	if err := child.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Record){
		func(r *Record) { r.EnclosingReplyContextID = "other-parent" },
		func(r *Record) { r.SourceRunID = "other-run" },
		func(r *Record) { r.SourceRequestEventID = "other-request" },
	} {
		changed := child
		mutate(&changed)
		if child.SameIdentity(changed) {
			t.Fatal("changed enclosing/original evidence retained immutable reply identity")
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*Record)
	}{
		{"self", func(r *Record) { r.ID = child.ID }},
		{"foreign_run", func(r *Record) { r.RunID = "foreign"; r.ReturnJoins = nil }},
		{"wrong_provider", func(r *Record) { r.ProviderFlowID = "other" }},
		{"terminal", func(r *Record) {
			r.State = StateTerminal
			r.AcceptedReplyEventID = "reply"
			r.TerminalAt = &r.CreatedAt
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrong := parent
			test.mutate(&wrong)
			before := child
			if err := child.BindEnclosing(wrong); err == nil || !reflect.DeepEqual(child, before) {
				t.Fatal("invalid enclosing admission changed immutable child")
			}
		})
	}
}

func TestIssue642ConcurrentRequestersKeepDistinctEnclosingReferences(t *testing.T) {
	first := a2ReplyOriginRecord(t)
	second := first
	second.ID, second.RequestEventID, second.SourceRequestEventID = "second", "request-2", "request-2"
	second.Origin.FlowInstance = "collector/two"
	second.ReturnJoins = nil
	var nested [2]Record
	for i, enclosing := range []Record{first, second} {
		nested[i] = enclosing
		nested[i].ID += "-inner"
		nested[i].RequestEventID += "-inner"
		nested[i].SourceRequestEventID = nested[i].RequestEventID
		nested[i].RequesterFlowID, nested[i].ProviderFlowID = enclosing.ProviderFlowID, "inner-provider"
		nested[i].ReturnJoins = nil
		if err := nested[i].BindEnclosing(enclosing); err != nil {
			t.Fatal(err)
		}
	}
	if nested[0].EnclosingReplyContextID != first.ID || nested[1].EnclosingReplyContextID != second.ID {
		t.Fatal("same provider collapsed two requesters' enclosing obligations")
	}
	before := nested[0]
	if err := nested[0].BindEnclosing(second); err == nil || !reflect.DeepEqual(before, nested[0]) {
		t.Fatal("provider-global newer requester retargeted an existing nested request")
	}
}
