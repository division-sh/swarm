package replycontext

import (
	"testing"
	"time"
)

func TestIssue642ReplyContinuationPreservesExactBindingAndSettlement(t *testing.T) {
	open := a2ReplyOriginRecord(t)
	open.CreatedAt, open.UpdatedAt = time.Unix(100, 0).UTC(), time.Unix(101, 0).UTC()
	terminal := open
	at := open.UpdatedAt.Add(time.Second)
	terminal.State, terminal.AcceptedReplyEventID = StateTerminal, "accepted"
	terminal.UpdatedAt, terminal.TerminalAt = at, &at
	for _, pair := range [][2]Record{{open, open}, {terminal, open}, {terminal, terminal}} {
		if err := pair[0].ValidateContinuationFrom(pair[1], open.UpdatedAt); err != nil {
			t.Fatalf("lawful unchanged/once-settled continuation refused: %v", err)
		}
	}
	for _, test := range []struct {
		name   string
		before Record
		after  Record
		change func(*Record)
	}{
		{"reopened", terminal, open, nil},
		{"other_run", open, terminal, func(r *Record) { r.RunID = "other"; r.ReturnJoins = nil }},
		{"other_original", open, terminal, func(r *Record) { r.SourceRequestEventID = "other" }},
		{"other_enclosing", open, terminal, func(r *Record) { r.EnclosingReplyContextID = "other" }},
		{"other_origin", open, terminal, func(r *Record) { r.Origin.FlowInstance = "other"; r.ReturnJoins = nil }},
		{"rewritten_birth", open, terminal, func(r *Record) { r.CreatedAt = r.CreatedAt.Add(time.Second) }},
		{"early_update", open, terminal, func(r *Record) { r.UpdatedAt = open.CreatedAt }},
		{"early_settlement", open, terminal, func(r *Record) { at := open.CreatedAt; r.TerminalAt = &at }},
		{"split_update", open, terminal, func(r *Record) { r.UpdatedAt = r.UpdatedAt.Add(time.Second) }},
		{"open_rewrite", open, open, func(r *Record) { r.UpdatedAt = r.UpdatedAt.Add(time.Second) }},
		{"second_reply", terminal, terminal, func(r *Record) { r.AcceptedReplyEventID = "second" }},
		{"terminal_rewrite", terminal, terminal, func(r *Record) { at := r.UpdatedAt.Add(time.Second); r.UpdatedAt, r.TerminalAt = at, &at }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.change != nil {
				test.change(&test.after)
			}
			if err := test.after.ValidateContinuationFrom(test.before, open.UpdatedAt); err == nil {
				t.Fatal("continuation accepted contradictory inherited evidence")
			}
		})
	}
	if err := terminal.ValidateContinuationFrom(open, terminal.UpdatedAt.Add(time.Second)); err == nil {
		t.Fatal("new reply settlement precedes child admission")
	}
	if err := terminal.ValidateContinuationFrom(terminal, terminal.UpdatedAt.Add(time.Second)); err != nil {
		t.Fatalf("unchanged inherited terminal evidence requires no new settlement: %v", err)
	}
}
