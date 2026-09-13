package apiv1

import (
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func TestDecisionCardRefusalProjectionUsesTypedEvidenceOnly(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{decisioncard.ErrSuperseded, "MAILBOX_CARD_SUPERSEDED"},
		{fmt.Errorf("locked owner: %w", decisioncard.ErrSuperseded), "MAILBOX_CARD_SUPERSEDED"},
		{errors.Join(errors.New("release diagnostic"), decisioncard.ErrSuperseded), "MAILBOX_CARD_SUPERSEDED"},
		{decisioncard.ErrAlreadyTerminal, MailboxAlreadyDecidedCode},
		{fmt.Errorf("former stage superseded: %w", decisioncard.ErrAlreadyTerminal), MailboxAlreadyDecidedCode},
		{decisioncard.ErrDraftNotAuthority, "MAILBOX_INPUT_DRAFT_NOT_AUTHORITY"},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			var app *ApplicationError
			if err := decisionCardAPIError("card", tc.err); !errors.As(err, &app) || app.Code != tc.code {
				t.Fatalf("projection = %v, want %s", err, tc.code)
			}
		})
	}
	for _, text := range []string{"decision card is superseded", "anchor no longer current", "source run superseded"} {
		err := errors.New(text)
		if got := decisionCardAPIError("card", err); got != err {
			t.Fatalf("untyped diagnostic acquired domain authority: %v", got)
		}
	}
}
