package apiv1

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
		{pipeline.ErrGateOfferChanged, BundleUnavailableCode},
		{fmt.Errorf("current offer: %w", pipeline.ErrGateOfferChanged), BundleUnavailableCode},
		{errors.Join(pipeline.ErrGateOfferChanged), BundleUnavailableCode},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			var app *ApplicationError
			if err := decisionCardAPIError("card", tc.err); !errors.As(err, &app) || app.Code != tc.code {
				t.Fatalf("projection = %v, want %s", err, tc.code)
			}
			details, object := app.Details.(map[string]any)
			if tc.code == BundleUnavailableCode && (app.Retryable || !object || len(details) != 0) {
				t.Fatalf("unavailable refusal changed schema or retryability: %+v", app)
			}
		})
	}
	for _, text := range []string{"decision card is superseded", "anchor no longer current", "source run superseded"} {
		err := errors.New(text)
		if got := decisionCardAPIError("card", err); got != err {
			t.Fatalf("untyped diagnostic acquired domain authority: %v", got)
		}
	}
	lookalike := errors.New(pipeline.ErrGateOfferChanged.Error())
	if got := decisionCardAPIError("card", lookalike); got != lookalike {
		t.Fatalf("lookalike offer diagnostic acquired domain authority: %v", got)
	}
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"cancellation", context.Canceled},
		{"release", errors.New("release failure")},
		{"storage", errors.New("storage read failure")},
		{"typed_card", decisioncard.ErrSuperseded},
		{"typed_application", NewApplicationError(MailboxAlreadyDecidedCode, false, map[string]any{})},
		{"typed_selected", &runfork.SelectedForkControlUnsupported{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, original := range []error{
				errors.Join(pipeline.ErrGateOfferChanged, tc.cause),
				fmt.Errorf("outer: %w", errors.Join(tc.cause, pipeline.ErrGateOfferChanged)),
			} {
				if got := decisionCardAPIError("card", original); got != original {
					t.Fatalf("independent failure was consumed by a mapper: got=%v original=%v", got, original)
				}
			}
		})
	}
}
