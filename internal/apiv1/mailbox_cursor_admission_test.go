package apiv1

import (
	"context"
	"encoding/base64"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type cursorUncalledNotices struct{ MailboxAPIStore }

func (cursorUncalledNotices) ListV1MailboxItems(context.Context, mailbox.V1ListOptions) ([]mailbox.V1Item, string, error) {
	panic("invalid cursor reached notice fetch")
}

type cursorUncalledCards struct{ decisioncard.Store }

func (cursorUncalledCards) ListDecisionCards(context.Context, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	panic("invalid cursor reached card fetch")
}

func TestMailboxCursorAdmissionBeforeFetch(t *testing.T) {
	valid := decisioncard.EncodeCursor(time.Now(), "opaque")
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{`, `{"card":null}`, `{"card":1}`, `{"card":{}}`, `{"card":""}`, `{"notice":" "}`,
		`{"notice":"bad!"}`, `{"card":"bad!"}`, fmt.Sprintf(`{"card":%q,"unknown":%q}`, valid, valid),
		fmt.Sprintf(`{"card":%q,"card":%q}`, valid, valid), fmt.Sprintf(`{"card":%q,"notice":null}`, valid),
	} {
		t.Run(raw, func(t *testing.T) {
			for _, anchor := range []string{"", "stage_gate"} {
				_, err := listMailboxProjection(context.Background(), Request{Params: map[string]any{
					"cursor": base64.RawURLEncoding.EncodeToString([]byte(raw)), "anchor_kind": anchor,
				}}, DecisionCardHandlerOptions{Mailbox: cursorUncalledNotices{}, Cards: cursorUncalledCards{}})
				if err == nil {
					t.Fatalf("admitted %s with anchor %q", raw, anchor)
				}
			}
		})
	}
}

func TestMailboxReadCapabilitiesNeverFallBackToUntaggedProtocol(t *testing.T) {
	state := newMutatingRuntimeProbeState(t, "mailbox.list")
	for _, missing := range []string{"cards", "notices", "effects"} {
		t.Run(missing, func(t *testing.T) {
			opts := state.options(t)
			switch missing {
			case "cards":
				opts.DecisionCards = nil
			case "notices":
				opts.Mailbox = nil
			case "effects":
				opts.DecisionCards = cursorUncalledCards{Store: opts.DecisionCards}
			}
			handlers := testOperatorHandlers(opts)
			for _, method := range []string{"mailbox.list", "mailbox.get"} {
				if handlers[method] != nil {
					t.Fatalf("%s survived missing %s", method, missing)
				}
			}
		})
	}
}

func TestMailboxCursorOwnershipCensus(t *testing.T) {
	for _, path := range []string{"operator_mailbox.go", "operator_capabilities.go", "../serveapp/main.go", "../store/internal/backend/decisionpersistence/helpers.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			id, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			switch id.Name {
			case "OperatorMailboxHandlers", "MailboxHandlerOptions", "mailboxListResult", "decisionCursor", "encodeDecisionCursor", "decodeDecisionCursor":
				t.Errorf("%s retains obsolete protocol/codec %s", path, id.Name)
			}
			return true
		})
	}
}
