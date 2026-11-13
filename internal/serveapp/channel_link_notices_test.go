//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/google/uuid"
)

type linkNoticeMailbox struct {
	tools.MailboxPersistence
	requests []tools.MailboxItem
	create   []bool
	failure  error
}

func (m *linkNoticeMailbox) InsertMailboxItem(ctx context.Context, item tools.MailboxItem) (string, error) {
	_, _, create := render.LinkNoticeBinding(ctx)
	m.requests, m.create = append(m.requests, item), append(m.create, create)
	return item.ID, m.failure
}

type linkNoticeDeliveryStore struct {
	render.Store
	parent channelnative.TransportParent
}

func (s linkNoticeDeliveryStore) CurrentChannelDeliveryParent(context.Context) (channelnative.TransportParent, bool, error) {
	return s.parent, true, nil
}

// Invariant: unknown creation keeps its identity; changed state only reads it, and successful publication alone clears an outage.
func TestChannelNoticeLinkWorkerCustodyAndCurrentness(t *testing.T) {
	now := time.Unix(100000, 0)
	op := channelonboarding.Operation{OperationID: uuid.NewString(), SessionConnectionID: uuid.NewString()}
	owner := &recoveryClockOwner{at: now, op: op}
	selected := channelonboarding.Result{Operation: channelonboarding.Operation{OperationID: uuid.NewString(), Provider: "telegram", AccountReference: "420079"},
		Binding: &operatorchannel.Binding{Status: operatorchannel.BindingCurrent, Revision: 1}}
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration, ID: selected.Operation.OperationID, Revision: 1,
		BindingOperationID: uuid.NewString(), BindingOperationRevision: 1}
	d := &serveChannelDeliveryDispatcher{sessions: owner, now: func() time.Time { return now }, sessionStates: map[string]string{},
		store: linkNoticeDeliveryStore{parent: parent}, deployment: &serveDeploymentTransport{}}
	d.deployment.read = func(context.Context, channelnative.TransportParent) (channelonboarding.Result, packs.OutboundBindingPlan, error) {
		return selected, packs.OutboundBindingPlan{}, nil
	}
	mailbox := &linkNoticeMailbox{failure: errors.New("unknown commit response")}
	n := &serveLinkNotices{dispatcher: d, hitl: channelonboarding.AccountReference{Provider: "telegram", Account: "420079"}, mailbox: mailbox,
		entries: map[string]*linkNoticeEntry{}, items: []serveSessionDiagnostic{{Operation: op, Fact: sessionprovider.SessionDiagnostic{Down: true, Socket: "reset"}}}}
	n.items[0].Fact.Down = false
	d.sessionStates[op.OperationID] = "disconnected"
	n.observe(op, nil, time.Time{})
	if n.entries[op.OperationID].drops != 0 {
		t.Fatal("presentation alone invented a drop")
	}
	n.items[0].Fact.Down = true
	poll := func() error {
		if !owner.connected && owner.failure == nil {
			d.sessionStates[op.OperationID] = "disconnected"
		}
		n.observe(op, owner.failure, owner.at)
		return n.reconcile(t.Context(), nil)
	}
	if poll() == nil || len(mailbox.requests) != 1 {
		t.Fatal("unknown response lost", mailbox.requests)
	}
	first := mailbox.requests[0]
	if poll() == nil || mailbox.requests[1].ID != first.ID {
		t.Fatal("unknown response allocated another identity")
	}
	owner.at, owner.connected = time.Time{}, true
	n.items[0].Fact = sessionprovider.SessionDiagnostic{}
	d.sessionStates[op.OperationID] = "recovered"
	mailbox.failure = nil
	if err := poll(); err != nil {
		t.Fatal(err)
	}
	if n.entries[op.OperationID].down != true {
		t.Fatal("intermediate recovery cleared before publication")
	}
	d.sessionStates[op.OperationID] = "connected"
	now = now.Add(time.Second)
	if err := poll(); err != nil {
		t.Fatal(err)
	}
	if n.entries[op.OperationID].down || mailbox.requests[len(mailbox.requests)-1].Summary != "Swarm Notice: WhatsApp link back after 1s, 0 more drops in between" {
		t.Fatal("real clear missing")
	}
	// A changed destination cannot create the old pending text even after an unknown response.
	owner.connected, owner.at = false, now
	n.items[0].Fact = sessionprovider.SessionDiagnostic{Down: true, Socket: "reset"}
	now = now.Add(10 * time.Minute)
	mailbox.failure = errors.New("unknown second response")
	if poll() == nil {
		t.Fatal("unknown response lost")
	}
	before := len(mailbox.requests)
	selected.Binding = &operatorchannel.Binding{Status: operatorchannel.BindingCurrent, Revision: 2}
	mailbox.failure = render.ErrLinkNoticeAbsent
	if err := poll(); err != nil || mailbox.create[before] || n.entries[op.OperationID].pending != nil {
		t.Fatal("stale destination was recreated", err)
	}
	owner.failure = context.Canceled
	before = len(mailbox.requests)
	if err := poll(); err != nil || len(mailbox.requests) != before || n.entries[op.OperationID].valid {
		t.Fatal("caller cancellation became a notice")
	}
	back := linkNoticeEvent{kind: "back", since: now, upAt: now.Add(4 * time.Minute), extra: 2, cause: "connection reset"}
	if got := linkNoticeText(back, true); got != "Swarm Notice: WhatsApp link back after 4m0s, 2 more drops in between; cause: connection reset" {
		t.Fatal("R199 self-link cause form changed", got)
	}
	if got := linkNoticeText(back, false); got != "Swarm Notice: WhatsApp link back after 4m0s, 2 more drops in between" {
		t.Fatal("R199 changed the ordinary back form", got)
	}
	n.capture(nil)
	if n.entries[op.OperationID].valid {
		t.Fatal("retired observation remained executable")
	}
	selected.Operation = op
	selected.Operation.Provider, selected.Operation.AccountReference = "whatsapp", "+15550000000"
	n.hitl = channelonboarding.AccountReference{Provider: "whatsapp", Account: "+15550000000"}
	self := &linkNoticeEntry{valid: true}
	n.entries[op.OperationID] = self
	self.observe(now, false, true, "logged out")
	d.sessionStates[op.OperationID] = "connected" // A terminal refusal leaves this old presentation behind.
	if target, err := n.destination(t.Context()); err != nil || target.Binding != nil {
		t.Fatal("failed self-link retained notice admission", err)
	}
	now = now.Add(time.Second)
	self.observe(now, true, false, "")
	mailbox.failure = nil
	before = len(mailbox.requests)
	if err := n.reconcile(t.Context(), nil); err != nil || len(mailbox.requests) != before+1 || self.next(now) != nil || self.lastBack != now || self.reported != 1 {
		t.Fatal("R199 summary was not admitted exactly once after recovery", err)
	}
	if got := mailbox.requests[before].Summary; got != "Swarm Notice: WhatsApp link back after 1s, 0 more drops in between; cause: logged out" {
		t.Fatal("R199 lost the captured cause", got)
	}
	if err := n.reconcile(t.Context(), nil); err != nil || len(mailbox.requests) != before+1 {
		t.Fatal("R199 repeated the same recovery notice", err)
	}
}
