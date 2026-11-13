package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type linkNoticePending struct {
	item    tools.MailboxItem
	binding operatorchannel.Binding
	event   linkNoticeEvent
}

type linkNoticeEntry struct {
	linkNoticeState
	connection string
	account    operatorchannel.SessionAccountAdmission
	valid      bool
	pending    *linkNoticePending
}

type serveLinkNotices struct {
	dispatcher *serveChannelDeliveryDispatcher
	hitl       channelonboarding.AccountReference
	mailbox    tools.MailboxPersistence
	items      []serveSessionDiagnostic
	entries    map[string]*linkNoticeEntry
}

func (n *serveLinkNotices) capture(items []serveSessionDiagnostic) {
	n.items = items
	for _, entry := range n.entries {
		entry.valid = false
	}
}

func (n *serveLinkNotices) destination(ctx context.Context) (channelonboarding.Result, error) {
	d := n.dispatcher
	parent, found, err := d.store.CurrentChannelDeliveryParent(ctx)
	if err != nil || !found {
		return channelonboarding.Result{}, err
	}
	var selected channelonboarding.Result
	if parent.Kind == channelnative.TransportDeploymentRegistration {
		selected, _, err = d.deployment.selectCurrent(ctx, parent)
	} else {
		var activations []channelonboarding.ConnectedChannelActivation
		activations, err = d.activations.ListCurrentConnectedChannelActivations(ctx)
		for _, activation := range activations {
			if activation.ActivationID == parent.ID {
				selected, err = d.onboarding.Get(ctx, activation.OperationID)
				break
			}
		}
	}
	if err != nil {
		return selected, err
	}
	account := selected.Operation.AccountReference
	if selected.Connection != nil && selected.Connection.AccountPresentation != "" {
		account = selected.Connection.AccountPresentation
	}
	if selected.Operation.Provider != n.hitl.Provider || account != n.hitl.Account || selected.Binding == nil || selected.Binding.Status != operatorchannel.BindingCurrent {
		return channelonboarding.Result{}, nil
	}
	if n.hitl.Provider == "whatsapp" && (d.sessionStates[selected.Operation.OperationID] != "connected" || n.entries[selected.Operation.OperationID] != nil && n.entries[selected.Operation.OperationID].down) {
		return channelonboarding.Result{}, nil
	}
	return selected, nil
}

func (n *serveLinkNotices) observe(op channelonboarding.Operation, observedErr error, at time.Time) {
	for _, item := range n.items {
		if item.Operation.OperationID != op.OperationID || item.Operation.SessionConnectionID != op.SessionConnectionID || item.Operation.SessionAccount != op.SessionAccount {
			continue
		}
		entry := n.entries[op.OperationID]
		if entry == nil || entry.connection != op.SessionConnectionID || entry.account != op.SessionAccount {
			entry = &linkNoticeEntry{connection: op.SessionConnectionID, account: op.SessionAccount}
			n.entries[op.OperationID] = entry
		}
		fact, state := item.Fact, n.dispatcher.sessionStates[op.OperationID]
		entry.valid = observedErr == nil || fact.Stopped
		if !entry.valid {
			return
		}
		if fact.Down || state == "disconnected" && !at.IsZero() || state == "not_linked" {
			entry.observe(n.dispatcher.now(), false, fact.Stopped || state == "not_linked", fact.Cause())
		}
		if observedErr == nil && !fact.Down && state == "connected" {
			entry.observe(n.dispatcher.now(), true, false, "")
		}
		return
	}
}

func (n *serveLinkNotices) reconcile(ctx context.Context, readErr error) error {
	if n == nil || readErr != nil {
		return nil
	}
	d := n.dispatcher
	selected, targetErr := n.destination(ctx)
	result := targetErr
	for id, entry := range n.entries {
		event := entry.next(d.now())
		text := ""
		if event != nil {
			text = linkNoticeText(*event, id == selected.Operation.OperationID)
		}
		current := entry.valid && targetErr == nil && selected.Binding != nil && text != ""
		if entry.pending == nil && current {
			entry.pending = &linkNoticePending{tools.MailboxItem{ID: uuid.NewString(), Type: render.LinkNoticeType, Summary: text}, *selected.Binding, *event}
		}
		if pending := entry.pending; pending != nil {
			create := current && pending.item.Summary == text && pending.binding == *selected.Binding && pending.event == *event
			_, err := n.mailbox.InsertMailboxItem(render.WithLinkNoticeBinding(ctx, pending.binding, create), pending.item)
			if err == nil {
				entry.admitted(pending.event, d.now())
				entry.pending = nil
			} else if !create && failures.OnlyBranches(err, func(e error) bool { return e == render.ErrLinkNoticeAbsent }) {
				entry.pending = nil
			} else {
				result = errors.Join(result, err)
			}
		}
	}
	return result
}

func linkNoticeText(event linkNoticeEvent, self bool) string {
	if event.kind == "back" {
		if self {
			return ""
		} // R199: self-operator causal recovery wording awaits the user.
		return fmt.Sprintf("Swarm Notice: WhatsApp link back after %s, %d more drops in between", event.upAt.Sub(event.since).Round(time.Second), event.extra)
	}
	kind := "down"
	if event.kind == "attention" {
		kind = "needs attention"
	}
	text := fmt.Sprintf("Swarm Notice: WhatsApp link %s: %s", kind, event.cause)
	if event.extra > 0 {
		text += fmt.Sprintf(", %d more drops in between", event.extra)
	}
	return text
}
