package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWorkflowTimerReceiverCancellationCauseHeldReadsPreserveRecoveryBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"receiver_cancellation_cause_held_activation", "receiver_cancellation_cause_held_target"})
}

func TestWorkflowTimerReceiverCancellationCauseDelayedEntryPreservesRecoveryBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"receiver_cancellation_cause_held_activation_delayed_entry", "receiver_cancellation_cause_held_target_delayed_entry"})
}

type timerReceiverReadFacts struct {
	Read                   string    `json:"read"`
	Phase                  string    `json:"phase"`
	At                     time.Time `json:"at"`
	PublisherError         string    `json:"publisher_error"`
	PublisherErrorAfter    string    `json:"publisher_error_after"`
	PublisherCause         string    `json:"publisher_cause"`
	PublisherDeadline      time.Time `json:"publisher_deadline"`
	ReceiverError          string    `json:"receiver_error"`
	ReceiverCause          string    `json:"receiver_cause"`
	ReceiverDeadline       time.Time `json:"receiver_deadline"`
	ReturnedError          string    `json:"returned_error"`
	AuthorizationCompleted bool      `json:"authorization_completed"`
	ActivationReads        int       `json:"activation_reads"`
	MutationCalls          int       `json:"mutation_calls"`
	MutationAcknowledged   int       `json:"mutation_acknowledged"`
}

type timerReceiverReadCut struct {
	publisher       context.Context
	read            string
	owner           *timerTransitionContentionOwner
	test            *testing.T
	inReceiver      bool
	readEntries     int
	readReturns     int
	activationReads int
	entry           timerReceiverReadFacts
	returned        timerReceiverReadFacts
	returnedError   error
	delayEntry      bool
	legacyEntryErr  error
	cancelReceiver  context.CancelCauseFunc
}

func (p *timerReceiverReadCut) beginReceiver(ctx context.Context) context.Context {
	p.inReceiver = true
	receiver, cancel := context.WithCancelCause(ctx)
	p.cancelReceiver = cancel
	p.test.Cleanup(func() { cancel(context.Canceled) })
	p.owner.holdActivationRead = p.read == "activation"
	p.owner.holdTargetRead = p.read == "target"
	if p.delayEntry {
		// Only the old oracle's deadline expires here, not the actual receiver.
		legacy, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		defer stop()
		if err := joinTimerTestInterruption(nil, legacy); err != nil {
			p.test.Fatal(err)
		}
		p.legacyEntryErr = legacy.Err()
		if !errors.Is(p.legacyEntryErr, context.DeadlineExceeded) || receiver.Err() != nil || p.publisher.Err() != nil {
			p.test.Fatalf("delayed entry did not preserve the actual lifetimes: legacy=%v receiver=%v publisher=%v", p.legacyEntryErr, receiver.Err(), p.publisher.Err())
		}
		p.test.Logf("M09_DELAYED_ENTRY legacy_deadline=%v receiver=%v publisher=%v", p.legacyEntryErr, receiver.Err(), p.publisher.Err())
	}
	return receiver
}

func (p *timerReceiverReadCut) enterRead(read string, ctx context.Context) {
	p.readEntries++
	p.entry = p.observe("held_read_entered", ctx, nil)
	if !p.inReceiver || read != p.read || p.readEntries != 1 ||
		p.entry.PublisherError != "<nil>" || p.entry.PublisherErrorAfter != "<nil>" || p.entry.ReceiverError != "<nil>" ||
		p.entry.ReceiverCause != "<nil>" || !p.entry.PublisherDeadline.IsZero() || !p.entry.ReceiverDeadline.IsZero() || p.cancelReceiver == nil {
		p.test.Fatalf("intended read was not reached with independently live lifetimes: %+v", p.entry)
	}
	// Cause is deadline-shaped; Err is Canceled. This is not deadline expiry.
	p.cancelReceiver(context.DeadlineExceeded)
}

func (p *timerReceiverReadCut) returnRead(read string, ctx context.Context, err error) {
	if !p.inReceiver {
		return
	}
	if read == "activation" && err == nil {
		p.activationReads++
	}
	if read != p.read {
		return
	}
	p.readReturns++
	p.returnedError = err
	p.returned = p.observe("held_read_returned", ctx, err)
}

func (p *timerReceiverReadCut) observe(phase string, ctx context.Context, err error) timerReceiverReadFacts {
	publisherDeadline, _ := p.publisher.Deadline()
	receiverDeadline, _ := ctx.Deadline()
	facts := timerReceiverReadFacts{
		Read: p.read, Phase: phase, At: time.Now().UTC(),
		PublisherError: fmt.Sprint(p.publisher.Err()), PublisherCause: fmt.Sprint(context.Cause(p.publisher)),
		PublisherDeadline: publisherDeadline, ReceiverDeadline: receiverDeadline,
		ReceiverError: fmt.Sprint(ctx.Err()), ReceiverCause: fmt.Sprint(context.Cause(ctx)),
		ReturnedError: fmt.Sprint(err), ActivationReads: p.activationReads,
		AuthorizationCompleted: p.read == "target" && p.readEntries == 1 && p.activationReads == 1,
		MutationCalls:          p.owner.calls, MutationAcknowledged: p.owner.acknowledged,
	}
	facts.PublisherErrorAfter = fmt.Sprint(p.publisher.Err())
	encoded, err := json.Marshal(facts)
	if err != nil {
		p.test.Fatal(err)
	}
	p.test.Logf("M09_CUT %s", encoded)
	return facts
}

func (p *timerReceiverReadCut) validate(capture *timerTransitionOutcomeCapture, publisherReturnErr error) {
	cut := p.returned
	if p.readEntries != 1 || p.readReturns != 1 || cut.PublisherError != "<nil>" || cut.PublisherErrorAfter != "<nil>" ||
		cut.ReceiverError != context.Canceled.Error() || cut.ReceiverCause != context.DeadlineExceeded.Error() ||
		!errors.Is(p.returnedError, context.Canceled) || errors.Is(p.returnedError, context.DeadlineExceeded) ||
		!cut.ReceiverDeadline.IsZero() || !cut.PublisherDeadline.IsZero() || cut.MutationCalls != 0 || cut.MutationAcknowledged != 0 ||
		publisherReturnErr != nil || capture.entryErr != nil || !errors.Is(capture.exitErr, context.Canceled) {
		p.test.Fatalf("read did not prove coordinated receiver cancellation while publisher remained live: entry=%+v return=%+v publisher_return=%v receiver_entry=%v receiver_exit=%v", p.entry, cut, publisherReturnErr, capture.entryErr, capture.exitErr)
	}
	if p.delayEntry && !errors.Is(p.legacyEntryErr, context.DeadlineExceeded) {
		p.test.Fatal("delayed entry did not cross the old oracle's deadline")
	}
	if cut.AuthorizationCompleted != (p.read == "target") ||
		(p.read == "activation" && (cut.ActivationReads != 0 || p.owner.interruptedActivationReads != 1)) ||
		(p.read == "target" && (cut.ActivationReads != 1 || p.owner.interruptedActivationReads != 0)) {
		p.test.Fatalf("receiver cancellation authorization phase not proven: %+v interrupted_reads=%d", cut, p.owner.interruptedActivationReads)
	}
	p.test.Logf("M09_ORACLE read=%s outer_deadline_predicate=false publisher_return=%v receiver_exit=%v receiver_cause=%s authorization_completed=%t mutation_entered=false", p.read, publisherReturnErr, capture.exitErr, cut.ReceiverCause, cut.AuthorizationCompleted)
}
