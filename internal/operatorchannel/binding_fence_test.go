package operatorchannel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRetainedBindingFenceDistinguishesAbsenceUnboundAndCurrent(t *testing.T) {
	for _, state := range []string{"absent", "unbound", "current", "foreign_interface"} {
		t.Run(state, func(t *testing.T) {
			service, store, original := retainedBindingServiceFixture(t)
			wantRevision := original.Revision
			switch state {
			case "absent":
				store.rows, wantRevision = nil, 0
			case "unbound":
				store.rows = []Binding{retainedUnboundTestBinding(original)}
			case "foreign_interface":
				store.rows[0].Interface.SemanticGeneration = "another-exact-generation"
				store.rows[0].Interface.Selector = store.rows[0].Interface.CanonicalSelector()
				wantRevision = 0
			}
			fence, err := service.RetainedBindingFence(context.Background(), original.Interface)
			current, hasCurrent := fence.CurrentBinding()
			if err != nil || fence.Revision != wantRevision || hasCurrent != (state == "current") {
				t.Fatal("retained mutation fence changed binding semantics", fence, err)
			}
			if state == "current" {
				if current != original || current.ProviderAuthority.RequireExecutable() == nil {
					t.Fatal("retained current identity changed or acquired execution", current)
				}
			} else if current != (Binding{}) || fence.CurrentIdentity != (Binding{}) {
				t.Fatal("absent/unbound fence granted identity", fence, current)
			}
			if state == "unbound" {
				if _, err := service.RetainedCurrentBinding(context.Background(), original.Interface); !errors.Is(err, ErrNotFound) {
					t.Fatal("unbound fence became a current binding", err)
				}
				if _, ready, err := service.CurrentBindingReadiness(context.Background(), original.Interface); ready || !errors.Is(err, ErrNotFound) {
					t.Fatal("unbound fence became READY", ready, err)
				}
			}
			if store.bootBinds != 0 {
				t.Fatal("fence observation revived proof authority")
			}
		})
	}
}

func TestRetainedBindingFenceRejectsContradictoryUnboundEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Binding)
	}{
		{"zero_revision", func(b *Binding) { b.Revision = 0 }},
		{"negative_revision", func(b *Binding) { b.Revision = -1 }},
		{"missing_operation", func(b *Binding) { b.OperationID = "" }},
		{"missing_time", func(b *Binding) { b.UpdatedAt = time.Time{} }},
		{"claimant", func(b *Binding) { b.ExternalAccountRef = "old-claimant" }},
		{"conversation", func(b *Binding) { b.ConversationRef = "old-conversation" }},
		{"scope", func(b *Binding) { b.ConversationScope = ConversationScopeDirect }},
		{"presentation", func(b *Binding) { b.AccountPresentation = "old-account" }},
		{"source", func(b *Binding) { b.Source = BindingSourceLiveVerification }},
		{"proof", func(b *Binding) { b.ProofID = uuid.NewString() }},
		{"proof_revision", func(b *Binding) { b.ProofRevision = 1 }},
		{"principal", func(b *Binding) { b.PrincipalID = uuid.NewString() }},
		{"interface", func(b *Binding) { b.Interface.Selector = "guessed" }},
		{"derived_stale", func(b *Binding) { b.Status = BindingStale }},
		{"derived_revoked", func(b *Binding) { b.Status = BindingRevoked }},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, original := retainedBindingServiceFixture(t)
			row := retainedUnboundTestBinding(original)
			test.mutate(&row)
			store.rows = []Binding{row}
			if fence, err := service.RetainedBindingFence(context.Background(), original.Interface); !errors.Is(err, ErrConflict) || fence != (BindingFence{}) {
				t.Fatal("contradictory fence became absence or identity", fence, err)
			}
		})
	}
	service, store, original := retainedBindingServiceFixture(t)
	row := retainedUnboundTestBinding(original)
	row.ProviderAuthority = original.ProviderAuthority
	store.rows = []Binding{row}
	if _, err := service.RetainedBindingFence(context.Background(), original.Interface); !errors.Is(err, ErrConflict) {
		t.Fatal("unbound row retained provider authority", err)
	}
	store.rows = []Binding{retainedUnboundTestBinding(original), retainedUnboundTestBinding(original)}
	if _, err := service.RetainedBindingFence(context.Background(), original.Interface); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate exact fences accepted", err)
	}
}

func TestRetainedBindingFencePreservesCancellationAndLookupErrors(t *testing.T) {
	for _, mode := range []string{"canceled_before", "canceled_lookup", "store_error", "joined_error"} {
		t.Run(mode, func(t *testing.T) {
			service, store, original := retainedBindingServiceFixture(t)
			store.rows = []Binding{retainedUnboundTestBinding(original)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("retained fence lookup failed")
			switch mode {
			case "canceled_before":
				cancel()
			case "canceled_lookup":
				store.afterRead = cancel
			case "store_error":
				store.err = cause
			case "joined_error":
				store.err, store.afterRead = cause, cancel
			}
			fence, err := service.RetainedBindingFence(ctx, original.Interface)
			if fence != (BindingFence{}) || err == nil || (ctx.Err() != nil && !errors.Is(err, context.Canceled)) || (store.err != nil && !errors.Is(err, cause)) {
				t.Fatal("fence lookup manufactured absence or hid its cause", fence, err)
			}
		})
	}
}

func retainedUnboundTestBinding(original Binding) Binding {
	return Binding{PrincipalID: original.PrincipalID, Interface: original.Interface, Status: BindingUnbound,
		Revision: original.Revision, OperationID: uuid.NewString(), UpdatedAt: original.UpdatedAt}
}
