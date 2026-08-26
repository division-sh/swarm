package channelonboarding

import (
	"context"
	"fmt"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

type CredentialWriteRequest struct {
	StoreKey string
	Value    string
	Receipt  string
}

type CredentialWriteResult struct {
	StoreKey string
	Receipt  string
	Epoch    string
}

type CredentialWriter struct {
	store     runtimecredentials.ReceiptWriter
	snapshots *runtimecredentials.SnapshotOwner
}

func NewCredentialWriter(store runtimecredentials.Store) (*CredentialWriter, error) {
	receiptWriter, ok := store.(runtimecredentials.ReceiptWriter)
	if !ok || receiptWriter == nil {
		return nil, fmt.Errorf("channel onboarding requires a receipt-capable credential store")
	}
	snapshots, err := runtimecredentials.NewSnapshotOwner(store)
	if err != nil {
		return nil, err
	}
	return &CredentialWriter{store: receiptWriter, snapshots: snapshots}, nil
}

func (w *CredentialWriter) Admit(ctx context.Context, req CredentialWriteRequest) (CredentialWriteResult, error) {
	if w == nil || w.store == nil {
		return CredentialWriteResult{}, fmt.Errorf("channel onboarding credential writer is required")
	}
	key, receipt := req.StoreKey, strings.TrimSpace(req.Receipt)
	if key == "" || key != strings.TrimSpace(key) || receipt == "" {
		return CredentialWriteResult{}, fmt.Errorf("channel onboarding credential key and receipt are required")
	}
	written, err := w.store.AdmitWithReceipt(ctx, key, req.Value, receipt)
	if err != nil {
		return CredentialWriteResult{}, err
	}
	if written.Key != key || written.Receipt != receipt || strings.TrimSpace(written.Epoch) == "" {
		return CredentialWriteResult{}, fmt.Errorf("credential store returned a contradictory write receipt")
	}
	observed, err := w.snapshots.ObserveSecretBinding(ctx, key)
	if err != nil {
		return CredentialWriteResult{}, err
	}
	if !observed.Bound() || observed.Epoch() != written.Epoch {
		return CredentialWriteResult{}, fmt.Errorf("credential write receipt does not match the current credential occurrence")
	}
	return CredentialWriteResult{StoreKey: key, Receipt: receipt, Epoch: observed.Epoch()}, nil
}

func (w *CredentialWriter) Observe(ctx context.Context, storeKey string) (CredentialWriteResult, error) {
	if w == nil || w.snapshots == nil {
		return CredentialWriteResult{}, fmt.Errorf("channel onboarding credential writer is required")
	}
	key := storeKey
	if key == "" || key != strings.TrimSpace(key) {
		return CredentialWriteResult{}, fmt.Errorf("channel onboarding credential key is required")
	}
	observed, err := w.snapshots.ObserveSecretBinding(ctx, key)
	if err != nil {
		return CredentialWriteResult{}, err
	}
	if !observed.Bound() {
		return CredentialWriteResult{}, fmt.Errorf("channel onboarding credential %q is missing", key)
	}
	return CredentialWriteResult{StoreKey: key, Epoch: observed.Epoch()}, nil
}
