package channelonboarding

import (
	"context"
	"errors"
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
	observer  runtimecredentials.ReceiptObserver
	deleter   runtimecredentials.ReceiptDeleter
	snapshots *runtimecredentials.SnapshotOwner
}

func NewCredentialWriter(store runtimecredentials.Store) (*CredentialWriter, error) {
	receiptWriter, ok := store.(runtimecredentials.ReceiptWriter)
	if !ok || receiptWriter == nil {
		return nil, fmt.Errorf("channel onboarding requires a receipt-capable credential store")
	}
	receiptDeleter, ok := store.(runtimecredentials.ReceiptDeleter)
	if !ok || receiptDeleter == nil {
		return nil, fmt.Errorf("channel onboarding requires receipt-fenced credential deletion")
	}
	snapshots, err := runtimecredentials.NewSnapshotOwner(store)
	if err != nil {
		return nil, err
	}
	receiptObserver, ok := store.(runtimecredentials.ReceiptObserver)
	if !ok || receiptObserver == nil {
		return nil, fmt.Errorf("channel onboarding requires receipt-capable credential observation")
	}
	return &CredentialWriter{store: receiptWriter, observer: receiptObserver, deleter: receiptDeleter, snapshots: snapshots}, nil
}

func (w *CredentialWriter) Release(ctx context.Context, admission CredentialAdmission) (bool, error) {
	if w == nil || w.deleter == nil {
		return false, fmt.Errorf("channel onboarding credential writer is required")
	}
	if err := admission.Validate(); err != nil {
		return false, err
	}
	if admission.Kind != CredentialAdmissionWritten {
		return false, nil
	}
	return w.deleter.DeleteWithReceipt(ctx, admission.StoreKey, admission.Receipt, admission.Epoch)
}

// ReleaseOperation reconciles both checkpointed admissions and deterministic
// operation writes that may exist before their selected-store checkpoint.
func (w *CredentialWriter) ReleaseOperation(ctx context.Context, op Operation) error {
	operationID := strings.TrimSpace(op.OperationID)
	if operationID == "" {
		return fmt.Errorf("channel onboarding credential cleanup requires an operation identity")
	}
	type expectedOccurrence struct {
		storeKey string
		receipt  string
	}
	expectedByRole := make(map[string]expectedOccurrence, len(op.CredentialReservations))
	var cleanupErr error
	for _, reservation := range op.CredentialReservations {
		if err := reservation.Validate(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if _, duplicate := expectedByRole[reservation.Role]; duplicate {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("duplicate channel credential reservation role %q", reservation.Role))
			continue
		}
		expectedByRole[reservation.Role] = expectedOccurrence{
			storeKey: operationCredentialStoreKey(reservation.StoreKey, operationID, reservation.Role),
			receipt:  credentialReceipt(operationID, reservation.Role),
		}
	}

	releases := make([]CredentialAdmission, 0, len(op.CredentialAdmissions)+len(expectedByRole))
	seen := make(map[string]struct{}, cap(releases))
	addRelease := func(admission CredentialAdmission) {
		occurrence := credentialAdmissionOccurrence(admission)
		if _, duplicate := seen[occurrence]; duplicate {
			return
		}
		seen[occurrence] = struct{}{}
		releases = append(releases, admission)
	}
	for _, admission := range op.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if admission.Kind != CredentialAdmissionWritten {
			continue
		}
		expected, found := expectedByRole[admission.Role]
		if !found || admission.StoreKey != expected.storeKey || admission.Receipt != expected.receipt {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("written channel credential %q is not owned by operation %s", admission.StoreKey, operationID))
			continue
		}
		addRelease(admission)
	}
	for role, expected := range expectedByRole {
		written, found, err := w.ObserveWritten(ctx, expected.storeKey, expected.receipt)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("observe operation credential %q: %w", expected.storeKey, err))
			continue
		}
		if !found {
			observed, observeErr := w.snapshots.ObserveSecretBinding(ctx, expected.storeKey)
			if observeErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("observe operation credential key %q: %w", expected.storeKey, observeErr))
			} else if observed.Bound() {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("operation credential %q is bound to a contradictory receipt", expected.storeKey))
			}
			continue
		}
		addRelease(CredentialAdmission{
			Role: role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten,
			Receipt: written.Receipt, Epoch: written.Epoch,
		})
	}
	for _, admission := range releases {
		released, err := w.Release(ctx, admission)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("release channel credential %q: %w", admission.StoreKey, err))
			continue
		}
		if released {
			continue
		}
		observed, observeErr := w.snapshots.ObserveSecretBinding(ctx, admission.StoreKey)
		if observeErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify released channel credential %q: %w", admission.StoreKey, observeErr))
		} else if observed.Bound() {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("channel credential %q changed before exact release", admission.StoreKey))
		}
	}
	return cleanupErr
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

func (w *CredentialWriter) ObserveWritten(ctx context.Context, storeKey, receipt string) (CredentialWriteResult, bool, error) {
	if w == nil || w.observer == nil || w.snapshots == nil {
		return CredentialWriteResult{}, false, fmt.Errorf("channel onboarding credential writer is required")
	}
	key, receipt := storeKey, strings.TrimSpace(receipt)
	if key == "" || key != strings.TrimSpace(key) || receipt == "" {
		return CredentialWriteResult{}, false, fmt.Errorf("channel onboarding credential key and receipt are required")
	}
	written, found, err := w.observer.ObserveReceipt(ctx, key, receipt)
	if err != nil || !found {
		return CredentialWriteResult{}, found, err
	}
	observed, err := w.snapshots.ObserveSecretBinding(ctx, key)
	if err != nil {
		return CredentialWriteResult{}, false, err
	}
	if !observed.Bound() || observed.Epoch() != written.Epoch {
		return CredentialWriteResult{}, false, fmt.Errorf("credential write receipt does not match the current credential occurrence")
	}
	return CredentialWriteResult{StoreKey: key, Receipt: receipt, Epoch: observed.Epoch()}, true, nil
}
