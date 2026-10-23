package generationauthority

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type nativeMutationAdmissionKey struct{}
type mutationAdmissionObservationKey struct{}

// AdmissionObservation reports entrance into the native fence, including an
// attempt that subsequently fails cleanup. It grants no mutation or retry right.
type AdmissionObservation struct{ attempts atomic.Uint64 }

func ObserveMutationAdmission(ctx context.Context) (context.Context, *AdmissionObservation) {
	observation := &AdmissionObservation{}
	return context.WithValue(ctx, mutationAdmissionObservationKey{}, observation), observation
}

func (o *AdmissionObservation) Attempts() uint64 { return o.attempts.Load() }

type nativeMutationAdmission struct {
	tx       *sql.Tx
	sqlite   bool
	original runtimeownership.MutationAuthority
	active   atomic.Bool
}

// PrepareMutationAdmission freezes the issuing owner before pool admission.
// It cannot start a second native write from an existing transaction loan.
func PrepareMutationAdmission(ctx context.Context) (context.Context, error) {
	if ctx != nil && ctx.Value(nativeMutationAdmissionKey{}) != nil {
		return nil, errors.New("runtime mutation cannot open a nested native transaction")
	}
	return runtimeownership.FreezeMutationAuthority(ctx)
}

// AdmitMutationTx is the one native runtime-write admission point, before the
// domain callback. The result is confined to this exact transaction's lifetime.
func AdmitMutationTx(ctx context.Context, tx *sql.Tx, sqlite bool) (context.Context, func(), error) {
	original, err := runtimeownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, nil, err
	}
	if observation, ok := ctx.Value(mutationAdmissionObservationKey{}).(*AdmissionObservation); ok {
		observation.attempts.Add(1)
	}
	if err := RequireCurrentRuntimeMutationTx(ctx, tx, original, sqlite); err != nil {
		return nil, nil, err
	}
	admission := &nativeMutationAdmission{tx: tx, sqlite: sqlite, original: original}
	admission.active.Store(true)
	return context.WithValue(ctx, nativeMutationAdmissionKey{}, admission), func() { admission.active.Store(false) }, nil
}

func exactNativeMutationAdmission(ctx context.Context, tx *sql.Tx, sqlite bool) *nativeMutationAdmission {
	if ctx == nil || tx == nil {
		return nil
	}
	admission, _ := ctx.Value(nativeMutationAdmissionKey{}).(*nativeMutationAdmission)
	if admission == nil || admission.tx != tx || admission.sqlite != sqlite || !admission.active.Load() {
		return nil
	}
	return admission
}

func grantAdmittedInNativeTransaction(ctx context.Context, tx *sql.Tx, expected runtimeownership.GrantEvidence, sqlite bool) bool {
	if ctx == nil {
		return false
	}
	admission, _ := ctx.Value(nativeMutationAdmissionKey{}).(*nativeMutationAdmission)
	if admission == nil || exactNativeMutationAdmission(ctx, tx, sqlite) == nil || !admission.original.HasRuntimeGrant() {
		return false
	}
	actual, actualErr := canonicaljson.Bytes(admission.original.GrantEvidence())
	want, wantErr := canonicaljson.Bytes(expected)
	return actualErr == nil && wantErr == nil && string(actual) == string(want)
}

func processAdmittedInNativeTransaction(ctx context.Context, tx *sql.Tx, expected processbinding.LeaseStamp, sqlite bool) bool {
	admission := exactNativeMutationAdmission(ctx, tx, sqlite)
	return admission != nil && admission.original.ProcessAuthority().LeaseStamp() == expected
}
