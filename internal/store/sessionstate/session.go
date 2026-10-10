package sessionstate

import (
	"context"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sessioncapture"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	private "github.com/division-sh/swarm/internal/store/internal/backend/sessionpersistence"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

var ErrSessionAccount = private.ErrSessionAccount

type Owner struct{ owner *private.Owner }
type Capture struct{ owner *private.CaptureStore }
type PublicationReader = private.PublicationReader
type ClaimReceiptReader = private.ClaimReceiptReader

func Open(ctx context.Context, path, expectedAccount string, nonempty bool) (*Owner, error) {
	owner, err := private.Open(ctx, path, expectedAccount, nonempty)
	if err != nil {
		return nil, err
	}
	return &Owner{owner: owner}, nil
}

func (o *Owner) Close() error { return o.owner.Close() }
func (o *Owner) CurrentDevice(ctx context.Context, account string) (*store.Device, error) {
	return o.owner.CurrentDevice(ctx, account)
}
func (o *Owner) NewDevice() *store.Device { return o.owner.NewDevice() }
func (o *Owner) GetDevice(ctx context.Context, jid types.JID) (*store.Device, error) {
	return o.owner.GetDevice(ctx, jid)
}
func (o *Owner) LIDMap() store.LIDStore { return o.owner.LIDMap() }
func (o *Owner) Captures(ctx context.Context, connectionID string) (*Capture, error) {
	capture, err := o.owner.Captures(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	return &Capture{owner: capture}, nil
}
func (s *Capture) Capture(ctx context.Context, event sessioncapture.Event) error {
	return s.owner.Capture(ctx, event)
}
func (s *Capture) Pending(ctx context.Context) ([]sessioncapture.Event, error) {
	return s.owner.Pending(ctx)
}
func (s *Capture) SettleNonClaim(ctx context.Context, fact authority.NonClaim) error {
	return s.owner.SettleNonClaim(ctx, fact)
}
func (s *Capture) NonClaimReceipts(ctx context.Context) ([]sessioncapture.Event, error) {
	return s.owner.NonClaimReceipts(ctx)
}
func (s *Capture) PendingPublications(ctx context.Context) ([]sessioncapture.PendingCapture, error) {
	return s.owner.PendingPublications(ctx)
}
func (s *Capture) RecordFailure(ctx context.Context, failure sessioncapture.CallbackFailure) error {
	return s.owner.RecordFailure(ctx, failure)
}
func (s *Capture) StagePublication(ctx context.Context, event sessioncapture.Event, request runtimeinbound.Request) error {
	return s.owner.StagePublication(ctx, event, request)
}
func (s *Capture) RetirePublished(ctx context.Context, event sessioncapture.Event, reader PublicationReader) error {
	return s.owner.RetirePublished(ctx, event, reader)
}
func (s *Capture) ReconcilePublished(ctx context.Context, event sessioncapture.Event, reader PublicationReader) (bool, error) {
	return s.owner.ReconcilePublished(ctx, event, reader)
}
func (s *Capture) ReconcileSessionClaim(ctx context.Context, event sessioncapture.Event, reader ClaimReceiptReader) (bool, error) {
	return s.owner.ReconcileSessionClaim(ctx, event, reader)
}

// Fixture owns a private-state test's close/reopen boundary without exporting
// database access. Fault and observation methods are named storage operations.
type Fixture struct{ owner *Owner }

func OpenSDKFixture(ctx context.Context, path string) (*Fixture, *Owner, error) {
	inner, err := private.OpenSDKFixture(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	owner := &Owner{owner: inner}
	return &Fixture{owner: owner}, owner, nil
}
func OpenCaptureFixture(path string) (*Fixture, *Owner, error) {
	inner, err := private.OpenCaptureFixture(path)
	if err != nil {
		return nil, nil, err
	}
	owner := &Owner{owner: inner}
	return &Fixture{owner: owner}, owner, nil
}
func (f *Fixture) Close() error { return f.owner.Close() }

type CaptureCorruption = private.CaptureCorruption
type CaptureFault = private.CaptureFault

const (
	CorruptConnection       = private.CorruptConnection
	CorruptAccount          = private.CorruptAccount
	CorruptConversation     = private.CorruptConversation
	CorruptEvent            = private.CorruptEvent
	CorruptKind             = private.CorruptKind
	CorruptByteCount        = private.CorruptByteCount
	CorruptEnvelope         = private.CorruptEnvelope
	CorruptDigest           = private.CorruptDigest
	CorruptSetupDisposition = private.CorruptSetupDisposition
	CaptureInsertFault      = private.CaptureInsertFault
	CaptureStageFault       = private.CaptureStageFault
	CaptureRetirementFault  = private.CaptureRetirementFault
)

func (f *Fixture) CorruptCaptureIndex(ctx context.Context, dimension CaptureCorruption) error {
	return f.owner.owner.CorruptCaptureIndex(ctx, dimension)
}
func (f *Fixture) CorruptFirstCaptureConversation(ctx context.Context) error {
	return f.owner.owner.CorruptFirstCaptureConversation(ctx)
}
func (f *Fixture) ReplaceCaptureEnvelope(ctx context.Context, raw, digest []byte) error {
	return f.owner.owner.ReplaceCaptureEnvelope(ctx, raw, digest)
}
func (f *Fixture) CorruptPublicationDigest(ctx context.Context) error {
	return f.owner.owner.CorruptPublicationDigest(ctx)
}
func (f *Fixture) ReplacePublicationRequest(ctx context.Context, raw, digest []byte) error {
	return f.owner.owner.ReplacePublicationRequest(ctx, raw, digest)
}
func (f *Fixture) SetCaptureFault(ctx context.Context, fault CaptureFault, enabled bool) error {
	return f.owner.owner.SetCaptureFault(ctx, fault, enabled)
}
func (f *Fixture) CaptureCount(ctx context.Context) (int, error) {
	return f.owner.owner.CaptureCount(ctx)
}
func (f *Fixture) CallbackFailureReason(ctx context.Context, id string) (string, error) {
	return f.owner.owner.CallbackFailureReason(ctx, id)
}
func (f *Fixture) CallbackFailureCount(ctx context.Context, connection, id, reason string) (int, error) {
	return f.owner.owner.CallbackFailureCount(ctx, connection, id, reason)
}
func (f *Fixture) CaptureRawRows(ctx context.Context) ([][]byte, error) {
	return f.owner.owner.CaptureRawRows(ctx)
}
func (f *Fixture) CaptureStoredRows(ctx context.Context) ([]string, error) {
	return f.owner.owner.CaptureStoredRows(ctx)
}
func (f *Fixture) SeedCaptureQuota(ctx context.Context, event sessioncapture.Event, count int) error {
	return f.owner.owner.SeedCaptureQuota(ctx, event, count)
}
