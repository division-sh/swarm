package sessionprovider

import (
	"context"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	capturedata "github.com/division-sh/swarm/internal/sessioncapture"
	"github.com/division-sh/swarm/internal/store/sessionstate"
)

type captureScope = capturedata.Scope
type capturedEvent = capturedata.Event
type captureSource = capturedata.Source

const maxCaptureEventBytes = capturedata.MaxCaptureEventBytes
const maxPendingCaptureCount = capturedata.MaxPendingCaptureCount
const maxPendingCaptureBytes = capturedata.MaxPendingCaptureBytes
const captureProvenanceKey = capturedata.CaptureProvenanceKey

var errCaptureConflict = capturedata.ErrCaptureConflict
var errCaptureCapacity = capturedata.ErrCaptureCapacity
var errCaptureScopeChanged = capturedata.ErrCaptureScopeChanged
var errCaptureMissing = capturedata.ErrCaptureMissing
var errCapturePublicationPending = capturedata.ErrCapturePublicationPending

type captureStore struct {
	owner        *sessionstate.Capture
	connectionID string
}

func newCaptureStore(ctx context.Context, owner *sessionstate.Owner, connectionID string) (*captureStore, error) {
	capture, err := owner.Captures(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	return &captureStore{owner: capture, connectionID: connectionID}, nil
}
func (s *captureStore) capture(ctx context.Context, event capturedEvent) error {
	return s.owner.Capture(ctx, event)
}
func (s *captureStore) pending(ctx context.Context) ([]capturedEvent, error) {
	return s.owner.Pending(ctx)
}
func (s *captureStore) recordFailure(ctx context.Context, failure callbackFailure) error {
	return s.owner.RecordFailure(ctx, failure)
}
func (s *captureStore) stagePublication(ctx context.Context, event capturedEvent, request runtimeinbound.Request) error {
	return s.owner.StagePublication(ctx, event, request)
}

type publicationReader = sessionstate.PublicationReader
type claimReceiptReader = sessionstate.ClaimReceiptReader

func (s *captureStore) retirePublished(ctx context.Context, event capturedEvent, reader publicationReader) error {
	return s.owner.RetirePublished(ctx, event, reader)
}
func (s *captureStore) reconcilePublished(ctx context.Context, event capturedEvent, reader publicationReader) (bool, error) {
	return s.owner.ReconcilePublished(ctx, event, reader)
}
func (s *captureStore) reconcileSessionClaim(ctx context.Context, event capturedEvent, reader claimReceiptReader) (bool, error) {
	return s.owner.ReconcileSessionClaim(ctx, event, reader)
}

type pendingCapture struct {
	sequence     int64
	event        capturedEvent
	request      *runtimeinbound.Request
	requestBytes []byte
}

func (s *captureStore) pendingPublications(ctx context.Context) ([]pendingCapture, error) {
	rows, err := s.owner.PendingPublications(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]pendingCapture, len(rows))
	for i, row := range rows {
		result[i] = pendingCapture{sequence: row.Sequence, event: row.Event, request: row.Request, requestBytes: row.RequestBytes}
	}
	return result, nil
}
