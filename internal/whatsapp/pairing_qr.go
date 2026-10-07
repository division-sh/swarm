package whatsapp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types/events"
)

const maxPairingQRCodes = 6
const maxPairingQRBytes = 4096
const firstPairingQRLifetime = 60 * time.Second
const nextPairingQRLifetime = 20 * time.Second

var errPairingScope = errors.New("WhatsApp pairing readback requires its exact authenticated onboarding scope")
var errPairingStopped = errors.New("WhatsApp pairing QR occurrence is no longer available")
var errPairingBounds = errors.New("WhatsApp pairing QR batch exceeds its finite bounds")
var errPairingFormat = errors.New("WhatsApp pairing QR rotation does not match its retained ADV secret")
var errPairingUnsupported = errors.New("WhatsApp passkey and non-multidevice pairing modes are unsupported")
var errPairingFailed = errors.New("WhatsApp pairing failed")

// This is pre-business onboarding scope, not a fabricated session admission.
// The authenticated onboarding owner must supply the retained operation facts.
type pairingQRScope struct {
	PrincipalID, OperationID, ConnectionID, OccurrenceID string
	Coordinate                                           channelonboarding.ChannelRuntimeContextCoordinate
}

func (s pairingQRScope) validate() error {
	if uuid.Validate(s.PrincipalID) != nil || uuid.Validate(s.OperationID) != nil ||
		uuid.Validate(s.ConnectionID) != nil || uuid.Validate(s.OccurrenceID) != nil {
		return errPairingScope
	}
	return s.Coordinate.ValidateContext()
}

type pairingQRStatus string

const (
	pairingAwaiting  pairingQRStatus = "awaiting_qr"
	pairingExpired   pairingQRStatus = "qr_expired"
	pairingSucceeded pairingQRStatus = "paired"
	pairingConnected pairingQRStatus = "connected"
	pairingFailed    pairingQRStatus = "failed"
	pairingStopped   pairingQRStatus = "stopped"
)

// Private principal-scoped readback only. Neither observation grants an account
// admission, executable health, human identity or a standing binding.
type pairingQRSnapshot struct {
	Status    pairingQRStatus
	Code      string
	ExpiresAt time.Time
	Paired    bool
	Connected bool
}

type pairingQR struct {
	mu        sync.Mutex
	scope     pairingQRScope
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	done      chan struct{}
	status    pairingQRStatus
	codes     []string
	index     int
	expires   time.Time
	paired    bool
	connected bool
	failure   error
}

func newPairingQR(ctx context.Context, scope pairingQRScope) (*pairingQR, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errPairingStopped
	}
	if err := scope.validate(); err != nil {
		return nil, err
	}
	owned, cancel := context.WithCancel(ctx)
	q := &pairingQR{scope: scope, ctx: owned, cancel: cancel,
		wake: make(chan struct{}, 1), done: make(chan struct{}), status: pairingAwaiting}
	go q.run()
	return q, nil
}

// One worker owns timer mutation. The wake channel carries no QR material and
// is never closed; repeated batches replace bounded state, not emit goroutines.
func (q *pairingQR) run() {
	defer close(q.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		q.mu.Lock()
		q.advanceLocked(time.Now())
		expires := q.expires
		q.mu.Unlock()
		var elapsed <-chan time.Time
		if !expires.IsZero() {
			timer.Reset(time.Until(expires))
			elapsed = timer.C
		} else {
			timer.Stop()
		}
		select {
		case <-q.ctx.Done():
			q.stop()
			return
		case <-q.wake:
		case <-elapsed:
		}
	}
}

func (q *pairingQR) clearLocked() {
	q.codes = nil
	q.index = 0
	q.expires = time.Time{}
}

func (q *pairingQR) advanceLocked(now time.Time) {
	for !q.expires.IsZero() && !now.Before(q.expires) {
		q.index++
		if q.index == len(q.codes) {
			q.clearLocked()
			q.status = pairingExpired
			return
		}
		q.expires = q.expires.Add(nextPairingQRLifetime)
	}
}

func (q *pairingQR) notifyLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *pairingQR) failLocked(err error) error {
	q.clearLocked()
	q.status, q.failure = pairingFailed, err
	q.connected = false
	q.notifyLocked()
	return err
}

func (q *pairingQR) acceptBatchLocked(event *events.QR) error {
	if event == nil || len(event.Codes) > maxPairingQRCodes {
		return q.failLocked(errPairingBounds)
	}
	for _, code := range event.Codes {
		if code == "" || len(code) > maxPairingQRBytes || !utf8.ValidString(code) {
			return q.failLocked(errPairingBounds)
		}
	}
	q.clearLocked()
	if len(event.Codes) == 0 {
		q.status = pairingExpired
	} else {
		q.codes = append([]string(nil), event.Codes...)
		q.expires, q.status = time.Now().Add(firstPairingQRLifetime), pairingAwaiting
	}
	q.notifyLocked()
	return nil
}

func (q *pairingQR) handle(raw any) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ctx.Err() != nil || q.status == pairingStopped {
		return errPairingStopped
	}
	q.advanceLocked(time.Now())
	if q.status == pairingFailed {
		return q.failure
	}
	switch event := raw.(type) {
	case *events.QR:
		if q.paired || q.connected {
			return nil // A later batch cannot reopen successful pairing.
		}
		return q.acceptBatchLocked(event)
	case *events.RotateADVSecret:
		if q.paired || q.connected {
			return nil
		}
		if event == nil {
			return q.failLocked(errPairingFormat)
		}
		rotated, err := rotatePairingQRCodes(q.codes[q.index:], event.OldSecret, event.NewSecret)
		if err != nil {
			return q.failLocked(err)
		}
		q.codes, q.index = rotated, 0
		q.notifyLocked()
	case *events.PairSuccess:
		if event == nil {
			return q.failLocked(errPairingFailed)
		}
		q.clearLocked()
		q.status, q.paired = pairingSucceeded, true
		if q.connected {
			q.status = pairingConnected
		}
		q.notifyLocked()
	case *events.Connected:
		if event == nil {
			return q.failLocked(errPairingFailed)
		}
		q.clearLocked()
		q.status, q.connected = pairingConnected, true
		q.notifyLocked()
	case *events.PairPasskeyRequest, *events.PairPasskeyConfirmation, *events.PairPasskeyError, *events.QRScannedWithoutMultidevice:
		return q.failLocked(errPairingUnsupported)
	case *events.PairError, *events.Disconnected, *events.LoggedOut, *events.ConnectFailure,
		*events.StreamReplaced, *events.StreamError, *events.ClientOutdated, *events.TemporaryBan:
		return q.failLocked(errPairingFailed)
	}
	return nil
}

func rotatePairingQRCodes(codes []string, oldSecret, newSecret string) ([]string, error) {
	for _, secret := range []string{oldSecret, newSecret} {
		decoded, err := base64.StdEncoding.DecodeString(secret)
		if err != nil || len(decoded) != 32 {
			return nil, errPairingFormat
		}
	}
	rotated := make([]string, len(codes))
	for index, code := range codes {
		parsed, err := url.Parse(code)
		if err != nil || parsed.Scheme != "https" || parsed.Host != "wa.me" ||
			parsed.Path != "/settings/linked_devices" || parsed.RawQuery != "" || parsed.User != nil {
			return nil, errPairingFormat
		}
		parts := strings.Split(parsed.Fragment, ",")
		if len(parts) != 5 || parts[3] != oldSecret && parts[3] != newSecret {
			return nil, errPairingFormat
		}
		parts[3] = newSecret
		parsed.Fragment, parsed.RawFragment = strings.Join(parts, ","), ""
		rotated[index] = parsed.String()
	}
	return rotated, nil
}

func (q *pairingQR) read(scope pairingQRScope) (pairingQRSnapshot, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if scope != q.scope || scope.validate() != nil {
		return pairingQRSnapshot{}, errPairingScope
	}
	if q.ctx.Err() != nil || q.status == pairingStopped {
		return pairingQRSnapshot{}, errors.Join(errPairingStopped, q.failure)
	}
	q.advanceLocked(time.Now())
	result := pairingQRSnapshot{Status: q.status, Paired: q.paired, Connected: q.connected}
	if len(q.codes) > 0 {
		result.Code, result.ExpiresAt = q.codes[q.index], q.expires
	}
	return result, q.failure
}

func (q *pairingQR) stop() {
	q.mu.Lock()
	q.clearLocked()
	q.status = pairingStopped
	q.mu.Unlock()
	q.cancel()
}

func (q *pairingQR) join(ctx context.Context) error {
	q.stop()
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("join WhatsApp pairing QR: %w", context.Cause(ctx))
	}
}
