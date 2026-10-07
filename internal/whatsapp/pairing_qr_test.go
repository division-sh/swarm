package whatsapp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types/events"
)

func pairingScopeFixture(t *testing.T) pairingQRScope {
	t.Helper()
	capture := captureFixture(t)
	return pairingQRScope{PrincipalID: capture.Scope.PrincipalID, OperationID: capture.Scope.OnboardingOperation,
		ConnectionID: capture.Scope.Session.ConnectionID, OccurrenceID: capture.OccurrenceID,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: capture.Scope.Source.BundleHash, BundleIdentity: capture.Scope.Source.BundleIdentity,
			PackInventoryGeneration: capture.Scope.Source.PackInventoryGeneration, PlanGeneration: capture.Scope.Source.PlanGeneration,
			RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1,
		}}
}

func pairingCodeFixture(ref, secret string) string {
	return "https://wa.me/settings/linked_devices#" + strings.Join([]string{ref, secret, secret, secret, "1"}, ",")
}

func assertPairingQRWorkerStateFixture(t *testing.T, q *pairingQR, code string, status pairingQRStatus) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	current := ""
	if len(q.codes) > 0 {
		current = q.codes[q.index]
	}
	if current != code || q.status != status {
		t.Fatalf("expiry worker state without read-side advancement: code=%q status=%s", current, q.status)
	}
}

func TestWhatsAppDirectQRActualExpiryAndBoundedReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scope := pairingScopeFixture(t)
		q, err := newPairingQR(context.Background(), scope)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := q.join(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		if err := q.handle(&events.QR{Codes: []string{"first", "second", "third"}}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		first, err := q.read(scope)
		if err != nil || first.Code != "first" || first.Paired || first.Connected {
			t.Fatalf("initial code manufactured readiness: %+v, %v", first, err)
		}
		time.Sleep(59 * time.Second)
		synctest.Wait()
		before, _ := q.read(scope)
		if before.Code != "first" {
			t.Fatalf("first code expired before its real 60s deadline: %+v", before)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		assertPairingQRWorkerStateFixture(t, q, "second", pairingAwaiting)
		second, _ := q.read(scope)
		if second.Code != "second" || !second.ExpiresAt.Equal(first.ExpiresAt.Add(20*time.Second)) {
			t.Fatalf("expiry did not advance to the 20s code: %+v", second)
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		assertPairingQRWorkerStateFixture(t, q, "third", pairingAwaiting)
		third, _ := q.read(scope)
		if third.Code != "third" {
			t.Fatalf("second timer expiry: %+v", third)
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		assertPairingQRWorkerStateFixture(t, q, "", pairingExpired)
		exhausted, _ := q.read(scope)
		if exhausted.Status != pairingExpired || exhausted.Code != "" || !exhausted.ExpiresAt.IsZero() {
			t.Fatalf("elapsed expiry retained a code: %+v", exhausted)
		}
		for index := 0; index < 64; index++ {
			batch := []string{fmt.Sprintf("replacement-%d", index)}
			if err := q.handle(&events.QR{Codes: batch}); err != nil {
				t.Fatal(err)
			}
			batch[0] = "mutated SDK event slice"
		}
		synctest.Wait()
		latest, _ := q.read(scope)
		if latest.Code != "replacement-63" || len(q.codes) != 1 {
			t.Fatalf("repeated batches queued material or retained the SDK slice: %+v", latest)
		}
		time.Sleep(10 * time.Second)
		if err := q.handle(&events.QR{Codes: []string{"fresh"}}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(50 * time.Second)
		synctest.Wait()
		fresh, _ := q.read(scope)
		if fresh.Code != "fresh" {
			t.Fatalf("old batch timer expired the replacement: %+v", fresh)
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assertPairingQRWorkerStateFixture(t, q, "", pairingExpired)
		fresh, _ = q.read(scope)
		if fresh.Status != pairingExpired || fresh.Code != "" {
			t.Fatalf("replacement did not expire at its own deadline: %+v", fresh)
		}
	})
}

func TestWhatsAppDirectQRADVRotationPreservesScopeAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scope := pairingScopeFixture(t)
		q, err := newPairingQR(context.Background(), scope)
		if err != nil {
			t.Fatal(err)
		}
		defer q.join(context.Background())
		oldSecret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 32)))
		newSecret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", 32)))
		if err := q.handle(&events.QR{Codes: []string{pairingCodeFixture("first", oldSecret), pairingCodeFixture("second", oldSecret)}}); err != nil {
			t.Fatal(err)
		}
		original, _ := q.read(scope)
		time.Sleep(10 * time.Second)
		rotation := &events.RotateADVSecret{OldSecret: oldSecret, NewSecret: newSecret}
		if err := q.handle(rotation); err != nil {
			t.Fatal(err)
		}
		rotated, _ := q.read(scope)
		want := "https://wa.me/settings/linked_devices#first," + oldSecret + "," + oldSecret + "," + newSecret + ",1"
		if rotated.Code != want || !rotated.ExpiresAt.Equal(original.ExpiresAt) || rotated.Paired || rotated.Connected {
			t.Fatalf("ADV rotation changed identity fields, expiry or authority: %+v", rotated)
		}
		if err := q.handle(rotation); err != nil {
			t.Fatalf("exact rotation repeat: %v", err)
		}
		time.Sleep(50 * time.Second)
		synctest.Wait()
		second, _ := q.read(scope)
		if !strings.Contains(second.Code, "#second,") || !strings.HasSuffix(second.Code, ","+newSecret+",1") {
			t.Fatalf("next code retained old ADV secret: %+v", second)
		}
		if err := q.handle(&events.RotateADVSecret{OldSecret: oldSecret, NewSecret: "invalid"}); !errors.Is(err, errPairingFormat) {
			t.Fatalf("invalid rotation = %v", err)
		}
		failed, err := q.read(scope)
		if !errors.Is(err, errPairingFormat) || failed.Code != "" {
			t.Fatal("bad rotation retained or disclosed stale QR material")
		}
	})
}

func TestWhatsAppDirectQRRefusesOverflowAndUnsupportedModes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event any
		want  error
	}{
		{"count", &events.QR{Codes: make([]string, maxPairingQRCodes+1)}, errPairingBounds},
		{"bytes", &events.QR{Codes: []string{strings.Repeat("x", maxPairingQRBytes+1)}}, errPairingBounds},
		{"empty_code", &events.QR{Codes: []string{""}}, errPairingBounds},
		{"nil_batch", (*events.QR)(nil), errPairingBounds},
		{"passkey_request", &events.PairPasskeyRequest{}, errPairingUnsupported},
		{"passkey_confirmation", &events.PairPasskeyConfirmation{SkipHandoffUX: true}, errPairingUnsupported},
		{"passkey_error", &events.PairPasskeyError{}, errPairingUnsupported},
		{"non_multidevice", &events.QRScannedWithoutMultidevice{}, errPairingUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := pairingScopeFixture(t)
			q, err := newPairingQR(context.Background(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer q.join(context.Background())
			if err := q.handle(&events.QR{Codes: []string{"previous"}}); err != nil {
				t.Fatal(err)
			}
			if err := q.handle(tc.event); !errors.Is(err, tc.want) {
				t.Fatalf("refusal = %v", err)
			}
			if got, err := q.read(scope); got.Code != "" || !errors.Is(err, tc.want) || got.Status != pairingFailed {
				t.Fatalf("failure left a visible QR: %+v, %v", got, err)
			}
			if err := q.handle(&events.QR{Codes: []string{"unadmitted retry"}}); !errors.Is(err, tc.want) {
				t.Fatalf("failed occurrence reopened: %v", err)
			}
		})
	}
}

func TestWhatsAppDirectQRADVRotationRefusesMismatchedEvidence(t *testing.T) {
	oldSecret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 32)))
	newSecret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", 32)))
	for _, tc := range []struct{ name, code, old, next string }{
		{"wrong_adv", pairingCodeFixture("ref", newSecret), oldSecret, oldSecret},
		{"not_uri", "opaque unexpected QR", oldSecret, newSecret},
		{"wrong_host", strings.Replace(pairingCodeFixture("ref", oldSecret), "wa.me", "unexpected.test", 1), oldSecret, newSecret},
		{"wrong_path", strings.Replace(pairingCodeFixture("ref", oldSecret), "linked_devices", "other", 1), oldSecret, newSecret},
		{"missing_parts", "https://wa.me/settings/linked_devices#ref," + oldSecret, oldSecret, newSecret},
		{"short_old", pairingCodeFixture("ref", oldSecret), base64.StdEncoding.EncodeToString([]byte("short")), newSecret},
		{"empty_new", pairingCodeFixture("ref", oldSecret), oldSecret, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := pairingScopeFixture(t)
			q, err := newPairingQR(context.Background(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer q.join(context.Background())
			if err := q.handle(&events.QR{Codes: []string{tc.code}}); err != nil {
				t.Fatal(err)
			}
			if err := q.handle(&events.RotateADVSecret{OldSecret: tc.old, NewSecret: tc.next}); !errors.Is(err, errPairingFormat) {
				t.Fatalf("rotation guessed a replacement: %v", err)
			}
			if got, err := q.read(scope); got.Code != "" || !errors.Is(err, errPairingFormat) {
				t.Fatalf("mismatched rotation retained QR disclosure: %+v, %v", got, err)
			}
		})
	}
}

func TestWhatsAppDirectQRPairAndConnectionObservationsStayDistinct(t *testing.T) {
	for _, connectedFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("connected_first_%t", connectedFirst), func(t *testing.T) {
			scope := pairingScopeFixture(t)
			q, err := newPairingQR(context.Background(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer q.join(context.Background())
			facts := []any{&events.PairSuccess{}, &events.Connected{}}
			if connectedFirst {
				facts[0], facts[1] = facts[1], facts[0]
			}
			for _, fact := range facts {
				if err := q.handle(fact); err != nil {
					t.Fatal(err)
				}
			}
			if err := q.handle(&events.QR{Codes: []string{"late code"}}); err != nil {
				t.Fatal(err)
			}
			got, err := q.read(scope)
			if err != nil || !got.Paired || !got.Connected || got.Status != pairingConnected || got.Code != "" {
				t.Fatalf("observation order changed terminal QR semantics: %+v, %v", got, err)
			}
			if err := q.handle(&events.Disconnected{}); !errors.Is(err, errPairingFailed) {
				t.Fatal("post-pairing disconnect was ignored")
			}
			got, err = q.read(scope)
			if !errors.Is(err, errPairingFailed) || got.Code != "" || got.Connected {
				t.Fatal("disconnect retained current connection evidence")
			}
		})
	}
}

func TestWhatsAppDirectQRTerminalAndReadbackScopeFences(t *testing.T) {
	for _, boundary := range []string{"pair_success", "connected", "cancel", "retire", "concurrent_terminal"} {
		t.Run(boundary, func(t *testing.T) {
			scope := pairingScopeFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			q, err := newPairingQR(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			defer q.join(context.Background())
			if err := q.handle(&events.QR{Codes: []string{"original"}}); err != nil {
				t.Fatal(err)
			}
			for _, mutate := range []func(*pairingQRScope){
				func(s *pairingQRScope) { s.PrincipalID = uuid.NewString() },
				func(s *pairingQRScope) { s.OperationID = uuid.NewString() },
				func(s *pairingQRScope) { s.OccurrenceID = uuid.NewString() },
				func(s *pairingQRScope) { s.ConnectionID = uuid.NewString() },
				func(s *pairingQRScope) { s.Coordinate.ContextPublicationGeneration++ },
				func(s *pairingQRScope) { s.Coordinate.RuntimeInstanceID = uuid.NewString() },
				func(s *pairingQRScope) { s.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) },
			} {
				foreign := scope
				mutate(&foreign)
				if got, err := q.read(foreign); got.Code != "" || !errors.Is(err, errPairingScope) {
					t.Fatal("foreign principal, responsibility, source or occurrence disclosed QR")
				}
			}
			switch boundary {
			case "pair_success":
				if err := q.handle(&events.PairSuccess{}); err != nil {
					t.Fatal(err)
				}
				paired, _ := q.read(scope)
				if !paired.Paired || paired.Connected || paired.Code != "" || paired.Status != pairingSucceeded {
					t.Fatalf("PairSuccess became Connected or retained QR: %+v", paired)
				}
			case "connected":
				if err := q.handle(&events.Connected{}); err != nil {
					t.Fatal(err)
				}
				connected, _ := q.read(scope)
				if connected.Paired || !connected.Connected || connected.Code != "" || connected.Status != pairingConnected {
					t.Fatalf("Connected manufactured PairSuccess: %+v", connected)
				}
			case "cancel":
				cancel()
			case "retire":
				q.stop()
			case "concurrent_terminal":
				var workers sync.WaitGroup
				start := make(chan struct{})
				for index := 0; index < 64; index++ {
					workers.Add(1)
					go func() {
						defer workers.Done()
						<-start
						_ = q.handle(&events.QR{Codes: []string{"racing replacement"}})
						_ = q.handle(&events.PairSuccess{})
						_ = q.handle(&events.Disconnected{})
					}()
				}
				close(start)
				workers.Wait()
			}
			_ = q.handle(&events.QR{Codes: []string{"late disclosure"}})
			if got, _ := q.read(scope); got.Code != "" {
				t.Fatal("late batch crossed terminal/cancellation/retirement fence")
			}
			if err := q.join(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-q.done:
			default:
				t.Fatal("QR join did not wait for its expiry worker")
			}
			successor := scope
			successor.OccurrenceID = uuid.NewString()
			next, err := newPairingQR(context.Background(), successor)
			if err != nil {
				t.Fatal(err)
			}
			defer next.join(context.Background())
			if err := next.handle(&events.QR{Codes: []string{"successor"}}); err != nil {
				t.Fatal(err)
			}
			if got, err := next.read(scope); got.Code != "" || !errors.Is(err, errPairingScope) {
				t.Fatal("predecessor readback crossed successor scope")
			}
		})
	}
}

func TestWhatsAppDirectQRRejectsInvalidBootstrapScope(t *testing.T) {
	for _, boundary := range []string{"principal", "operation", "connection", "occurrence", "source", "live_context", "nil_context", "canceled_context"} {
		t.Run(boundary, func(t *testing.T) {
			scope := pairingScopeFixture(t)
			ctx := context.Background()
			switch boundary {
			case "principal":
				scope.PrincipalID = ""
			case "operation":
				scope.OperationID = ""
			case "connection":
				scope.ConnectionID = ""
			case "occurrence":
				scope.OccurrenceID = ""
			case "source":
				scope.Coordinate.BundleHash = ""
			case "live_context":
				scope.Coordinate.ContextPublicationGeneration = 0
			case "nil_context":
				ctx = nil
			case "canceled_context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if q, err := newPairingQR(ctx, scope); err == nil {
				_ = q.join(context.Background())
				t.Fatal("unowned bootstrap could start or disclose pairing material")
			}
		})
	}
}

func TestWhatsAppDirectQRWorkerInventory(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "pairing_qr.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"go": 1, "NewTimer": 1, "AfterFunc": 0, "After": 0, "NewTicker": 0}
	got := make(map[string]int, len(want))
	for name := range want {
		got[name] = 0
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			got["go"]++
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if method, ok := call.Fun.(*ast.SelectorExpr); ok {
				if _, listed := got[method.Sel.Name]; listed {
					got[method.Sel.Name]++
				}
			}
		}
		return true
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QR expiry is not one bounded joined worker: got %v, want %v", got, want)
	}
}
