//go:build linux || darwin

package whatsapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func sessionStateFixture(t *testing.T, base, connectionID, account string) *sessionState {
	t.Helper()
	state, err := openSessionState(context.Background(), base, connectionID, account)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := state.close(ctx); err != nil {
			t.Error(err)
		}
	})
	return state
}

func TestWhatsAppSessionStateRealSDKRestartRetainsExactAccount(t *testing.T) {
	base, connectionID := t.TempDir(), uuid.NewString()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	state := sessionStateFixture(t, base, connectionID, "")
	device := newSDKDeviceFixture(t, state.container)
	account, originalNoise := device.ID.ToNonAD().String(), *device.NoiseKey.Priv
	peer := newSDKPeer(t)
	first, err := state.newOccurrence(peer.ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	peer.attach(t, first.client)
	if err := first.connect(); err != nil || !first.client.WaitForConnection(5*time.Second) {
		t.Fatal("first owned SDK connection failed", err)
	}
	if err := first.client.Store.Sessions.PutSession(context.Background(), "retained_peer.0", []byte("retained SDK session")); err != nil {
		t.Fatal(err)
	}
	if err := state.close(peer.ctx); err != nil {
		t.Fatal(err)
	}
	state = sessionStateFixture(t, base, connectionID, account)
	second, err := state.newOccurrence(peer.ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if second.client == first.client || second.occurrenceID == first.occurrenceID ||
		second.connectionID != first.connectionID || *second.client.Store.NoiseKey.Priv != originalNoise {
		t.Fatal("restart cloned authority, changed pairing, or reused the old occurrence")
	}
	retained, err := second.client.Store.Sessions.GetSession(context.Background(), "retained_peer.0")
	if err != nil || !bytes.Equal(retained, []byte("retained SDK session")) {
		t.Fatal("ordinary state close/reopen lost SDK state", err)
	}
	peer.attach(t, second.client)
	if err := second.connect(); err != nil || !second.client.WaitForConnection(5*time.Second) {
		t.Fatal("retained owned SDK connection failed", err)
	}
	done := make(chan error, 1)
	go func() { done <- runOccurrenceProbe(peer.ctx, second, "send", "EXPLICIT_RESTART_SEND") }()
	accepted := peer.next(t)
	peer.acknowledge(t, accepted)
	if err := awaitOccurrenceProbe(t, peer.ctx, done); err != nil {
		t.Fatal(err)
	}
}

func TestWhatsAppSessionStateFailedJoinRetainsDatabaseAndPossession(t *testing.T) {
	for _, boundary := range []string{"occurrence", "connection"} {
		t.Run(boundary, func(t *testing.T) {
			base, connectionID := t.TempDir(), uuid.NewString()
			if err := os.Chmod(base, 0o700); err != nil {
				t.Fatal(err)
			}
			state := sessionStateFixture(t, base, connectionID, "")
			paired := newSDKDeviceFixture(t, state.container)
			account := paired.ID.ToNonAD().String()
			occurrence, err := state.newOccurrence(context.Background(), uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			entered, finish := make(chan struct{}), make(chan struct{})
			var finishOnce sync.Once
			defer finishOnce.Do(func() { close(finish) })
			done := make(chan error, 1)
			go func() {
				done <- occurrence.client.Store.EventBuffer.DoDecryptionTxn(context.Background(), func(ctx context.Context) error {
					close(entered)
					<-finish
					return occurrence.client.Store.Sessions.PutSession(ctx, "late_peer.0", []byte("joined SDK transaction"))
				})
			}()
			<-entered
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if boundary == "occurrence" {
				err = state.retireOccurrence(canceled)
			} else {
				err = state.close(canceled)
			}
			if !errors.Is(err, context.Canceled) || state.database == nil || state.closed {
				t.Fatal("unjoined provider state was released", err)
			}
			if _, err := state.newOccurrence(context.Background(), uuid.NewString()); err == nil {
				t.Fatal("successor used state before the old occurrence fully joined")
			}
			if other, err := openSessionState(context.Background(), base, connectionID, account); !errors.Is(err, errSessionPossession) {
				if other != nil {
					_ = other.close(context.Background())
				}
				t.Fatal("failed join released exclusive possession", err)
			}
			finishOnce.Do(func() { close(finish) })
			if err := <-done; err != nil {
				t.Fatal("admitted SDK transaction could not finish", err)
			}
			if boundary == "occurrence" {
				if err := state.retireOccurrence(context.Background()); err != nil {
					t.Fatal(err)
				}
				fresh, err := state.newOccurrence(context.Background(), uuid.NewString())
				if err != nil || fresh == occurrence {
					t.Fatal("fully joined state could not create a fresh occurrence", err)
				}
			}
			if err := state.close(context.Background()); err != nil {
				t.Fatal(err)
			}
			reopened := sessionStateFixture(t, base, connectionID, account)
			device, err := reopened.device(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			retained, err := device.Sessions.GetSession(context.Background(), "late_peer.0")
			if err != nil || !bytes.Equal(retained, []byte("joined SDK transaction")) {
				t.Fatal("join/reopen lost the admitted SDK transaction", err)
			}
		})
	}
}

func TestWhatsAppSessionStateRefusesUnsafeOrConflictingState(t *testing.T) {
	for _, cell := range []string{"missing_database", "missing_account", "foreign_account", "multiple_accounts", "database_symlink", "database_mode", "database_hardlink", "journal_symlink", "journal_mode", "corrupt_database", "invalid_account_reference"} {
		t.Run(cell, func(t *testing.T) {
			base, connectionID := t.TempDir(), uuid.NewString()
			if err := os.Chmod(base, 0o700); err != nil {
				t.Fatal(err)
			}
			state := sessionStateFixture(t, base, connectionID, "")
			account := "synthetic_test_account@" + types.DefaultUserServer
			if cell != "missing_account" {
				device := newSDKDeviceFixture(t, state.container)
				account = device.ID.ToNonAD().String()
				if cell == "multiple_accounts" {
					second := state.container.NewDevice()
					jid := types.NewJID("another_test_account", types.DefaultUserServer)
					second.ID, second.Account = &jid, device.Account
					if err := second.Save(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			}
			path := filepath.Join(state.directory.path, "provider.db")
			if err := state.close(context.Background()); err != nil {
				t.Fatal(err)
			}
			switch cell {
			case "missing_database":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "foreign_account":
				account = "foreign_test_account@" + types.DefaultUserServer
			case "database_symlink":
				if err := os.Rename(path, path+"-original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+"-original", path); err != nil {
					t.Fatal(err)
				}
			case "database_mode":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "database_hardlink":
				if err := os.Link(path, path+"-linked"); err != nil {
					t.Fatal(err)
				}
			case "journal_symlink":
				if err := os.Symlink(path, path+"-journal"); err != nil {
					t.Fatal(err)
				}
			case "journal_mode":
				if err := os.WriteFile(path+"-journal", []byte("unsafe journal"), 0o644); err != nil {
					t.Fatal(err)
				}
			case "corrupt_database":
				if err := os.WriteFile(path, []byte("corrupt provider state must not be repaired"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "invalid_account_reference":
				account = "guessed_account"
			}
			before, readErr := os.ReadFile(path)
			refused, err := openSessionState(context.Background(), base, connectionID, account)
			if err == nil || refused != nil {
				if refused != nil {
					_ = refused.close(context.Background())
				}
				t.Fatal("unsafe or conflicting provider state was accepted", err)
			}
			after, err := os.ReadFile(path)
			if readErr == nil && (err != nil || !bytes.Equal(before, after)) ||
				errors.Is(readErr, os.ErrNotExist) && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refusal repaired, created, replaced or deleted provider state")
			}
		})
	}
}

func TestWhatsAppSessionStateCanceledOpenCreatesNothing(t *testing.T) {
	base := t.TempDir()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if state, err := openSessionState(ctx, base, uuid.NewString(), ""); err == nil || state != nil {
		t.Fatal("canceled bootstrap opened provider state", err)
	}
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) != 0 {
		t.Fatal("canceled bootstrap created local state", err)
	}
}

func TestWhatsAppSessionStateCloseJoinsCallbackWithoutBlockingIdentityRead(t *testing.T) {
	base, connectionID := t.TempDir(), uuid.NewString()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	state := sessionStateFixture(t, base, connectionID, "")
	newSDKDeviceFixture(t, state.container)
	occurrence, err := state.newOccurrence(context.Background(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	defer finishOnce.Do(func() { close(finish) })
	guard, err := occurrence.bindCallbacks(func(context.Context, any) error {
		close(entered)
		<-finish
		if state.currentOccurrence() != occurrence {
			return errCaptureScopeChanged
		}
		return nil
	}, func(context.Context, callbackFailure) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	captured := make(chan bool, 1)
	go func() { captured <- guard.receive(&events.Message{}) }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- state.close(context.Background()) }()
	<-occurrence.ctx.Done()
	finishOnce.Do(func() { close(finish) })
	if <-captured {
		t.Fatal("callback reported success after its connection retired")
	}
	if err := <-closed; err != nil {
		t.Fatal("connection close did not join the callback", err)
	}
	if state.currentOccurrence() != nil {
		t.Fatal("joined connection retained an executable occurrence")
	}
}

func TestWhatsAppSessionDirectoryDescriptorFailureKeepsPossession(t *testing.T) {
	base, connectionID := t.TempDir(), uuid.NewString()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := openSessionDirectory(base, connectionID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = directory.lock.Close()
		_ = directory.base.Close()
	})
	if err := directory.directory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := directory.release(); err == nil || directory.released {
		t.Fatal("failed descriptor close reported released possession")
	}
	if other, err := openSessionDirectory(base, connectionID); !errors.Is(err, errSessionPossession) {
		if other != nil {
			_ = other.release()
		}
		t.Fatal("descriptor failure released the only possession evidence", err)
	}
}
