//go:build linux || darwin

package sessionprovider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

var errSessionAccount = errors.New("WhatsApp private state does not match its exact retained account")

// sessionState possesses the provider database, not executable admission. The
// onboarding/activation owner must reserve responsibility before opening it.
type sessionState struct {
	lifecycleMu     sync.Mutex
	mu              sync.Mutex
	directory       *sessionDirectory
	file            *os.File
	database        *sql.DB
	container       *sqlstore.Container
	expectedAccount string
	occurrence      *clientOccurrence
	retiring        bool
	closed          bool
}

func openSessionState(ctx context.Context, basePath, connectionID, expectedAccount string) (*sessionState, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errClientOccurrenceFenced
	}
	if expectedAccount != "" {
		jid, err := types.ParseJID(expectedAccount)
		if err != nil || jid.User == "" || jid.Server != types.DefaultUserServer || jid.ToNonAD().String() != expectedAccount {
			return nil, errSessionAccount
		}
	}
	directory, err := openSessionDirectory(basePath, connectionID)
	if err != nil {
		return nil, err
	}
	s := &sessionState{directory: directory, expectedAccount: expectedAccount}
	if err := s.openDatabase(ctx); err != nil {
		if cleanupErr := s.close(context.WithoutCancel(ctx)); cleanupErr != nil {
			return s, errors.Join(err, cleanupErr)
		}
		return nil, err
	}
	return s, nil
}

func (s *sessionState) openDatabase(ctx context.Context) error {
	flags := unix.O_RDWR
	if s.expectedAccount == "" {
		flags |= unix.O_CREAT
	}
	file, err := openPrivateAt(int(s.directory.directory.Fd()), "provider.db", flags, false)
	if err != nil {
		return err
	}
	s.file = file
	for _, name := range []string{"provider.db-journal", "provider.db-wal", "provider.db-shm"} {
		file, err := openPrivateAt(int(s.directory.directory.Fd()), name, unix.O_RDONLY, false)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	address := url.URL{Scheme: "file", Path: filepath.Join(s.directory.path, "provider.db")}
	query := url.Values{"mode": {"rw"}, "_pragma": {"foreign_keys(1)", "busy_timeout(5000)"}}
	address.RawQuery = query.Encode()
	database, err := sql.Open("sqlite", address.String())
	if err != nil {
		return err
	}
	s.database = database
	database.SetMaxOpenConns(1)
	s.container = sqlstore.NewWithDB(database, "sqlite", waLog.Noop)
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != 0 {
		if _, err := s.device(ctx); err != nil {
			return err
		}
	} else if s.expectedAccount != "" {
		return errSessionAccount
	}
	if err := s.container.Upgrade(ctx); err != nil {
		return err
	}
	_, err = s.device(ctx)
	return err
}

func (s *sessionState) device(ctx context.Context) (*store.Device, error) {
	devices, err := s.container.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	if len(devices) > 1 {
		return nil, errSessionAccount
	}
	if len(devices) == 0 {
		if s.expectedAccount != "" {
			return nil, errSessionAccount
		}
		return s.container.NewDevice(), nil
	}
	device := devices[0]
	if device.ID == nil || device.ID.User == "" || device.ID.Server != types.DefaultUserServer ||
		s.expectedAccount != "" && device.ID.ToNonAD().String() != s.expectedAccount {
		return nil, errSessionAccount
	}
	return device, nil
}

func (s *sessionState) newOccurrence(ctx context.Context, occurrenceID string) (*clientOccurrence, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.retiring || s.closed || s.occurrence != nil || ctx == nil || ctx.Err() != nil {
		s.mu.Unlock()
		return nil, errClientOccurrenceFenced
	}
	s.mu.Unlock()
	device, err := s.device(ctx)
	if err != nil {
		return nil, err
	}
	occurrence, err := newClientOccurrence(ctx, s.directory.connectionID, occurrenceID, device, s.container.LIDMap, waLog.Noop)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.occurrence = occurrence
	s.mu.Unlock()
	return occurrence, nil
}

func (s *sessionState) currentOccurrence() *clientOccurrence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.occurrence
}

func (s *sessionState) ownsConnectedOccurrence(ctx context.Context, occurrence *clientOccurrence) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ctx != nil && ctx.Err() == nil && occurrence != nil && !s.closed && !s.retiring && s.occurrence == occurrence &&
		occurrence.ctx.Err() == nil && occurrence.client.IsConnected() && occurrence.client.IsLoggedIn()
}

func (s *sessionState) retireOccurrence(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.retireOccurrenceLocked(ctx)
}

func (s *sessionState) retireOccurrenceLocked(ctx context.Context) error {
	occurrence := s.currentOccurrence()
	if occurrence == nil {
		return nil
	}
	if err := occurrence.join(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.occurrence = nil
	s.mu.Unlock()
	return nil
}

func (s *sessionState) close(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.retiring = true
	s.mu.Unlock()
	if err := s.retireOccurrenceLocked(ctx); err != nil {
		return err
	}
	if s.database != nil {
		if err := s.database.Close(); err != nil {
			return fmt.Errorf("close WhatsApp provider database: %w", err)
		}
		s.database = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("close WhatsApp provider state descriptor: %w", err)
		}
		s.file = nil
	}
	if err := s.directory.release(); err != nil {
		return err
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
