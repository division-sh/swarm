package sessionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

var ErrSessionAccount = errors.New("WhatsApp private state does not match its exact retained account")

// Owner contains only the connection-private SDK/capture database. It is never
// a selected runtime store and does not issue native execution authority.
type Owner struct {
	db        *sql.DB
	container *sqlstore.Container
}

func Open(ctx context.Context, path, expectedAccount string, nonempty bool) (*Owner, error) {
	owner, err := openDatabase(path)
	if err != nil {
		return nil, err
	}
	if nonempty {
		_, err = owner.CurrentDevice(ctx, expectedAccount)
	} else if expectedAccount != "" {
		err = ErrSessionAccount
	}
	if err == nil {
		err = owner.container.Upgrade(ctx)
	}
	if err == nil {
		_, err = owner.CurrentDevice(ctx, expectedAccount)
	}
	if err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func openDatabase(path string) (*Owner, error) {
	address := url.URL{Scheme: "file", Path: filepath.Clean(path)}
	address.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"foreign_keys(1)", "busy_timeout(5000)", "synchronous(FULL)"}}.Encode()
	db, err := sql.Open("sqlite", address.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &Owner{db: db, container: sqlstore.NewWithDB(db, "sqlite", waLog.Noop)}, nil
}

func (o *Owner) Close() error { return o.db.Close() }

func (o *Owner) CurrentDevice(ctx context.Context, expectedAccount string) (*store.Device, error) {
	devices, err := o.container.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	if len(devices) > 1 {
		return nil, ErrSessionAccount
	}
	if len(devices) == 0 {
		if expectedAccount != "" {
			return nil, ErrSessionAccount
		}
		return o.NewDevice(), nil
	}
	device := devices[0]
	if device.ID == nil || device.ID.User == "" || device.ID.Server != types.DefaultUserServer ||
		expectedAccount != "" && device.ID.ToNonAD().String() != expectedAccount {
		return nil, ErrSessionAccount
	}
	return o.wrapDevice(device), nil
}

func (o *Owner) NewDevice() *store.Device { return o.wrapDevice(o.container.NewDevice()) }

func (o *Owner) GetDevice(ctx context.Context, jid types.JID) (*store.Device, error) {
	device, err := o.container.GetDevice(ctx, jid)
	if err != nil || device == nil {
		return device, err
	}
	return o.wrapDevice(device), nil
}

func (o *Owner) LIDMap() store.LIDStore { return &sdkLIDStorage{lids: o.container.LIDMap} }

func (o *Owner) Captures(ctx context.Context, connectionID string) (*CaptureStore, error) {
	return newCaptureStore(ctx, o.db, connectionID)
}

// The SDK's container can initialize schema and open storage. Devices expose
// only its exact save/delete interface, never the SQL-capable container itself.
type deviceContainer struct{ owner *Owner }

func (o *Owner) wrapDevice(device *store.Device) *store.Device {
	if inner, raw := device.Identities.(*sqlstore.SQLStore); raw {
		device.SetAllStores(&sdkStorage{session: inner})
	}
	if _, raw := device.LIDs.(*sqlstore.CachedLIDMap); raw {
		device.LIDs = o.LIDMap()
	}
	if _, raw := device.Container.(*sqlstore.Container); raw {
		device.Container = deviceContainer{owner: o}
	}
	return device
}

//go:generate go run ./generate
type sdkStorage struct {
	session store.AllSessionSpecificStores
}
type sdkLIDStorage struct {
	lids    store.LIDStore
}

func (c deviceContainer) PutDevice(ctx context.Context, device *store.Device) error {
	err := c.owner.container.PutDevice(ctx, device)
	c.owner.wrapDevice(device)
	return err
}

func (c deviceContainer) DeleteDevice(ctx context.Context, device *store.Device) error {
	return c.owner.container.DeleteDevice(ctx, device)
}

func openFixtureFile(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	return file.Close()
}

func OpenSDKFixture(ctx context.Context, path string) (*Owner, error) {
	if err := openFixtureFile(path); err != nil {
		return nil, err
	}
	owner, err := openDatabase(path)
	if err != nil {
		return nil, err
	}
	if err := owner.container.Upgrade(ctx); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func OpenCaptureFixture(path string) (*Owner, error) {
	if err := openFixtureFile(path); err != nil {
		return nil, err
	}
	return openDatabase(path)
}
