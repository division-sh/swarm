package sessionprovider

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/sessionstate"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func openSDKStoreFixture(t *testing.T, path string) (*sessionstate.Fixture, *sessionstate.Owner) {
	t.Helper()
	fixture, owner, err := sessionstate.OpenSDKFixture(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	return fixture, owner
}

func newSDKDeviceFixture(t *testing.T, container *sessionstate.Owner) *store.Device {
	t.Helper()
	device := container.NewDevice()
	jid := types.NewJID("synthetic_test_account", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte{1}, AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := device.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	// This fixture represents an already paired account. Materialize its
	// uploaded prekeys rather than making authentication generate a fresh batch.
	prekeys, err := device.PreKeys.GetOrGenPreKeys(context.Background(), uint32(whatsmeow.MinPreKeyCount))
	if err != nil {
		t.Fatal(err)
	}
	if err := device.PreKeys.MarkPreKeysAsUploaded(context.Background(), prekeys[len(prekeys)-1].KeyID); err != nil {
		t.Fatal(err)
	}
	return device
}

func TestWhatsAppPrivateSDKCapabilitiesCannotExportContainerAdministration(t *testing.T) {
	_, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	check := func() {
		t.Helper()
		for name, capability := range map[string]any{
			"container": device.Container, "identities": device.Identities, "sessions": device.Sessions,
			"prekeys": device.PreKeys, "sender_keys": device.SenderKeys, "app_state_keys": device.AppStateKeys,
			"app_state": device.AppState, "contacts": device.Contacts, "settings": device.ChatSettings,
			"message_secrets": device.MsgSecrets, "privacy": device.PrivacyTokens, "salt": device.NCTSalt,
			"event_buffer": device.EventBuffer, "lids": device.LIDs, "global_lids": owner.LIDMap(),
		} {
			if _, exposed := capability.(interface{ Upgrade(context.Context) error }); exposed {
				t.Fatalf("%s exposes the SDK container's schema authority", name)
			}
		}
	}
	check()
	if err := device.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	check()
	guard, err := guardSDKStores(device, owner.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := device.Save(context.Background()); err != nil {
			t.Fatal(err)
		}
		if device.Container != guard || device.Identities != guard || device.LIDs != guard {
			t.Fatal("private persistence resealing erased the runtime write fence")
		}
		check()
	}
	if err := guard.fence.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := device.Save(context.Background()); !errors.Is(err, errSDKStoreFenced) {
		t.Fatalf("late save bypassed the runtime write fence: %v", err)
	}
}

func TestWhatsAppPinnedSDKDatabaseCloseDoesNotJoinTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider.db")
	db, container := openSDKStoreFixture(t, path)
	device := newSDKDeviceFixture(t, container)
	entered, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- device.EventBuffer.DoDecryptionTxn(context.Background(), func(ctx context.Context) error {
			close(entered)
			<-finish
			return device.Sessions.PutSession(ctx, "delayed_peer.0", []byte("late SDK write"))
		})
	}()
	<-entered
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatalf("unsafe-control late SDK transaction did not commit: %v", err)
	}
	_, reopened := openSDKStoreFixture(t, path)
	retained, err := reopened.GetDevice(context.Background(), *device.ID)
	if err != nil || retained == nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := retained.Sessions.GetSession(context.Background(), "delayed_peer.0")
	if err != nil || string(got) != "late SDK write" {
		t.Fatalf("unsafe-control close unexpectedly prevented late write: %q %v", got, err)
	}
}

func TestWhatsAppSDKStoreJoinWaitsForNestedTransactionAndRejectsLateWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider.db")
	db, container := openSDKStoreFixture(t, path)
	device := newSDKDeviceFixture(t, container)
	guard, err := guardSDKStores(device, container.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var lateContext context.Context
	go func() {
		done <- device.EventBuffer.DoDecryptionTxn(context.Background(), func(ctx context.Context) error {
			lateContext = context.WithoutCancel(ctx)
			close(entered)
			<-finish
			return device.Sessions.PutSession(ctx, "delayed_peer.0", []byte("joined SDK write"))
		})
	}()
	<-entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := guard.fence.join(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("unfinished state joined: %v", err)
	}
	if err := device.Sessions.PutSession(context.Background(), "new_peer.0", []byte("refused")); !errors.Is(err, errSDKStoreFenced) {
		t.Fatalf("new unowned SDK work entered fenced state: %v", err)
	}
	select {
	case <-guard.fence.drained:
		t.Fatal("old transaction released state possession before finishing")
	default:
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := guard.fence.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := device.Sessions.PutSession(lateContext, "late_peer.0", []byte("refused")); !errors.Is(err, errSDKStoreFenced) {
		t.Fatalf("ended transaction context reopened old state: %v", err)
	}
	_, reopened := openSDKStoreFixture(t, path)
	retained, err := reopened.GetDevice(context.Background(), *device.ID)
	if err != nil || retained == nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := retained.Sessions.GetSession(context.Background(), "delayed_peer.0")
	if err != nil || string(got) != "joined SDK write" {
		t.Fatalf("joined work lost: %q %v", got, err)
	}
	for _, address := range []string{"new_peer.0", "late_peer.0"} {
		got, err := retained.Sessions.GetSession(context.Background(), address)
		if err != nil || len(got) != 0 {
			t.Fatalf("stale SDK work touched successor state: %s %q %v", address, got, err)
		}
	}
}

func TestWhatsAppSDKStoreDeviceSaveKeepsEveryStateFieldGuarded(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := container.NewDevice()
	guard, err := guardSDKStores(device, container.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	jid := types.NewJID("synthetic_test_account", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte{1}, AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := device.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	value := reflect.ValueOf(device).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if field.Type.Kind() != reflect.Interface || field.Type.PkgPath() != "go.mau.fi/whatsmeow/store" {
			continue
		}
		if got := value.Field(i).Interface(); got != guard {
			t.Fatalf("SDK save replaced guarded state field %s with %T", field.Name, got)
		}
	}
	if err := device.Sessions.PutSession(context.Background(), "ready_peer.0", []byte("normal control")); err != nil {
		t.Fatal(err)
	}
}

func TestWhatsAppSDKStoreRefusesImplicitAndRetiredLogout(t *testing.T) {
	for _, mode := range []string{"implicit", "explicit", "retired"} {
		t.Run(mode, func(t *testing.T) {
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			guard, err := guardSDKStores(device, container.LIDMap())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if mode != "implicit" {
				ctx = context.WithValue(ctx, explicitLogoutKey{}, guard.fence)
			}
			if mode == "retired" {
				guard.fence.fence()
			}
			originalID := *device.ID
			err = device.Delete(ctx)
			if mode == "explicit" {
				if err != nil || !device.Deleted {
					t.Fatalf("explicit logout control: %v", err)
				}
			} else if !errors.Is(err, errExplicitLogoutRequired) || device.Deleted {
				t.Fatalf("implicit/stale deletion accepted: %v", err)
			}
			retained, err := container.GetDevice(context.Background(), originalID)
			if err != nil || (retained == nil) != (mode == "explicit") {
				t.Fatalf("wrong retained device state: %v %v", retained != nil, err)
			}
		})
	}
}

func TestWhatsAppSDKStoreInterfaceInventoryHasNoPromotedBypass(t *testing.T) {
	fset := token.NewFileSet()
	methods := map[string]*ast.FuncDecl{}
	for _, path := range []string{"store_guard.go", "store_guard_generated.go"} {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil {
				continue
			}
			receiver, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if name, ok := receiver.X.(*ast.Ident); ok && name.Name == "sdkStores" {
				methods[fn.Name.Name] = fn
			}
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[store.AllStores](), reflect.TypeFor[store.DeviceContainer]()} {
		for i := 0; i < typ.NumMethod(); i++ {
			method := typ.Method(i)
			fn := methods[method.Name]
			if fn == nil {
				t.Fatalf("SDK method %s is not an explicit guarded consumer", method.Name)
			}
			var acquires, releases bool
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "acquire" || selector.Sel.Name == "acquireCurrent") {
						acquires = true
					}
				}
				if def, ok := node.(*ast.DeferStmt); ok {
					if name, ok := def.Call.Fun.(*ast.Ident); ok && name.Name == "release" {
						releases = true
					}
				}
				return true
			})
			if !acquires || !releases {
				t.Fatalf("SDK method %s bypasses state admission/join", method.Name)
			}
		}
	}
}

func TestWhatsAppSDKStoreDisablesHiddenPlaintextAndRetrySpools(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	_, err := guardSDKStores(device, container.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	if err := device.EventBuffer.PutBufferedEvent(context.Background(), [32]byte{1}, []byte("plaintext"), time.Now()); !errors.Is(err, errSDKDecryptedBufferDisabled) {
		t.Fatalf("hidden decrypted-event spool: %v", err)
	}
	if err := device.EventBuffer.AddOutgoingEvent(context.Background(), *device.ID, "outbound", "wa", []byte("plaintext")); !errors.Is(err, errSDKReplayStoreDisabled) {
		t.Fatalf("hidden outbound spool: %v", err)
	}
}

func TestWhatsAppEverySDKStateMethodRefusesAfterFence(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	guard, err := guardSDKStores(device, container.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	guard.fence.fence()
	for _, contract := range []reflect.Type{reflect.TypeFor[store.AllStores](), reflect.TypeFor[store.DeviceContainer]()} {
		for i := 0; i < contract.NumMethod(); i++ {
			name := contract.Method(i).Name
			t.Run(name, func(t *testing.T) {
				method := reflect.ValueOf(guard).MethodByName(name)
				args := []reflect.Value{reflect.ValueOf(context.Background())}
				for i := 1; i < method.Type().NumIn(); i++ {
					args = append(args, reflect.Zero(method.Type().In(i)))
				}
				var results []reflect.Value
				if method.Type().IsVariadic() {
					results = method.CallSlice(args)
				} else {
					results = method.Call(args)
				}
				if results[len(results)-1].IsNil() {
					t.Fatal("fenced SDK state method returned success")
				}
			})
		}
	}
}
