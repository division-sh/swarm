//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// Invariant: stored-device request validity is distinct from native write permission, both kinds/stores.
func TestWhatsAppActivityRequestKeepsOriginalAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, registered := range []bool{false, true} {
			if !t.Run(fmt.Sprintf("%s/registered=%t", backend, registered), func(t *testing.T) {
				f, c, _ := recoveryNativeFixture(t, backend, registered)
				validate := func(ctx context.Context) error {
					return c.ValidateActivityRequestAccount(ctx, f.operation.SessionAccount, f.operation.OperationID, f.operation.Revision)
				}
				if err := validate(f.ctx); err != nil {
					t.Fatal("healthy request", err)
				}
				f.peer.mu.Lock()
				wire := f.peer.peers[len(f.peer.peers)-1]
				f.peer.mu.Unlock()
				if err := wire.conn.CloseNow(); err != nil {
					t.Fatal(err)
				}
				awaitDiagnostic(t, func() bool { return c.state.currentOccurrence().diagnosticSnapshot().Down })
				if err := validate(f.ctx); err != nil {
					t.Fatal("positive transient request refused", err)
				}
				account, err := c.AdmitActivityAccount(f.ctx, f.operation.SessionAccount)
				account.Close()
				if err == nil {
					t.Fatal("request validity minted live write authority")
				}
				if err := c.ValidateActivityRequestAccount(f.ctx, f.operation.SessionAccount, uuid.NewString(), f.operation.Revision); err == nil {
					t.Fatal("different parent admitted")
				}
				if err := c.ValidateActivityRequestAccount(f.ctx, f.operation.SessionAccount, f.operation.OperationID, f.operation.Revision+1); err == nil {
					t.Fatal("future revision admitted")
				}
				operation, original := c.operation, c.state.expectedAccount
				c.operation.PrincipalID = uuid.NewString()
				if err := validate(f.ctx); err == nil {
					t.Error("stale original owner admitted")
				}
				c.operation = operation
				c.state.expectedAccount = "15550000999@s.whatsapp.net"
				if err := validate(f.ctx); err == nil {
					t.Error("different native device admitted")
				}
				c.state.expectedAccount = original
				operations, fault := c.operations, errors.New("request storage failure")
				c.operations = diagnosticOperationFault{operations, fault}
				if err := validate(f.ctx); !errors.Is(err, fault) {
					t.Error("storage failure hidden", err)
				}
				c.operations = operations
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				if err := validate(ctx); err == nil {
					t.Fatal("cancellation admitted")
				}
				c.state.currentOccurrence().callbacks.fail("capture_failed", fault)
				if err := validate(f.ctx); err == nil {
					t.Fatal("capture/transport mixture admitted")
				}
				if err := c.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				if err := validate(f.ctx); err == nil {
					t.Fatal("closed control admitted request")
				}
			}) {
				t.Fatal("first backend/kind failed")
			}
		}
	}
}
