//go:build linux || darwin

package sessionprovider

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestWhatsAppHeldAuthenticationCheckpointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "held-auth-checkpoint"})
			if err != nil {
				t.Fatal("start original bootstrap for the authentication cut")
			}
			accountRef := pairedBootstrapStateFixture(t, f, started.Operation)
			f.peer = newSDKPeerMode(t, false)
			if err := f.ResumeSession(f.ctx, started.Operation, f.candidate); err != nil {
				t.Fatal("resume original bootstrap before held authentication")
			}
			connection := f.connection
			occurrence := connection.state.currentOccurrence()
			socket := pairingSocketFixture(t, f.peer)
			before, ok, err := connection.CheckpointPairing(f.ctx, started.Operation.Revision)
			flags := authCheckpointFlags(connection, occurrence)
			flags["checkpoint_ok"], flags["checkpoint_error"] = ok, err != nil
			trace, _ := json.Marshal(flags)
			t.Logf("before SDK auth success: %s", trace)
			if err != nil || ok || flags["sdk_logged_in"] || !flags["transport_connected"] || !flags["same_occurrence"] ||
				!flags["live"] || flags["fenced"] || flags["state_closed"] || flags["state_retiring"] || !flags["bootstrap_completed"] ||
				before.OperationID != started.Operation.OperationID || before.Revision != started.Operation.Revision ||
				before.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
				t.Fatal("authentication cut did not isolate login from completed bootstrap; stop for the other predicate")
			}
			if err := socket.send(f.peer.ctx, waBinary.Node{Tag: "success", Attrs: waBinary.Attrs{
				"lid": types.NewJID("100000000001", types.HiddenUserServer), "t": time.Now().Unix()}}); err != nil {
				t.Fatal("release SDK authentication success")
			}
			if err := occurrence.awaitConnected(f.peer.ctx); err != nil {
				t.Fatal("released SDK authentication did not reach the owned connected milestone")
			}
			after, ok, err := connection.CheckpointPairing(f.ctx, started.Operation.Revision)
			flags = authCheckpointFlags(connection, occurrence)
			flags["checkpoint_ok"], flags["checkpoint_error"] = ok, err != nil
			trace, _ = json.Marshal(flags)
			t.Logf("after owned SDK connected milestone: %s", trace)
			if err != nil || !ok || !flags["sdk_logged_in"] || !flags["transport_connected"] || !flags["same_occurrence"] ||
				!flags["live"] || flags["fenced"] || flags["state_closed"] || flags["state_retiring"] || f.connection != connection ||
				after.OperationID != started.Operation.OperationID || after.SessionConnectionID != started.Operation.SessionConnectionID ||
				after.Coordinate != started.Operation.Coordinate || after.Revision != started.Operation.Revision+1 ||
				after.SessionAccount.AccountRef != accountRef || after.IdentityOperationID != "" || after.ActivationRevision != 0 {
				t.Fatal("owned login milestone did not checkpoint the exact original responsibility")
			}
		})
	}
}

func authCheckpointFlags(c *RuntimeConnection, occurrence *clientOccurrence) map[string]bool {
	occurrence.mu.Lock()
	fenced := occurrence.fenced
	occurrence.mu.Unlock()
	c.state.mu.Lock()
	closed, retiring := c.state.closed, c.state.retiring
	c.state.mu.Unlock()
	return map[string]bool{
		"same_occurrence": c.state.currentOccurrence() == occurrence,
		"live":            c.ctx.Err() == nil && occurrence.ctx.Err() == nil,
		"fenced":          fenced, "transport_connected": occurrence.client.IsConnected(),
		"state_closed": closed, "state_retiring": retiring,
		"sdk_logged_in": occurrence.client.IsLoggedIn(), "bootstrap_completed": c.CheckBootstrap(c.ctx) == nil,
	}
}
