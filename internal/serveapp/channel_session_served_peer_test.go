//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/division-sh/swarm/internal/testutil/whatsappfixture"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/types"
)

// Only the external TLS/Noise peer is replaced. RunServe constructs the real
// SDK, selected persistence, capture, identity and target owners.
type serveNativeProtocolPeer struct {
	t            *testing.T
	ctx          context.Context
	account      types.JID
	lid          types.JID
	mu           sync.Mutex
	wire         *whatsappfixture.Transport
	registration *waWa6.ClientPayload_DevicePairingRegistrationData
	prekeys      []waBinary.Node
	connections  int
	paired       chan struct{}
	pairOnce     sync.Once
	keysReady    chan struct{}
	keysOnce     sync.Once
	receipts     chan waBinary.Node
}

func newServeNativeProtocolPeer(t *testing.T) *serveNativeProtocolPeer {
	t.Helper()
	noise := whatsappfixture.NewNoiseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	peer := &serveNativeProtocolPeer{t: t, ctx: ctx,
		account: types.NewJID("15551234567", types.DefaultUserServer),
		lid:     types.NewJID("100000000001", types.HiddenUserServer), paired: make(chan struct{}),
		keysReady: make(chan struct{}), receipts: make(chan waBinary.Node, 16)}
	peer.account.Device, peer.lid.Device = 1, 1
	var mu sync.Mutex
	var sockets []*websocket.Conn
	var workers sync.WaitGroup
	closing := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if closing {
			mu.Unlock()
			http.Error(w, "closed", http.StatusServiceUnavailable)
			return
		}
		workers.Add(1)
		mu.Unlock()
		defer workers.Done()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"web.whatsapp.com"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(socket.FrameMaxSize)
		mu.Lock()
		sockets = append(sockets, conn)
		mu.Unlock()
		wire, err := noise.Handshake(ctx, conn)
		if err != nil {
			if ctx.Err() == nil {
				t.Error(err)
			}
			return
		}
		peer.serve(wire)
	}))
	base := http.DefaultTransport
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig.ServerName = "example.com"
	dial := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "web.whatsapp.com:443" {
			return dial.DialContext(ctx, network, server.Listener.Addr().String())
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("native served peer refuses external destination %q", address)
		}
		return dial.DialContext(ctx, network, address)
	}
	http.DefaultTransport = transport
	// Register this before RunServe: its LIFO cleanup joins SDK work first.
	t.Cleanup(func() {
		mu.Lock()
		closing = true
		owned := append([]*websocket.Conn(nil), sockets...)
		mu.Unlock()
		cancel()
		for _, conn := range owned {
			_ = conn.CloseNow()
		}
		server.Close()
		workers.Wait()
		transport.CloseIdleConnections()
		http.DefaultTransport = base
	})
	return peer
}

func (p *serveNativeProtocolPeer) serve(wire *whatsappfixture.Transport) {
	payload := wire.ClientPayload()
	p.mu.Lock()
	p.wire = wire
	p.connections++
	if payload.DevicePairingData != nil {
		p.registration = payload.DevicePairingData
	}
	p.mu.Unlock()
	if payload.DevicePairingData != nil {
		if err := wire.Send(p.ctx, whatsappfixture.PairingRequest("SERVED_PAIR")); err != nil {
			p.t.Error(err)
			return
		}
	} else if err := p.success(wire); err != nil {
		p.t.Error(err)
		return
	}
	for {
		node, err := wire.Read(p.ctx)
		if err != nil {
			if errors.Is(err, whatsappfixture.ErrMalformedNode) {
				p.t.Error(err)
			}
			return
		}
		if node.Tag != "iq" {
			if node.Tag == "receipt" {
				select {
				case p.receipts <- *node:
				default:
					p.t.Error("served peer receipt evidence overflow")
					return
				}
			}
			continue
		}
		if node.Attrs["id"] == "PAIR_NATIVE" && node.Attrs["type"] == "result" {
			signature := node.GetChildByTag("pair-device-sign")
			if len(signature.GetChildren()) != 1 {
				p.t.Error("SDK did not complete signed pairing")
				return
			}
			p.pairOnce.Do(func() { close(p.paired) })
			if err := p.success(wire); err != nil {
				p.t.Error(err)
				return
			}
			continue
		}
		if node.Attrs["type"] == "result" {
			continue
		}
		if node.Attrs["xmlns"] == "encrypt" && node.Attrs["type"] == "set" {
			list := node.GetChildByTag("list")
			keys := list.GetChildren()
			if len(keys) != 0 {
				p.mu.Lock()
				p.prekeys = keys
				p.mu.Unlock()
				p.keysOnce.Do(func() { close(p.keysReady) })
			}
		}
		response := waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": node.Attrs["id"], "type": "result"}}
		if node.Attrs["xmlns"] == "encrypt" && node.Attrs["type"] == "get" {
			p.mu.Lock()
			count := len(p.prekeys)
			p.mu.Unlock()
			response.Content = []waBinary.Node{{Tag: "count", Attrs: waBinary.Attrs{"value": strconv.Itoa(count)}}}
		}
		if err := wire.Send(p.ctx, response); err != nil {
			if p.ctx.Err() == nil {
				p.t.Error(err)
			}
			return
		}
	}
}

func (p *serveNativeProtocolPeer) success(wire *whatsappfixture.Transport) error {
	return wire.Send(p.ctx, waBinary.Node{Tag: "success", Attrs: waBinary.Attrs{"lid": p.lid, "t": time.Now().Unix()}})
}

func (p *serveNativeProtocolPeer) pair(qr string) {
	p.t.Helper()
	p.mu.Lock()
	wire := p.wire
	p.mu.Unlock()
	if wire == nil {
		p.t.Fatal("RunServe did not connect the actual SDK")
	}
	if err := wire.Send(p.ctx, whatsappfixture.SignedPairingResponse(p.t, qr, p.account, p.lid)); err != nil {
		p.t.Fatal(err)
	}
	select {
	case <-p.paired:
	case <-time.After(5 * time.Second):
		p.t.Fatal("real SDK did not verify and persist signed pairing")
	}
}

func (p *serveNativeProtocolPeer) claim(challenge string) types.JID {
	p.t.Helper()
	select {
	case <-p.keysReady:
	case <-time.After(30 * time.Second):
		p.t.Fatal("real SDK did not upload its public prekeys")
	}
	p.mu.Lock()
	wire, registration, key := p.wire, p.registration, p.prekeys[0]
	p.prekeys = p.prekeys[1:]
	p.mu.Unlock()
	from := types.NewJID("15551234568", types.DefaultUserServer)
	from.Device = 1
	message := whatsappfixture.EncryptPairingText(p.t, registration, key, p.account, from, challenge, "SERVED_CLAIM")
	if err := wire.Send(p.ctx, message); err != nil {
		p.t.Fatal(err)
	}
	select {
	case receipt := <-p.receipts:
		if receipt.Attrs["id"] != "SERVED_CLAIM" {
			p.t.Fatal("SDK acknowledged a different claim", receipt)
		}
	case <-time.After(5 * time.Second):
		p.t.Fatal("SDK did not acknowledge the durable encrypted claim")
	}
	return from.ToNonAD()
}
