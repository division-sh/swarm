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
	"go.mau.fi/whatsmeow/proto/waE2E"
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
	disconnected int
	paired       chan struct{}
	pairOnce     sync.Once
	receipts     chan waBinary.Node
	sender       *whatsappfixture.SignalSender
	from         types.JID
	fromLID      types.JID
	sent         chan servedNativeMessage
}

type servedNativeMessage struct {
	ID   string
	To   types.JID
	Body *waE2E.Message
}

func newServeNativeProtocolPeer(t *testing.T) *serveNativeProtocolPeer {
	t.Helper()
	noise := whatsappfixture.NewNoiseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	peer := &serveNativeProtocolPeer{t: t, ctx: ctx,
		account: types.NewJID("15551234567", types.DefaultUserServer),
		lid:     types.NewJID("100000000001", types.HiddenUserServer), paired: make(chan struct{}),
		receipts: make(chan waBinary.Node, 16), sent: make(chan servedNativeMessage, 16)}
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
		if peer.sender != nil {
			if err := peer.sender.Close(); err != nil {
				t.Error(err)
			}
		}
		transport.CloseIdleConnections()
		http.DefaultTransport = base
	})
	return peer
}

func (p *serveNativeProtocolPeer) serve(wire *whatsappfixture.Transport) {
	defer func() {
		p.mu.Lock()
		p.disconnected++
		p.mu.Unlock()
	}()
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
			if node.Tag == "message" {
				if err := p.acceptMessage(wire, node); err != nil {
					p.t.Error(err)
					return
				}
			}
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
			}
		}
		response := waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": node.Attrs["id"], "type": "result"}}
		if node.Attrs["xmlns"] == "usync" {
			content, err := p.deviceList(node)
			if err != nil {
				p.t.Error(err)
				return
			}
			response.Content = content
		}
		if node.Attrs["xmlns"] == "encrypt" && node.Attrs["type"] == "get" {
			if key, requested := node.GetOptionalChildByTag("key"); requested {
				var users []waBinary.Node
				for _, user := range key.GetChildren() {
					p.mu.Lock()
					sender := p.sender
					p.mu.Unlock()
					jid, ok := user.Attrs["jid"].(types.JID)
					if sender == nil || !ok {
						p.t.Error("SDK requested an unowned public prekey")
						return
					}
					bundle, err := sender.PublicPreKey(p.ctx, jid)
					if err != nil {
						p.t.Error(err)
						return
					}
					users = append(users, bundle)
				}
				response.Content = []waBinary.Node{{Tag: "list", Content: users}}
			} else {
				p.mu.Lock()
				count := len(p.prekeys)
				p.mu.Unlock()
				response.Content = []waBinary.Node{{Tag: "count", Attrs: waBinary.Attrs{"value": strconv.Itoa(count)}}}
			}
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
	p.mu.Lock()
	registration := p.registration
	p.mu.Unlock()
	from := types.NewJID("15551234568", types.DefaultUserServer)
	from.Device = 1
	fromLID := types.NewJID("100000000002", types.HiddenUserServer)
	fromLID.Device = 1
	sender := whatsappfixture.NewPairingSender(p.t, registration, p.account, from, fromLID)
	p.mu.Lock()
	p.sender, p.from, p.fromLID = sender, from, fromLID
	p.mu.Unlock()
	p.text(challenge, "SERVED_CLAIM")
	return from.ToNonAD()
}

func (p *serveNativeProtocolPeer) text(text, id string) {
	p.t.Helper()
	p.mu.Lock()
	wire, sender := p.wire, p.sender
	p.mu.Unlock()
	if wire == nil || sender == nil {
		p.t.Fatal("encrypted input requires the original connected sender")
	}
	message := sender.EncryptText(p.t, text, id)
	if err := wire.Send(p.ctx, message); err != nil {
		p.t.Fatal(err)
	}
	select {
	case receipt := <-p.receipts:
		if receipt.Attrs["id"] != id {
			p.t.Fatal("SDK acknowledged a different durable input", receipt)
		}
	case <-time.After(5 * time.Second):
		p.t.Fatal("SDK did not acknowledge the durable encrypted input", id)
	}
}

func (p *serveNativeProtocolPeer) deviceList(request *waBinary.Node) ([]waBinary.Node, error) {
	list, found := request.GetOptionalChildByTag("usync", "list")
	if !found {
		return nil, fmt.Errorf("SDK device query has no requested users")
	}
	p.mu.Lock()
	from, fromLID := p.from, p.fromLID
	p.mu.Unlock()
	var users []waBinary.Node
	for _, user := range list.GetChildren() {
		jid, ok := user.Attrs["jid"].(types.JID)
		if user.Tag != "user" || !ok || jid != from.ToNonAD() && jid != fromLID.ToNonAD() && jid != p.account.ToNonAD() && jid != p.lid.ToNonAD() {
			return nil, fmt.Errorf("SDK requested an unowned external device list: %v", user.Attrs)
		}
		lid := p.lid.ToNonAD()
		if jid == from.ToNonAD() || jid == fromLID.ToNonAD() {
			lid = fromLID.ToNonAD()
		}
		users = append(users, waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{
			{Tag: "lid", Attrs: waBinary.Attrs{"val": lid}},
			{Tag: "devices", Content: []waBinary.Node{{Tag: "device-list", Content: []waBinary.Node{
				{Tag: "device", Attrs: waBinary.Attrs{"id": "1"}},
			}}}},
		}})
	}
	return []waBinary.Node{{Tag: "usync", Content: []waBinary.Node{{Tag: "list", Content: users}}}}, nil
}

func (p *serveNativeProtocolPeer) acceptMessage(wire *whatsappfixture.Transport, node *waBinary.Node) error {
	p.mu.Lock()
	sender := p.sender
	p.mu.Unlock()
	if sender == nil {
		return fmt.Errorf("SDK sent before genuine operator input")
	}
	message, err := sender.DecryptOutbound(p.ctx, node)
	if err != nil {
		return err
	}
	id, ok := node.Attrs["id"].(string)
	if !ok || id == "" {
		return fmt.Errorf("SDK sent without an exact message identity")
	}
	select {
	case p.sent <- servedNativeMessage{ID: id, To: node.Attrs["to"].(types.JID), Body: message}:
	default:
		return fmt.Errorf("outbound plaintext evidence overflow")
	}
	return wire.Send(p.ctx, waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": id, "class": "message", "t": time.Now().Unix()}})
}
