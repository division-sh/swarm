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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/division-sh/swarm/internal/testutil/whatsappfixture"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/types"
)

// Only the external TLS/Noise peer is replaced. RunServe constructs the real
// SDK, selected persistence, capture, identity and target owners.
type serveNativeProtocolPeer struct {
	t             *testing.T
	ctx           context.Context
	account       types.JID
	lid           types.JID
	mu            sync.Mutex
	wire          *whatsappfixture.Transport
	registration  *waWa6.ClientPayload_DevicePairingRegistrationData
	prekeys       []waBinary.Node
	prekeyUploads int
	postLogins    int
	connections   int
	disconnected  int
	paired        chan struct{}
	pairOnce      sync.Once
	receipts      chan waBinary.Node
	sender        *whatsappfixture.SignalSender
	receivers     map[types.JID]servedNativeReceiver
	ownedSenders  []*whatsappfixture.SignalSender
	from          types.JID
	fromLID       types.JID
	sent          chan servedNativeMessage
	malformedAck  string
	withholdAck   string
	clientTrust   servedNativeClientTrust
}

type servedNativeMessage struct {
	ID   string
	To   types.JID
	Body *waE2E.Message
}

type servedNativeReceiver struct {
	sender *whatsappfixture.SignalSender
	lid    types.JID
}

func newServeNativeProtocolPeer(t *testing.T) *serveNativeProtocolPeer {
	t.Helper()
	noise := whatsappfixture.NewNoiseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	peer := &serveNativeProtocolPeer{t: t, ctx: ctx,
		account: types.NewJID("15551234567", types.DefaultUserServer),
		lid:     types.NewJID("100000000001", types.HiddenUserServer), paired: make(chan struct{}),
		receipts: make(chan waBinary.Node, 16), sent: make(chan servedNativeMessage, 16), receivers: map[types.JID]servedNativeReceiver{}}
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
	peer.clientTrust = servedNativeClientTrust{Address: server.Listener.Addr().String(),
		Certificate: append([]byte(nil), server.Certificate().Raw...), NoiseRoot: whatsmeow.WACertPubKey}
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
		for _, sender := range peer.ownedSenders {
			if err := sender.Close(); err != nil {
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
				p.prekeyUploads++
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
					jid, ok := user.Attrs["jid"].(types.JID)
					receiver, owned := p.receiver(jid)
					if !owned || !ok {
						p.t.Error("SDK requested an unowned public prekey")
						return
					}
					bundle, err := receiver.sender.PublicPreKey(p.ctx, jid)
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
		if node.Attrs["xmlns"] == "passive" && node.Attrs["type"] == "set" {
			if _, active := node.GetOptionalChildByTag("active"); active {
				p.mu.Lock()
				p.postLogins++
				p.mu.Unlock()
			}
		}
	}
}

func (p *serveNativeProtocolPeer) awaitPostLogin() (int, int) {
	p.t.Helper()
	deadline := time.Now().Add(serveRuntimeReadyTimeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		connected, uploads, completed := p.connections, p.prekeyUploads, p.postLogins
		p.mu.Unlock()
		if completed == connected && connected > 0 && uploads > 0 {
			return completed, uploads
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatal("SDK did not complete its genuine prekey upload/post-login before the planned restart")
	return 0, 0
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
	p.receivers[from.ToNonAD()] = servedNativeReceiver{sender: sender, lid: fromLID.ToNonAD()}
	p.receivers[fromLID.ToNonAD()] = servedNativeReceiver{sender: sender, lid: fromLID.ToNonAD()}
	p.ownedSenders = append(p.ownedSenders, sender)
	p.mu.Unlock()
	p.text(challenge, "SERVED_CLAIM")
	return from.ToNonAD()
}

func (p *serveNativeProtocolPeer) receiver(jid types.JID) (servedNativeReceiver, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	receiver, found := p.receivers[jid.ToNonAD()]
	return receiver, found
}

func (p *serveNativeProtocolPeer) customer(text, id string) (types.JID, types.JID) {
	p.t.Helper()
	p.mu.Lock()
	registration := p.registration
	p.mu.Unlock()
	from := types.NewJID("15551234571", types.DefaultUserServer)
	lid := types.NewJID("100000000003", types.HiddenUserServer)
	from.Device, lid.Device = 1, 1
	sender := whatsappfixture.NewPairingSender(p.t, registration, p.account, from, lid)
	p.mu.Lock()
	p.receivers[from.ToNonAD()] = servedNativeReceiver{sender: sender, lid: lid.ToNonAD()}
	p.receivers[lid.ToNonAD()] = servedNativeReceiver{sender: sender, lid: lid.ToNonAD()}
	p.ownedSenders = append(p.ownedSenders, sender)
	p.mu.Unlock()
	p.sendEncrypted(sender.EncryptText(p.t, text, id), id)
	return from.ToNonAD(), lid.ToNonAD()
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
	p.sendEncrypted(message, id)
}

func (p *serveNativeProtocolPeer) reply(text, id string, original servedNativeMessage) {
	p.t.Helper()
	p.mu.Lock()
	sender := p.sender
	p.mu.Unlock()
	if sender == nil {
		p.t.Fatal("encrypted reply requires the original sender")
	}
	p.sendEncrypted(sender.EncryptReply(p.t, text, id, original.ID, original.Body.GetConversation()), id)
}

func (p *serveNativeProtocolPeer) sendEncrypted(message waBinary.Node, id string) {
	p.t.Helper()
	p.mu.Lock()
	wire := p.wire
	p.mu.Unlock()
	if wire == nil {
		p.t.Fatal("encrypted input requires the original connected wire")
	}
	if err := wire.Send(p.ctx, message); err != nil {
		p.t.Fatal(err)
	}
	select {
	case receipt := <-p.receipts:
		if receipt.Attrs["id"] != id || receipt.Attrs["type"] == "retry" {
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
	var users []waBinary.Node
	for _, user := range list.GetChildren() {
		jid, ok := user.Attrs["jid"].(types.JID)
		receiver, owned := p.receiver(jid)
		if user.Tag != "user" || !ok || !owned && jid != p.account.ToNonAD() && jid != p.lid.ToNonAD() {
			return nil, fmt.Errorf("SDK requested an unowned external device list: %v", user.Attrs)
		}
		lid := p.lid.ToNonAD()
		if owned {
			lid = receiver.lid
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
	to, valid := node.Attrs["to"].(types.JID)
	receiver, owned := p.receiver(to)
	if !valid || !owned {
		return fmt.Errorf("SDK sent to an unowned recipient")
	}
	message, err := receiver.sender.DecryptOutbound(p.ctx, node)
	if err != nil {
		return err
	}
	id, ok := node.Attrs["id"].(string)
	if !ok || id == "" {
		return fmt.Errorf("SDK sent without an exact message identity")
	}
	select {
	case p.sent <- servedNativeMessage{ID: id, To: to, Body: message}:
	default:
		return fmt.Errorf("outbound plaintext evidence overflow")
	}
	p.mu.Lock()
	malformed := message.GetConversation() == p.malformedAck && p.malformedAck != ""
	withheld := p.withholdAck != "" && strings.HasPrefix(message.GetConversation(), p.withholdAck)
	p.mu.Unlock()
	if withheld {
		return nil
	}
	timestamp := time.Now().Unix()
	if malformed {
		timestamp = 0
	}
	return wire.Send(p.ctx, waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": id, "class": "message", "t": timestamp}})
}
