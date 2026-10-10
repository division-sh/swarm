package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/division-sh/swarm/internal/testutil/whatsappfixture"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/types"
)

// This finite peer exercises the unmodified SDK over TLS, its certificate check,
// Noise XX and binary nodes. Its generated test trust root is restored only
// after all test-owned peers have joined. It is not live WhatsApp qualification.
type sdkPeer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	server   *httptest.Server
	noise    *whatsappfixture.NoiseServer
	frames   chan sdkPeerFrame
	protocol chan sdkPeerFrame
	failures chan error
	mu       sync.Mutex
	closing  bool
	peers    []*sdkPeerSocket
	wg       sync.WaitGroup
	paired   bool
	ready    chan *sdkPeerSocket
	devices  map[types.JID]uint16
}

type sdkPeerSocket struct {
	conn *websocket.Conn
	wire *whatsappfixture.Transport
	done chan struct{}
}

type sdkPeerFrame struct {
	peer *sdkPeerSocket
	node *waBinary.Node
}

func newSDKPeer(t *testing.T) *sdkPeer {
	return newSDKPeerMode(t, true)
}

func newSDKPeerMode(t *testing.T, paired bool) *sdkPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	p := &sdkPeer{ctx: ctx, cancel: cancel, noise: whatsappfixture.NewNoiseServer(t),
		frames: make(chan sdkPeerFrame, 64), protocol: make(chan sdkPeerFrame, 64), failures: make(chan error, 8),
		paired: paired, ready: make(chan *sdkPeerSocket, 1)}
	p.server = httptest.NewTLSServer(http.HandlerFunc(p.serve))
	t.Cleanup(func() {
		p.mu.Lock()
		p.closing = true
		peers := append([]*sdkPeerSocket(nil), p.peers...)
		p.mu.Unlock()
		cancel()
		for _, peer := range peers {
			_ = peer.conn.CloseNow()
		}
		p.server.Close()
		p.wg.Wait()
		select {
		case err := <-p.failures:
			t.Errorf("SDK peer: %v", err)
		default:
		}
	})
	return p
}

type sdkPeerTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (rt sdkPeerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != "web.whatsapp.com" {
		return nil, fmt.Errorf("test peer refuses unexpected destination %q", req.URL.Host)
	}
	copy := req.Clone(req.Context())
	copy.URL.Scheme, copy.URL.Host = rt.target.Scheme, rt.target.Host
	copy.Host = rt.target.Host
	return rt.base.RoundTrip(copy)
}

func (p *sdkPeer) attach(t *testing.T, client *whatsmeow.Client) {
	t.Helper()
	target, err := url.Parse(p.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := *p.server.Client()
	httpClient.Transport = sdkPeerTransport{target: target, base: httpClient.Transport}
	client.SetPreLoginHTTPClient(&httpClient)
	client.SetWebsocketHTTPClient(&httpClient)
}

func (p *sdkPeer) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		http.Error(w, "closed", http.StatusServiceUnavailable)
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"web.whatsapp.com"}})
	if err != nil {
		p.fail(err)
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(socket.FrameMaxSize)
	peer := &sdkPeerSocket{conn: conn, done: make(chan struct{})}
	defer close(peer.done)
	p.mu.Lock()
	p.peers = append(p.peers, peer)
	p.mu.Unlock()
	if err = p.handshake(peer); err != nil {
		p.fail(err)
		return
	}
	if p.paired {
		if err = peer.send(p.ctx, waBinary.Node{Tag: "success", Attrs: waBinary.Attrs{
			"lid": types.NewJID("100000000001", types.HiddenUserServer), "t": time.Now().Unix(),
		}}); err != nil {
			p.fail(err)
			return
		}
	}
	if !p.paired {
		select {
		case p.ready <- peer:
		case <-p.ctx.Done():
			return
		}
	}
	for {
		node, err := peer.wire.Read(p.ctx)
		if err != nil {
			if errors.Is(err, whatsappfixture.ErrMalformedNode) {
				p.fail(err)
			}
			return // Test-owned disconnects deliberately interrupt pending effects.
		}
		if node.Tag == "message" || node.Tag == "iq" && node.AttrGetter().OptionalString("xmlns") == "md" {
			select {
			case p.frames <- sdkPeerFrame{peer: peer, node: node}:
			case <-p.ctx.Done():
				return
			}
			continue
		}
		if node.Tag == "iq" && node.AttrGetter().OptionalString("type") == "error" {
			select {
			case p.protocol <- sdkPeerFrame{peer: peer, node: node}:
			case <-p.ctx.Done():
				return
			}
		} else if node.Tag == "iq" {
			response := waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": node.Attrs["id"], "type": "result"}}
			if node.AttrGetter().OptionalString("xmlns") == "usync" {
				var users []waBinary.Node
				p.mu.Lock()
				requested := node.GetChildByTag("usync", "list")
				for _, user := range requested.GetChildren() {
					jid := user.AttrGetter().JID("jid")
					if device, ok := p.devices[jid]; ok {
						users = append(users, waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{
							{Tag: "devices", Content: []waBinary.Node{{Tag: "device-list", Content: []waBinary.Node{{Tag: "device", Attrs: waBinary.Attrs{"id": int(device)}}}}}},
						}})
					}
				}
				p.mu.Unlock()
				response.Content = []waBinary.Node{{Tag: "usync", Content: []waBinary.Node{{Tag: "list", Content: users}}}}
			}
			if node.AttrGetter().OptionalString("xmlns") == "encrypt" && node.AttrGetter().OptionalString("type") == "get" {
				response.Content = []waBinary.Node{{Tag: "count", Attrs: waBinary.Attrs{"value": "100"}}}
			}
			if err := peer.send(p.ctx, response); err != nil {
				return
			}
		} else if node.Tag == "ack" || node.Tag == "receipt" {
			select {
			case p.protocol <- sdkPeerFrame{peer: peer, node: node}:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *sdkPeer) admitMessageRecipient(jid types.JID, device uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.devices == nil {
		p.devices = make(map[types.JID]uint16)
	}
	p.devices[jid] = device
}

func (p *sdkPeer) fail(err error) {
	if p.ctx.Err() != nil {
		return
	}
	select {
	case p.failures <- err:
	default:
	}
}

func (p *sdkPeer) handshake(peer *sdkPeerSocket) (err error) {
	peer.wire, err = p.noise.Handshake(p.ctx, peer.conn)
	return err
}

func (p *sdkPeerSocket) send(ctx context.Context, node waBinary.Node) error {
	return p.wire.Send(ctx, node)
}

func (p *sdkPeer) next(t *testing.T) sdkPeerFrame {
	t.Helper()
	select {
	case frame := <-p.frames:
		return frame
	case err := <-p.failures:
		t.Fatal(err)
	case <-p.ctx.Done():
		t.Fatal("timed out waiting for SDK business frame")
	}
	return sdkPeerFrame{}
}

func (p *sdkPeer) acknowledge(t *testing.T, frame sdkPeerFrame) {
	t.Helper()
	response := waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": frame.node.Attrs["id"], "t": time.Now().Unix()}}
	if frame.node.Tag == "iq" {
		response.Tag = "iq"
		response.Attrs["type"] = "result"
	}
	if err := frame.peer.send(p.ctx, response); err != nil {
		t.Fatal(err)
	}
}
