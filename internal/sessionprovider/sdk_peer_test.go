package sessionprovider

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCert"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/keys"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/protobuf/proto"
)

// This finite peer exercises the unmodified SDK over TLS, its certificate check,
// Noise XX and binary nodes. Its generated test trust root is restored only
// after all test-owned peers have joined. It is not live WhatsApp qualification.
type sdkPeer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	server   *httptest.Server
	static   *keys.KeyPair
	cert     []byte
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
	conn  *websocket.Conn
	read  sdkPeerCipher
	write sdkPeerCipher
	mu    sync.Mutex
	done  chan struct{}
}

type sdkPeerCipher struct {
	key     cipher.AEAD
	counter uint32
}

func (c *sdkPeerCipher) nonce() []byte {
	var nonce [12]byte
	binary.BigEndian.PutUint32(nonce[8:], c.counter)
	c.counter++
	return nonce[:]
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
	p := &sdkPeer{ctx: ctx, cancel: cancel, static: keys.NewKeyPair(),
		frames: make(chan sdkPeerFrame, 64), protocol: make(chan sdkPeerFrame, 64), failures: make(chan error, 8),
		paired: paired, ready: make(chan *sdkPeerSocket, 1)}
	root, intermediate := keys.NewKeyPair(), keys.NewKeyPair()
	oldRoot := whatsmeow.WACertPubKey
	whatsmeow.WACertPubKey = *root.Pub
	makeCertificate := func(serial, issuer uint32, key *keys.KeyPair, signer *keys.KeyPair) *waCert.CertChain_NoiseCertificate {
		details, err := proto.Marshal(&waCert.CertChain_NoiseCertificate_Details{
			Serial: proto.Uint32(serial), IssuerSerial: proto.Uint32(issuer), Key: key.Pub[:],
			NotBefore: proto.Uint64(uint64(time.Now().Add(-time.Minute).Unix())),
			NotAfter:  proto.Uint64(uint64(time.Now().Add(time.Hour).Unix())),
		})
		if err != nil {
			t.Fatal(err)
		}
		signature := ecc.CalculateSignature(ecc.NewDjbECPrivateKey(*signer.Priv), details)
		return &waCert.CertChain_NoiseCertificate{Details: details, Signature: signature[:]}
	}
	var err error
	p.cert, err = proto.Marshal(&waCert.CertChain{
		Intermediate: makeCertificate(1, 0, intermediate, root),
		Leaf:         makeCertificate(2, 1, p.static, intermediate),
	})
	if err != nil {
		t.Fatal(err)
	}
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
		whatsmeow.WACertPubKey = oldRoot
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
		frame, err := readSDKPeerFrame(p.ctx, conn, nil)
		if err != nil {
			return // Test-owned disconnects deliberately interrupt pending effects.
		}
		plain, err := peer.read.key.Open(nil, peer.read.nonce(), frame, nil)
		if err != nil {
			p.fail(err)
			return
		}
		unpacked, err := waBinary.Unpack(plain)
		if err != nil {
			p.fail(err)
			return
		}
		node, err := waBinary.Unmarshal(unpacked)
		if err != nil {
			p.fail(err)
			return
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

func readSDKPeerFrame(ctx context.Context, conn *websocket.Conn, header []byte) ([]byte, error) {
	kind, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageBinary || len(data) < len(header)+3 || !bytes.Equal(data[:len(header)], header) {
		return nil, errors.New("unexpected SDK frame/header")
	}
	data = data[len(header):]
	n := int(data[0])<<16 | int(data[1])<<8 | int(data[2])
	if n != len(data)-3 {
		return nil, errors.New("unexpected SDK frame length")
	}
	return data[3:], nil
}

func writeSDKPeerFrame(ctx context.Context, conn *websocket.Conn, data []byte) error {
	frame := append([]byte{byte(len(data) >> 16), byte(len(data) >> 8), byte(len(data))}, data...)
	return conn.Write(ctx, websocket.MessageBinary, frame)
}

func (p *sdkPeer) handshake(peer *sdkPeerSocket) error {
	data, err := readSDKPeerFrame(p.ctx, peer.conn, socket.WAConnHeader)
	if err != nil {
		return err
	}
	var hello waWa6.HandshakeMessage
	if err = proto.Unmarshal(data, &hello); err != nil {
		return err
	}
	clientEphemeral := hello.GetClientHello().GetEphemeral()
	if len(clientEphemeral) != 32 {
		return errors.New("invalid SDK client ephemeral key")
	}
	// Use the pinned public handshake engine, including its omission of an empty
	// first-message payload. Track only the chaining key to obtain responder
	// transport keys: Finish exposes an initiator socket, not responder keys.
	state := socket.NewNoiseHandshake()
	state.Start(socket.NoiseStartPattern, socket.WAConnHeader)
	chainingKey := []byte(socket.NoiseStartPattern)
	mix := func(private, public []byte) error {
		secret, err := curve25519.X25519(private, public)
		if err != nil {
			return err
		}
		if err := state.MixIntoKey(secret); err != nil {
			return err
		}
		keys := make([]byte, 64)
		if _, err := io.ReadFull(hkdf.New(sha256.New, secret, chainingKey, nil), keys); err != nil {
			return err
		}
		chainingKey = keys[:32]
		return nil
	}
	serverEphemeral := keys.NewKeyPair()
	state.Authenticate(clientEphemeral)
	state.Authenticate(serverEphemeral.Pub[:])
	if err := mix(serverEphemeral.Priv[:], clientEphemeral); err != nil {
		return err
	}
	static := state.Encrypt(p.static.Pub[:])
	if err := mix(p.static.Priv[:], clientEphemeral); err != nil {
		return err
	}
	data, err = proto.Marshal(&waWa6.HandshakeMessage{ServerHello: &waWa6.HandshakeMessage_ServerHello{
		Ephemeral: serverEphemeral.Pub[:], Static: static, Payload: state.Encrypt(p.cert),
	}})
	if err != nil {
		return err
	}
	if err = writeSDKPeerFrame(p.ctx, peer.conn, data); err != nil {
		return err
	}
	data, err = readSDKPeerFrame(p.ctx, peer.conn, nil)
	if err != nil {
		return err
	}
	var finish waWa6.HandshakeMessage
	if err = proto.Unmarshal(data, &finish); err != nil {
		return err
	}
	clientStatic, err := state.Decrypt(finish.GetClientFinish().GetStatic())
	if err != nil {
		return err
	}
	if err := mix(serverEphemeral.Priv[:], clientStatic); err != nil {
		return err
	}
	if _, err = state.Decrypt(finish.GetClientFinish().GetPayload()); err != nil {
		return err
	}
	transportKeys := make([]byte, 64)
	if _, err := io.ReadFull(hkdf.New(sha256.New, nil, chainingKey, nil), transportKeys); err != nil {
		return err
	}
	if peer.read.key, err = gcmutil.Prepare(transportKeys[:32]); err != nil {
		return err
	}
	peer.write.key, err = gcmutil.Prepare(transportKeys[32:])
	return err
}

func (p *sdkPeerSocket) send(ctx context.Context, node waBinary.Node) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := waBinary.Marshal(node)
	if err != nil {
		return err
	}
	data = p.write.key.Seal(nil, p.write.nonce(), data, nil)
	return writeSDKPeerFrame(ctx, p.conn, data)
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
