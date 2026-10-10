package whatsappfixture

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
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
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/keys"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/protobuf/proto"
)

// NoiseServer is the finite test peer's authenticated wire boundary. It has no
// SDK state, account admission, capture, publication or channel execution owner.
// Do not use in parallel tests: the SDK certificate root is process-global.
type NoiseServer struct {
	static *keys.KeyPair
	cert   []byte
}

func NewNoiseServer(t *testing.T) *NoiseServer {
	t.Helper()
	s := &NoiseServer{static: keys.NewKeyPair()}
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
	s.cert, err = proto.Marshal(&waCert.CertChain{
		Intermediate: makeCertificate(1, 0, intermediate, root),
		Leaf:         makeCertificate(2, 1, s.static, intermediate),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { whatsmeow.WACertPubKey = oldRoot })
	return s
}

type Transport struct {
	conn  *websocket.Conn
	read  noiseCipher
	write noiseCipher
	mu    sync.Mutex
}

type noiseCipher struct {
	key     cipher.AEAD
	counter uint32
}

func (c *noiseCipher) nonce() []byte {
	var nonce [12]byte
	binary.BigEndian.PutUint32(nonce[8:], c.counter)
	c.counter++
	return nonce[:]
}

func (p *Transport) Read(ctx context.Context) (*waBinary.Node, error) {
	frame, err := ReadFrame(ctx, p.conn, nil)
	if err != nil {
		return nil, err
	}
	plain, err := p.read.key.Open(nil, p.read.nonce(), frame, nil)
	if err != nil {
		return nil, err
	}
	unpacked, err := waBinary.Unpack(plain)
	if err != nil {
		return nil, err
	}
	return waBinary.Unmarshal(unpacked)
}

func ReadFrame(ctx context.Context, conn *websocket.Conn, header []byte) ([]byte, error) {
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

func WriteFrame(ctx context.Context, conn *websocket.Conn, data []byte) error {
	frame := append([]byte{byte(len(data) >> 16), byte(len(data) >> 8), byte(len(data))}, data...)
	return conn.Write(ctx, websocket.MessageBinary, frame)
}

func (s *NoiseServer) Handshake(ctx context.Context, conn *websocket.Conn) (*Transport, error) {
	peer := &Transport{conn: conn}
	data, err := ReadFrame(ctx, peer.conn, socket.WAConnHeader)
	if err != nil {
		return nil, err
	}
	var hello waWa6.HandshakeMessage
	if err = proto.Unmarshal(data, &hello); err != nil {
		return nil, err
	}
	clientEphemeral := hello.GetClientHello().GetEphemeral()
	if len(clientEphemeral) != 32 {
		return nil, errors.New("invalid SDK client ephemeral key")
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
		return nil, err
	}
	static := state.Encrypt(s.static.Pub[:])
	if err := mix(s.static.Priv[:], clientEphemeral); err != nil {
		return nil, err
	}
	data, err = proto.Marshal(&waWa6.HandshakeMessage{ServerHello: &waWa6.HandshakeMessage_ServerHello{
		Ephemeral: serverEphemeral.Pub[:], Static: static, Payload: state.Encrypt(s.cert),
	}})
	if err != nil {
		return nil, err
	}
	if err = WriteFrame(ctx, peer.conn, data); err != nil {
		return nil, err
	}
	data, err = ReadFrame(ctx, peer.conn, nil)
	if err != nil {
		return nil, err
	}
	var finish waWa6.HandshakeMessage
	if err = proto.Unmarshal(data, &finish); err != nil {
		return nil, err
	}
	clientStatic, err := state.Decrypt(finish.GetClientFinish().GetStatic())
	if err != nil {
		return nil, err
	}
	if err := mix(serverEphemeral.Priv[:], clientStatic); err != nil {
		return nil, err
	}
	if _, err = state.Decrypt(finish.GetClientFinish().GetPayload()); err != nil {
		return nil, err
	}
	transportKeys := make([]byte, 64)
	if _, err := io.ReadFull(hkdf.New(sha256.New, nil, chainingKey, nil), transportKeys); err != nil {
		return nil, err
	}
	if peer.read.key, err = gcmutil.Prepare(transportKeys[:32]); err != nil {
		return nil, err
	}
	peer.write.key, err = gcmutil.Prepare(transportKeys[32:])
	return peer, err
}

func (p *Transport) Send(ctx context.Context, node waBinary.Node) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := waBinary.Marshal(node)
	if err != nil {
		return err
	}
	data = p.write.key.Seal(nil, p.write.nonce(), data, nil)
	return WriteFrame(ctx, p.conn, data)
}
