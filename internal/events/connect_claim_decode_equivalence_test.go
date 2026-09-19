package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

// Independent pre-optimization reader: do not delegate its wire checks to the
// optimized implementation.
func originalConnectClaimDecode(c *ConnectExecutionClaim, raw []byte) error {
	if c == nil {
		return fmt.Errorf("connect execution claim destination is nil")
	}
	var encoded struct {
		Digest            string                          `json:"sha256"`
		ReceiverPinDigest string                          `json:"receiver_pin_sha256"`
		RecipientKind     string                          `json:"recipient_kind"`
		RecipientID       string                          `json:"recipient_id"`
		HandlerNode       *runtimeidentity.ExecutableNode `json:"handler_node"`
		HandlerEvent      string                          `json:"handler_event"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil {
		return fmt.Errorf("decode connect execution claim: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return fmt.Errorf("connect execution claim must be an object")
	}
	if encoded.Digest == "" {
		if len(fields) != 0 {
			return fmt.Errorf("empty connect execution claim cannot carry partial fields")
		}
		*c = ConnectExecutionClaim{}
		return nil
	}
	if len(encoded.Digest) != sha256.Size*2 || encoded.Digest != strings.ToLower(encoded.Digest) ||
		len(encoded.ReceiverPinDigest) != sha256.Size*2 || encoded.ReceiverPinDigest != strings.ToLower(encoded.ReceiverPinDigest) {
		return fmt.Errorf("connect execution claim digest is invalid")
	}
	decoded, err := hex.DecodeString(encoded.Digest)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("connect execution claim digest is invalid")
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	pinDecoded, err := hex.DecodeString(encoded.ReceiverPinDigest)
	if err != nil || len(pinDecoded) != sha256.Size {
		return fmt.Errorf("connect execution claim receiver pin digest is invalid")
	}
	var pinDigest [sha256.Size]byte
	copy(pinDigest[:], pinDecoded)
	kind, ok := deliveryRecipientKindFromCode(encoded.RecipientKind)
	if !ok || strings.TrimSpace(encoded.RecipientID) == "" || eventidentity.Normalize(encoded.HandlerEvent) != encoded.HandlerEvent || encoded.HandlerEvent == "" {
		return fmt.Errorf("connect execution claim recipient or handler event is invalid")
	}
	handlerNode := runtimeidentity.ExecutableNode{}
	if encoded.HandlerNode != nil {
		handlerNode = *encoded.HandlerNode
	}
	if kind == deliveryRecipientNode && !handlerNode.Valid() {
		return fmt.Errorf("connect node execution claim handler owner is invalid")
	}
	if kind == deliveryRecipientAgent && !handlerNode.Empty() {
		return fmt.Errorf("connect agent execution claim cannot carry a node handler owner")
	}
	if kind == deliveryRecipientNode {
		if recipient, err := runtimeidentity.ParseExecutableNodeKey(strings.TrimSpace(encoded.RecipientID)); err != nil || !recipient.Equal(handlerNode) {
			return fmt.Errorf("connect node execution claim recipient contradicts handler owner")
		}
	}
	*c = ConnectExecutionClaim{
		digest: digest, receiverPinDigest: pinDigest, recipientKind: kind,
		recipientID: strings.TrimSpace(encoded.RecipientID), handlerNode: handlerNode,
		handlerEvent: encoded.HandlerEvent, present: true,
	}
	return nil
}

func checkConnectClaimCodecEquivalence(t testing.TB, raw []byte) {
	t.Helper()
	seed := ConnectExecutionClaim{present: true, recipientID: "unchanged-on-error"}
	old, current := seed, seed
	oldErr := originalConnectClaimDecode(&old, raw)
	currentErr := current.UnmarshalJSON(raw)
	if (oldErr == nil) != (currentErr == nil) || !old.Equal(current) {
		t.Fatalf("wire=%q old=%#v/%v current=%#v/%v", raw, old, oldErr, current, currentErr)
	}
	if currentErr == nil {
		a, ae := old.MarshalJSON()
		b, be := current.MarshalJSON()
		if ae != nil || be != nil || !bytes.Equal(a, b) {
			t.Fatalf("wire output changed: old=%s/%v current=%s/%v", a, ae, b, be)
		}
	}
}

func TestConnectClaimSingleObjectDecodeMatchesOriginal(t *testing.T) {
	for _, path := range []string{".", "a/review", "b/review", "outer/inner"} {
		node := identitytest.ExecutableNode(t, path, "shared")
		claim, err := AdmitConnectExecutionClaim(sha256.Sum256([]byte("edge")), sha256.Sum256([]byte("pin")), MustNodeDeliveryRecipient(node), node, "item.received")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := claim.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		checkConnectClaimCodecEquivalence(t, raw)
		checkConnectClaimCodecEquivalence(t, bytes.Replace(raw, []byte("handler_node"), []byte("HANDLER_NODE"), 1))
		checkConnectClaimCodecEquivalence(t, append(append([]byte(nil), raw[:len(raw)-1]...), []byte(",\"handler_node\":null}")...))
	}
	valid := fmt.Sprintf("{\"sha256\":%q,\"receiver_pin_sha256\":%q,\"recipient_kind\":\"agent\",\"recipient_id\":\"worker\",\"handler_event\":\"item.received\"}", strings.Repeat("a", 64), strings.Repeat("b", 64))
	for _, raw := range []string{
		"{}", "{ }", " \t\r\n{ \r\n\t }\n", "null", "[]", "true", "0", "\"text\"", "",
		"{", "}", "{\u00a0}", "\u00a0{}", "{}\u00a0", "{} {}", "{} null", "{}\x00",
		"{\"sha256\":null}", "{\"sha256\":\"\"}", "{\"SHA256\":\"\"}", "{\"handler_node\":null}",
		"{\"unknown\":null}", "{\"sha256\":\"\",\"sha256\":null}", valid,
		valid + "\n", valid + "{}", valid + "garbage",
		strings.Replace(valid, "sha256", "SHA256", 1),
		strings.Replace(valid, "worker", "worker\xff", 1),
		strings.Replace(valid, "\"worker\"", "\"\\ud800\"", 1),
		strings.Replace(valid, "\"worker\"", "null", 1),
		strings.Replace(valid, "\"worker\"", "1e999", 1),
		strings.TrimSuffix(valid, "}") + ",\"sha256\":null}",
		strings.TrimSuffix(valid, "}") + ",\"sha256\":\"\"}",
		strings.TrimSuffix(valid, "}") + ",\"handler_node\":null}",
		strings.TrimSuffix(valid, "}") + ",\"unknown\":{}}",
		strings.TrimSuffix(valid, "}") + ",\"handler_node\":{}}",
	} {
		checkConnectClaimCodecEquivalence(t, []byte(raw))
	}
	for _, key := range []string{"sha256", "receiver_pin_sha256", "recipient_kind", "recipient_id", "handler_event", "handler_node"} {
		for _, value := range []string{"null", "true", "0", "1e999", "\"\"", "{}", "[]"} {
			checkConnectClaimCodecEquivalence(t, []byte("{\""+key+"\":"+value+"}"))
		}
	}
}

func FuzzConnectClaimSingleObjectDecodeMatchesOriginal(f *testing.F) {
	for _, raw := range []string{"{}", "{ }", "null", "{\"sha256\":null}", "{} {}", "{\"handler_node\":{}}"} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) { checkConnectClaimCodecEquivalence(t, raw) })
}
