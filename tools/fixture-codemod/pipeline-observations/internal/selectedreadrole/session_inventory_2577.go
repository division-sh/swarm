package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

//go:embed testdata/session_inventory_2577_successor.json
var sessionInventory2577Packet []byte

const SessionInventory2577PacketPath = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/session_inventory_2577_successor.json"
const SessionInventory2577Path = "internal/runtime/channel_runtime_supported_surface_test.go"
const SessionInventory2577Before = "f98083f8cef12a0b02e894b729b87c8b9f634921"
const SessionInventory2577After = "f9802ccaf6938dd3744aed977af40455aa588079"
const SessionInventory2577Digest = "af233288a071ff624ecc9d0eab1a52b1bb1398741b21036e16ebe556e0a8140e"

func readSessionInventory2577Packet(root string) (repair2698CleanupPacket, error) {
	var packet repair2698CleanupPacket
	raw, err := os.ReadFile(filepath.Join(root, SessionInventory2577PacketPath))
	if err != nil || !bytes.Equal(raw, sessionInventory2577Packet) || fmt.Sprintf("%x", sha256.Sum256(raw)) != SessionInventory2577Digest {
		return packet, fmt.Errorf("session inventory continuation certificate changed or missing: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&packet); err != nil {
		return packet, err
	}
	if decoder.Decode(new(any)) != io.EOF || packet.BeforeCommit != SessionInventory2577Before ||
		packet.AfterCommit != SessionInventory2577After || len(packet.Files) != 1 ||
		packet.Files[0].File != SessionInventory2577Path ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/2577#issuecomment-6223472352" {
		return packet, fmt.Errorf("session inventory continuation lost its exact one-file boundary")
	}
	file := packet.Files[0]
	before, err := sessionInventory2577SignedSource(root, packet.BeforeCommit, file.BeforeSHA256)
	if err != nil {
		return packet, err
	}
	after, err := sessionInventory2577SignedSource(root, packet.AfterCommit, file.AfterSHA256)
	if err != nil {
		return packet, err
	}
	if err := exactSessionInventory2577Selection(before, after); err != nil {
		return packet, err
	}
	return packet, nil
}

func sessionInventory2577SignedSource(root, ref, digest string) ([]byte, error) {
	raw, err := exec.Command("git", "-C", root, "show", ref+":"+SessionInventory2577Path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != digest {
		return nil, fmt.Errorf("session inventory continuation lost its signed source %s: %v", ref, err)
	}
	return raw, nil
}

func exactSessionInventory2577Selection(before, after []byte) error {
	replacements := [][2]string{
		{"\tif err != nil || len(plans) != 1 {\n\t\tt.Fatalf(\"CompileChannelInventory = %#v, %v\", plans, err)\n\t}\n\ttriggerEntry", "\tif err != nil {\n\t\tt.Fatal(\"CompileChannelInventory\", err)\n\t}\n\tplan := packfixture.ChannelPlanByID(t, plans, \"provider.telegram.hitl_channel\")\n\ttriggerEntry"},
		{"channelIdentity := plans[0].ChannelIdentity()", "channelIdentity := plan.ChannelIdentity()"},
		{"structuralSubject, err := plans[0].CapabilitySubject()", "structuralSubject, err := plan.CapabilitySubject()"},
		{"packs.NewOutboundBindingPlanWithCredentials(\"ops\", plans[0], \"42\", nil,", "packs.NewOutboundBindingPlanWithCredentials(\"ops\", plan, \"42\", nil,"},
	}
	expected := bytes.Clone(before)
	for _, replacement := range replacements {
		old, next := []byte(replacement[0]), []byte(replacement[1])
		if bytes.Count(expected, old) != 1 {
			return fmt.Errorf("session inventory continuation lost its exact selection site")
		}
		expected = bytes.Replace(expected, old, next, 1)
	}
	if !bytes.Equal(after, expected) {
		return fmt.Errorf("session inventory continuation changed more than canonical plan selection")
	}
	return nil
}

// This view is historical evidence only; live extraction uses actual source.
func sessionInventory2577HistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != SessionInventory2577Path {
		return actual, nil
	}
	if _, err := os.Stat(filepath.Join(root, SessionInventory2577PacketPath)); os.IsNotExist(err) &&
		fmt.Sprintf("%x", sha256.Sum256(actual)) != "488baa15e9b201b364533aa720989004d010b2a7ad9f29e482e7695a3322014f" {
		return actual, nil // Immutable predecessor images predate this packet.
	}
	packet, err := readSessionInventory2577Packet(root)
	if err != nil {
		return nil, err
	}
	if err := sessionInventory2577CurrentSource(root, actual, packet); err != nil {
		return nil, err
	}
	previous, err := sessionInventory2577SignedSource(root, packet.BeforeCommit, packet.Files[0].BeforeSHA256)
	if err != nil {
		return nil, err
	}
	if _, err := readSessionInventory2577Packet(root); err != nil {
		return nil, err
	}
	if err := sessionInventory2577CurrentSource(root, actual, packet); err != nil {
		return nil, err
	}
	return previous, nil
}

func sessionInventory2577CurrentSource(root string, actual []byte, packet repair2698CleanupPacket) error {
	current, err := os.ReadFile(filepath.Join(root, SessionInventory2577Path))
	if err != nil || !bytes.Equal(current, actual) || fmt.Sprintf("%x", sha256.Sum256(current)) != packet.Files[0].AfterSHA256 {
		return fmt.Errorf("session inventory continuation differs from actual copied source: %v", err)
	}
	return nil
}

func CatchUp2542HistoricalFunction(root, path, name, current string) (string, error) {
	if path != SessionInventory2577Path {
		return catchUp2542HistoricalFunction(root, path, name, current)
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	if _, err := sessionInventory2577HistoricalSource(root, path, raw); err != nil {
		return "", err
	}
	previous, err := catchUp2542HistoricalFunction(root, path, name, current)
	if err != nil {
		return "", err
	}
	if _, err := sessionInventory2577HistoricalSource(root, path, raw); err != nil {
		return "", err
	}
	return previous, nil
}
