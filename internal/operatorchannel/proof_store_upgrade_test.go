package operatorchannel

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFileProofStoreLoadsRC1ArtifactWithoutRewriting(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "rc1-credential-proof-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != "c48eec9b6c441ea55608bde1f4d109bfe81b7ce4cb8c0fecd56e8d67054a5747" {
		t.Fatalf("released rc.1 artifact changed: %s", got)
	}
	store := proofUpgradeStore(t, raw)
	proofs, err := store.List(context.Background())
	if err != nil || len(proofs) != 1 {
		t.Fatalf("load released rc.1 artifact: proofs=%d err=%v", len(proofs), err)
	}
	proof := proofs[0]
	if proof.Status != ProofActive || proof.Revision != 1 || proof.ExternalAccountRef == "" || proof.ProviderAuthority.Kind != ProviderAuthorityCredential {
		t.Fatal("released proof lost its original identity, revision, status or authority kind")
	}
	read, found, err := store.Get(context.Background(), proof.Interface)
	if err != nil || !found || !reflect.DeepEqual(read, proof) {
		t.Fatalf("read exact released proof: found=%v err=%v", found, err)
	}
	requireProofUpgradeBytes(t, store, raw)
}

func TestFileProofStoreRC1AuthorityAndStatusShapes(t *testing.T) {
	for _, kind := range []string{"credential", "session", "session_with_credential"} {
		for _, status := range []ProofStatus{ProofActive, ProofRevoked} {
			for _, scope := range []ConversationScope{ConversationScopeDirect, ConversationScopeShared} {
				t.Run(kind+"/"+string(status)+"/"+string(scope), func(t *testing.T) {
					proof := proofUpgradeShape(kind, status, scope)
					raw := proofUpgradeJSON(t, proof, true)
					store := proofUpgradeStore(t, raw)
					proofs, err := store.List(context.Background())
					if err != nil || len(proofs) != 1 || !reflect.DeepEqual(proofs[0], proof) {
						t.Fatalf("historical shape changed: proofs=%d err=%v", len(proofs), err)
					}
					if kind != "credential" && proofs[0].ProviderAuthority.RequireExecutable() == nil {
						t.Fatal("historical structural session proof minted native execution admission")
					}
					requireProofUpgradeBytes(t, store, raw)
					if status != ProofActive {
						return
					}
					revoked, err := store.Revoke(context.Background(), proof.Interface, proof.Revision, proof.VerifiedAt.Add(time.Hour))
					if err != nil || revoked.Status != ProofRevoked || revoked.Revision != proof.Revision+1 {
						t.Fatalf("explicit revoke after old read: revision=%d status=%s err=%v", revoked.Revision, revoked.Status, err)
					}
					saved, err := os.ReadFile(store.Path())
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(saved), `"external_account_reference"`) || strings.Contains(string(saved), `"account_reference"`) || !strings.Contains(string(saved), `"external_account"`) {
						t.Fatal("explicit save did not use current-only account members")
					}
					if kind != "credential" && !strings.Contains(string(saved), `"operator_account"`) {
						t.Fatal("explicit session save omitted the current account member")
					}
				})
			}
		}
	}
}

func TestFileProofStoreV2NamedAccountFieldsRefuseAmbiguity(t *testing.T) {
	for _, field := range []struct {
		name, current, retired string
		path                   []string
	}{
		{name: "external", current: "external_account", retired: "external_account_reference"},
		{name: "session", current: "operator_account", retired: "account_reference", path: []string{"provider_authority", "session_account"}},
	} {
		for _, historical := range []bool{false, true} {
			for _, value := range []struct {
				name  string
				value any
			}{
				{name: "null"}, {name: "number", value: 3}, {name: "boolean", value: true},
				{name: "array", value: []any{"x"}}, {name: "object", value: map[string]any{"x": "y"}},
			} {
				t.Run(fmt.Sprintf("%s/historical=%t/%s", field.name, historical, value.name), func(t *testing.T) {
					doc, record := proofUpgradeDocument(t, "session_with_credential", historical)
					object := record
					for _, member := range field.path {
						object = object[member].(map[string]any)
					}
					name := field.current
					if historical {
						name = field.retired
					}
					object[name] = value.value
					requireProofUpgradeRefusal(t, proofUpgradeMarshal(t, doc))
				})
			}
		}
		for _, coexist := range []string{"same", "different", "null"} {
			t.Run(field.name+"/both/"+coexist, func(t *testing.T) {
				doc, record := proofUpgradeDocument(t, "session_with_credential", true)
				object := record
				for _, member := range field.path {
					object = object[member].(map[string]any)
				}
				object[field.current] = object[field.retired]
				if coexist == "different" {
					object[field.current] = "different-account"
				} else if coexist == "null" {
					object[field.current] = nil
				}
				requireProofUpgradeRefusal(t, proofUpgradeMarshal(t, doc))
			})
		}
	}
}

func TestFileProofStoreV2KeepsUnknownDuplicateAndSemanticRefusals(t *testing.T) {
	for _, path := range [][]string{
		nil, {"entry"}, {"entry", "interface"}, {"entry", "provider_authority"}, {"entry", "provider_authority", "session_account"},
	} {
		t.Run("unknown/"+strings.Join(path, "/"), func(t *testing.T) {
			doc, record := proofUpgradeDocument(t, "session_with_credential", true)
			object := doc
			for _, member := range path {
				if member == "entry" {
					object = record
				} else {
					object = object[member].(map[string]any)
				}
			}
			object["unexpected"] = "not-admitted"
			requireProofUpgradeRefusal(t, proofUpgradeMarshal(t, doc))
		})
	}
	for _, historical := range []bool{false, true} {
		for _, field := range []string{"version", "entries", "interface_ref", "kind", "external_account", "operator_account"} {
			if historical {
				field = strings.ReplaceAll(strings.ReplaceAll(field, "external_account", "external_account_reference"), "operator_account", "account_reference")
			}
			t.Run(fmt.Sprintf("duplicate/historical=%t/%s", historical, field), func(t *testing.T) {
				doc, _ := proofUpgradeDocument(t, "session_with_credential", historical)
				raw := proofUpgradeMarshal(t, doc)
				prefix := fmt.Sprintf("%q:", field)
				if !strings.Contains(raw, prefix) {
					t.Fatalf("missing duplicate target %s", field)
				}
				raw = strings.Replace(raw, prefix, fmt.Sprintf("%q:null,%s", field, prefix), 1)
				requireProofUpgradeRefusal(t, raw)
			})
		}
	}
	for _, change := range []string{"version", "key", "proof_id", "revision", "status", "consent", "credential_seal", "session_id", "retired_at_wrong_path", "trailing"} {
		t.Run("semantic/"+change, func(t *testing.T) {
			doc, record := proofUpgradeDocument(t, "session_with_credential", true)
			switch change {
			case "version":
				doc["version"] = 3
			case "key":
				doc["entries"] = map[string]any{"foreign": record}
			case "proof_id":
				record["proof_id"] = uuid.NewString()
			case "revision":
				record["revision"] = 0
			case "status":
				record["status"] = "unsupported"
			case "consent":
				record["consent_scopes"] = []any{"ungranted"}
			case "credential_seal":
				record["provider_authority"].(map[string]any)["credential_value_seal"] = "invalid"
			case "session_id":
				record["provider_authority"].(map[string]any)["session_account"].(map[string]any)["connection_id"] = "invalid"
			case "retired_at_wrong_path":
				record["account_reference"] = "not-a-top-level-member"
			}
			raw := proofUpgradeMarshal(t, doc)
			if change == "trailing" {
				raw += `{}`
			}
			requireProofUpgradeRefusal(t, raw)
		})
	}
}

func TestFileProofStoreV2PreservesInt64Revision(t *testing.T) {
	proof := proofUpgradeShape("credential", ProofActive, ConversationScopeDirect)
	proof.Revision = math.MaxInt64
	store := proofUpgradeStore(t, proofUpgradeJSON(t, proof, true))
	read, found, err := store.Get(context.Background(), proof.Interface)
	if err != nil || !found || read.Revision != proof.Revision {
		t.Fatalf("int64 revision changed: found=%v revision=%d err=%v", found, read.Revision, err)
	}
}

func proofUpgradeShape(kind string, status ProofStatus, scope ConversationScope) VerifiedProof {
	proof := testVerifiedProof(time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC))
	proof.Status, proof.ConversationScope = status, scope
	if status == ProofRevoked {
		proof.Revision = 2
	}
	if kind != "credential" {
		proof.ProviderAuthority.Kind = ProviderAuthoritySession
		proof.ProviderAuthority.Session = SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(), AccountRef: "fixture-session-account", AdmissionID: uuid.NewString(), Revision: 1}
		if kind == "session" {
			proof.ProviderAuthority.Credential = ProviderAuthority{}.Credential
		}
	}
	return proof
}

func proofUpgradeJSON(t *testing.T, proof VerifiedProof, historical bool) []byte {
	t.Helper()
	var record map[string]any
	decoder := json.NewDecoder(strings.NewReader(mustProofRecordJSON(t, proof)))
	decoder.UseNumber()
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if historical {
		record["external_account_reference"] = record["external_account"]
		delete(record, "external_account")
		if session, ok := record["provider_authority"].(map[string]any)["session_account"].(map[string]any); ok {
			session["account_reference"] = session["operator_account"]
			delete(session, "operator_account")
		}
	}
	return []byte(proofUpgradeMarshal(t, map[string]any{"version": 2, "entries": map[string]any{proof.Interface.Key(): record}}))
}

func proofUpgradeDocument(t *testing.T, kind string, historical bool) (map[string]any, map[string]any) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(proofUpgradeJSON(t, proofUpgradeShape(kind, ProofActive, ConversationScopeDirect), historical), &doc); err != nil {
		t.Fatal(err)
	}
	for _, record := range doc["entries"].(map[string]any) {
		return doc, record.(map[string]any)
	}
	t.Fatal("missing test proof")
	return nil, nil
}

func proofUpgradeMarshal(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func proofUpgradeStore(t *testing.T, raw []byte) *FileProofStore {
	t.Helper()
	store, err := NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Path(), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return store
}

func requireProofUpgradeBytes(t *testing.T, store *FileProofStore, want []byte) {
	t.Helper()
	got, err := os.ReadFile(store.Path())
	if err != nil || string(got) != string(want) {
		t.Fatalf("proof read rewrote persisted bytes: err=%v", err)
	}
}

func requireProofUpgradeRefusal(t *testing.T, raw string) {
	t.Helper()
	store := proofUpgradeStore(t, []byte(raw))
	if _, err := store.List(context.Background()); err == nil {
		t.Fatal("malformed or ambiguous proof was admitted")
	}
	requireProofUpgradeBytes(t, store, []byte(raw))
}
