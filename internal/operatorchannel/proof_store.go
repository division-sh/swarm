package operatorchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

const proofDocumentVersion = 2

var ErrProofStoreLocked = errors.New("operator channel proof store is locked")

type FileProofStore struct {
	path string
	mu   sync.Mutex
}

type proofDocument struct {
	Version     int                    `json:"version"`
	Entries     map[string]proofRecord `json:"entries"`
	Unsupported bool                   `json:"-"`
}

type proofRecord struct {
	Format                      string            `json:"format"`
	ProofID                     string            `json:"proof_id"`
	Revision                    int64             `json:"revision"`
	Status                      ProofStatus       `json:"status"`
	Interface                   InterfaceIdentity `json:"interface"`
	ExternalAccountRef          string            `json:"external_account_reference"`
	ConversationRef             string            `json:"conversation_reference"`
	ConversationScope           ConversationScope `json:"conversation_scope"`
	AccountPresentation         string            `json:"account_presentation,omitempty"`
	Method                      string            `json:"method"`
	Challenge                   string            `json:"challenge"`
	OriginalOperationID         string            `json:"original_operation_id"`
	MintingStoreID              string            `json:"minting_store_id"`
	MintingDeploymentID         string            `json:"minting_deployment_id"`
	VerifiedAt                  time.Time         `json:"verified_at"`
	OperatorConfirmed           bool              `json:"operator_confirmed"`
	ConsentScopes               []ConsentScope    `json:"consent_scopes"`
	ProviderCredentialKey       string            `json:"provider_credential_key"`
	ProviderCredentialValueSeal string            `json:"provider_credential_value_seal"`
}

func NewFileProofStore(swarmDir string) (*FileProofStore, error) {
	swarmDir = strings.TrimSpace(swarmDir)
	if swarmDir == "" {
		return nil, fmt.Errorf("operator channel proof store requires the resolved Swarm state directory")
	}
	return &FileProofStore{path: filepath.Join(filepath.Clean(swarmDir), "operator-channel-proofs.json")}, nil
}

func (s *FileProofStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *FileProofStore) List(ctx context.Context) ([]VerifiedProof, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(doc.Entries))
	for key := range doc.Entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]VerifiedProof, 0, len(keys))
	for _, key := range keys {
		proof, err := doc.Entries[key].proof()
		if err != nil {
			return nil, err
		}
		out = append(out, proof)
	}
	return out, nil
}

func (s *FileProofStore) Get(ctx context.Context, identity InterfaceIdentity) (VerifiedProof, bool, error) {
	if err := identity.Validate(); err != nil {
		return VerifiedProof{}, false, err
	}
	if err := contextError(ctx); err != nil {
		return VerifiedProof{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.loadLocked()
	if err != nil {
		return VerifiedProof{}, false, err
	}
	record, ok := doc.Entries[identity.Key()]
	if !ok {
		return VerifiedProof{}, false, nil
	}
	proof, err := record.proof()
	return proof, err == nil, err
}

func (s *FileProofStore) Put(ctx context.Context, proof VerifiedProof) error {
	if err := proof.Validate(); err != nil {
		return err
	}
	if proof.ProofID != ProofIDForInterface(proof.Interface) {
		return fmt.Errorf("%w: proof_id does not match exact interface identity", ErrProofUnavailable)
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		if doc.Unsupported {
			if err := s.quarantineUnsupportedLocked(); err != nil {
				return err
			}
			doc = proofDocument{Version: proofDocumentVersion, Entries: map[string]proofRecord{}}
		}
		key := proof.Interface.Key()
		if record, found := doc.Entries[key]; found {
			existing, err := record.proof()
			if err != nil {
				return err
			}
			if reflect.DeepEqual(existing, proof) {
				return nil
			}
			if existing.ProofID != proof.ProofID || proof.Revision != existing.Revision+1 {
				return fmt.Errorf("%w: proof revision must advance exactly from %d", ErrRevisionConflict, existing.Revision)
			}
		}
		doc.Entries[key] = recordFromProof(proof)
		return s.saveLocked(doc)
	})
}

func (s *FileProofStore) Revoke(ctx context.Context, identity InterfaceIdentity, expectedRevision int64, now time.Time) (VerifiedProof, error) {
	if err := identity.Validate(); err != nil {
		return VerifiedProof{}, err
	}
	if expectedRevision < 1 || now.IsZero() {
		return VerifiedProof{}, fmt.Errorf("%w: expected proof revision and revocation time are required", ErrInvalidRequest)
	}
	if err := contextError(ctx); err != nil {
		return VerifiedProof{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var revoked VerifiedProof
	err := s.withWriteLockLocked(func() error {
		doc, err := s.loadLocked()
		if err != nil {
			return err
		}
		key := identity.Key()
		record, found := doc.Entries[key]
		if !found {
			return ErrNotFound
		}
		proof, err := record.proof()
		if err != nil {
			return err
		}
		if proof.Status == ProofRevoked && proof.Revision == expectedRevision+1 {
			revoked = cloneProof(proof)
			return nil
		}
		if proof.Revision != expectedRevision || proof.Status != ProofActive {
			return ErrRevisionConflict
		}
		proof.Revision++
		proof.Status = ProofRevoked
		proof.VerifiedAt = now.UTC().Truncate(time.Microsecond)
		doc.Entries[key] = recordFromProof(proof)
		revoked = cloneProof(proof)
		return s.saveLocked(doc)
	})
	return revoked, err
}

func (s *FileProofStore) loadLocked() (proofDocument, error) {
	doc := proofDocument{Version: proofDocumentVersion, Entries: map[string]proofRecord{}}
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return doc, nil
	}
	if err != nil {
		return proofDocument{}, fmt.Errorf("read operator channel proof store %s: %w", s.path, err)
	}
	if len(raw) == 0 {
		return proofDocument{}, fmt.Errorf("operator channel proof store %s is empty and corrupt", s.path)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return proofDocument{}, fmt.Errorf("parse operator channel proof store %s: %w", s.path, err)
	}
	if header.Version == 1 {
		doc.Unsupported = true
		return doc, nil
	}
	if header.Version != proofDocumentVersion {
		return proofDocument{}, fmt.Errorf("operator channel proof store %s has unsupported format version %d", s.path, header.Version)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return proofDocument{}, fmt.Errorf("parse operator channel proof store %s: %w", s.path, err)
	}
	if doc.Version != proofDocumentVersion || doc.Entries == nil {
		return proofDocument{}, fmt.Errorf("operator channel proof store %s has unsupported or incomplete format", s.path)
	}
	for key, record := range doc.Entries {
		proof, err := record.proof()
		if err != nil {
			return proofDocument{}, fmt.Errorf("operator channel proof store %s contains invalid entry %q: %w", s.path, key, err)
		}
		if key != proof.Interface.Key() || proof.Format != ProofFormat || proof.ProofID != ProofIDForInterface(proof.Interface) || proof.Revision < 1 {
			return proofDocument{}, fmt.Errorf("operator channel proof store %s contains conflicting entry %q", s.path, key)
		}
		if proof.Status != ProofActive && proof.Status != ProofRevoked {
			return proofDocument{}, fmt.Errorf("operator channel proof store %s contains unsupported proof status %q", s.path, proof.Status)
		}
		if proof.Status == ProofActive {
			if err := proof.Validate(); err != nil {
				return proofDocument{}, err
			}
		}
	}
	return doc, nil
}

func (s *FileProofStore) withWriteLockLocked(fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create operator channel proof directory: %w", err)
	}
	unlock, err := lockProofFile(s.path + ".lock")
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (s *FileProofStore) saveLocked(doc proofDocument) error {
	doc.Version = proofDocumentVersion
	if doc.Entries == nil {
		doc.Entries = map[string]proofRecord{}
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode operator channel proof store: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".operator-channel-proofs-*.json")
	if err != nil {
		return fmt.Errorf("create temporary operator channel proof store: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary operator channel proof store: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("restrict temporary operator channel proof store: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temporary operator channel proof store: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary operator channel proof store: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace operator channel proof store: %w", err)
	}
	return nil
}

func (s *FileProofStore) quarantineUnsupportedLocked() error {
	quarantinePath := s.path + ".unsupported-v1"
	if _, err := os.Stat(quarantinePath); err == nil {
		quarantinePath += "-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect operator channel proof quarantine: %w", err)
	}
	if err := os.Rename(s.path, quarantinePath); err != nil {
		return fmt.Errorf("quarantine unsupported operator channel proof store: %w", err)
	}
	return nil
}

func recordFromProof(proof VerifiedProof) proofRecord {
	return proofRecord{
		Format: proof.Format, ProofID: proof.ProofID, Revision: proof.Revision, Status: proof.Status,
		Interface: proof.Interface, ExternalAccountRef: proof.ExternalAccountRef, ConversationRef: proof.ConversationRef,
		ConversationScope: proof.ConversationScope, AccountPresentation: proof.AccountPresentation,
		Method: proof.Method, Challenge: proof.Challenge, OriginalOperationID: proof.OriginalOperationID,
		MintingStoreID: proof.MintingStoreID, MintingDeploymentID: proof.MintingDeploymentID,
		VerifiedAt: proof.VerifiedAt, OperatorConfirmed: proof.OperatorConfirmed,
		ConsentScopes:               append([]ConsentScope(nil), proof.ConsentScopes...),
		ProviderCredentialKey:       proof.ProviderCredential.Key,
		ProviderCredentialValueSeal: proof.ProviderCredential.Seal.String(),
	}
}

func (r proofRecord) proof() (VerifiedProof, error) {
	seal, err := runtimecredentials.ParseValueSeal(r.ProviderCredentialValueSeal)
	if err != nil || strings.TrimSpace(r.ProviderCredentialKey) == "" {
		return VerifiedProof{}, fmt.Errorf("provider credential evidence is invalid")
	}
	return VerifiedProof{
		Format: r.Format, ProofID: r.ProofID, Revision: r.Revision, Status: r.Status,
		Interface: r.Interface, ExternalAccountRef: r.ExternalAccountRef, ConversationRef: r.ConversationRef,
		ConversationScope: r.ConversationScope, AccountPresentation: r.AccountPresentation,
		Method: r.Method, Challenge: r.Challenge, OriginalOperationID: r.OriginalOperationID,
		MintingStoreID: r.MintingStoreID, MintingDeploymentID: r.MintingDeploymentID,
		VerifiedAt: r.VerifiedAt, OperatorConfirmed: r.OperatorConfirmed,
		ConsentScopes:      append([]ConsentScope(nil), r.ConsentScopes...),
		ProviderCredential: runtimecredentials.ValueEvidence{Key: r.ProviderCredentialKey, Seal: seal},
	}, nil
}

func cloneProof(proof VerifiedProof) VerifiedProof {
	proof.ConsentScopes = append([]ConsentScope(nil), proof.ConsentScopes...)
	return proof
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
