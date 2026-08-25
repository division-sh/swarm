package operatorchannel

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Service struct {
	store        Store
	proofs       ProofStore
	deploymentID string
	interfaces   []InterfaceIdentity

	mu        sync.RWMutex
	principal Principal
}

func NewService(store Store, proofs ProofStore, admittedInterfaces []InterfaceIdentity, deploymentID string) (*Service, error) {
	if store == nil || proofs == nil || strings.TrimSpace(deploymentID) == "" {
		return nil, fmt.Errorf("operator channel service requires selected store, proof store, and deployment identity")
	}
	interfaces, err := normalizeInterfaces(admittedInterfaces)
	if err != nil {
		return nil, err
	}
	return &Service{store: store, proofs: proofs, deploymentID: strings.TrimSpace(deploymentID), interfaces: interfaces}, nil
}

func normalizeInterfaces(admittedInterfaces []InterfaceIdentity) ([]InterfaceIdentity, error) {
	interfaces := make([]InterfaceIdentity, 0, len(admittedInterfaces))
	seen := map[string]struct{}{}
	for _, identity := range admittedInterfaces {
		identity = identity.Normalized()
		if err := identity.Validate(); err != nil {
			return nil, err
		}
		if identity.InterfaceRef != InterfaceHITLChannelV2 {
			continue
		}
		if _, exists := seen[identity.Key()]; exists {
			continue
		}
		seen[identity.Key()] = struct{}{}
		interfaces = append(interfaces, identity)
	}
	sort.Slice(interfaces, func(i, j int) bool { return interfaces[i].Key() < interfaces[j].Key() })
	return interfaces, nil
}

func (s *Service) ReplaceInterfaces(admittedInterfaces []InterfaceIdentity) error {
	if s == nil {
		return fmt.Errorf("operator channel service is required")
	}
	interfaces, err := normalizeInterfaces(admittedInterfaces)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.interfaces = interfaces
	s.mu.Unlock()
	return nil
}

func (s *Service) Bootstrap(ctx context.Context, now time.Time) (Principal, []Binding, error) {
	if s == nil {
		return Principal{}, nil, fmt.Errorf("operator channel service is required")
	}
	principal, err := s.store.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		return Principal{}, nil, err
	}
	s.mu.Lock()
	s.principal = principal
	s.mu.Unlock()
	if err := s.RecoverProofResponsibilities(ctx, now); err != nil {
		return Principal{}, nil, err
	}
	proofs, err := s.proofs.List(ctx)
	if err != nil {
		return Principal{}, nil, err
	}
	s.mu.RLock()
	interfaces := append([]InterfaceIdentity(nil), s.interfaces...)
	s.mu.RUnlock()
	current := make(map[string]InterfaceIdentity, len(interfaces))
	for _, identity := range interfaces {
		current[identity.Key()] = identity
	}
	bound := []Binding{}
	for _, proof := range proofs {
		if proof.Status != ProofActive {
			continue
		}
		identity, ok := current[proof.Interface.Key()]
		if !ok {
			continue
		}
		binding, err := s.store.BindOperatorChannelFromProof(ctx, BootBindRequest{PrincipalID: principal.ID, Interface: identity, Proof: proof, RequestedAt: now})
		if err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Principal{}, nil, err
		}
		bound = append(bound, binding)
	}
	return principal, bound, nil
}

func (s *Service) Principal() (Principal, error) {
	if s == nil {
		return Principal{}, fmt.Errorf("operator channel service is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.principal.Validate(); err != nil {
		return Principal{}, fmt.Errorf("operator channel principal is not bootstrapped: %w", err)
	}
	return s.principal, nil
}

func (s *Service) ResolveInterface(selector string) (InterfaceIdentity, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return InterfaceIdentity{}, fmt.Errorf("%w: interface selector is required", ErrInvalidRequest)
	}
	s.mu.RLock()
	interfaces := append([]InterfaceIdentity(nil), s.interfaces...)
	s.mu.RUnlock()
	matches := []InterfaceIdentity{}
	for _, identity := range interfaces {
		identity = identity.Normalized()
		if selector == identity.Selector {
			matches = append(matches, identity)
		}
	}
	if len(matches) == 0 {
		return InterfaceIdentity{}, fmt.Errorf("%w: operator channel interface %q is not active", ErrNotFound, selector)
	}
	if len(matches) != 1 {
		return InterfaceIdentity{}, fmt.Errorf("%w: operator channel interface %q is ambiguous; use an exact selector from channel.list", ErrConflict, selector)
	}
	return matches[0], nil
}

func (s *Service) Begin(ctx context.Context, selector string, kind OperationKind, expectedRevision int64, requestKey, requestHash string, saveProof bool, now time.Time) (Operation, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, err
	}
	identity, err := s.ResolveInterface(selector)
	if err != nil {
		return Operation{}, err
	}
	plannedProofID := ""
	plannedProofRevision := int64(0)
	if saveProof {
		plannedProofID = ProofIDForInterface(identity)
		plannedProofRevision = 1
		existing, found, err := s.proofs.Get(ctx, identity)
		if err != nil {
			return Operation{}, err
		}
		if found {
			if existing.ProofID != plannedProofID || existing.Revision < 1 {
				return Operation{}, fmt.Errorf("%w: existing proof identity or revision conflicts with the active interface", ErrProofUnavailable)
			}
			plannedProofRevision = existing.Revision + 1
		}
	}
	return s.store.BeginChannelBinding(ctx, BeginRequest{
		OperationID: NewOperationID(), Kind: kind, PrincipalID: principal.ID, Interface: identity,
		ExpectedRevision: expectedRevision, RequestKeyHash: requestKey, RequestHash: requestHash,
		SaveProof: saveProof, PlannedProofID: plannedProofID, PlannedProofRevision: plannedProofRevision,
		RequestedAt: now, ExpiresAt: now.Add(DefaultChallengeTTL),
	})
}

func (s *Service) Confirm(ctx context.Context, operationID string, expectedRevision int64, approve bool, now time.Time) (Operation, Binding, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, Binding{}, err
	}
	op, binding, err := s.store.ConfirmChannelBinding(ctx, ConfirmRequest{OperationID: strings.TrimSpace(operationID), PrincipalID: principal.ID, ExpectedRevision: expectedRevision, Approve: approve, ConfirmedAt: now})
	if err != nil {
		return op, binding, err
	}
	if op.State == StateBound && op.ProofStatus == ProofPending {
		if err := s.materializeProof(ctx, ProofResponsibility{Operation: op, Binding: binding, Proof: proofFromOperation(op, binding)}, now); err != nil {
			return op, binding, err
		}
		op.ProofStatus = ProofActive
	}
	return op, binding, nil
}

func (s *Service) Unbind(ctx context.Context, selector string, expectedRevision int64, requestKey, requestHash string, now time.Time) (Operation, Binding, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, Binding{}, err
	}
	identity, err := s.ResolveInterface(selector)
	if err != nil {
		return Operation{}, Binding{}, err
	}
	return s.store.UnbindOperatorChannel(ctx, UnbindRequest{OperationID: NewOperationID(), PrincipalID: principal.ID, Interface: identity, ExpectedRevision: expectedRevision, RequestKeyHash: requestKey, RequestHash: requestHash, RequestedAt: now})
}

func (s *Service) RevokeProof(ctx context.Context, selector string, expectedRevision int64, now time.Time) (VerifiedProof, error) {
	identity, err := s.ResolveInterface(selector)
	if err != nil {
		return VerifiedProof{}, err
	}
	revoked, err := s.proofs.Revoke(ctx, identity, expectedRevision, now)
	if err != nil {
		return VerifiedProof{}, err
	}
	operations, listErr := s.store.ListOperatorChannelOperations(ctx, s.mustPrincipalID())
	if listErr != nil {
		return VerifiedProof{}, listErr
	}
	for _, op := range operations {
		if op.ProofID == revoked.ProofID && op.ProofRevision == expectedRevision && op.State == StateBound {
			if err := s.store.CompleteProofResponsibility(ctx, op.OperationID, op.ProofID, op.ProofRevision, ProofRevoked, "machine-local proof revoked", now); err != nil && !errors.Is(err, ErrRevisionConflict) {
				return VerifiedProof{}, err
			}
		}
	}
	return revoked, nil
}

func (s *Service) RecoverProofResponsibilities(ctx context.Context, now time.Time) error {
	responsibilities, err := s.store.ListPendingProofResponsibilities(ctx)
	if err != nil {
		return err
	}
	for _, responsibility := range responsibilities {
		if err := s.materializeProof(ctx, responsibility, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) materializeProof(ctx context.Context, responsibility ProofResponsibility, now time.Time) error {
	proof := responsibility.Proof
	proof.MintingDeploymentID = s.deploymentID
	if err := s.proofs.Put(ctx, proof); err != nil {
		_ = s.store.CompleteProofResponsibility(context.WithoutCancel(ctx), responsibility.Operation.OperationID, proof.ProofID, proof.Revision, ProofFailed, err.Error(), now)
		return fmt.Errorf("materialize verified account proof: %w", err)
	}
	return s.store.CompleteProofResponsibility(ctx, responsibility.Operation.OperationID, proof.ProofID, proof.Revision, ProofActive, "", now)
}

func proofFromOperation(op Operation, binding Binding) VerifiedProof {
	return VerifiedProof{
		Format: ProofFormat, ProofID: op.ProofID, Revision: op.ProofRevision, Status: ProofActive,
		Interface: op.Interface, ExternalAccountRef: op.ExternalAccountRef, ConversationRef: op.ConversationRef,
		ConversationScope: op.ConversationScope, AccountPresentation: op.AccountPresentation,
		Method: string(op.Kind), Challenge: op.Challenge, OriginalOperationID: op.OperationID,
		MintingStoreID: op.PrincipalID, VerifiedAt: op.CompletedAt, OperatorConfirmed: true,
		ConsentScopes: []ConsentScope{ConsentNotify, ConsentDecide},
	}
}

func (s *Service) Readback(ctx context.Context) ([]Readback, error) {
	principal, err := s.Principal()
	if err != nil {
		return nil, err
	}
	bindings, err := s.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	operations, err := s.store.ListOperatorChannelOperations(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	proofs, err := s.proofs.List(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	interfaces := append([]InterfaceIdentity(nil), s.interfaces...)
	s.mu.RUnlock()
	current := map[string]InterfaceIdentity{}
	activeInterfaces := map[string]InterfaceIdentity{}
	for _, identity := range interfaces {
		identity = identity.Normalized()
		current[identity.Key()] = identity
		activeInterfaces[identity.Key()] = identity
	}
	bindingByKey := map[string]Binding{}
	for _, binding := range bindings {
		binding.Interface = binding.Interface.Normalized()
		bindingByKey[binding.Interface.Key()] = binding
		if _, exists := current[binding.Interface.Key()]; !exists {
			current[binding.Interface.Key()] = binding.Interface
		}
	}
	proofByKey := map[string]VerifiedProof{}
	for _, proof := range proofs {
		proof.Interface = proof.Interface.Normalized()
		proofByKey[proof.Interface.Key()] = proof
	}
	pendingByKey := map[string]Operation{}
	for _, op := range operations {
		op.Interface = op.Interface.Normalized()
		if op.State.Terminal() {
			continue
		}
		key := op.Interface.Key()
		if existing, ok := pendingByKey[key]; !ok || existing.RequestedAt.Before(op.RequestedAt) {
			copy := op
			copy.AccountPresentation = MaskPresentation(copy.AccountPresentation)
			copy.ExternalAccountRef = MaskPresentation(copy.ExternalAccountRef)
			copy.ConversationRef = MaskPresentation(copy.ConversationRef)
			pendingByKey[key] = copy
		}
	}
	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Readback, 0, len(keys))
	for _, key := range keys {
		identity := current[key]
		read := Readback{PrincipalID: principal.ID, Interface: identity, Status: BindingUnbound, Reason: "no local binding"}
		if op, ok := pendingByKey[key]; ok {
			read.PendingOperation = &op
		}
		binding, found := bindingByKey[key]
		if !found {
			out = append(out, read)
			continue
		}
		read.BindingRevision, read.ExternalAccountRef, read.ConversationRef = binding.Revision, MaskPresentation(binding.ExternalAccountRef), MaskPresentation(binding.ConversationRef)
		read.ConversationScope, read.AccountPresentation, read.Source = binding.ConversationScope, MaskPresentation(binding.AccountPresentation), binding.Source
		read.ProofID, read.ProofRevision = binding.ProofID, binding.ProofRevision
		if binding.Status == BindingUnbound {
			read.Reason = "explicit local unbind fence"
			out = append(out, read)
			continue
		}
		active, activeFound := activeInterfaces[key]
		if !activeFound || active.SemanticGeneration != binding.Interface.SemanticGeneration {
			read.Status, read.Reason = BindingStale, "channel semantic generation changed; reconnect required"
			out = append(out, read)
			continue
		}
		if binding.ProofID != "" {
			proof, found := proofByKey[key]
			if !found || proof.Status != ProofActive || proof.ProofID != binding.ProofID || proof.Revision != binding.ProofRevision {
				read.Status, read.Reason, read.ProofStatus = BindingRevoked, "machine-local verified account proof is missing, revoked, or superseded", ProofRevoked
				out = append(out, read)
				continue
			}
			read.ProofStatus = proof.Status
			read.ConsentScopes = append([]ConsentScope(nil), proof.ConsentScopes...)
		}
		read.Status, read.Reason = BindingCurrent, ""
		out = append(out, read)
	}
	return out, nil
}

func (s *Service) mustPrincipalID() string {
	principal, _ := s.Principal()
	return principal.ID
}

func RequestIdentity(method, principalID, idempotencyKey, requestHash string) (string, string) {
	key := Hash(method, principalID, idempotencyKey)
	if strings.TrimSpace(idempotencyKey) == "" {
		key = Hash(method, principalID, requestHash, NewOperationID())
	}
	return key, strings.TrimSpace(requestHash)
}
