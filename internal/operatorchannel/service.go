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
	if op.State == StateBound && (op.ProofStatus == ProofPending || op.ProofStatus == ProofFailed) {
		if err := s.materializeProof(ctx, ProofResponsibility{Operation: op, Binding: binding, Proof: proofFromOperation(op, binding)}, now); err != nil {
			return op, binding, err
		}
		op.ProofStatus = ProofActive
	}
	return op, binding, nil
}

// ExpireOperation settles one exact overdue identity ceremony. The selected
// store owns the terminal transition; callers cannot infer expiry locally.
func (s *Service) ExpireOperation(ctx context.Context, operationID string, expectedRevision int64, now time.Time) (Operation, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, err
	}
	return s.store.ExpireChannelBinding(ctx, ExpireRequest{
		OperationID: strings.TrimSpace(operationID), PrincipalID: principal.ID,
		ExpectedRevision: expectedRevision, ExpiredAt: now,
	})
}

func (s *Service) Unbind(ctx context.Context, selector string, expectedRevision int64, requestKey, requestHash string, now time.Time) (Operation, Binding, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, Binding{}, err
	}
	identity, err := s.ResolveRetainedInterface(ctx, selector)
	if err != nil {
		return Operation{}, Binding{}, err
	}
	return s.store.UnbindOperatorChannel(ctx, UnbindRequest{OperationID: NewOperationID(), PrincipalID: principal.ID, Interface: identity, ExpectedRevision: expectedRevision, RequestKeyHash: requestKey, RequestHash: requestHash, RequestedAt: now})
}

func (s *Service) RevokeProof(ctx context.Context, selector string, expectedRevision int64, now time.Time) (VerifiedProof, error) {
	identity, err := s.ResolveRetainedInterface(ctx, selector)
	if err != nil {
		return VerifiedProof{}, err
	}
	revoked, err := s.proofs.Revoke(ctx, identity, expectedRevision, now)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return VerifiedProof{}, fmt.Errorf("%w: no machine-local proof exists for operator channel interface %q", ErrProofUnavailable, identity.Selector)
		}
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

func (s *Service) CurrentProof(ctx context.Context, selector string) (VerifiedProof, error) {
	identity, err := s.ResolveRetainedInterface(ctx, selector)
	if err != nil {
		return VerifiedProof{}, err
	}
	proof, found, err := s.proofs.Get(ctx, identity)
	if err != nil {
		return VerifiedProof{}, err
	}
	if !found || proof.Status != ProofActive {
		return VerifiedProof{}, fmt.Errorf("%w: no active machine-local proof exists for operator channel interface %q", ErrProofUnavailable, identity.Selector)
	}
	return proof, nil
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
	proof, err := validatedResponsibilityProof(responsibility)
	if err != nil {
		_ = s.store.CompleteProofResponsibility(context.WithoutCancel(ctx), responsibility.Operation.OperationID, responsibility.Operation.ProofID, responsibility.Operation.ProofRevision, ProofFailed, err.Error(), now)
		return fmt.Errorf("materialize verified account proof: %w", err)
	}
	existing, found, err := s.proofs.Get(ctx, proof.Interface)
	if err == nil && found && existing.ProofID == proof.ProofID && existing.Revision == proof.Revision {
		if !proofMatchesResponsibility(existing, proof) {
			err = fmt.Errorf("%w: existing proof %q revision %d contradicts its durable responsibility", ErrRevisionConflict, proof.ProofID, proof.Revision)
		} else {
			// The file write is already durable. Its deployment occurrence is
			// immutable even when a later serve completes the database handoff.
			proof = existing
		}
	} else if err == nil {
		proof.MintingDeploymentID = s.deploymentID
		err = s.proofs.Put(ctx, proof)
	}
	if err != nil {
		_ = s.store.CompleteProofResponsibility(context.WithoutCancel(ctx), responsibility.Operation.OperationID, proof.ProofID, proof.Revision, ProofFailed, err.Error(), now)
		return fmt.Errorf("materialize verified account proof: %w", err)
	}
	return s.store.CompleteProofResponsibility(ctx, responsibility.Operation.OperationID, proof.ProofID, proof.Revision, ProofActive, "", now)
}

func validatedResponsibilityProof(responsibility ProofResponsibility) (VerifiedProof, error) {
	operation := responsibility.Operation
	binding := responsibility.Binding
	if operation.State != StateBound || !operation.SaveProof ||
		(operation.ProofStatus != ProofPending && operation.ProofStatus != ProofFailed) ||
		binding.Status != BindingCurrent || binding.Source != BindingSourceLiveVerification ||
		binding.PrincipalID != operation.PrincipalID || binding.Interface.Normalized() != operation.Interface.Normalized() ||
		binding.ExternalAccountRef != operation.ExternalAccountRef || binding.ConversationRef != operation.ConversationRef ||
		binding.ConversationScope != operation.ConversationScope || binding.AccountPresentation != operation.AccountPresentation ||
		binding.Revision != operation.BindingRevision || binding.OperationID != operation.OperationID ||
		binding.ProofID != operation.ProofID || binding.ProofRevision != operation.ProofRevision ||
		!binding.UpdatedAt.Equal(operation.CompletedAt) {
		return VerifiedProof{}, fmt.Errorf("%w: proof responsibility contradicts its committed operation or binding", ErrRevisionConflict)
	}
	proof := proofFromOperation(operation, binding)
	if !immutableProofFactsMatch(responsibility.Proof, proof) {
		return VerifiedProof{}, fmt.Errorf("%w: durable proof projection contradicts its committed operation or binding", ErrRevisionConflict)
	}
	return proof, nil
}

func proofMatchesResponsibility(existing, responsibility VerifiedProof) bool {
	if err := existing.Validate(); err != nil || strings.TrimSpace(existing.MintingDeploymentID) == "" {
		return false
	}
	return immutableProofFactsMatch(existing, responsibility)
}

func immutableProofFactsMatch(existing, responsibility VerifiedProof) bool {
	return existing.Format == responsibility.Format &&
		existing.ProofID == responsibility.ProofID &&
		existing.Revision == responsibility.Revision &&
		existing.Status == responsibility.Status &&
		existing.Interface.Normalized() == responsibility.Interface.Normalized() &&
		existing.ExternalAccountRef == responsibility.ExternalAccountRef &&
		existing.ConversationRef == responsibility.ConversationRef &&
		existing.ConversationScope == responsibility.ConversationScope &&
		existing.AccountPresentation == responsibility.AccountPresentation &&
		existing.Method == responsibility.Method &&
		existing.Challenge == responsibility.Challenge &&
		existing.OriginalOperationID == responsibility.OriginalOperationID &&
		existing.MintingStoreID == responsibility.MintingStoreID &&
		existing.VerifiedAt.Equal(responsibility.VerifiedAt) &&
		existing.OperatorConfirmed == responsibility.OperatorConfirmed &&
		equalConsentScopes(existing.ConsentScopes, responsibility.ConsentScopes)
}

func equalConsentScopes(left, right []ConsentScope) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
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

type retainedLifecycleProjection struct {
	active     map[string]InterfaceIdentity
	identities map[string]InterfaceIdentity
	bindings   map[string]Binding
	proofs     map[string]VerifiedProof
	pending    map[string]Operation
}

func (s *Service) retainedProjection(ctx context.Context) (retainedLifecycleProjection, error) {
	principal, err := s.Principal()
	if err != nil {
		return retainedLifecycleProjection{}, err
	}
	bindings, err := s.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil {
		return retainedLifecycleProjection{}, err
	}
	operations, err := s.store.ListOperatorChannelOperations(ctx, principal.ID)
	if err != nil {
		return retainedLifecycleProjection{}, err
	}
	proofs, err := s.proofs.List(ctx)
	if err != nil {
		return retainedLifecycleProjection{}, err
	}
	s.mu.RLock()
	interfaces := append([]InterfaceIdentity(nil), s.interfaces...)
	s.mu.RUnlock()

	projection := retainedLifecycleProjection{
		active: map[string]InterfaceIdentity{}, identities: map[string]InterfaceIdentity{}, bindings: map[string]Binding{},
		proofs: map[string]VerifiedProof{}, pending: map[string]Operation{},
	}
	addIdentity := func(identity InterfaceIdentity) error {
		identity = identity.Normalized()
		if err := identity.Validate(); err != nil {
			return err
		}
		if identity.InterfaceRef != InterfaceHITLChannelV2 {
			return fmt.Errorf("%w: retained operator channel identity uses unsupported interface %q", ErrInvalidRequest, identity.InterfaceRef)
		}
		key := identity.Key()
		if existing, found := projection.identities[key]; found && existing != identity {
			return fmt.Errorf("%w: retained operator channel identity %q is contradictory", ErrConflict, identity.Selector)
		}
		projection.identities[key] = identity
		return nil
	}
	for _, identity := range interfaces {
		identity = identity.Normalized()
		if err := addIdentity(identity); err != nil {
			return retainedLifecycleProjection{}, err
		}
		projection.active[identity.Key()] = identity
	}
	for _, binding := range bindings {
		binding.Interface = binding.Interface.Normalized()
		if binding.PrincipalID != principal.ID {
			return retainedLifecycleProjection{}, fmt.Errorf("%w: retained operator channel binding belongs to another principal", ErrConflict)
		}
		if err := addIdentity(binding.Interface); err != nil {
			return retainedLifecycleProjection{}, err
		}
		key := binding.Interface.Key()
		if _, found := projection.bindings[key]; found {
			return retainedLifecycleProjection{}, fmt.Errorf("%w: duplicate retained operator channel binding %q", ErrConflict, binding.Interface.Selector)
		}
		projection.bindings[key] = binding
	}
	for _, proof := range proofs {
		proof.Interface = proof.Interface.Normalized()
		if err := addIdentity(proof.Interface); err != nil {
			return retainedLifecycleProjection{}, err
		}
		key := proof.Interface.Key()
		if _, found := projection.proofs[key]; found {
			return retainedLifecycleProjection{}, fmt.Errorf("%w: duplicate retained operator channel proof %q", ErrConflict, proof.Interface.Selector)
		}
		projection.proofs[key] = proof
	}
	for _, operation := range operations {
		if operation.State.Terminal() {
			continue
		}
		operation.Interface = operation.Interface.Normalized()
		if operation.PrincipalID != principal.ID {
			return retainedLifecycleProjection{}, fmt.Errorf("%w: retained operator channel operation belongs to another principal", ErrConflict)
		}
		if err := addIdentity(operation.Interface); err != nil {
			return retainedLifecycleProjection{}, err
		}
		key := operation.Interface.Key()
		if existing, found := projection.pending[key]; !found || existing.RequestedAt.Before(operation.RequestedAt) ||
			(existing.RequestedAt.Equal(operation.RequestedAt) && existing.OperationID < operation.OperationID) {
			projection.pending[key] = ProjectOperationReadback(operation)
		}
	}
	return projection, nil
}

func (s *Service) ResolveRetainedInterface(ctx context.Context, selector string) (InterfaceIdentity, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return InterfaceIdentity{}, fmt.Errorf("%w: interface selector is required", ErrInvalidRequest)
	}
	projection, err := s.retainedProjection(ctx)
	if err != nil {
		return InterfaceIdentity{}, err
	}
	matches := []InterfaceIdentity{}
	for _, identity := range projection.identities {
		if identity.Selector == selector {
			matches = append(matches, identity)
		}
	}
	if len(matches) == 0 {
		return InterfaceIdentity{}, fmt.Errorf("%w: retained operator channel interface %q was not found", ErrNotFound, selector)
	}
	if len(matches) != 1 {
		return InterfaceIdentity{}, fmt.Errorf("%w: retained operator channel interface %q is ambiguous", ErrConflict, selector)
	}
	return matches[0], nil
}

// GetOperation returns one exact durable identity operation for trusted
// lifecycle composition. Public API readback continues to use masked rows.
func (s *Service) GetOperation(ctx context.Context, operationID string) (Operation, error) {
	principal, err := s.Principal()
	if err != nil {
		return Operation{}, err
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return Operation{}, fmt.Errorf("%w: operation_id is required", ErrInvalidRequest)
	}
	operations, err := s.store.ListOperatorChannelOperations(ctx, principal.ID)
	if err != nil {
		return Operation{}, err
	}
	for _, operation := range operations {
		if operation.OperationID == operationID {
			return operation, nil
		}
	}
	return Operation{}, fmt.Errorf("%w: operator channel operation %q was not found", ErrNotFound, operationID)
}

// CurrentBinding returns the unmasked selected-store identity fact to trusted
// runtime composition. It never creates or recovers a binding.
func (s *Service) CurrentBinding(ctx context.Context, identity InterfaceIdentity) (Binding, error) {
	principal, err := s.Principal()
	if err != nil {
		return Binding{}, err
	}
	identity = identity.Normalized()
	if err := identity.Validate(); err != nil {
		return Binding{}, err
	}
	bindings, err := s.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil {
		return Binding{}, err
	}
	for _, binding := range bindings {
		if binding.Interface.Normalized().Key() != identity.Key() || binding.Status != BindingCurrent {
			continue
		}
		return binding, nil
	}
	return Binding{}, fmt.Errorf("%w: current operator channel binding %q was not found", ErrNotFound, identity.Selector)
}

// CurrentBindingReadiness returns the exact current binding and whether its
// optional machine-proof link is current. Proofless bindings are valid without
// manufacturing proof evidence.
func (s *Service) CurrentBindingReadiness(ctx context.Context, identity InterfaceIdentity) (Binding, bool, error) {
	binding, err := s.CurrentBinding(ctx, identity)
	if err != nil {
		return Binding{}, false, err
	}
	if strings.TrimSpace(binding.ProofID) == "" {
		return binding, true, nil
	}
	proof, found, err := s.proofs.Get(ctx, binding.Interface)
	if err != nil {
		return Binding{}, false, err
	}
	current := found && proof.Status == ProofActive && proof.ProofID == binding.ProofID && proof.Revision == binding.ProofRevision
	return binding, current, nil
}

func (s *Service) Readback(ctx context.Context) ([]Readback, error) {
	principal, err := s.Principal()
	if err != nil {
		return nil, err
	}
	projection, err := s.retainedProjection(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(projection.identities))
	for key := range projection.identities {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Readback, 0, len(keys))
	for _, key := range keys {
		identity := projection.identities[key]
		read := Readback{PrincipalID: principal.ID, Interface: identity, Status: BindingUnbound, Reason: "no local binding"}
		if op, ok := projection.pending[key]; ok {
			read.PendingOperation = &op
		}
		proof, proofFound := projection.proofs[key]
		if proofFound {
			read.ProofID, read.ProofRevision, read.ProofStatus = proof.ProofID, proof.Revision, proof.Status
			read.ConsentScopes = append([]ConsentScope(nil), proof.ConsentScopes...)
		}
		binding, found := projection.bindings[key]
		if !found {
			if proofFound && proof.Status == ProofRevoked {
				read.Status, read.Reason = BindingRevoked, "machine-local verified account proof is revoked; no local binding"
			} else if _, active := projection.active[key]; !active {
				read.Status, read.Reason = BindingStale, "retained channel interface is no longer active"
			} else if proofFound {
				read.Reason = "active machine-local verified account proof; no local binding"
			}
			out = append(out, read)
			continue
		}
		projectBindingReadback(&read, binding)
		if binding.Status == BindingUnbound {
			read.Reason = "explicit local unbind fence"
			out = append(out, read)
			continue
		}
		active, activeFound := projection.active[key]
		if !activeFound || active.SemanticGeneration != binding.Interface.SemanticGeneration {
			read.Status, read.Reason = BindingStale, "channel semantic generation changed; reconnect required"
			out = append(out, read)
			continue
		}
		if binding.ProofID != "" {
			if !proofFound || proof.Status != ProofActive || proof.ProofID != binding.ProofID || proof.Revision != binding.ProofRevision {
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

func projectBindingReadback(read *Readback, binding Binding) {
	read.BindingRevision, read.ExternalAccountRef, read.ConversationRef = binding.Revision, MaskPresentation(binding.ExternalAccountRef), MaskPresentation(binding.ConversationRef)
	read.ConversationScope, read.AccountPresentation, read.Source = binding.ConversationScope, MaskClaimantPresentation(binding.AccountPresentation, binding.ExternalAccountRef), binding.Source
	if strings.TrimSpace(binding.ProofID) != "" {
		read.ProofID, read.ProofRevision = binding.ProofID, binding.ProofRevision
	}
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
