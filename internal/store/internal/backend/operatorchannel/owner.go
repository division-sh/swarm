package operatorchannel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	domain "github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

type schemaRequirement func() error

type PostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent schemaRequirement
}

type SQLiteOwner struct {
	backend        *sqlitebackend.Backend
	requireCurrent schemaRequirement
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent schemaRequirement) (*PostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, fmt.Errorf("postgres operator channel owner requires backend and schema owner")
	}
	return &PostgresOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent schemaRequirement) (*SQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, fmt.Errorf("sqlite operator channel owner requires backend and schema owner")
	}
	return &SQLiteOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

type transactionRunner interface {
	mutate(context.Context, string, func(context.Context, *sql.Tx) error) error
	query() queryer
	dialect() dialect
	require() error
}

type postgresRunner struct{ owner *PostgresOwner }

func (r postgresRunner) mutate(ctx context.Context, _ string, fn func(context.Context, *sql.Tx) error) error {
	tx, err := r.owner.backend.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return errors.Join(err, rollback(tx))
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(err, rollback(tx))
	}
	return nil
}
func (r postgresRunner) query() queryer   { return r.owner.backend }
func (r postgresRunner) dialect() dialect { return dialectPostgres }
func (r postgresRunner) require() error   { return r.owner.requireCurrent() }

type sqliteRunner struct{ owner *SQLiteOwner }

func (r sqliteRunner) mutate(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	return r.owner.backend.RunTransaction(ctx, label, fn)
}
func (r sqliteRunner) query() queryer   { return r.owner.backend }
func (r sqliteRunner) dialect() dialect { return dialectSQLite }
func (r sqliteRunner) require() error   { return r.owner.requireCurrent() }

type dialect uint8

const (
	dialectPostgres dialect = iota + 1
	dialectSQLite
)

func (d dialect) bind(query string) string {
	if d != dialectPostgres {
		return query
	}
	var out strings.Builder
	index := 1
	for _, ch := range query {
		if ch == '?' {
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(index))
			index++
			continue
		}
		out.WriteRune(ch)
	}
	return out.String()
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func rollback(tx *sql.Tx) error {
	if tx == nil {
		return nil
	}
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func (s *PostgresOwner) EnsureOperatorPrincipal(ctx context.Context, now time.Time) (domain.Principal, error) {
	return ensurePrincipal(ctx, postgresRunner{s}, now)
}
func (s *SQLiteOwner) EnsureOperatorPrincipal(ctx context.Context, now time.Time) (domain.Principal, error) {
	return ensurePrincipal(ctx, sqliteRunner{s}, now)
}

func ensurePrincipal(ctx context.Context, runner transactionRunner, now time.Time) (domain.Principal, error) {
	if err := runner.require(); err != nil {
		return domain.Principal{}, err
	}
	now = canonicalTime(now)
	var out domain.Principal
	err := runner.mutate(ctx, "ensure operator principal", func(txctx context.Context, tx *sql.Tx) error {
		if runner.dialect() == dialectPostgres {
			if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext('operator_principal_singleton'))`); err != nil {
				return fmt.Errorf("lock operator principal singleton: %w", err)
			}
		}
		principal, found, err := loadPrincipal(txctx, tx, runner.dialect())
		if err != nil {
			return err
		}
		if found {
			out = principal
			return nil
		}
		out = domain.Principal{ID: uuid.NewString(), CreatedAt: now}
		_, err = tx.ExecContext(txctx, runner.dialect().bind(`INSERT INTO operator_principals (singleton_id, principal_id, created_at) VALUES (1, ?, ?)`), out.ID, out.CreatedAt)
		return err
	})
	return out, err
}

func loadPrincipal(ctx context.Context, db queryer, d dialect) (domain.Principal, bool, error) {
	var principal domain.Principal
	var created any
	err := db.QueryRowContext(ctx, d.bind(`SELECT principal_id, created_at FROM operator_principals WHERE singleton_id = 1`)).Scan(&principal.ID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Principal{}, false, nil
	}
	if err != nil {
		return domain.Principal{}, false, fmt.Errorf("load operator principal: %w", err)
	}
	principal.CreatedAt, err = timeValue(created)
	if err != nil {
		return domain.Principal{}, false, err
	}
	return principal, true, principal.Validate()
}

func (s *PostgresOwner) BeginChannelBinding(ctx context.Context, req domain.BeginRequest) (domain.Operation, error) {
	return beginBinding(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) BeginChannelBinding(ctx context.Context, req domain.BeginRequest) (domain.Operation, error) {
	return beginBinding(ctx, sqliteRunner{s}, req)
}

func beginBinding(ctx context.Context, runner transactionRunner, req domain.BeginRequest) (domain.Operation, error) {
	if err := runner.require(); err != nil {
		return domain.Operation{}, err
	}
	if err := validateBegin(req); err != nil {
		return domain.Operation{}, err
	}
	challenge, err := domain.NewChallenge()
	if err != nil {
		return domain.Operation{}, err
	}
	var out domain.Operation
	err = runner.mutate(ctx, "begin operator channel binding", func(txctx context.Context, tx *sql.Tx) error {
		if err := requirePrincipal(txctx, tx, runner.dialect(), req.PrincipalID); err != nil {
			return err
		}
		existing, found, err := loadOperationByRequestKey(txctx, tx, runner.dialect(), req.RequestKeyHash, true)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestHash != req.RequestHash {
				return fmt.Errorf("%w: request key was already used with different input", domain.ErrConflict)
			}
			out = existing
			return nil
		}
		binding, bindingFound, err := loadBinding(txctx, tx, runner.dialect(), req.Interface.Key(), true)
		if err != nil {
			return err
		}
		unresolved, err := unresolvedProofResponsibilityIDs(txctx, tx, runner.dialect(), req.Interface.Key(), true)
		if err != nil {
			return err
		}
		if len(unresolved) != 0 {
			return fmt.Errorf("%w: operator channel interface has unresolved proof responsibility; replay its confirmation or unbind first", domain.ErrConflict)
		}
		currentRevision := int64(0)
		if bindingFound {
			currentRevision = binding.Revision
		}
		if currentRevision != req.ExpectedRevision {
			return fmt.Errorf("%w: current binding revision is %d", domain.ErrRevisionConflict, currentRevision)
		}
		switch req.Kind {
		case domain.OperationConnect:
			if bindingFound && binding.Status == domain.BindingCurrent {
				return fmt.Errorf("%w: connect requires an unbound interface", domain.ErrConflict)
			}
		case domain.OperationReconnect, domain.OperationRebind:
			if !bindingFound || binding.Status != domain.BindingCurrent {
				return fmt.Errorf("%w: %s requires a current binding", domain.ErrConflict, req.Kind)
			}
		}
		out = domain.Operation{
			OperationID: req.OperationID, Kind: req.Kind, PrincipalID: req.PrincipalID,
			Interface: req.Interface.Normalized(), Challenge: challenge, State: domain.StateAwaitingClaim,
			Revision: 1, SaveProof: req.SaveProof, ProofStatus: domain.ProofSkipped,
			PlannedProofID: req.PlannedProofID, PlannedProofRevision: req.PlannedProofRevision,
			ProviderCredential: req.ProviderCredential,
			RequestedAt:        canonicalTime(req.RequestedAt), ExpiresAt: canonicalTime(req.ExpiresAt),
		}
		return insertOperation(txctx, tx, runner.dialect(), out, req.RequestKeyHash, req.RequestHash, req.ExpectedRevision, "")
	})
	return out, err
}

func validateBegin(req domain.BeginRequest) error {
	if req.OperationID == "" || req.PrincipalID == "" || req.RequestKeyHash == "" || req.RequestHash == "" || !req.Kind.Valid() || req.Kind == domain.OperationUnbind {
		return fmt.Errorf("%w: complete connect, reconnect, or rebind request is required", domain.ErrInvalidRequest)
	}
	if _, err := uuid.Parse(req.OperationID); err != nil {
		return fmt.Errorf("%w: operation_id must be a UUID", domain.ErrInvalidRequest)
	}
	if err := req.Interface.Validate(); err != nil {
		return err
	}
	if req.ExpectedRevision < 0 || req.RequestedAt.IsZero() || !req.ExpiresAt.After(req.RequestedAt) {
		return fmt.Errorf("%w: revision and bounded challenge lifetime are required", domain.ErrInvalidRequest)
	}
	if req.SaveProof != (req.PlannedProofID != "" && req.PlannedProofRevision > 0) {
		return fmt.Errorf("%w: saved proof operations require one exact planned proof identity and revision", domain.ErrInvalidRequest)
	}
	if req.SaveProof && req.PlannedProofID != domain.ProofIDForInterface(req.Interface) {
		return fmt.Errorf("%w: planned proof identity does not match the exact interface", domain.ErrInvalidRequest)
	}
	if err := req.ProviderCredential.Validate(); err != nil {
		return fmt.Errorf("%w: provider credential evidence is required", domain.ErrInvalidRequest)
	}
	return nil
}

func (s *PostgresOwner) ConfirmChannelBinding(ctx context.Context, req domain.ConfirmRequest) (domain.Operation, domain.Binding, error) {
	return confirmBinding(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) ConfirmChannelBinding(ctx context.Context, req domain.ConfirmRequest) (domain.Operation, domain.Binding, error) {
	return confirmBinding(ctx, sqliteRunner{s}, req)
}

func (s *PostgresOwner) ExpireChannelBinding(ctx context.Context, req domain.ExpireRequest) (domain.Operation, error) {
	return expireBinding(ctx, postgresRunner{s}, req)
}

func (s *SQLiteOwner) ExpireChannelBinding(ctx context.Context, req domain.ExpireRequest) (domain.Operation, error) {
	return expireBinding(ctx, sqliteRunner{s}, req)
}

func expireBinding(ctx context.Context, runner transactionRunner, req domain.ExpireRequest) (domain.Operation, error) {
	if err := runner.require(); err != nil {
		return domain.Operation{}, err
	}
	if strings.TrimSpace(req.OperationID) == "" || strings.TrimSpace(req.PrincipalID) == "" || req.ExpectedRevision < 1 || req.ExpiredAt.IsZero() {
		return domain.Operation{}, fmt.Errorf("%w: expiry identity, revision, and time are required", domain.ErrInvalidRequest)
	}
	var out domain.Operation
	err := runner.mutate(ctx, "expire operator channel binding", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperationByID(txctx, tx, runner.dialect(), req.OperationID, true)
		if err != nil || !found {
			if !found && err == nil {
				return domain.ErrNotFound
			}
			return err
		}
		if op.PrincipalID != req.PrincipalID {
			return domain.ErrRevisionConflict
		}
		if op.State == domain.StateExpired {
			if op.Revision != req.ExpectedRevision+1 {
				return domain.ErrRevisionConflict
			}
			out = op
			return nil
		}
		if op.State.Terminal() {
			return domain.ErrOperationTerminal
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		if op.State != domain.StateAwaitingClaim && op.State != domain.StateAwaitingConfirmation {
			return fmt.Errorf("%w: operation is %s", domain.ErrConflict, op.State)
		}
		now := canonicalTime(req.ExpiredAt)
		if op.ExpiresAt.After(now) {
			return fmt.Errorf("%w: operation challenge has not expired", domain.ErrConflict)
		}
		op.State, op.Revision, op.CompletedAt = domain.StateExpired, op.Revision+1, now
		if err := updateOperationTerminal(txctx, tx, runner.dialect(), op); err != nil {
			return err
		}
		out = op
		return nil
	})
	return out, err
}

func confirmBinding(ctx context.Context, runner transactionRunner, req domain.ConfirmRequest) (domain.Operation, domain.Binding, error) {
	if err := runner.require(); err != nil {
		return domain.Operation{}, domain.Binding{}, err
	}
	if req.OperationID == "" || req.PrincipalID == "" || req.ExpectedRevision < 1 || req.ConfirmedAt.IsZero() {
		return domain.Operation{}, domain.Binding{}, fmt.Errorf("%w: confirmation identity, revision, and time are required", domain.ErrInvalidRequest)
	}
	var out domain.Operation
	var binding domain.Binding
	var terminalErr error
	err := runner.mutate(ctx, "confirm operator channel binding", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperationByID(txctx, tx, runner.dialect(), req.OperationID, true)
		if err != nil || !found {
			if !found && err == nil {
				return domain.ErrNotFound
			}
			return err
		}
		if op.PrincipalID != req.PrincipalID {
			return domain.ErrRevisionConflict
		}
		if op.State.Terminal() {
			if op.State == domain.StateExpired {
				if op.Revision != req.ExpectedRevision+1 {
					return domain.ErrRevisionConflict
				}
				out = op
				terminalErr = domain.ErrOperationTerminal
				return nil
			}
			if op.Revision != req.ExpectedRevision+1 || (op.State == domain.StateBound) != req.Approve || (op.State == domain.StateRejected) == req.Approve {
				return domain.ErrRevisionConflict
			}
			out = op
			if op.State == domain.StateBound {
				binding, _, err = loadBinding(txctx, tx, runner.dialect(), op.Interface.Key(), false)
			}
			return err
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		if op.State != domain.StateAwaitingConfirmation {
			return fmt.Errorf("%w: operation is %s", domain.ErrConflict, op.State)
		}
		now := canonicalTime(req.ConfirmedAt)
		if !op.ExpiresAt.After(now) {
			op.State, op.Revision, op.CompletedAt = domain.StateExpired, op.Revision+1, now
			if err := updateOperationTerminal(txctx, tx, runner.dialect(), op); err != nil {
				return err
			}
			out = op
			terminalErr = domain.ErrOperationTerminal
			return nil
		}
		if !req.Approve {
			op.State, op.Revision, op.CompletedAt = domain.StateRejected, op.Revision+1, now
			if err := updateOperationTerminal(txctx, tx, runner.dialect(), op); err != nil {
				return err
			}
			out = op
			return nil
		}
		current, currentFound, err := loadBinding(txctx, tx, runner.dialect(), op.Interface.Key(), true)
		if err != nil {
			return err
		}
		currentRevision := int64(0)
		if currentFound {
			currentRevision = current.Revision
		}
		if currentRevision != op.ExpectedBindingRevision {
			return domain.ErrRevisionConflict
		}
		sameClaimant := currentFound && current.Status == domain.BindingCurrent &&
			current.ExternalAccountRef == op.ExternalAccountRef && current.ConversationRef == op.ConversationRef && current.ConversationScope == op.ConversationScope
		switch op.Kind {
		case domain.OperationConnect:
			if currentFound && current.Status == domain.BindingCurrent {
				return fmt.Errorf("%w: connect cannot replace a current binding", domain.ErrConflict)
			}
		case domain.OperationReconnect:
			if !sameClaimant {
				return fmt.Errorf("%w: reconnect claimant differs from the current binding", domain.ErrConflict)
			}
		case domain.OperationRebind:
			if !currentFound || current.Status != domain.BindingCurrent || sameClaimant {
				return fmt.Errorf("%w: rebind requires a different claimant and a current binding", domain.ErrConflict)
			}
		}
		bindingRevision := currentRevision + 1
		proofStatus := domain.ProofSkipped
		proofID := ""
		proofRevision := int64(0)
		if op.SaveProof {
			proofStatus, proofID, proofRevision = domain.ProofPending, op.PlannedProofID, op.PlannedProofRevision
		}
		binding = domain.Binding{
			PrincipalID: op.PrincipalID, Interface: op.Interface, ExternalAccountRef: op.ExternalAccountRef,
			ConversationRef: op.ConversationRef, ConversationScope: op.ConversationScope,
			AccountPresentation: op.AccountPresentation, Revision: bindingRevision, Status: domain.BindingCurrent,
			Source: domain.BindingSourceLiveVerification, ProofID: proofID, ProofRevision: proofRevision,
			OperationID: op.OperationID, UpdatedAt: now, ProviderCredential: op.ProviderCredential,
		}
		if err := upsertBinding(txctx, tx, runner.dialect(), binding); err != nil {
			return err
		}
		op.State, op.Revision, op.BindingRevision, op.CompletedAt = domain.StateBound, op.Revision+1, bindingRevision, now
		op.ProofStatus, op.ProofID, op.ProofRevision = proofStatus, proofID, proofRevision
		if err := updateOperationBound(txctx, tx, runner.dialect(), op); err != nil {
			return err
		}
		out = op
		return nil
	})
	if err != nil {
		return out, binding, err
	}
	return out, binding, terminalErr
}

func (s *PostgresOwner) UnbindOperatorChannel(ctx context.Context, req domain.UnbindRequest) (domain.Operation, domain.Binding, error) {
	return unbind(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) UnbindOperatorChannel(ctx context.Context, req domain.UnbindRequest) (domain.Operation, domain.Binding, error) {
	return unbind(ctx, sqliteRunner{s}, req)
}

func unbind(ctx context.Context, runner transactionRunner, req domain.UnbindRequest) (domain.Operation, domain.Binding, error) {
	if err := runner.require(); err != nil {
		return domain.Operation{}, domain.Binding{}, err
	}
	if req.OperationID == "" || req.PrincipalID == "" || req.RequestKeyHash == "" || req.RequestHash == "" || req.ExpectedRevision < 1 || req.RequestedAt.IsZero() {
		return domain.Operation{}, domain.Binding{}, fmt.Errorf("%w: complete unbind request is required", domain.ErrInvalidRequest)
	}
	if err := req.Interface.Validate(); err != nil {
		return domain.Operation{}, domain.Binding{}, err
	}
	var op domain.Operation
	var binding domain.Binding
	err := runner.mutate(ctx, "unbind operator channel", func(txctx context.Context, tx *sql.Tx) error {
		existing, found, err := loadOperationByRequestKey(txctx, tx, runner.dialect(), req.RequestKeyHash, true)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestHash != req.RequestHash {
				return domain.ErrConflict
			}
			op = existing
			binding, _, err = loadBinding(txctx, tx, runner.dialect(), req.Interface.Key(), false)
			return err
		}
		current, found, err := loadBinding(txctx, tx, runner.dialect(), req.Interface.Key(), true)
		if err != nil {
			return err
		}
		if !found || current.Status != domain.BindingCurrent || current.Revision != req.ExpectedRevision || current.PrincipalID != req.PrincipalID {
			return domain.ErrRevisionConflict
		}
		now := canonicalTime(req.RequestedAt)
		if err := dischargeProofResponsibilities(txctx, tx, runner.dialect(), req.Interface.Key(), now); err != nil {
			return err
		}
		op = domain.Operation{OperationID: req.OperationID, Kind: domain.OperationUnbind, PrincipalID: req.PrincipalID, Interface: req.Interface.Normalized(), State: domain.StateUnbound, Revision: 1, BindingRevision: current.Revision + 1, SaveProof: false, ProofStatus: domain.ProofSkipped, RequestedAt: now, CompletedAt: now}
		if err := insertOperation(txctx, tx, runner.dialect(), op, req.RequestKeyHash, req.RequestHash, req.ExpectedRevision, ""); err != nil {
			return err
		}
		binding = domain.Binding{PrincipalID: req.PrincipalID, Interface: req.Interface.Normalized(), Revision: current.Revision + 1, Status: domain.BindingUnbound, OperationID: op.OperationID, UpdatedAt: now}
		return upsertBinding(txctx, tx, runner.dialect(), binding)
	})
	return op, binding, err
}

func (s *PostgresOwner) BindOperatorChannelFromProof(ctx context.Context, req domain.BootBindRequest) (domain.Binding, error) {
	return bindFromProof(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) BindOperatorChannelFromProof(ctx context.Context, req domain.BootBindRequest) (domain.Binding, error) {
	return bindFromProof(ctx, sqliteRunner{s}, req)
}

func bindFromProof(ctx context.Context, runner transactionRunner, req domain.BootBindRequest) (domain.Binding, error) {
	if err := runner.require(); err != nil {
		return domain.Binding{}, err
	}
	if req.PrincipalID == "" || req.RequestedAt.IsZero() || req.Proof.Validate() != nil || req.Interface.Key() != req.Proof.Interface.Key() {
		return domain.Binding{}, fmt.Errorf("%w: exact active proof and interface are required", domain.ErrInvalidRequest)
	}
	var binding domain.Binding
	err := runner.mutate(ctx, "bind operator channel from proof", func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := loadBinding(txctx, tx, runner.dialect(), req.Interface.Key(), true)
		if err != nil {
			return err
		}
		if found {
			binding = current
			if current.Status == domain.BindingUnbound {
				return fmt.Errorf("%w: explicit unbind fence blocks proof reuse", domain.ErrConflict)
			}
			if current.Status != domain.BindingCurrent || current.ProviderCredential != req.Proof.ProviderCredential ||
				current.ExternalAccountRef != req.Proof.ExternalAccountRef || current.ConversationRef != req.Proof.ConversationRef ||
				current.ConversationScope != req.Proof.ConversationScope || current.ProofID != req.Proof.ProofID || current.ProofRevision != req.Proof.Revision {
				return fmt.Errorf("%w: current binding contradicts the verified proof", domain.ErrConflict)
			}
			return nil
		}
		now := canonicalTime(req.RequestedAt)
		opID := uuid.NewString()
		op := domain.Operation{
			OperationID: opID, Kind: domain.OperationReconnect, PrincipalID: req.PrincipalID, Interface: req.Interface.Normalized(),
			Challenge: req.Proof.Challenge, State: domain.StateBound, Revision: 1, BindingRevision: 1,
			ExternalAccountRef: req.Proof.ExternalAccountRef, ConversationRef: req.Proof.ConversationRef,
			ConversationScope: req.Proof.ConversationScope, AccountPresentation: req.Proof.AccountPresentation,
			SaveProof: true, ProofID: req.Proof.ProofID, ProofRevision: req.Proof.Revision, ProofStatus: domain.ProofActive,
			PlannedProofID: req.Proof.ProofID, PlannedProofRevision: req.Proof.Revision,
			RequestedAt: now, CompletedAt: now, ProviderCredential: req.Proof.ProviderCredential,
		}
		requestKey := domain.Hash("boot-proof", req.PrincipalID, req.Interface.Key(), req.Proof.ProofID, strconv.FormatInt(req.Proof.Revision, 10))
		if err := insertOperation(txctx, tx, runner.dialect(), op, requestKey, requestKey, 0, "boot-proof"); err != nil {
			return err
		}
		binding = domain.Binding{PrincipalID: req.PrincipalID, Interface: req.Interface.Normalized(), ExternalAccountRef: req.Proof.ExternalAccountRef, ConversationRef: req.Proof.ConversationRef, ConversationScope: req.Proof.ConversationScope, AccountPresentation: req.Proof.AccountPresentation, Revision: 1, Status: domain.BindingCurrent, Source: domain.BindingSourceLocalProof, ProofID: req.Proof.ProofID, ProofRevision: req.Proof.Revision, OperationID: opID, UpdatedAt: now, ProviderCredential: req.Proof.ProviderCredential}
		return upsertBinding(txctx, tx, runner.dialect(), binding)
	})
	return binding, err
}

func (s *PostgresOwner) ListOperatorChannelOperations(ctx context.Context, principalID string) ([]domain.Operation, error) {
	return listOperations(ctx, postgresRunner{s}, principalID)
}
func (s *SQLiteOwner) ListOperatorChannelOperations(ctx context.Context, principalID string) ([]domain.Operation, error) {
	return listOperations(ctx, sqliteRunner{s}, principalID)
}

func listOperations(ctx context.Context, runner transactionRunner, principalID string) ([]domain.Operation, error) {
	if err := runner.require(); err != nil {
		return nil, err
	}
	rows, err := runner.query().QueryContext(ctx, runner.dialect().bind(operationSelect+` WHERE principal_id = ? ORDER BY requested_at, operation_id`), strings.TrimSpace(principalID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Operation{}
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (s *PostgresOwner) ListOperatorChannelBindings(ctx context.Context, principalID string) ([]domain.Binding, error) {
	return listBindings(ctx, postgresRunner{s}, principalID)
}
func (s *SQLiteOwner) ListOperatorChannelBindings(ctx context.Context, principalID string) ([]domain.Binding, error) {
	return listBindings(ctx, sqliteRunner{s}, principalID)
}

func listBindings(ctx context.Context, runner transactionRunner, principalID string) ([]domain.Binding, error) {
	if err := runner.require(); err != nil {
		return nil, err
	}
	rows, err := runner.query().QueryContext(ctx, runner.dialect().bind(bindingSelect+` WHERE principal_id = ? ORDER BY interface_ref, channel_pack_id`), strings.TrimSpace(principalID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Binding{}
	for rows.Next() {
		binding, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, binding)
	}
	return out, rows.Err()
}

func (s *PostgresOwner) ListPendingProofResponsibilities(ctx context.Context) ([]domain.ProofResponsibility, error) {
	return listProofResponsibilities(ctx, postgresRunner{s})
}
func (s *SQLiteOwner) ListPendingProofResponsibilities(ctx context.Context) ([]domain.ProofResponsibility, error) {
	return listProofResponsibilities(ctx, sqliteRunner{s})
}

func listProofResponsibilities(ctx context.Context, runner transactionRunner) ([]domain.ProofResponsibility, error) {
	if err := runner.require(); err != nil {
		return nil, err
	}
	rows, err := runner.query().QueryContext(ctx, runner.dialect().bind(operationSelect+` WHERE state = 'bound' AND proof_status IN ('pending','failed') ORDER BY requested_at, operation_id`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.ProofResponsibility{}
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		binding, found, err := loadBinding(ctx, runner.query(), runner.dialect(), op.Interface.Key(), false)
		if err != nil || !found {
			if !found && err == nil {
				err = fmt.Errorf("proof responsibility %s has no binding", op.OperationID)
			}
			return nil, err
		}
		proof := proofFrom(op, binding)
		out = append(out, domain.ProofResponsibility{Operation: op, Binding: binding, Proof: proof})
	}
	return out, rows.Err()
}

func proofFrom(op domain.Operation, binding domain.Binding) domain.VerifiedProof {
	return domain.VerifiedProof{
		Format: domain.ProofFormat, ProofID: op.ProofID, Revision: op.ProofRevision, Status: domain.ProofActive,
		Interface: op.Interface, ExternalAccountRef: op.ExternalAccountRef, ConversationRef: op.ConversationRef,
		ConversationScope: op.ConversationScope, AccountPresentation: op.AccountPresentation,
		Method: string(op.Kind), Challenge: op.Challenge, OriginalOperationID: op.OperationID,
		MintingStoreID: op.PrincipalID, MintingDeploymentID: op.PrincipalID,
		VerifiedAt: op.CompletedAt, OperatorConfirmed: true, ConsentScopes: []domain.ConsentScope{domain.ConsentNotify, domain.ConsentDecide},
		ProviderCredential: op.ProviderCredential,
	}
}

func (s *PostgresOwner) CompleteProofResponsibility(ctx context.Context, operationID, proofID string, proofRevision int64, status domain.ProofStatus, failure string, now time.Time) error {
	return completeProof(ctx, postgresRunner{s}, operationID, proofID, proofRevision, status, failure, now)
}
func (s *SQLiteOwner) CompleteProofResponsibility(ctx context.Context, operationID, proofID string, proofRevision int64, status domain.ProofStatus, failure string, now time.Time) error {
	return completeProof(ctx, sqliteRunner{s}, operationID, proofID, proofRevision, status, failure, now)
}

func completeProof(ctx context.Context, runner transactionRunner, operationID, proofID string, proofRevision int64, status domain.ProofStatus, failure string, now time.Time) error {
	if err := runner.require(); err != nil {
		return err
	}
	if status != domain.ProofActive && status != domain.ProofFailed && status != domain.ProofRevoked {
		return domain.ErrInvalidRequest
	}
	return runner.mutate(ctx, "complete operator channel proof", func(txctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(txctx, runner.dialect().bind(`UPDATE operator_channel_operations SET proof_status = ?, proof_failure = ?, updated_at = ? WHERE operation_id = ? AND proof_id = ? AND proof_revision = ? AND state = 'bound' AND proof_status IN ('pending','failed','active')`), string(status), strings.TrimSpace(failure), canonicalTime(now), strings.TrimSpace(operationID), strings.TrimSpace(proofID), proofRevision)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return domain.ErrRevisionConflict
		}
		return nil
	})
}

func unresolvedProofResponsibilityIDs(ctx context.Context, db queryer, d dialect, interfaceKey string, forUpdate bool) ([]string, error) {
	query := `SELECT operation_id FROM operator_channel_operations WHERE interface_key = ? AND state = 'bound' AND proof_status IN ('pending','failed') ORDER BY operation_id`
	if forUpdate && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	rows, err := db.QueryContext(ctx, d.bind(query), interfaceKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func dischargeProofResponsibilities(ctx context.Context, tx *sql.Tx, d dialect, interfaceKey string, now time.Time) error {
	_, err := tx.ExecContext(ctx, d.bind(`UPDATE operator_channel_operations SET proof_status = 'revoked', proof_failure = 'operator channel unbound before proof responsibility completed', updated_at = ? WHERE interface_key = ? AND state = 'bound' AND proof_status IN ('pending','failed')`), now, interfaceKey)
	return err
}

// SettleInboundClaimTx is called only inside the selected-store inbound
// publication transaction. It never owns or exports a transaction beyond this
// closed operation.
func (s *PostgresOwner) SettleInboundClaimTx(ctx context.Context, tx *sql.Tx, claim domain.InboundClaim, now time.Time) (domain.ClaimSettlement, error) {
	return settleInboundClaimTx(ctx, tx, dialectPostgres, claim, now)
}
func (s *SQLiteOwner) SettleInboundClaimTx(ctx context.Context, tx *sql.Tx, claim domain.InboundClaim, now time.Time) (domain.ClaimSettlement, error) {
	return settleInboundClaimTx(ctx, tx, dialectSQLite, claim, now)
}

func settleInboundClaimTx(ctx context.Context, tx *sql.Tx, d dialect, claim domain.InboundClaim, now time.Time) (domain.ClaimSettlement, error) {
	if tx == nil {
		return domain.ClaimSettlement{}, fmt.Errorf("operator channel claim transaction is required")
	}
	if err := claim.Validate(); err != nil {
		return domain.ClaimSettlement{}, err
	}
	now = canonicalTime(now)
	op, found, err := loadOperationByChallenge(ctx, tx, d, claim.Challenge, true)
	if err != nil {
		return domain.ClaimSettlement{}, err
	}
	if !found {
		if err := insertClaimReceipt(ctx, tx, d, claim, domain.Operation{}, domain.DispositionRejectedClaim, "unknown challenge", now); err != nil {
			return domain.ClaimSettlement{}, err
		}
		return domain.ClaimSettlement{Consumed: true, Disposition: domain.DispositionRejectedClaim}, nil
	}
	if op.Interface.Key() != claim.Interface.Key() || op.Interface.SemanticGeneration != claim.Interface.SemanticGeneration {
		if err := insertClaimReceipt(ctx, tx, d, claim, op, domain.DispositionRejectedClaim, "wrong interface or semantic generation", now); err != nil {
			return domain.ClaimSettlement{}, err
		}
		return domain.ClaimSettlement{Consumed: true, Disposition: domain.DispositionRejectedClaim, Operation: op}, nil
	}
	if op.State == domain.StateAwaitingConfirmation {
		same := op.ExternalAccountRef == claim.ExternalAccountRef && op.ConversationRef == claim.ConversationRef && op.ConversationScope == claim.ConversationScope
		disposition, reason := domain.DispositionConsumedBinding, "duplicate claim"
		if !same {
			disposition, reason = domain.DispositionRejectedClaim, "challenge already claimed by a different account"
		}
		if err := insertClaimReceipt(ctx, tx, d, claim, op, disposition, reason, now); err != nil {
			return domain.ClaimSettlement{}, err
		}
		return domain.ClaimSettlement{Consumed: true, Disposition: disposition, Operation: op}, nil
	}
	if op.State != domain.StateAwaitingClaim || !op.ExpiresAt.After(now) {
		if op.State == domain.StateAwaitingClaim {
			op.State, op.Revision, op.CompletedAt = domain.StateExpired, op.Revision+1, now
			if err := updateOperationTerminal(ctx, tx, d, op); err != nil {
				return domain.ClaimSettlement{}, err
			}
		}
		if err := insertClaimReceipt(ctx, tx, d, claim, op, domain.DispositionRejectedClaim, "challenge expired or terminal", now); err != nil {
			return domain.ClaimSettlement{}, err
		}
		return domain.ClaimSettlement{Consumed: true, Disposition: domain.DispositionRejectedClaim, Operation: op}, nil
	}
	op.State, op.Revision, op.ClaimedAt = domain.StateAwaitingConfirmation, op.Revision+1, now
	op.ExternalAccountRef, op.ConversationRef, op.ConversationScope = claim.ExternalAccountRef, claim.ConversationRef, claim.ConversationScope
	op.AccountPresentation, op.ClaimDisposition = claim.AccountPresentation, domain.DispositionConsumedBinding
	result, err := tx.ExecContext(ctx, d.bind(`UPDATE operator_channel_operations SET state = 'awaiting_confirmation', operation_revision = ?, external_account_reference = ?, conversation_reference = ?, conversation_scope = ?, account_presentation = ?, claim_provider = ?, claim_provider_event_id = ?, claim_publication_id = ?, claim_authorization = ?, claim_disposition = ?, claimed_at = ?, updated_at = ? WHERE operation_id = ? AND state = 'awaiting_claim' AND operation_revision = ?`),
		op.Revision, op.ExternalAccountRef, op.ConversationRef, string(op.ConversationScope), nullable(op.AccountPresentation), claim.Provider, claim.ProviderEventID, claim.PublicationID, claim.ProviderAuthorization, domain.DispositionConsumedBinding, now, now, op.OperationID, op.Revision-1)
	if err != nil {
		return domain.ClaimSettlement{}, err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		if err == nil {
			err = domain.ErrRevisionConflict
		}
		return domain.ClaimSettlement{}, err
	}
	if err := insertClaimReceipt(ctx, tx, d, claim, op, domain.DispositionConsumedBinding, "claim awaiting operator confirmation", now); err != nil {
		return domain.ClaimSettlement{}, err
	}
	return domain.ClaimSettlement{Consumed: true, Disposition: domain.DispositionConsumedBinding, Operation: op}, nil
}

func requirePrincipal(ctx context.Context, db queryer, d dialect, principalID string) error {
	var got string
	if err := db.QueryRowContext(ctx, d.bind(`SELECT principal_id FROM operator_principals WHERE singleton_id = 1`)).Scan(&got); err != nil {
		return err
	}
	if got != strings.TrimSpace(principalID) {
		return fmt.Errorf("%w: authenticated principal does not match selected store", domain.ErrConflict)
	}
	return nil
}

const operationSelect = `SELECT operation_id, operation_kind, principal_id, interface_ref, channel_pack_id, channel_pack_version, channel_manifest_hash, semantic_generation, provider_credential_key, provider_credential_value_seal, request_hash, expected_binding_revision, challenge, state, operation_revision, binding_revision, external_account_reference, conversation_reference, conversation_scope, account_presentation, claim_disposition, save_proof, planned_proof_id, planned_proof_revision, proof_id, proof_revision, proof_status, requested_at, expires_at, claimed_at, completed_at FROM operator_channel_operations`

func loadOperationByRequestKey(ctx context.Context, db queryer, d dialect, key string, forUpdate bool) (domain.Operation, bool, error) {
	query := operationSelect + ` WHERE request_key_hash = ?`
	if forUpdate && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanOperationFound(db.QueryRowContext(ctx, d.bind(query), strings.TrimSpace(key)))
}

func loadOperationByID(ctx context.Context, db queryer, d dialect, id string, forUpdate bool) (domain.Operation, bool, error) {
	query := operationSelect + ` WHERE operation_id = ?`
	if forUpdate && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanOperationFound(db.QueryRowContext(ctx, d.bind(query), strings.TrimSpace(id)))
}

func loadOperationByChallenge(ctx context.Context, db queryer, d dialect, challenge string, forUpdate bool) (domain.Operation, bool, error) {
	query := operationSelect + ` WHERE challenge = ?`
	if forUpdate && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanOperationFound(db.QueryRowContext(ctx, d.bind(query), strings.TrimSpace(challenge)))
}

type scanner interface{ Scan(...any) error }

func scanOperationFound(row scanner) (domain.Operation, bool, error) {
	op, err := scanOperation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Operation{}, false, nil
	}
	return op, err == nil, err
}

func scanOperation(row scanner) (domain.Operation, error) {
	var op domain.Operation
	var kind, state, proofStatus string
	var providerCredentialKey, providerCredentialSeal, challenge, account, conversation, scope, presentation, disposition, plannedProofID, proofID sql.NullString
	var bindingRevision, plannedProofRevision, proofRevision sql.NullInt64
	var requested, expires, claimed, completed any
	if err := row.Scan(&op.OperationID, &kind, &op.PrincipalID, &op.Interface.InterfaceRef, &op.Interface.ChannelPackID, &op.Interface.ChannelPackVersion, &op.Interface.ChannelManifestHash, &op.Interface.SemanticGeneration, &providerCredentialKey, &providerCredentialSeal, &op.RequestHash, &op.ExpectedBindingRevision, &challenge, &state, &op.Revision, &bindingRevision, &account, &conversation, &scope, &presentation, &disposition, &op.SaveProof, &plannedProofID, &plannedProofRevision, &proofID, &proofRevision, &proofStatus, &requested, &expires, &claimed, &completed); err != nil {
		return domain.Operation{}, err
	}
	op.Kind, op.State, op.ConversationScope, op.ProofStatus = domain.OperationKind(kind), domain.OperationState(state), domain.ConversationScope(scope.String), domain.ProofStatus(proofStatus)
	op.Challenge, op.ExternalAccountRef, op.ConversationRef, op.AccountPresentation, op.ClaimDisposition, op.ProofID = challenge.String, account.String, conversation.String, presentation.String, disposition.String, proofID.String
	op.BindingRevision, op.ProofRevision = bindingRevision.Int64, proofRevision.Int64
	op.PlannedProofID, op.PlannedProofRevision = plannedProofID.String, plannedProofRevision.Int64
	if providerCredentialKey.Valid || providerCredentialSeal.Valid {
		seal, sealErr := runtimecredentials.ParseValueSeal(providerCredentialSeal.String)
		if sealErr != nil || strings.TrimSpace(providerCredentialKey.String) == "" {
			return domain.Operation{}, fmt.Errorf("stored operator channel operation has invalid provider credential evidence")
		}
		op.ProviderCredential = runtimecredentials.ValueEvidence{Key: providerCredentialKey.String, Seal: seal}
	}
	var err error
	if op.RequestedAt, err = timeValue(requested); err != nil {
		return domain.Operation{}, err
	}
	if op.ExpiresAt, err = nullableTimeValue(expires); err != nil {
		return domain.Operation{}, err
	}
	if op.ClaimedAt, err = nullableTimeValue(claimed); err != nil {
		return domain.Operation{}, err
	}
	if op.CompletedAt, err = nullableTimeValue(completed); err != nil {
		return domain.Operation{}, err
	}
	op.Interface = op.Interface.Normalized()
	return op, nil
}

func insertOperation(ctx context.Context, tx *sql.Tx, d dialect, op domain.Operation, requestKey, requestHash string, expectedRevision int64, claimProvider string) error {
	_, err := tx.ExecContext(ctx, d.bind(`INSERT INTO operator_channel_operations (operation_id, operation_kind, principal_id, interface_key, interface_ref, channel_pack_id, channel_pack_version, channel_manifest_hash, semantic_generation, provider_credential_key, provider_credential_value_seal, request_key_hash, request_hash, expected_binding_revision, challenge, state, operation_revision, binding_revision, external_account_reference, conversation_reference, conversation_scope, account_presentation, claim_provider, claim_disposition, save_proof, planned_proof_id, planned_proof_revision, proof_id, proof_revision, proof_status, requested_at, expires_at, claimed_at, completed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
		op.OperationID, string(op.Kind), op.PrincipalID, op.Interface.Key(), op.Interface.InterfaceRef, op.Interface.ChannelPackID, op.Interface.ChannelPackVersion, op.Interface.ChannelManifestHash, op.Interface.SemanticGeneration, nullable(op.ProviderCredential.Key), nullable(op.ProviderCredential.Seal.String()), requestKey, requestHash, expectedRevision,
		nullable(op.Challenge), string(op.State), op.Revision, nullableInt(op.BindingRevision), nullable(op.ExternalAccountRef), nullable(op.ConversationRef), nullable(string(op.ConversationScope)), nullable(op.AccountPresentation), nullable(claimProvider), nullable(op.ClaimDisposition), op.SaveProof, nullable(op.PlannedProofID), nullableInt(op.PlannedProofRevision), nullable(op.ProofID), nullableInt(op.ProofRevision), string(op.ProofStatus), op.RequestedAt, nullableTime(op.ExpiresAt), nullableTime(op.ClaimedAt), nullableTime(op.CompletedAt), op.RequestedAt)
	return err
}

func updateOperationTerminal(ctx context.Context, tx *sql.Tx, d dialect, op domain.Operation) error {
	_, err := tx.ExecContext(ctx, d.bind(`UPDATE operator_channel_operations SET state = ?, operation_revision = ?, completed_at = ?, updated_at = ? WHERE operation_id = ?`), string(op.State), op.Revision, op.CompletedAt, op.CompletedAt, op.OperationID)
	return err
}

func updateOperationBound(ctx context.Context, tx *sql.Tx, d dialect, op domain.Operation) error {
	_, err := tx.ExecContext(ctx, d.bind(`UPDATE operator_channel_operations SET state = 'bound', operation_revision = ?, binding_revision = ?, proof_id = ?, proof_revision = ?, proof_status = ?, completed_at = ?, updated_at = ? WHERE operation_id = ?`), op.Revision, op.BindingRevision, nullable(op.ProofID), nullableInt(op.ProofRevision), string(op.ProofStatus), op.CompletedAt, op.CompletedAt, op.OperationID)
	return err
}

const bindingSelect = `SELECT principal_id, interface_ref, channel_pack_id, channel_pack_version, channel_manifest_hash, semantic_generation, provider_credential_key, provider_credential_value_seal, external_account_reference, conversation_reference, conversation_scope, account_presentation, binding_revision, status, source, proof_id, proof_revision, operation_id, updated_at FROM operator_channel_bindings`

func loadBinding(ctx context.Context, db queryer, d dialect, key string, forUpdate bool) (domain.Binding, bool, error) {
	query := bindingSelect + ` WHERE interface_key = ?`
	if forUpdate && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	binding, err := scanBinding(db.QueryRowContext(ctx, d.bind(query), key))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Binding{}, false, nil
	}
	return binding, err == nil, err
}

func scanBinding(row scanner) (domain.Binding, error) {
	var binding domain.Binding
	var providerCredentialKey, providerCredentialSeal, account, conversation, scope, presentation, source, proofID sql.NullString
	var proofRevision sql.NullInt64
	var status string
	var updated any
	err := row.Scan(&binding.PrincipalID, &binding.Interface.InterfaceRef, &binding.Interface.ChannelPackID, &binding.Interface.ChannelPackVersion, &binding.Interface.ChannelManifestHash, &binding.Interface.SemanticGeneration, &providerCredentialKey, &providerCredentialSeal, &account, &conversation, &scope, &presentation, &binding.Revision, &status, &source, &proofID, &proofRevision, &binding.OperationID, &updated)
	if err != nil {
		return domain.Binding{}, err
	}
	binding.ExternalAccountRef, binding.ConversationRef, binding.AccountPresentation, binding.ProofID = account.String, conversation.String, presentation.String, proofID.String
	binding.ConversationScope, binding.Status, binding.Source = domain.ConversationScope(scope.String), domain.BindingStatus(status), domain.BindingSource(source.String)
	binding.ProofRevision = proofRevision.Int64
	if providerCredentialKey.Valid || providerCredentialSeal.Valid {
		seal, sealErr := runtimecredentials.ParseValueSeal(providerCredentialSeal.String)
		if sealErr != nil || strings.TrimSpace(providerCredentialKey.String) == "" {
			return domain.Binding{}, fmt.Errorf("stored operator channel binding has invalid provider credential evidence")
		}
		binding.ProviderCredential = runtimecredentials.ValueEvidence{Key: providerCredentialKey.String, Seal: seal}
	}
	binding.UpdatedAt, err = timeValue(updated)
	binding.Interface = binding.Interface.Normalized()
	return binding, err
}

func upsertBinding(ctx context.Context, tx *sql.Tx, d dialect, binding domain.Binding) error {
	query := `INSERT INTO operator_channel_bindings (interface_key, principal_id, interface_ref, channel_pack_id, channel_pack_version, channel_manifest_hash, semantic_generation, provider_credential_key, provider_credential_value_seal, external_account_reference, conversation_reference, conversation_scope, account_presentation, binding_revision, status, source, proof_id, proof_revision, operation_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (interface_key) DO UPDATE SET principal_id = excluded.principal_id, interface_ref = excluded.interface_ref, channel_pack_id = excluded.channel_pack_id, channel_pack_version = excluded.channel_pack_version, channel_manifest_hash = excluded.channel_manifest_hash, semantic_generation = excluded.semantic_generation, provider_credential_key = excluded.provider_credential_key, provider_credential_value_seal = excluded.provider_credential_value_seal, external_account_reference = excluded.external_account_reference, conversation_reference = excluded.conversation_reference, conversation_scope = excluded.conversation_scope, account_presentation = excluded.account_presentation, binding_revision = excluded.binding_revision, status = excluded.status, source = excluded.source, proof_id = excluded.proof_id, proof_revision = excluded.proof_revision, operation_id = excluded.operation_id, updated_at = excluded.updated_at`
	_, err := tx.ExecContext(ctx, d.bind(query), binding.Interface.Key(), binding.PrincipalID, binding.Interface.InterfaceRef, binding.Interface.ChannelPackID, binding.Interface.ChannelPackVersion, binding.Interface.ChannelManifestHash, binding.Interface.SemanticGeneration, nullable(binding.ProviderCredential.Key), nullable(binding.ProviderCredential.Seal.String()), nullable(binding.ExternalAccountRef), nullable(binding.ConversationRef), nullable(string(binding.ConversationScope)), nullable(binding.AccountPresentation), binding.Revision, string(binding.Status), nullable(string(binding.Source)), nullable(binding.ProofID), nullableInt(binding.ProofRevision), binding.OperationID, binding.UpdatedAt)
	return err
}

func insertClaimReceipt(ctx context.Context, tx *sql.Tx, d dialect, claim domain.InboundClaim, op domain.Operation, disposition, reason string, now time.Time) error {
	_, err := tx.ExecContext(ctx, d.bind(`INSERT INTO operator_channel_claim_receipts (publication_id, provider, provider_event_id, interface_key, challenge, operation_id, disposition, reason, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (publication_id) DO NOTHING`), claim.PublicationID, claim.Provider, claim.ProviderEventID, claim.Interface.Key(), claim.Challenge, nullable(op.OperationID), disposition, reason, now)
	return err
}

func nullable(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
func nullableInt(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}
func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
func canonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC().Truncate(time.Microsecond)
}

func timeValue(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("stored time is required")
	}
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999"} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed.UTC(), nil
			}
		}
	case []byte:
		return timeValue(string(typed))
	}
	return time.Time{}, fmt.Errorf("decode stored time %T", value)
}
func nullableTimeValue(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	return timeValue(value)
}
