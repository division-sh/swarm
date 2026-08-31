package channelonboarding

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
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
		return nil, fmt.Errorf("postgres channel onboarding owner requires backend and schema owner")
	}
	return &PostgresOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent schemaRequirement) (*SQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, fmt.Errorf("sqlite channel onboarding owner requires backend and schema owner")
	}
	return &SQLiteOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

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
		} else {
			out.WriteRune(ch)
		}
	}
	return out.String()
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type runner interface {
	require() error
	mutate(context.Context, string, func(context.Context, *sql.Tx) error) error
	query() queryer
	dialect() dialect
}

type postgresRunner struct{ owner *PostgresOwner }

func (r postgresRunner) require() error   { return r.owner.requireCurrent() }
func (r postgresRunner) query() queryer   { return r.owner.backend }
func (r postgresRunner) dialect() dialect { return dialectPostgres }
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

type sqliteRunner struct{ owner *SQLiteOwner }

func (r sqliteRunner) require() error   { return r.owner.requireCurrent() }
func (r sqliteRunner) query() queryer   { return r.owner.backend }
func (r sqliteRunner) dialect() dialect { return dialectSQLite }
func (r sqliteRunner) mutate(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	return r.owner.backend.RunTransaction(ctx, label, fn)
}

func rollback(tx *sql.Tx) error {
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func (s *PostgresOwner) ReserveChannelOnboarding(ctx context.Context, req domain.StartRequest) (domain.Operation, error) {
	return reserve(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) ReserveChannelOnboarding(ctx context.Context, req domain.StartRequest) (domain.Operation, error) {
	return reserve(ctx, sqliteRunner{s}, req)
}

func reserve(ctx context.Context, r runner, req domain.StartRequest) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	if err := req.Validate(); err != nil {
		return domain.Operation{}, err
	}
	if _, err := uuid.Parse(req.OperationID); err != nil {
		return domain.Operation{}, fmt.Errorf("%w: operation_id must be a UUID", domain.ErrInvalidRequest)
	}
	req.RequestedAt = canonicalTime(req.RequestedAt)
	var out domain.Operation
	err := r.mutate(ctx, "reserve channel onboarding", func(txctx context.Context, tx *sql.Tx) error {
		existing, found, err := loadOperationByRequestKey(txctx, tx, r.dialect(), req.RequestKeyHash, true)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestHash != req.RequestHash {
				return domain.ErrConflict
			}
			out = existing
			return nil
		}
		var active string
		err = tx.QueryRowContext(txctx, r.dialect().bind(`SELECT operation_id FROM channel_onboarding_operations WHERE slot_key=? AND phase NOT IN ('succeeded','failed','retired') LIMIT 1`), req.SlotKey()).Scan(&active)
		if err == nil {
			return domain.ErrConflict
		}
		if err != sql.ErrNoRows {
			return err
		}
		i := req.Interface.Normalized()
		c := req.Coordinate.Normalized()
		reservations, err := json.Marshal(req.CredentialReservations)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(txctx, r.dialect().bind(`INSERT INTO channel_onboarding_operations (
			operation_id,request_key_hash,request_hash,slot_key,principal_id,verb,provider,
			interface_key,interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
			bundle_hash,bundle_source,bundle_identity,pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,
			target_selector,target_generation,activation_posture,identity_ceremony,phase,operation_revision,save_proof,
			credential_reservations,credential_admissions,requested_at,updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,'[]',?,?)`),
			req.OperationID, req.RequestKeyHash, req.RequestHash, req.SlotKey(), req.PrincipalID, string(req.Verb), req.Provider,
			i.Key(), i.InterfaceRef, i.ChannelPackID, i.ChannelPackVersion, i.ChannelManifestHash, i.SemanticGeneration,
			c.BundleHash, c.BundleSource, c.BundleIdentity, c.PackInventoryGeneration, c.RuntimeInstanceID, c.ContextPublicationGeneration, c.PlanGeneration.Diagnostic(),
			req.TargetSelector, c.TargetGeneration, string(req.Posture), string(req.Ceremony), string(domain.PhasePreparing), req.SaveProof, string(reservations),
			req.RequestedAt, req.RequestedAt)
		if err != nil {
			return fmt.Errorf("insert channel onboarding operation: %w", err)
		}
		out, _, err = loadOperation(txctx, tx, r.dialect(), req.OperationID, false)
		return err
	})
	return out, err
}

func (s *PostgresOwner) GetChannelOnboarding(ctx context.Context, operationID string) (domain.Operation, error) {
	return getOperation(ctx, postgresRunner{s}, operationID)
}
func (s *SQLiteOwner) GetChannelOnboarding(ctx context.Context, operationID string) (domain.Operation, error) {
	return getOperation(ctx, sqliteRunner{s}, operationID)
}

func (s *PostgresOwner) ListChannelOnboardingOperations(ctx context.Context) ([]domain.Operation, error) {
	return listOperations(ctx, postgresRunner{s})
}
func (s *SQLiteOwner) ListChannelOnboardingOperations(ctx context.Context) ([]domain.Operation, error) {
	return listOperations(ctx, sqliteRunner{s})
}

func listOperations(ctx context.Context, r runner) ([]domain.Operation, error) {
	if err := r.require(); err != nil {
		return nil, err
	}
	rows, err := r.query().QueryContext(ctx, operationSelect+` ORDER BY requested_at,operation_id`)
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

func getOperation(ctx context.Context, r runner, operationID string) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	op, found, err := loadOperation(ctx, r.query(), r.dialect(), operationID, false)
	if err != nil {
		return domain.Operation{}, err
	}
	if !found {
		return domain.Operation{}, domain.ErrNotFound
	}
	return op, nil
}

func (s *PostgresOwner) AdvanceChannelOnboarding(ctx context.Context, req domain.AdvanceRequest) (domain.Operation, error) {
	return advance(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) AdvanceChannelOnboarding(ctx context.Context, req domain.AdvanceRequest) (domain.Operation, error) {
	return advance(ctx, sqliteRunner{s}, req)
}

func advance(ctx context.Context, r runner, req domain.AdvanceRequest) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	if strings.TrimSpace(req.OperationID) == "" || req.ExpectedRevision < 1 || !req.Phase.Valid() || req.Now.IsZero() {
		return domain.Operation{}, domain.ErrInvalidRequest
	}
	for _, admission := range req.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			return domain.Operation{}, err
		}
	}
	if req.RebindCoordinate != nil {
		coordinate := req.RebindCoordinate.Normalized()
		if err := coordinate.Validate(); err != nil {
			return domain.Operation{}, err
		}
		req.RebindCoordinate = &coordinate
	}
	var out domain.Operation
	err := r.mutate(ctx, "advance channel onboarding", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(txctx, tx, r.dialect(), req.OperationID, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		coordinateOnlyTerminalRebind := req.RebindCoordinate != nil && op.Phase == domain.PhaseSucceeded && req.Phase == op.Phase
		if !validTransition(op.Phase, req.Phase) && !coordinateOnlyTerminalRebind {
			return domain.ErrConflict
		}
		var reboundActivation *domain.ConnectedChannelActivation
		if req.RebindCoordinate != nil && !op.Coordinate.Matches(*req.RebindCoordinate) {
			if !op.Coordinate.MatchesDurableIdentity(*req.RebindCoordinate) {
				return domain.ErrConflict
			}
			activation, found, err := loadActivationBySlot(txctx, tx, r.dialect(), op.SlotKey, true)
			if err != nil {
				return err
			}
			if found && activation.OperationID == op.OperationID {
				if !activation.Coordinate.Matches(op.Coordinate) || activation.Revision != op.ActivationRevision {
					return domain.ErrRevisionConflict
				}
				activation.Coordinate = *req.RebindCoordinate
				activation.Revision++
				activation.UpdatedAt = canonicalTime(req.Now)
				reboundActivation = &activation
				op.ActivationRevision = activation.Revision
			}
			op.Coordinate = *req.RebindCoordinate
			if req.ClearConfirmationOperationID {
				op.ConfirmationOperationID = ""
			}
		} else if req.ClearConfirmationOperationID {
			return domain.ErrInvalidRequest
		}
		if req.ReplaceCredentialAdmissions {
			op.CredentialAdmissions = append([]domain.CredentialAdmission(nil), req.CredentialAdmissions...)
		}
		if strings.TrimSpace(req.IdentityOperationID) != "" {
			op.IdentityOperationID = strings.TrimSpace(req.IdentityOperationID)
		}
		if req.BindingRevision > 0 {
			op.BindingRevision = req.BindingRevision
		}
		if strings.TrimSpace(req.ConfirmationOperationID) != "" {
			op.ConfirmationOperationID = strings.TrimSpace(req.ConfirmationOperationID)
		}
		wasTerminal := op.Phase.Terminal()
		op.Phase, op.Revision, op.UpdatedAt = req.Phase, op.Revision+1, canonicalTime(req.Now)
		if op.Phase.Terminal() && !wasTerminal {
			op.CompletedAt = op.UpdatedAt
		}
		op.FailureCode, op.FailureMessage = strings.TrimSpace(req.FailureCode), strings.TrimSpace(req.FailureMessage)
		if err := updateOperation(txctx, tx, r.dialect(), op); err != nil {
			return err
		}
		if reboundActivation != nil {
			c := reboundActivation.Coordinate.Normalized()
			result, err := tx.ExecContext(txctx, r.dialect().bind(`UPDATE connected_channel_activations SET
				operation_revision=?,bundle_hash=?,bundle_source=?,bundle_identity=?,pack_inventory_generation=?,
				runtime_instance_id=?,context_publication_generation=?,plan_generation=?,target_generation=?,activation_revision=?,updated_at=?
				WHERE activation_id=? AND status='current' AND activation_revision=?`),
				op.Revision, c.BundleHash, c.BundleSource, c.BundleIdentity, c.PackInventoryGeneration,
				c.RuntimeInstanceID, c.ContextPublicationGeneration, c.PlanGeneration.Diagnostic(), c.TargetGeneration, reboundActivation.Revision, reboundActivation.UpdatedAt,
				reboundActivation.ActivationID, reboundActivation.Revision-1)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return domain.ErrRevisionConflict
			}
		}
		out = op
		return nil
	})
	return out, err
}

func validTransition(from, to domain.Phase) bool {
	if from.Terminal() {
		return false
	}
	if from == to {
		return true
	}
	if to == domain.PhaseFailed || to == domain.PhaseRetired {
		return true
	}
	allowed := map[domain.Phase]domain.Phase{
		domain.PhasePreparing:                    domain.PhaseCredentialsAdmitted,
		domain.PhaseCredentialsAdmitted:          domain.PhaseActivatingProvider,
		domain.PhaseActivatingProvider:           domain.PhaseAwaitingExternalIdentity,
		domain.PhaseAwaitingExternalIdentity:     domain.PhaseAwaitingOperatorConfirmation,
		domain.PhaseAwaitingOperatorConfirmation: domain.PhasePublishingActivation,
		domain.PhasePublishingProcessActivation:  domain.PhasePromotingRegistration,
		domain.PhasePromotingRegistration:        domain.PhaseRetiringPredecessor,
		domain.PhaseRetiringPredecessor:          domain.PhaseDeliveringConfirmation,
		domain.PhaseDeliveringConfirmation:       domain.PhaseSucceeded,
	}
	if from == domain.PhaseCredentialsAdmitted && to == domain.PhasePreparing {
		return true
	}
	return allowed[from] == to
}

func (s *PostgresOwner) PublishConnectedChannelActivation(ctx context.Context, req domain.PublishActivationRequest) (domain.Operation, domain.ConnectedChannelActivation, error) {
	return publishActivation(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) PublishConnectedChannelActivation(ctx context.Context, req domain.PublishActivationRequest) (domain.Operation, domain.ConnectedChannelActivation, error) {
	return publishActivation(ctx, sqliteRunner{s}, req)
}

func publishActivation(ctx context.Context, r runner, req domain.PublishActivationRequest) (domain.Operation, domain.ConnectedChannelActivation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, domain.ConnectedChannelActivation{}, err
	}
	if _, err := uuid.Parse(req.ActivationID); err != nil || req.ExpectedRevision < 1 || req.BindingRevision < 1 || strings.TrimSpace(req.ConversationRef) == "" || req.Now.IsZero() || ((req.ProofID == "") != (req.ProofRevision == 0)) {
		return domain.Operation{}, domain.ConnectedChannelActivation{}, domain.ErrInvalidRequest
	}
	var outOp domain.Operation
	var out domain.ConnectedChannelActivation
	err := r.mutate(ctx, "publish connected channel activation", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(txctx, tx, r.dialect(), req.OperationID, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		if op.Phase != domain.PhasePublishingActivation {
			return domain.ErrConflict
		}
		now := canonicalTime(req.Now)
		if op.Verb == domain.VerbRebind {
			identityKey := op.Interface.Normalized().Key()
			if _, err := tx.ExecContext(txctx, r.dialect().bind(`UPDATE connected_channel_activations SET status='retired',activation_revision=activation_revision+1,retirement_reason='identity_rebound',retired_at=?,updated_at=? WHERE interface_key=? AND slot_key<>? AND status='current'`), now, now, identityKey, op.SlotKey); err != nil {
				return fmt.Errorf("retire sibling channel activations for rebound identity: %w", err)
			}
		}
		activationRevision := int64(1)
		prior, priorFound, err := loadActivationBySlot(txctx, tx, r.dialect(), op.SlotKey, true)
		if err != nil {
			return err
		}
		if priorFound {
			activationRevision = prior.Revision + 1
			if _, err := tx.ExecContext(txctx, r.dialect().bind(`UPDATE connected_channel_activations SET status='retired',activation_revision=?,retirement_reason='superseded',retired_at=?,updated_at=? WHERE activation_id=? AND status='current'`), activationRevision, now, now, prior.ActivationID); err != nil {
				return err
			}
		}
		i, c := op.Interface.Normalized(), op.Coordinate.Normalized()
		admissions, err := marshalCredentialAdmissions(op.CredentialAdmissions)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(txctx, r.dialect().bind(`INSERT INTO connected_channel_activations (
			activation_id,slot_key,operation_id,operation_revision,principal_id,provider,
			interface_key,interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
			bundle_hash,bundle_source,bundle_identity,pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,
			target_selector,target_generation,activation_posture,binding_revision,conversation_reference,proof_id,proof_revision,credential_admissions,
			activation_revision,status,created_at,updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'current',?,?)`),
			req.ActivationID, op.SlotKey, op.OperationID, op.Revision+1, op.PrincipalID, op.Provider,
			i.Key(), i.InterfaceRef, i.ChannelPackID, i.ChannelPackVersion, i.ChannelManifestHash, i.SemanticGeneration,
			c.BundleHash, c.BundleSource, c.BundleIdentity, c.PackInventoryGeneration, c.RuntimeInstanceID, c.ContextPublicationGeneration, c.PlanGeneration.Diagnostic(),
			op.TargetSelector, c.TargetGeneration, string(op.Posture), req.BindingRevision, strings.TrimSpace(req.ConversationRef), nullable(req.ProofID), nullableInt(req.ProofRevision), string(admissions),
			activationRevision, now, now)
		if err != nil {
			return fmt.Errorf("insert connected channel activation: %w", err)
		}
		op.BindingRevision, op.ActivationRevision = req.BindingRevision, activationRevision
		op.Phase, op.Revision, op.UpdatedAt = domain.PhasePublishingProcessActivation, op.Revision+1, now
		if err := updateOperation(txctx, tx, r.dialect(), op); err != nil {
			return err
		}
		outOp = op
		out, _, err = loadActivationByID(txctx, tx, r.dialect(), req.ActivationID, false)
		return err
	})
	return outOp, out, err
}

func (s *PostgresOwner) GetConnectedChannelActivation(ctx context.Context, slotKey string) (domain.ConnectedChannelActivation, error) {
	return getActivation(ctx, postgresRunner{s}, slotKey)
}
func (s *SQLiteOwner) GetConnectedChannelActivation(ctx context.Context, slotKey string) (domain.ConnectedChannelActivation, error) {
	return getActivation(ctx, sqliteRunner{s}, slotKey)
}
func getActivation(ctx context.Context, r runner, slotKey string) (domain.ConnectedChannelActivation, error) {
	if err := r.require(); err != nil {
		return domain.ConnectedChannelActivation{}, err
	}
	activation, found, err := loadActivationBySlot(ctx, r.query(), r.dialect(), slotKey, false)
	if err != nil {
		return activation, err
	}
	if !found {
		return activation, domain.ErrNotFound
	}
	return activation, nil
}

func (s *PostgresOwner) ListCurrentConnectedChannelActivations(ctx context.Context) ([]domain.ConnectedChannelActivation, error) {
	return listActivations(ctx, postgresRunner{s})
}
func (s *SQLiteOwner) ListCurrentConnectedChannelActivations(ctx context.Context) ([]domain.ConnectedChannelActivation, error) {
	return listActivations(ctx, sqliteRunner{s})
}
func listActivations(ctx context.Context, r runner) ([]domain.ConnectedChannelActivation, error) {
	if err := r.require(); err != nil {
		return nil, err
	}
	rows, err := r.query().QueryContext(ctx, activationSelect+` WHERE status='current' ORDER BY slot_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ConnectedChannelActivation
	for rows.Next() {
		activation, err := scanActivation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, activation)
	}
	return out, rows.Err()
}

func (s *PostgresOwner) RetireConnectedChannelActivation(ctx context.Context, req domain.RetireActivationRequest) (domain.ConnectedChannelActivation, error) {
	return retireActivation(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) RetireConnectedChannelActivation(ctx context.Context, req domain.RetireActivationRequest) (domain.ConnectedChannelActivation, error) {
	return retireActivation(ctx, sqliteRunner{s}, req)
}
func retireActivation(ctx context.Context, r runner, req domain.RetireActivationRequest) (domain.ConnectedChannelActivation, error) {
	if err := r.require(); err != nil {
		return domain.ConnectedChannelActivation{}, err
	}
	if strings.TrimSpace(req.SlotKey) == "" || req.ExpectedActivationRevision < 1 || strings.TrimSpace(req.Reason) == "" || req.Now.IsZero() {
		return domain.ConnectedChannelActivation{}, domain.ErrInvalidRequest
	}
	var out domain.ConnectedChannelActivation
	err := r.mutate(ctx, "retire connected channel activation", func(txctx context.Context, tx *sql.Tx) error {
		activation, found, err := loadActivationBySlot(txctx, tx, r.dialect(), req.SlotKey, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if activation.Revision != req.ExpectedActivationRevision {
			return domain.ErrRevisionConflict
		}
		now := canonicalTime(req.Now)
		result, err := tx.ExecContext(txctx, r.dialect().bind(`UPDATE connected_channel_activations SET status='retired',activation_revision=activation_revision+1,retirement_reason=?,retired_at=?,updated_at=? WHERE activation_id=? AND status='current' AND activation_revision=?`), strings.TrimSpace(req.Reason), now, now, activation.ActivationID, req.ExpectedActivationRevision)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return domain.ErrRevisionConflict
		}
		out, _, err = loadActivationByID(txctx, tx, r.dialect(), activation.ActivationID, false)
		return err
	})
	return out, err
}

func (s *PostgresOwner) ReserveChannelTeardown(ctx context.Context, req domain.ReserveTeardownRequest) (domain.TeardownOperation, error) {
	return reserveTeardown(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) ReserveChannelTeardown(ctx context.Context, req domain.ReserveTeardownRequest) (domain.TeardownOperation, error) {
	return reserveTeardown(ctx, sqliteRunner{s}, req)
}

func reserveTeardown(ctx context.Context, r runner, req domain.ReserveTeardownRequest) (domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return domain.TeardownOperation{}, err
	}
	if err := req.Validate(); err != nil {
		return domain.TeardownOperation{}, err
	}
	req.RequestedAt = canonicalTime(req.RequestedAt)
	var out domain.TeardownOperation
	err := r.mutate(ctx, "reserve channel teardown", func(txctx context.Context, tx *sql.Tx) error {
		existing, found, err := loadTeardownByRequestKey(txctx, tx, r.dialect(), req.RequestKeyHash, true)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestHash != req.RequestHash || existing.Kind != req.Kind {
				return domain.ErrConflict
			}
			out = existing
			return nil
		}
		scope := req.Scope
		i := scope.Interface.Normalized()
		var interfaceKey, interfaceRef, channelPackID, channelPackVersion, channelManifestHash, semanticGeneration any
		if req.Kind != domain.TeardownContextRetirement {
			interfaceKey = nullable(i.Key())
			interfaceRef = nullable(i.InterfaceRef)
			channelPackID = nullable(i.ChannelPackID)
			channelPackVersion = nullable(i.ChannelPackVersion)
			channelManifestHash = nullable(i.ChannelManifestHash)
			semanticGeneration = nullable(i.SemanticGeneration)
		}
		_, err = tx.ExecContext(txctx, r.dialect().bind(`INSERT INTO channel_onboarding_teardowns (
			teardown_id,request_key_hash,request_hash,kind,principal_id,
			interface_key,interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
			bundle_hash,bundle_source,context_publication_generation,expected_binding_revision,expected_proof_revision,phase,teardown_revision,
			retired_operations,retired_activations,requested_at,updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'reserved',1,0,0,?,?)`),
			req.TeardownID, req.RequestKeyHash, req.RequestHash, string(req.Kind), req.PrincipalID,
			interfaceKey, interfaceRef, channelPackID, channelPackVersion, channelManifestHash, semanticGeneration,
			nullable(scope.BundleHash), nullable(scope.BundleSource), nullableUint(scope.ContextPublicationGeneration), nullableInt(req.ExpectedBindingRevision), nullableInt(req.ExpectedProofRevision), req.RequestedAt, req.RequestedAt)
		if err != nil {
			return fmt.Errorf("insert channel teardown: %w", err)
		}
		out, _, err = loadTeardown(txctx, tx, r.dialect(), req.TeardownID, false)
		return err
	})
	return out, err
}

func (s *PostgresOwner) GetChannelTeardown(ctx context.Context, teardownID string) (domain.TeardownOperation, error) {
	return getTeardown(ctx, postgresRunner{s}, teardownID)
}
func (s *SQLiteOwner) GetChannelTeardown(ctx context.Context, teardownID string) (domain.TeardownOperation, error) {
	return getTeardown(ctx, sqliteRunner{s}, teardownID)
}

func getTeardown(ctx context.Context, r runner, teardownID string) (domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return domain.TeardownOperation{}, err
	}
	op, found, err := loadTeardown(ctx, r.query(), r.dialect(), strings.TrimSpace(teardownID), false)
	if err != nil {
		return domain.TeardownOperation{}, err
	}
	if !found {
		return domain.TeardownOperation{}, domain.ErrNotFound
	}
	return op, nil
}

func (s *PostgresOwner) ListChannelTeardowns(ctx context.Context) ([]domain.TeardownOperation, error) {
	return listTeardowns(ctx, postgresRunner{s})
}
func (s *SQLiteOwner) ListChannelTeardowns(ctx context.Context) ([]domain.TeardownOperation, error) {
	return listTeardowns(ctx, sqliteRunner{s})
}

func listTeardowns(ctx context.Context, r runner) ([]domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return nil, err
	}
	rows, err := r.query().QueryContext(ctx, teardownSelect+` ORDER BY requested_at,teardown_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.TeardownOperation
	for rows.Next() {
		op, found, err := scanTeardownRow(rows)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, op)
		}
	}
	return out, rows.Err()
}

func (s *PostgresOwner) RetireChannelTeardownAuthority(ctx context.Context, req domain.RetireTeardownAuthorityRequest) (domain.TeardownOperation, error) {
	return retireTeardownAuthority(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) RetireChannelTeardownAuthority(ctx context.Context, req domain.RetireTeardownAuthorityRequest) (domain.TeardownOperation, error) {
	return retireTeardownAuthority(ctx, sqliteRunner{s}, req)
}

func retireTeardownAuthority(ctx context.Context, r runner, req domain.RetireTeardownAuthorityRequest) (domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return domain.TeardownOperation{}, err
	}
	if strings.TrimSpace(req.TeardownID) == "" || req.ExpectedRevision < 1 || strings.TrimSpace(req.Reason) == "" || req.Now.IsZero() {
		return domain.TeardownOperation{}, domain.ErrInvalidRequest
	}
	var out domain.TeardownOperation
	err := r.mutate(ctx, "retire channel teardown authority", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadTeardown(txctx, tx, r.dialect(), req.TeardownID, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		if op.Phase != domain.TeardownReserved {
			return domain.ErrConflict
		}
		now := canonicalTime(req.Now)
		where, args := `interface_key=?`, []any{op.Scope.Interface.Key()}
		if op.Kind == domain.TeardownContextRetirement {
			where, args = `bundle_hash=? AND bundle_source=? AND context_publication_generation=?`, []any{op.Scope.BundleHash, op.Scope.BundleSource, op.Scope.ContextPublicationGeneration}
		}
		operationQuery := `UPDATE channel_onboarding_operations SET phase='retired',operation_revision=operation_revision+1,failure_code='authority_retired',failure_message=?,updated_at=?,completed_at=? WHERE phase NOT IN ('succeeded','failed','retired') AND ` + where
		operationArgs := append([]any{strings.TrimSpace(req.Reason), now, now}, args...)
		operationResult, err := tx.ExecContext(txctx, r.dialect().bind(operationQuery), operationArgs...)
		if err != nil {
			return err
		}
		activationQuery := `UPDATE connected_channel_activations SET status='retired',activation_revision=activation_revision+1,retirement_reason=?,retired_at=?,updated_at=? WHERE status='current' AND ` + where
		activationArgs := append([]any{strings.TrimSpace(req.Reason), now, now}, args...)
		activationResult, err := tx.ExecContext(txctx, r.dialect().bind(activationQuery), activationArgs...)
		if err != nil {
			return err
		}
		retiredOperations, err := operationResult.RowsAffected()
		if err != nil {
			return err
		}
		retiredActivations, err := activationResult.RowsAffected()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(txctx, r.dialect().bind(`UPDATE channel_onboarding_teardowns SET phase='authority_retired',teardown_revision=teardown_revision+1,retired_operations=?,retired_activations=?,updated_at=? WHERE teardown_id=? AND teardown_revision=? AND phase='reserved'`), retiredOperations, retiredActivations, now, op.TeardownID, op.Revision)
		if err != nil {
			return err
		}
		out, _, err = loadTeardown(txctx, tx, r.dialect(), op.TeardownID, false)
		return err
	})
	return out, err
}

func (s *PostgresOwner) CompleteChannelTeardown(ctx context.Context, req domain.CompleteTeardownRequest) (domain.TeardownOperation, error) {
	return completeTeardown(ctx, postgresRunner{s}, req)
}
func (s *SQLiteOwner) CompleteChannelTeardown(ctx context.Context, req domain.CompleteTeardownRequest) (domain.TeardownOperation, error) {
	return completeTeardown(ctx, sqliteRunner{s}, req)
}

func completeTeardown(ctx context.Context, r runner, req domain.CompleteTeardownRequest) (domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return domain.TeardownOperation{}, err
	}
	if strings.TrimSpace(req.TeardownID) == "" || req.ExpectedRevision < 1 || req.Now.IsZero() {
		return domain.TeardownOperation{}, domain.ErrInvalidRequest
	}
	var out domain.TeardownOperation
	err := r.mutate(ctx, "complete channel teardown", func(txctx context.Context, tx *sql.Tx) error {
		op, found, err := loadTeardown(txctx, tx, r.dialect(), req.TeardownID, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Phase.Terminal() {
			out = op
			return nil
		}
		if op.Revision != req.ExpectedRevision || (req.Succeeded && op.Phase != domain.TeardownAuthorityRetired) || (!req.Succeeded && op.Phase != domain.TeardownReserved && op.Phase != domain.TeardownAuthorityRetired) {
			return domain.ErrRevisionConflict
		}
		phase := domain.TeardownFailed
		if req.Succeeded {
			phase = domain.TeardownSucceeded
		}
		now := canonicalTime(req.Now)
		_, err = tx.ExecContext(txctx, r.dialect().bind(`UPDATE channel_onboarding_teardowns SET phase=?,teardown_revision=teardown_revision+1,failure_code=?,failure_message=?,updated_at=?,completed_at=? WHERE teardown_id=? AND teardown_revision=?`), string(phase), nullable(req.FailureCode), nullable(req.FailureMessage), now, now, op.TeardownID, op.Revision)
		if err != nil {
			return err
		}
		out, _, err = loadTeardown(txctx, tx, r.dialect(), op.TeardownID, false)
		return err
	})
	return out, err
}

const teardownSelect = `SELECT teardown_id,request_key_hash,request_hash,kind,principal_id,
	interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
	bundle_hash,bundle_source,context_publication_generation,expected_binding_revision,expected_proof_revision,phase,teardown_revision,
	retired_operations,retired_activations,failure_code,failure_message,requested_at,updated_at,completed_at
	FROM channel_onboarding_teardowns`

func loadTeardownByRequestKey(ctx context.Context, q queryer, d dialect, key string, lock bool) (domain.TeardownOperation, bool, error) {
	query := teardownSelect + ` WHERE request_key_hash=?`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanTeardownRow(q.QueryRowContext(ctx, d.bind(query), key))
}

func loadTeardown(ctx context.Context, q queryer, d dialect, id string, lock bool) (domain.TeardownOperation, bool, error) {
	query := teardownSelect + ` WHERE teardown_id=?`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanTeardownRow(q.QueryRowContext(ctx, d.bind(query), id))
}

func scanTeardownRow(row rowScanner) (domain.TeardownOperation, bool, error) {
	var op domain.TeardownOperation
	var kind, phase string
	var interfaceRef, packID, packVersion, manifestHash, semanticGeneration sql.NullString
	var bundleHash, bundleSource, failureCode, failureMessage sql.NullString
	var publicationGeneration, bindingRevision, proofRevision sql.NullInt64
	var requested, updated, completed any
	err := row.Scan(&op.TeardownID, &op.RequestKeyHash, &op.RequestHash, &kind, &op.PrincipalID,
		&interfaceRef, &packID, &packVersion, &manifestHash, &semanticGeneration,
		&bundleHash, &bundleSource, &publicationGeneration, &bindingRevision, &proofRevision, &phase, &op.Revision,
		&op.RetiredOperations, &op.RetiredActivations, &failureCode, &failureMessage, &requested, &updated, &completed)
	if err == sql.ErrNoRows {
		return domain.TeardownOperation{}, false, nil
	}
	if err != nil {
		return domain.TeardownOperation{}, false, err
	}
	op.Kind, op.Phase = domain.TeardownKind(kind), domain.TeardownPhase(phase)
	op.Scope = domain.TeardownScope{
		Interface:  operatorInterface(interfaceRef.String, packID.String, packVersion.String, manifestHash.String, semanticGeneration.String),
		BundleHash: bundleHash.String, BundleSource: bundleSource.String, ContextPublicationGeneration: uint64(publicationGeneration.Int64),
	}
	op.ExpectedBindingRevision, op.ExpectedProofRevision = bindingRevision.Int64, proofRevision.Int64
	op.FailureCode, op.FailureMessage = failureCode.String, failureMessage.String
	var timeErr error
	if op.RequestedAt, timeErr = timeValue(requested); timeErr != nil {
		return domain.TeardownOperation{}, false, timeErr
	}
	if op.UpdatedAt, timeErr = timeValue(updated); timeErr != nil {
		return domain.TeardownOperation{}, false, timeErr
	}
	if op.CompletedAt, timeErr = nullableTimeValue(completed); timeErr != nil {
		return domain.TeardownOperation{}, false, timeErr
	}
	return op, true, nil
}

func operatorInterface(ref, packID, version, manifest, generation string) operatorchannel.InterfaceIdentity {
	return operatorchannel.InterfaceIdentity{
		InterfaceRef: ref, ChannelPackID: packID, ChannelPackVersion: version,
		ChannelManifestHash: manifest, SemanticGeneration: generation,
	}.Normalized()
}

const operationSelect = `SELECT operation_id,request_key_hash,request_hash,slot_key,principal_id,verb,provider,
	interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
	bundle_hash,bundle_source,bundle_identity,pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,
	target_selector,target_generation,activation_posture,identity_ceremony,phase,operation_revision,save_proof,
	credential_reservations,credential_admissions,identity_operation_id,binding_revision,activation_revision,confirmation_operation_id,
	failure_code,failure_message,requested_at,updated_at,completed_at FROM channel_onboarding_operations`

func loadOperationByRequestKey(ctx context.Context, q queryer, d dialect, key string, lock bool) (domain.Operation, bool, error) {
	query := operationSelect + ` WHERE request_key_hash=?`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanOperationRow(q.QueryRowContext(ctx, d.bind(query), key))
}

func loadOperation(ctx context.Context, q queryer, d dialect, operationID string, lock bool) (domain.Operation, bool, error) {
	query := operationSelect + ` WHERE operation_id=?`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanOperationRow(q.QueryRowContext(ctx, d.bind(query), operationID))
}

func scanOperation(scanner interface{ Scan(...any) error }) (domain.Operation, error) {
	op, found, err := scanOperationRow(scanner)
	if err != nil {
		return domain.Operation{}, err
	}
	if !found {
		return domain.Operation{}, sql.ErrNoRows
	}
	return op, nil
}

type rowScanner interface{ Scan(...any) error }

func scanOperationRow(row rowScanner) (domain.Operation, bool, error) {
	var op domain.Operation
	var verb, posture, ceremony, phase string
	var planGeneration string
	var reservations, admissions string
	var identityOp, confirmationOp, failureCode, failureMessage sql.NullString
	var bindingRevision, activationRevision sql.NullInt64
	var completed any
	var requested, updated any
	err := row.Scan(&op.OperationID, &op.RequestKeyHash, &op.RequestHash, &op.SlotKey, &op.PrincipalID, &verb, &op.Provider,
		&op.Interface.InterfaceRef, &op.Interface.ChannelPackID, &op.Interface.ChannelPackVersion, &op.Interface.ChannelManifestHash, &op.Interface.SemanticGeneration,
		&op.Coordinate.BundleHash, &op.Coordinate.BundleSource, &op.Coordinate.BundleIdentity, &op.Coordinate.PackInventoryGeneration, &op.Coordinate.RuntimeInstanceID, &op.Coordinate.ContextPublicationGeneration, &planGeneration,
		&op.TargetSelector, &op.Coordinate.TargetGeneration, &posture, &ceremony, &phase, &op.Revision, &op.SaveProof,
		&reservations, &admissions, &identityOp, &bindingRevision, &activationRevision, &confirmationOp, &failureCode, &failureMessage, &requested, &updated, &completed)
	if err == sql.ErrNoRows {
		return domain.Operation{}, false, nil
	}
	if err != nil {
		return domain.Operation{}, false, err
	}
	op.Verb, op.Posture, op.Ceremony, op.Phase = domain.Verb(verb), domain.ActivationPosture(posture), domain.IdentityCeremony(ceremony), domain.Phase(phase)
	op.Coordinate.PlanGeneration, err = plangeneration.Parse(planGeneration)
	if err != nil {
		return domain.Operation{}, false, fmt.Errorf("decode channel onboarding plan generation: %w", err)
	}
	op.Interface = op.Interface.Normalized()
	if err := json.Unmarshal([]byte(reservations), &op.CredentialReservations); err != nil {
		return domain.Operation{}, false, err
	}
	if op.CredentialAdmissions, err = unmarshalCredentialAdmissions(admissions); err != nil {
		return domain.Operation{}, false, err
	}
	op.IdentityOperationID, op.ConfirmationOperationID = identityOp.String, confirmationOp.String
	op.BindingRevision, op.ActivationRevision = bindingRevision.Int64, activationRevision.Int64
	op.FailureCode, op.FailureMessage = failureCode.String, failureMessage.String
	var timeErr error
	if op.RequestedAt, timeErr = timeValue(requested); timeErr != nil {
		return domain.Operation{}, false, timeErr
	}
	if op.UpdatedAt, timeErr = timeValue(updated); timeErr != nil {
		return domain.Operation{}, false, timeErr
	}
	if op.CompletedAt, timeErr = nullableTimeValue(completed); timeErr != nil {
		return domain.Operation{}, false, timeErr
	}
	return op, true, nil
}

func updateOperation(ctx context.Context, tx *sql.Tx, d dialect, op domain.Operation) error {
	admissions, err := marshalCredentialAdmissions(op.CredentialAdmissions)
	if err != nil {
		return err
	}
	c := op.Coordinate.Normalized()
	_, err = tx.ExecContext(ctx, d.bind(`UPDATE channel_onboarding_operations SET
		bundle_hash=?,bundle_source=?,bundle_identity=?,pack_inventory_generation=?,runtime_instance_id=?,context_publication_generation=?,plan_generation=?,target_generation=?,
		phase=?,operation_revision=?,credential_admissions=?,identity_operation_id=?,binding_revision=?,activation_revision=?,confirmation_operation_id=?,failure_code=?,failure_message=?,updated_at=?,completed_at=?
		WHERE operation_id=?`),
		c.BundleHash, c.BundleSource, c.BundleIdentity, c.PackInventoryGeneration, c.RuntimeInstanceID, c.ContextPublicationGeneration, c.PlanGeneration.Diagnostic(), c.TargetGeneration,
		string(op.Phase), op.Revision, string(admissions), nullable(op.IdentityOperationID), nullableInt(op.BindingRevision), nullableInt(op.ActivationRevision), nullable(op.ConfirmationOperationID), nullable(op.FailureCode), nullable(op.FailureMessage), op.UpdatedAt, nullableTime(op.CompletedAt), op.OperationID)
	return err
}

const activationSelect = `SELECT activation_id,slot_key,operation_id,operation_revision,principal_id,provider,
	interface_ref,channel_pack_id,channel_pack_version,channel_manifest_hash,semantic_generation,
	bundle_hash,bundle_source,bundle_identity,pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,
	target_selector,target_generation,activation_posture,binding_revision,conversation_reference,proof_id,proof_revision,credential_admissions,
	activation_revision,status,retirement_reason,created_at,updated_at,retired_at FROM connected_channel_activations`

func loadActivationBySlot(ctx context.Context, q queryer, d dialect, slot string, lock bool) (domain.ConnectedChannelActivation, bool, error) {
	query := activationSelect + ` WHERE slot_key=? AND status='current'`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanActivationRow(q.QueryRowContext(ctx, d.bind(query), slot))
}

func loadActivationByID(ctx context.Context, q queryer, d dialect, id string, lock bool) (domain.ConnectedChannelActivation, bool, error) {
	query := activationSelect + ` WHERE activation_id=?`
	if lock && d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	return scanActivationRow(q.QueryRowContext(ctx, d.bind(query), id))
}

func scanActivationRow(row rowScanner) (domain.ConnectedChannelActivation, bool, error) {
	var a domain.ConnectedChannelActivation
	var posture, status, admissions string
	var planGeneration string
	var proofID, retirement sql.NullString
	var proofRevision sql.NullInt64
	var created, updated, retired any
	err := row.Scan(&a.ActivationID, &a.SlotKey, &a.OperationID, &a.OperationRevision, &a.PrincipalID, &a.Provider,
		&a.Interface.InterfaceRef, &a.Interface.ChannelPackID, &a.Interface.ChannelPackVersion, &a.Interface.ChannelManifestHash, &a.Interface.SemanticGeneration,
		&a.Coordinate.BundleHash, &a.Coordinate.BundleSource, &a.Coordinate.BundleIdentity, &a.Coordinate.PackInventoryGeneration, &a.Coordinate.RuntimeInstanceID, &a.Coordinate.ContextPublicationGeneration, &planGeneration,
		&a.TargetSelector, &a.Coordinate.TargetGeneration, &posture, &a.BindingRevision, &a.ConversationRef, &proofID, &proofRevision, &admissions, &a.Revision, &status, &retirement, &created, &updated, &retired)
	if err == sql.ErrNoRows {
		return domain.ConnectedChannelActivation{}, false, nil
	}
	if err != nil {
		return domain.ConnectedChannelActivation{}, false, err
	}
	a.Interface = a.Interface.Normalized()
	a.Coordinate.PlanGeneration, err = plangeneration.Parse(planGeneration)
	if err != nil {
		return domain.ConnectedChannelActivation{}, false, fmt.Errorf("decode connected channel activation plan generation: %w", err)
	}
	a.Posture, a.Status = domain.ActivationPosture(posture), domain.ActivationStatus(status)
	a.ProofID, a.ProofRevision, a.RetirementReason = proofID.String, proofRevision.Int64, retirement.String
	if a.CredentialAdmissions, err = unmarshalCredentialAdmissions(admissions); err != nil {
		return a, false, err
	}
	var timeErr error
	if a.CreatedAt, timeErr = timeValue(created); timeErr != nil {
		return a, false, timeErr
	}
	if a.UpdatedAt, timeErr = timeValue(updated); timeErr != nil {
		return a, false, timeErr
	}
	if a.RetiredAt, timeErr = nullableTimeValue(retired); timeErr != nil {
		return a, false, timeErr
	}
	return a, true, nil
}

type credentialAdmissionRecord struct {
	Role      string                         `json:"role"`
	StoreKey  string                         `json:"store_key"`
	Kind      domain.CredentialAdmissionKind `json:"kind"`
	Receipt   string                         `json:"receipt,omitempty"`
	ValueSeal string                         `json:"value_seal"`
}

func marshalCredentialAdmissions(admissions []domain.CredentialAdmission) ([]byte, error) {
	records := make([]credentialAdmissionRecord, 0, len(admissions))
	for _, admission := range admissions {
		if err := admission.Validate(); err != nil {
			return nil, err
		}
		records = append(records, credentialAdmissionRecord{
			Role: admission.Role, StoreKey: admission.StoreKey, Kind: admission.Kind,
			Receipt: admission.Receipt, ValueSeal: admission.ValueSeal.String(),
		})
	}
	return json.Marshal(records)
}

func unmarshalCredentialAdmissions(raw string) ([]domain.CredentialAdmission, error) {
	var records []credentialAdmissionRecord
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		return nil, err
	}
	admissions := make([]domain.CredentialAdmission, 0, len(records))
	for _, record := range records {
		seal, err := runtimecredentials.ParseValueSeal(record.ValueSeal)
		if err != nil {
			return nil, fmt.Errorf("decode channel credential admission: %w", err)
		}
		admission := domain.CredentialAdmission{
			Role: record.Role, StoreKey: record.StoreKey, Kind: record.Kind,
			Receipt: record.Receipt, ValueSeal: seal,
		}
		if err := admission.Validate(); err != nil {
			return nil, err
		}
		admissions = append(admissions, admission)
	}
	return admissions, nil
}

func scanActivation(rows *sql.Rows) (domain.ConnectedChannelActivation, error) {
	a, _, err := scanActivationRow(rows)
	return a, err
}

func nullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}
func nullableInt(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}
func nullableUint(value uint64) any {
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
func canonicalTime(value time.Time) time.Time { return value.UTC().Truncate(time.Microsecond) }

func timeValue(value any) (time.Time, error) {
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
