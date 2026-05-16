package destructivereset

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultOperationName = "runtime.destructive_reset"
	defaultLockKey       = "swarm:runtime:destructive-reset"

	QuiescenceControlledBy = DefaultOperationName
	QuiescenceReasonCode   = "runtime_nuke_cancelled"
	QuiescenceDeliveryNote = "runtime destructive reset cancelled delivery"
)

var (
	ErrInvalidRequest       = errors.New("invalid destructive reset request")
	ErrOperationInProgress  = errors.New("destructive reset operation already in progress")
	ErrIdempotencyConflict  = errors.New("destructive reset idempotency conflict")
	ErrPlannerNotConfigured = errors.New("destructive reset planner is not configured")
	ErrLockNotConfigured    = errors.New("destructive reset lock manager is not configured")
	ErrLockLeaseMissing     = errors.New("destructive reset lock lease is missing")
)

type Request struct {
	ActorTokenID   string
	IdempotencyKey string
	RequestHash    string
	DryRun         bool
	RequestedAt    time.Time
}

type Result struct {
	OperationName string    `json:"operation_name"`
	DryRun        bool      `json:"dry_run"`
	PlannedAt     time.Time `json:"planned_at"`
	Plan          Plan      `json:"plan"`
}

type QuiescenceRequest struct {
	Result       Result
	ActorTokenID string
	RequestedAt  time.Time
}

type QuiescenceResult struct {
	OperationName        string             `json:"operation_name"`
	DryRun               bool               `json:"dry_run"`
	AppliedAt            time.Time          `json:"applied_at"`
	ReasonCode           string             `json:"reason_code"`
	ControlledBy         string             `json:"controlled_by"`
	Runs                 []QuiescedRun      `json:"runs"`
	Deliveries           []QuiescedDelivery `json:"deliveries"`
	PipelineReceiptCount int                `json:"pipeline_receipt_count"`
}

type CleanupRequest struct {
	Result       Result
	Quiescence   QuiescenceResult
	ActorTokenID string
	RequestedAt  time.Time
}

type CleanupResult struct {
	OperationName string               `json:"operation_name"`
	DryRun        bool                 `json:"dry_run"`
	AppliedAt     time.Time            `json:"applied_at"`
	RunIDs        []string             `json:"run_ids"`
	Tables        []CleanupTableResult `json:"tables"`
}

type CleanupTableResult struct {
	Table            string `json:"table"`
	TableKind        string `json:"table_kind"`
	Classification   string `json:"classification"`
	PredicateOwner   string `json:"predicate_owner"`
	DeleteOrderGroup int    `json:"delete_order_group"`
	MatchedRows      int64  `json:"matched_rows"`
	DeletedRows      int64  `json:"deleted_rows"`
	PreservedRows    int64  `json:"preserved_rows"`
}

type CleanupCatalogEntry struct {
	Table             string
	TableKind         string
	Classification    string
	PredicateOwner    string
	DeleteOrderGroup  int
	PreservationProof string
}

type QuiescedRun struct {
	RunID          string `json:"run_id"`
	PreviousStatus string `json:"previous_status"`
	Status         string `json:"status"`
	ReasonCode     string `json:"reason_code"`
	Changed        bool   `json:"changed"`
}

type QuiescedDelivery struct {
	DeliveryID      string `json:"delivery_id"`
	RunID           string `json:"run_id"`
	EventID         string `json:"event_id"`
	SubscriberType  string `json:"subscriber_type"`
	SubscriberID    string `json:"subscriber_id"`
	PreviousStatus  string `json:"previous_status"`
	Status          string `json:"status"`
	ReasonCode      string `json:"reason_code"`
	PreviousReason  string `json:"previous_reason,omitempty"`
	ActiveSessionID string `json:"active_session_id,omitempty"`
	Changed         bool   `json:"changed"`
}

type Plan struct {
	ActiveRuns          []RunRef             `json:"active_runs"`
	ActiveDeliveries    []DeliveryRef        `json:"active_deliveries"`
	RunScopedTables     []TableRef           `json:"run_scoped_tables"`
	EntityContainers    []ContainerRef       `json:"entity_containers"`
	Preserved           PreservedResources   `json:"preserved"`
	DownstreamContracts []DownstreamContract `json:"downstream_contracts"`
	ResetSeams          []ResetSeam          `json:"reset_seams"`
}

type RunRef struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type DeliveryRef struct {
	DeliveryID string `json:"delivery_id"`
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
}

type TableRef struct {
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Action string `json:"action"`
}

type ContainerRef struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Action string `json:"action"`
}

type PreservedResources struct {
	SystemContainers        []string `json:"system_containers"`
	OperatorManagedBoundary string   `json:"operator_managed_boundary"`
	SchemaMigrations        bool     `json:"schema_migrations"`
	AuthTokens              bool     `json:"auth_tokens"`
	BundleContracts         bool     `json:"bundle_contracts"`
}

type DownstreamContract struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Owner       string `json:"owner"`
	Description string `json:"description"`
}

type ResetSeam struct {
	ID             string `json:"id"`
	Classification string `json:"classification"`
	RequiredAction string `json:"required_action"`
}

type Inventory struct {
	ActiveRuns       []RunRef
	ActiveDeliveries []DeliveryRef
	RunScopedTables  []TableRef
	EntityContainers []ContainerRef
	Preserved        PreservedResources
}

type InventoryReader interface {
	ReadResetInventory(context.Context) (Inventory, error)
}

type Planner interface {
	BuildPlan(context.Context, Request) (Plan, error)
}

type LockManager interface {
	TryAcquire(context.Context, string) (LockLease, bool, error)
}

type LockLease interface {
	Release(context.Context) error
}

type IdempotencyStore interface {
	LoadResetResult(context.Context, IdempotencyKey) (StoredResult, bool, error)
	StoreResetResult(context.Context, StoredResult) error
}

type QuiescenceStore interface {
	ApplyDestructiveResetQuiescence(context.Context, QuiescenceRequest) (QuiescenceResult, error)
}

type CleanupStore interface {
	ApplyDestructiveResetCleanup(context.Context, CleanupRequest) (CleanupResult, error)
}

type IdempotencyKey struct {
	OperationName  string
	ActorTokenID   string
	IdempotencyKey string
}

type StoredResult struct {
	Key         IdempotencyKey
	RequestHash string
	Result      Result
	StoredAt    time.Time
}

type IdempotencyConflictError struct {
	Key                    IdempotencyKey
	OriginalRequestHash    string
	ConflictingRequestHash string
}

func (e *IdempotencyConflictError) Error() string {
	return ErrIdempotencyConflict.Error()
}

func (e *IdempotencyConflictError) Is(target error) bool {
	return target == ErrIdempotencyConflict
}

func (r Request) normalize(now time.Time) (Request, error) {
	r.ActorTokenID = strings.TrimSpace(r.ActorTokenID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	r.RequestHash = strings.TrimSpace(r.RequestHash)
	if r.ActorTokenID == "" {
		return Request{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if r.IdempotencyKey != "" && r.RequestHash == "" {
		return Request{}, fmt.Errorf("%w: request hash is required when idempotency key is present", ErrInvalidRequest)
	}
	if r.RequestedAt.IsZero() {
		r.RequestedAt = now
	}
	r.RequestedAt = r.RequestedAt.UTC()
	return r, nil
}

func (k IdempotencyKey) normalized() IdempotencyKey {
	k.OperationName = strings.TrimSpace(k.OperationName)
	if k.OperationName == "" {
		k.OperationName = DefaultOperationName
	}
	k.ActorTokenID = strings.TrimSpace(k.ActorTokenID)
	k.IdempotencyKey = strings.TrimSpace(k.IdempotencyKey)
	return k
}
