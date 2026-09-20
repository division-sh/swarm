package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

// Singleton operations take the same group-first path as segment operations.
// The immutable group pointer is installed before registry publication.
type pipelineClaimOperationMutex struct {
	member sync.Mutex
	group  *publicationGroup
}

func (m *pipelineClaimOperationMutex) Lock() {
	if m.group != nil {
		m.group.mu.Lock()
	}
	m.member.Lock()
}
func (m *pipelineClaimOperationMutex) Unlock() {
	m.member.Unlock()
	if m.group != nil {
		m.group.mu.Unlock()
	}
}

type fanOutPublicationGroups struct {
	mu     sync.Mutex
	groups map[fanoutobligation.Claim]*publicationGroup
}

func (r *fanOutPublicationGroups) register(g *publicationGroup) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups == nil {
		r.groups = make(map[fanoutobligation.Claim]*publicationGroup)
	}
	if r.groups[g.claim] != nil {
		return pipelineobligation.ErrBusy
	}
	r.groups[g.claim] = g
	return nil
}
func (r *fanOutPublicationGroups) get(claim fanoutobligation.Claim) *publicationGroup {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groups[claim]
}
func (r *fanOutPublicationGroups) remove(g *publicationGroup) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups[g.claim] == g {
		delete(r.groups, g.claim)
	}
}

type publicationGroupMember struct {
	ordinal             int
	event               events.Event
	claim               pipelineobligation.Claim
	state               *pipelineClaimState
	preparationRejected bool
	prepared            bool
}

type publicationGroup struct {
	mu                    sync.Mutex
	postgres              *PipelinePostgresOwner
	sqlite                *PipelineSQLiteOwner
	admission             FanOutAdmission
	grant                 startupownership.GrantEvidence
	registry              *fanOutPublicationGroups
	claim                 fanoutobligation.Claim
	intent                fanoutobligation.Intent
	session               *postgresbackend.SessionAuthority
	preparationAdmission  *postgresbackend.PublicationAdmission
	releaseSession        func() error
	members               map[int]*publicationGroupMember
	sealed                bool
	end                   int
	restrictAfterRollback bool
	committed             bool
	closed                bool
	preparationFailed     bool
	publicationTx         atomic.Pointer[sql.Tx]
}

func (s *fanOutPostgresOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	g := &publicationGroup{postgres: s.PipelinePostgresOwner, admission: s.admission, grant: s.grant, registry: &s.publicationGroups, claim: claim}
	if err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		g.intent, err = observeFanOutClaim(ctx, tx, s.admission, s.grant, true, claim, time.Now)
		return err
	}); err != nil {
		return nil, err
	}
	var err error
	g.session, g.releaseSession, err = s.reservePostgresPipelineClaimConnection(ctx)
	if err != nil {
		return nil, err
	}
	// A pool reset may return a connection after cancellation. It is not an
	// admitted group until acquisition has finished under the caller's context.
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, g.releaseSession())
	}
	if err := g.initialize(); err != nil {
		return nil, errors.Join(err, g.releaseSession())
	}
	return g, nil
}

func (s *fanOutSQLiteOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	g := &publicationGroup{sqlite: s.PipelineSQLiteOwner, admission: s.admission, grant: s.grant, registry: &s.publicationGroups, claim: claim}
	if err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		g.intent, err = observeFanOutClaim(ctx, tx, s.admission, s.grant, false, claim, s.now)
		return err
	}); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.initialize(); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *publicationGroup) initialize() error {
	g.members = make(map[int]*publicationGroupMember)
	g.end = g.intent.ChunkEndOrdinal()
	return g.registry.register(g)
}

func (g *publicationGroup) Claim(ctx context.Context, ordinal int, event events.Event) (pipelineobligation.Claim, error) {
	claims, err := g.ClaimBatch(ctx, []pipelineobligation.PublicationClaimRequest{{Ordinal: ordinal, Event: event}})
	if err != nil {
		return pipelineobligation.Claim{}, err
	}
	return claims[0], nil
}

func (g *publicationGroup) ClaimBatch(ctx context.Context, requests []pipelineobligation.PublicationClaimRequest) ([]pipelineobligation.Claim, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.preparationFailed || g.sealed || len(requests) > fanoutobligation.MaxChunkSize {
		return nil, errors.New("publication group is closed, sealed or exceeds its admitted range")
	}
	ordinals, eventIDs := make(map[int]bool), make(map[string]bool)
	for _, member := range g.members {
		ordinals[member.ordinal] = true
		eventIDs[member.event.ID()] = true
	}
	for _, request := range requests {
		if request.Ordinal < g.intent.Cursor || request.Ordinal >= g.intent.ChunkEndOrdinal() || ordinals[request.Ordinal] || eventIDs[request.Event.ID()] {
			return nil, errors.New("publication claim batch contains duplicate or out-of-range identity")
		}
		if err := fanoutobligation.ValidateCommittedOrdinalEvent(g.claim.Key, g.intent.Request.PlanRef, g.intent.Request.Capsule, request.Ordinal, request.Event); err != nil {
			return nil, err
		}
		ordinals[request.Ordinal], eventIDs[request.Event.ID()] = true, true
	}
	if len(requests) == 0 {
		return nil, nil
	}
	// A batch holds every parent fence until admission commits. Acquire those
	// distinct keys in a stable order, while preserving the caller's row order.
	order := make([]int, len(requests))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return requests[order[i]].Event.ID() < requests[order[j]].Event.ID() })
	claims := make([]pipelineobligation.Claim, len(requests))
	operation := func(ctx context.Context) error {
		for _, index := range order {
			request := requests[index]
			claim, err := g.claimMember(ctx, request.Ordinal, request.Event)
			if err != nil {
				g.preparationFailed = true
				for _, member := range g.members {
					err = errors.Join(err, g.releaseMember(context.WithoutCancel(ctx), member))
				}
				return err
			}
			claims[index] = claim
		}
		return nil
	}
	var err error
	if g.postgres != nil {
		err = postgresbackend.RunPublicationAdmission(ctx, g.session, func(ctx context.Context, admission *postgresbackend.PublicationAdmission) error {
			g.preparationAdmission = admission
			defer func() { g.preparationAdmission = nil }()
			return operation(ctx)
		})
	} else {
		err = operation(ctx)
	}
	if err != nil {
		g.preparationFailed = true
		for _, member := range g.members {
			err = errors.Join(err, g.releaseMember(context.WithoutCancel(ctx), member))
		}
		return nil, err
	}
	return claims, nil
}

func (g *publicationGroup) claimMember(ctx context.Context, ordinal int, event events.Event) (pipelineobligation.Claim, error) {
	var claim pipelineobligation.Claim
	var state *pipelineClaimState
	var err error
	if g.postgres != nil {
		claim, err = g.postgres.claimPostgresPipelineEventInGroup(ctx, event.ID(), pipelineobligation.PurposePublication, g)
		if err == nil {
			state, err = g.postgres.postgresPipelineClaimState(claim)
		}
	} else {
		claim, err = g.sqlite.claimSQLitePipelineEventInGroup(ctx, event.ID(), pipelineobligation.PurposePublication, g)
		if err == nil {
			state, err = g.sqlite.sqlitePipelineClaimState(claim)
		}
	}
	if err != nil {
		return pipelineobligation.Claim{}, errors.Join(err, g.cleanupAcquiredClaim(ctx, claim))
	}
	g.members[ordinal] = &publicationGroupMember{ordinal: ordinal, event: event.Clone(), claim: claim, state: state}
	return claim, nil
}

func (g *publicationGroup) Seal(ctx context.Context, end int, claims []pipelineobligation.Claim) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.closed || g.preparationFailed || g.committed || end <= g.intent.Cursor || end > g.intent.ChunkEndOrdinal() {
		return errors.New("invalid publication group attempt seal")
	}
	if g.sealed && (!g.restrictAfterRollback || end >= g.end) {
		return errors.New("publication group reseal requires exact safe rollback and a reduced prefix")
	}
	wanted := make(map[int]bool)
	for _, claim := range claims {
		member, err := g.member(claim)
		if err != nil {
			return err
		}
		if member.ordinal >= end || wanted[member.ordinal] {
			return errors.New("publication group seal contains duplicate or excluded member")
		}
		if err := g.current(member); err != nil {
			return err
		}
		if !member.prepared {
			return errors.New("publication group seal requires canonical prepared event evidence")
		}
		wanted[member.ordinal] = true
	}
	for ordinal, member := range g.members {
		if ordinal < end && !wanted[ordinal] {
			if err := g.current(member); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
				return errors.New("publication group seal omits live prepared member")
			}
			member.preparationRejected = true
		}
		if ordinal >= end && g.current(member) == nil {
			return errors.New("excluded publication group member must be released before sealing")
		}
	}
	g.sealed, g.end, g.restrictAfterRollback = true, end, false
	return nil
}

// Acquisition may have installed a registry entry before session loss is
// observed. Cleanup reads only that issuer/token, never a successor by event ID.
func (g *publicationGroup) cleanupAcquiredClaim(ctx context.Context, claim pipelineobligation.Claim) error {
	if claim.EventID() == "" {
		return nil
	}
	var state *pipelineClaimState
	if g.postgres != nil {
		r := g.postgres.postgresPipelineClaims()
		r.mu.Lock()
		token, err := r.issuer.Token(claim)
		if err == nil {
			state = r.claims[token]
		}
		r.mu.Unlock()
		if state != nil && state.operationMu.group == g && state.postgresLease != nil {
			if g.preparationAdmission != nil {
				return g.preparationAdmission.ReleaseLease(context.WithoutCancel(ctx), state.postgresLease)
			}
			return state.postgresLease.ReleaseTerminal(context.WithoutCancel(ctx))
		}
	} else {
		state, _ = g.sqlite.sqlitePipelineClaimState(claim)
		if state != nil && state.operationMu.group == g {
			return g.sqlite.releaseSQLitePipelineClaimLocked(claim, state)
		}
	}
	return nil
}

func (g *publicationGroup) member(claim pipelineobligation.Claim) (*publicationGroupMember, error) {
	for _, member := range g.members {
		if member.claim == claim {
			return member, nil
		}
	}
	return nil, pipelineobligation.ErrStaleClaim
}

func (g *publicationGroup) ValidateCommittedMembership(claims []pipelineobligation.Claim) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.validateCommittedMembership(claims)
}

// The seal, exact claims and acknowledged committed flag are immutable local
// evidence after publication. Registry/session liveness is deliberately absent:
// retiring this group's callbacks cannot consume anyone else's capability.
func (g *publicationGroup) validateCommittedMembership(claims []pipelineobligation.Claim) error {
	if !g.sealed || !g.committed || len(claims) > fanoutobligation.MaxChunkSize {
		return errors.New("publication group has no exact committed attempt")
	}
	seen := make(map[int]bool)
	for _, claim := range claims {
		member, err := g.member(claim)
		if err != nil {
			return err
		}
		if member.ordinal < g.intent.Cursor || member.ordinal >= g.end || !member.prepared || member.preparationRejected || seen[member.ordinal] {
			return errors.New("committed publication set contains duplicate or excluded member")
		}
		seen[member.ordinal] = true
	}
	for _, member := range g.members {
		if member.ordinal < g.end && !member.preparationRejected && !seen[member.ordinal] {
			return errors.New("committed publication set omits accepted member")
		}
	}
	return nil
}

func (g *publicationGroup) ValidateCommitted(ctx context.Context, claims []pipelineobligation.Claim) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.validateCommittedMembership(claims); err != nil {
		return err
	}
	if g.closed {
		return errors.New("publication group has no live committed attempt")
	}
	for _, claim := range claims {
		member, _ := g.member(claim)
		if err := g.current(member); err != nil {
			return err
		}
	}
	operation := func(ctx context.Context, tx *sql.Tx) error {
		owned, err := g.admission.ObserveFanOutRunTx(ctx, tx, g.grant, g.claim.Key.RunID)
		if err != nil {
			return err
		}
		if !owned {
			return pipelineobligation.ErrStaleClaim
		}
		ready, err := fanOutRunAcceptsTurnTx(ctx, tx, g.claim.Key.RunID, false)
		if err != nil {
			return err
		}
		if !ready {
			return pipelineobligation.ErrStaleClaim
		}
		return nil
	}
	if g.postgres != nil {
		return g.postgres.backend.RunReadTransaction(ctx, operation)
	}
	return g.sqlite.backend.RunReadTransaction(ctx, operation)
}

func (g *publicationGroup) current(member *publicationGroupMember) error {
	var state *pipelineClaimState
	var err error
	if g.postgres != nil {
		state, err = g.postgres.postgresPipelineClaimState(member.claim)
	} else {
		state, err = g.sqlite.sqlitePipelineClaimState(member.claim)
	}
	if err != nil {
		return err
	}
	if state != member.state || state.operationMu.group != g {
		return pipelineobligation.ErrStaleClaim
	}
	if g.postgres != nil && state.postgresLease.Session() != g.session {
		return pipelineobligation.ErrStaleClaim
	}
	return nil
}

func (g *publicationGroup) releaseMember(ctx context.Context, member *publicationGroupMember) error {
	member.state.operationMu.member.Lock()
	defer member.state.operationMu.member.Unlock()
	if err := g.current(member); err != nil {
		if errors.Is(err, pipelineobligation.ErrStaleClaim) {
			return nil
		}
		return err
	}
	if g.postgres != nil {
		return g.postgres.releasePostgresPipelineClaimLocked(ctx, member.claim, member.state)
	}
	return g.sqlite.releaseSQLitePipelineClaimLocked(member.claim, member.state)
}

func (g *publicationGroup) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	var err error
	for _, member := range g.members {
		err = errors.Join(err, g.releaseMember(context.WithoutCancel(ctx), member))
	}
	if g.releaseSession != nil {
		err = errors.Join(err, g.releaseSession())
		g.releaseSession = nil
	}
	g.registry.remove(g)
	return err
}

func (g *publicationGroup) validateCommittedMemberTx(ctx context.Context, tx pipelineQueryer, member *publicationGroupMember) error {
	if !g.sealed || member.ordinal >= g.end {
		return errors.New("publication group lacks exact sealed attempt evidence")
	}
	key := g.claim.Key
	var kind, eventID string
	query := `SELECT outcome_kind,COALESCE(event_id,'') FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	if g.postgres != nil {
		query = `SELECT outcome_kind,COALESCE(event_id::text,'') FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	}
	err := tx.QueryRowContext(ctx, query, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, member.ordinal).Scan(&kind, &eventID)
	if err != nil {
		return err
	}
	if kind != string(fanoutobligation.OutcomeCommitted) || eventID != member.event.ID() {
		return errors.New("publication group durable ordinal identity conflicts")
	}
	// Read and admit this exact singleton once in the current transaction.
	var admitted events.AdmittedEvent
	var found bool
	if g.postgres != nil {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	}
	if err != nil {
		return err
	}
	if !found {
		return errors.New("publication group committed event is absent or corrupt")
	}
	return validatePublicationMemberEvent(member, admitted)
}

func validatePublicationMemberEvent(member *publicationGroupMember, admitted events.AdmittedEvent) error {
	if admitted.Event().RunID() == "" {
		return errors.New("publication group committed event is absent or corrupt")
	}
	want, err := events.IntegrityProjection(member.event)
	if err != nil {
		return err
	}
	actual, err := events.IntegrityProjection(admitted.Event())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, actual) {
		return errors.New("publication group committed event changed")
	}
	return nil
}

// This evidence is local to one transaction invocation, never a replacement for
// the fresh settlement observation or the group's immutable membership proof.
func (g *publicationGroup) validateCommittedMembersTx(ctx context.Context, tx pipelineQueryer, members []*publicationGroupMember) error {
	if len(members) > fanoutobligation.MaxChunkSize {
		return errors.New("publication settlement exceeds bounded range")
	}
	if len(members) == 0 {
		return nil
	}
	if len(members) == 1 {
		return g.validateCommittedMemberTx(ctx, tx, members[0])
	}
	// On invalid evidence retain the singleton's member-order error precedence,
	// missing-event error and canonical corruption wrappers. No writes precede it.
	individual := func(fallback error) error {
		for _, member := range members {
			if err := g.validateCommittedMemberTx(ctx, tx, member); err != nil {
				return err
			}
		}
		// Never turn failed batch evidence into authority if an independent
		// writer changes it while the error-path reads are in progress.
		return fallback
	}
	key := g.claim.Key
	args := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath}
	placeholders := make([]string, len(members))
	ids := make([]string, len(members))
	wanted := make(map[int]bool, len(members))
	for i, member := range members {
		if !g.sealed || member.ordinal >= g.end {
			return individual(errors.New("publication group lacks exact sealed attempt evidence"))
		}
		if wanted[member.ordinal] {
			return errors.New("duplicate or excluded settlement member")
		}
		wanted[member.ordinal] = true
		args = append(args, member.ordinal)
		placeholders[i] = fmt.Sprintf("$%d", len(args))
		ids[i] = member.event.ID()
	}
	eventIDColumn := "event_id"
	if g.postgres != nil {
		eventIDColumn = "event_id::text"
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,outcome_kind,COALESCE(`+eventIDColumn+`,'') FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return err
	}
	type outcome struct{ kind, eventID string }
	outcomes := make(map[int]outcome, len(members))
	for rows.Next() {
		var ordinal int
		var fact outcome
		if err := rows.Scan(&ordinal, &fact.kind, &fact.eventID); err != nil {
			rows.Close()
			return individual(err)
		}
		if !wanted[ordinal] {
			rows.Close()
			return errors.New("publication group durable ordinal identity conflicts")
		}
		delete(wanted, ordinal)
		outcomes[ordinal] = fact
	}
	readErr := rows.Err()
	closeErr := rows.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	for _, member := range members {
		fact, found := outcomes[member.ordinal]
		if !found {
			return individual(sql.ErrNoRows)
		}
		if fact.kind != string(fanoutobligation.OutcomeCommitted) || fact.eventID != member.event.ID() {
			return individual(errors.New("publication group durable ordinal identity conflicts"))
		}
	}
	var admitted []eventrecord.AdmittedRecord
	if g.postgres != nil {
		admitted, err = eventrecordpostgres.LoadAdmittedMany(ctx, tx, ids)
	} else {
		admitted, err = eventrecordsqlite.LoadAdmittedMany(ctx, tx, ids)
	}
	if err != nil {
		if errors.Is(err, eventrecord.ErrMissing) || errors.Is(err, eventrecord.ErrCorrupt) {
			if errors.Is(err, eventrecord.ErrMissing) {
				err = errors.New("publication group committed event is absent or corrupt")
			}
			return individual(err)
		}
		// SQL failures may abort a PostgreSQL transaction. Preserve the batch
		// loader's context and wrapped native cause, without attempting a replay.
		return err
	}
	for i, member := range members {
		if err := validatePublicationMemberEvent(member, admitted[i].Event); err != nil {
			return individual(err)
		}
	}
	return nil
}

func (g *publicationGroup) admitSettlementTx(ctx context.Context, tx *sql.Tx) error {
	ready, err := admitFanOutRun(ctx, tx, g.admission, g.grant, g.claim.Key.RunID, false)
	if err != nil {
		return err
	}
	if !ready {
		return pipelineobligation.ErrStaleClaim
	}
	return nil
}

func (g *publicationGroup) settlementMembers(members []pipelineobligation.PublicationSettlementMember) ([]*publicationGroupMember, error) {
	if len(members) > fanoutobligation.MaxChunkSize {
		return nil, errors.New("publication settlement exceeds bounded range")
	}
	selected := make([]*publicationGroupMember, 0, len(members))
	seen := make(map[int]bool)
	for _, request := range members {
		if err := request.Disposition.ValidateFor(request.Claim.Purpose()); err != nil {
			return nil, err
		}
		if !request.Disposition.Terminal() {
			return nil, errors.New("publication group accepts only terminal dispositions")
		}
		member, err := g.member(request.Claim)
		if err != nil {
			return nil, err
		}
		if seen[member.ordinal] || member.ordinal >= g.end {
			return nil, errors.New("duplicate or excluded settlement member")
		}
		seen[member.ordinal] = true
		selected = append(selected, member)
	}
	return selected, nil
}

func (g *publicationGroup) Settle(ctx context.Context, requests []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out pipelineobligation.PublicationGroupOutcome
	if g.closed || !g.sealed || !g.committed {
		return out, errors.New("publication group is not a live committed attempt")
	}
	members, err := g.settlementMembers(requests)
	if err != nil {
		return out, err
	}
	if len(members) == 0 {
		return out, nil
	}
	locked := append([]*publicationGroupMember(nil), members...)
	sort.Slice(locked, func(i, j int) bool { return locked[i].event.ID() < locked[j].event.ID() })
	for _, member := range locked {
		member.state.operationMu.member.Lock()
	}
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].state.operationMu.member.Unlock()
		}
	}()
	for _, member := range members {
		if err := g.current(member); err != nil {
			return out, err
		}
	}
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return out, err
	}
	defer handoff.Rollback()
	dispositions := make(map[int]pipelineobligation.Disposition)
	for i, member := range members {
		dispositions[member.ordinal] = requests[i].Disposition
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ordinal < members[j].ordinal })
	effects := newRevisionEffects()
	operation := func(ctx context.Context, tx *sql.Tx) error {
		if err := handoff.ResetAttempt(); err != nil {
			return err
		}
		transactiontest.Mark(ctx, transactiontest.PipelineSettlement)
		if err := g.admitSettlementTx(ctx, tx); err != nil {
			return err
		}
		for _, member := range members {
			if err := g.current(member); err != nil {
				return err
			}
		}
		if err := g.validateCommittedMembersTx(ctx, tx, members); err != nil {
			return err
		}
		for _, member := range members {
			candidates, now := g.candidatesAndClock()
			if err := settlePipelineMemberTx(ctx, tx, g.postgres != nil, now(), member.claim, dispositions[member.ordinal], effects, candidates, handoff); err != nil {
				return err
			}
		}
		return nil
	}
	var committed bool
	if g.postgres != nil {
		committed, err = postgresbackend.RunAuthorityTransactionOutcome(ctx, g.session, func(ctx context.Context, tx *sql.Tx) error {
			if err := operation(ctx, tx); err != nil {
				return err
			}
			_, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects)
			return err
		})
	} else {
		committed, err = g.sqlite.runRuntimeMutationOutcome(ctx, "settle fan-out publication segment", effects, operation)
	}
	if !committed {
		return out, err
	}
	for _, member := range members {
		out.Results = append(out.Results, pipelineobligation.PublicationSettlementResult{Claim: member.claim, Outcome: pipelineobligation.CommittedSettlement(dispositions[member.ordinal].Successful())})
		if g.postgres != nil {
			if lease := member.state.postgresLease; lease != nil {
				err = errors.Join(err, lease.ReleaseTerminal(context.WithoutCancel(ctx)))
				member.state.postgresLease = nil
			}
		} else {
			err = errors.Join(err, g.sqlite.releaseSQLitePipelineClaimLocked(member.claim, member.state))
		}
	}
	return out, errors.Join(err, handoff.Commit())
}

func (g *publicationGroup) candidatesAndClock() (CompletionCandidateRequester, func() time.Time) {
	if g.postgres != nil {
		return g.postgres.candidateRequests, func() time.Time { return time.Now().UTC() }
	}
	return g.sqlite.candidateRequests, g.sqlite.now
}

var _ pipelineobligation.PublicationGroup = (*publicationGroup)(nil)

// Ungranted private fixtures cannot open production publication authority.
func (*PipelinePostgresOwner) BeginFanOutPublicationGroup(context.Context, fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	return nil, errors.New("fan-out publication group requires an exact granted owner")
}
func (*PipelineSQLiteOwner) BeginFanOutPublicationGroup(context.Context, fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	return nil, errors.New("fan-out publication group requires an exact granted owner")
}
