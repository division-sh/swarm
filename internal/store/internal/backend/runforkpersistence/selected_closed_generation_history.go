package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

// ClosedSelectedGenerationHistory is partial cohort evidence, not a completion
// receipt. Operation, copied lineage, turn, lifetime and drain membership must
// still be proved before the reset owner may remove any live association.
type ClosedSelectedGenerationHistory struct {
	Execution                                                   runfork.SelectedContractRuntimeExecution
	BindingID                                                   string
	Preparation                                                 runfork.SelectedForkPreparationBinding
	PreparationJSON, DeclarationJSON, EvidenceJSON, FailureJSON json.RawMessage
	CreatedAt, UpdatedAt, TerminalAt                            time.Time
	Grants                                                      []activityjournal.SelectedGrantFact
}

// ClosedSelectedInstalledJournalCohort is not the full reset manifest. Copied
// journals belong to the complete closed cohort, never a producing generation.
// Other retained fact families remain separate required proofs.
type ClosedSelectedInstalledJournalCohort struct {
	Generations    []ClosedSelectedGenerationHistory
	Journals       []activityjournal.InstalledSelectedJournalHistory
	CopiedJournals []ClosedSelectedCopiedJournalHistory
}

func ReadClosedSelectedInstalledJournalCohortTx(ctx context.Context, tx *sql.Tx, forkRunID string, sqlite bool, original semanticview.OriginalLoopCarriage) (ClosedSelectedInstalledJournalCohort, error) {
	var absent ClosedSelectedInstalledJournalCohort
	generations, err := ReadClosedSelectedGenerationHistoryTx(ctx, tx, forkRunID, sqlite)
	if err != nil {
		return absent, err
	}
	var histories [][]activityjournal.SelectedGrantFact
	for _, fact := range generations {
		histories = append(histories, fact.Grants)
	}
	journals, candidates, err := activityjournal.ReadSelectedTerminalJournalHistoryTx(ctx, tx, forkRunID, histories, sqlite)
	if err != nil {
		return absent, err
	}
	var copied []ClosedSelectedCopiedJournalHistory
	if len(candidates) != 0 {
		permanent, err := readClosedSelectedPermanentForkCohortTx(ctx, tx, generations, sqlite)
		if err != nil {
			return absent, fmt.Errorf("copied journal requires its exact source-lineage membership owner: %w", err)
		}
		for _, candidate := range candidates {
			fact, err := readClosedSelectedCopiedJournalTx(ctx, tx, permanent, candidate, sqlite, original)
			if err != nil {
				return absent, fmt.Errorf("copied journal requires its exact source-lineage membership owner: %w", err)
			}
			copied = append(copied, fact)
		}
	}
	if err := ctx.Err(); err != nil {
		return absent, err
	}
	return ClosedSelectedInstalledJournalCohort{Generations: generations, Journals: journals, CopiedJournals: copied}, nil
}

// ReadClosedSelectedGenerationHistoryTx captures every generation after the
// original generation/reset fence, never through a latest-generation lookup.
// Its caller must retain this transaction through validation/application, or
// capture again after acquiring the reset fence in its eventual transaction.
func ReadClosedSelectedGenerationHistoryTx(ctx context.Context, tx *sql.Tx, forkRunID string, sqlite bool) ([]ClosedSelectedGenerationHistory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(forkRunID)
	if tx == nil || err != nil || id == uuid.Nil || id.String() != forkRunID {
		return nil, errors.New("closed selected history requires its native transaction and exact child UUID")
	}
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return nil, err
	}
	facts, err := readClosedSelectedGenerationRows(ctx, tx, forkRunID)
	if err != nil {
		return nil, err
	}
	for i := range facts {
		fact := &facts[i]
		if i != 0 && (fact.BindingID != facts[0].BindingID || fact.Execution.SourceRunID != facts[0].Execution.SourceRunID ||
			!sameSelectedForkPointIdentity(fact.Execution.ForkPoint, facts[0].Execution.ForkPoint)) {
			return nil, errors.New("closed selected generations disagree on their permanent binding point")
		}
		if err := requireForkLiveParentsTx(ctx, tx, fact.Execution.ExecutionID, true); err != nil {
			return nil, err
		}
		fact.Grants, err = activityjournal.ReadRetiredSelectedGrantHistoryTx(ctx, tx, fact.Execution.ExecutionID, sqlite)
		if err != nil {
			return nil, err
		}
		for _, grant := range fact.Grants {
			if err := validateClosedSelectedGrantFrame(*fact, grant); err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

func readClosedSelectedGenerationRows(ctx context.Context, tx *sql.Tx, runID string) (facts []ClosedSelectedGenerationHistory, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT execution_id,fork_run_id,source_run_id,binding_id,
		fork_point_kind,fork_revision,COALESCE(CAST(fork_event_id AS TEXT),''),generation,
		executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
		effective_config_fingerprint,declaration_plan_fingerprint,declaration_plan,preparation_fingerprint,preparation_binding,
		state,execution_owner,lease_expires_at,fence_generation,failure,evidence,created_at,updated_at,terminal_at,
		COUNT(*) OVER() AS history_count,MAX(generation) OVER() AS history_max_generation
		FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation`, runID)
	if err != nil {
		return nil, fmt.Errorf("read all selected execution generations: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			facts = nil
		}
	}()
	var total, maximum uint64
	seen := make(map[string]bool)
	for rows.Next() {
		var fact ClosedSelectedGenerationHistory
		e := &fact.Execution
		var kind string
		var revision int64
		var owner sql.NullString
		var lease, created, updated, terminal any
		var declaration, preparation, failure, evidence []byte
		var count, max uint64
		if err := rows.Scan(&e.ExecutionID, &e.ForkRunID, &e.SourceRunID, &fact.BindingID, &kind, &revision, &e.ForkEventID, &e.Generation,
			&e.ExecutableCoordinateFingerprint, &e.AdmissionFingerprint, &e.ContainerPlanFingerprint, &e.ActorCensusFingerprint,
			&e.EffectiveConfigFingerprint, &e.DeclarationPlanFingerprint, &declaration, &e.PreparationFingerprint, &preparation,
			&e.State, &owner, &lease, &e.FenceGeneration, &failure, &evidence, &created, &updated, &terminal, &count, &max); err != nil {
			return nil, fmt.Errorf("decode closed selected generation: %w", err)
		}
		fact.DeclarationJSON, fact.PreparationJSON, fact.FailureJSON, fact.EvidenceJSON = declaration, preparation, failure, evidence
		e.ExecutionOwner = owner.String
		e.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointKind(kind), Revision: revision, EventID: e.ForkEventID}
		if e.ForkRunID != runID || e.Generation != uint64(len(facts)+1) || seen[e.ExecutionID] || count == 0 || count != max ||
			len(facts) != 0 && (count != total || max != maximum) {
			return nil, errors.New("closed selected generation census has missing, repeated or inconsistent coordinates")
		}
		if e.State != "closed" || lease != nil || e.FenceGeneration == 0 {
			return nil, errors.New("selected generation is not closed without a live lease")
		}
		if err := decodeClosedSelectedGeneration(&fact, created, updated, terminal); err != nil {
			return nil, err
		}
		seen[e.ExecutionID], total, maximum = true, count, max
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate closed selected generations: %w", err)
	}
	if len(facts) == 0 || uint64(len(facts)) != total || total != maximum {
		return nil, errors.New("closed selected generation census is absent or incomplete")
	}
	return facts, nil
}

func decodeClosedSelectedGeneration(fact *ClosedSelectedGenerationHistory, created, updated, terminal any) error {
	e := fact.Execution
	for _, value := range []string{e.ExecutionID, e.ForkRunID, e.SourceRunID, fact.BindingID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return errors.New("closed selected generation has an inexact identity")
		}
	}
	if e.SourceRunID == e.ForkRunID {
		return errors.New("closed selected generation has no distinct source")
	}
	if err := e.ForkPoint.Validate(); err != nil {
		return err
	}
	if err := canonicaljson.DecodeInto(fact.PreparationJSON, &fact.Preparation); err != nil {
		return err
	}
	p := fact.Preparation
	fingerprint, err := p.Fingerprint()
	if err != nil {
		return err
	}
	coordinate, err := e.CoordinateFingerprint()
	if err != nil {
		return err
	}
	if fingerprint != e.PreparationFingerprint || coordinate != e.ExecutableCoordinateFingerprint || p.ForkRunID != e.ForkRunID ||
		p.SourceRunID != e.SourceRunID || !sameSelectedForkPointIdentity(p.ForkPoint, e.ForkPoint) || p.ForkEventID != e.ForkEventID ||
		p.DeclarationPlanFingerprint != e.DeclarationPlanFingerprint {
		return errors.New("closed selected generation conflicts with its exact preparation frame")
	}
	var declarations agenttopology.SelectedDeclarationPlan
	if err := canonicaljson.DecodeInto(fact.DeclarationJSON, &declarations); err != nil {
		return err
	}
	if err := declarations.Validate(); err != nil {
		return err
	}
	if declarations.Revision != e.DeclarationPlanFingerprint || declarations.BundleHash != p.Coordinates.BundleHash {
		return errors.New("closed selected generation conflicts with its captured declaration plan")
	}
	if len(fact.FailureJSON) != 0 && string(fact.FailureJSON) != "null" {
		if _, err := failures.UnmarshalEnvelope(fact.FailureJSON); err != nil {
			return err
		}
	}
	if _, err := canonicaljson.Decode(fact.EvidenceJSON); err != nil {
		return err
	}
	return decodeClosedSelectedGenerationClocks(fact, created, updated, terminal)
}

func decodeClosedSelectedGenerationClocks(fact *ClosedSelectedGenerationHistory, created, updated, terminal any) error {
	for _, clock := range []struct {
		raw    any
		target *time.Time
	}{{created, &fact.CreatedAt}, {updated, &fact.UpdatedAt}, {terminal, &fact.TerminalAt}} {
		value, present, err := runstate.DecodeBindingTimestamp(clock.raw)
		if err != nil {
			return err
		}
		if !present || value.IsZero() {
			return errors.New("closed selected generation lacks a physical clock")
		}
		*clock.target = value
	}
	if fact.TerminalAt.Before(fact.CreatedAt) || fact.UpdatedAt.Before(fact.TerminalAt) {
		return errors.New("closed selected generation has inconsistent physical clocks")
	}
	return nil
}

func validateClosedSelectedGrantFrame(fact ClosedSelectedGenerationHistory, grant activityjournal.SelectedGrantFact) error {
	e, g := fact.Execution, grant.Evidence
	b := g.SelectedFork
	if b == nil || b.BindingID != fact.BindingID || b.ForkRunID != e.ForkRunID || b.ExecutionID != e.ExecutionID ||
		b.ExecutionGeneration != e.Generation || b.FenceGeneration > e.FenceGeneration || b.ExecutionOwner != e.ExecutionOwner ||
		b.AdmissionFingerprint != e.AdmissionFingerprint || b.ContainerPlanFingerprint != e.ContainerPlanFingerprint ||
		b.ActorCensusFingerprint != e.ActorCensusFingerprint || b.EffectiveConfigFingerprint != e.EffectiveConfigFingerprint ||
		b.DeclarationPlanFingerprint != e.DeclarationPlanFingerprint || b.PreparationFingerprint != e.PreparationFingerprint ||
		g.BundleHash != fact.Preparation.Coordinates.BundleHash || g.RuntimeGeneration != e.Generation ||
		g.ProcessAuthorityID != fact.Preparation.Coordinates.ProcessAuthorityID ||
		g.ProcessOwnerID != fact.Preparation.Coordinates.ProcessOwnerID || g.ProcessBootID != fact.Preparation.Coordinates.ProcessBootID {
		return errors.New("retired grant history differs from its closed selected execution frame")
	}
	return nil
}
