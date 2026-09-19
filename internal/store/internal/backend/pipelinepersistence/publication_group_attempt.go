package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func requireFanOutPublicationGroup(admission FanOutAdmission, group *publicationGroup, command pipeline.FanOutChunkCommand) error {
	if admission == nil || group != nil {
		return nil
	}
	for _, outcome := range command.Outcomes {
		if outcome.Publication != nil {
			return errors.New("granted fan-out publication requires its sealed publication group")
		}
	}
	return nil
}

func (g *publicationGroup) RecordPrepared(ctx context.Context, claim pipelineobligation.Claim, preparation pipelineobligation.PublicationPreparation) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.closed || g.sealed {
		return errors.New("publication group preparation is already sealed")
	}
	member, err := g.member(claim)
	if err != nil {
		return err
	}
	if member.prepared {
		return errors.New("publication group member preparation is already frozen")
	}
	if err := g.current(member); err != nil {
		return err
	}
	plan, ok := preparation.(runtimebus.EnginePublicationPlan)
	if !ok || plan.PublicationCommand().Commit.PipelineClaim != claim {
		return errors.New("publication preparation requires the exact canonical plan and claim")
	}
	if err := preparation.ValidatePreparedFanOutEvent(member.event); err != nil {
		return err
	}
	member.event = plan.PublicationCommand().Commit.Event.Event().Clone()
	member.prepared = true
	return nil
}

func (g *publicationGroup) lockAttempt(command pipeline.FanOutChunkCommand) (func(), error) {
	g.mu.Lock()
	if g.closed || !g.sealed || g.committed || g.restrictAfterRollback || command.Claim != g.claim || len(command.Outcomes) != g.end-g.intent.Cursor {
		g.mu.Unlock()
		return nil, errors.New("fan-out publication attempt does not match its sealed group")
	}
	var members []*publicationGroupMember
	for i, outcome := range command.Outcomes {
		if outcome.Ordinal != g.intent.Cursor+i {
			g.mu.Unlock()
			return nil, errors.New("publication attempt ordinal is not contiguous")
		}
		member := g.members[outcome.Ordinal]
		if outcome.Publication == nil {
			if member != nil && !member.preparationRejected {
				g.mu.Unlock()
				return nil, errors.New("publication attempt rejects a claimed event")
			}
			if err := outcome.Validate(); err != nil {
				g.mu.Unlock()
				return nil, err
			}
			continue
		}
		plan, ok := outcome.Publication.(runtimebus.EnginePublicationPlan)
		if !ok || member == nil || plan.PublicationCommand().Commit.PipelineClaim != member.claim {
			g.mu.Unlock()
			return nil, errors.New("publication attempt substitutes exact member claim")
		}
		actual, err := events.IntegrityProjection(plan.PublicationCommand().Commit.Event.Event())
		if err != nil {
			g.mu.Unlock()
			return nil, err
		}
		want, err := events.IntegrityProjection(member.event)
		if err != nil {
			g.mu.Unlock()
			return nil, err
		}
		if !reflect.DeepEqual(actual, want) {
			g.mu.Unlock()
			return nil, errors.New("publication attempt changes prepared event")
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].event.ID() < members[j].event.ID() })
	for _, member := range members {
		member.state.operationMu.member.Lock()
	}
	unlock := func() {
		for i := len(members) - 1; i >= 0; i-- {
			members[i].state.operationMu.member.Unlock()
		}
		g.mu.Unlock()
	}
	for _, member := range members {
		if err := g.current(member); err != nil {
			unlock()
			return nil, err
		}
	}
	return unlock, nil
}

func (g *publicationGroup) runPostgresPublication(ctx context.Context, effects *revisionEffects, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error) {
	if err := g.postgres.requireCurrentSchema(); err != nil {
		return false, err
	}
	return postgresbackend.RunAuthorityTransactionOutcome(ctx, g.session, func(ctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(ctx, tx, story); err != nil {
			return err
		}
		if err := story.Finalize(ctx); err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizePostgres(ctx, tx, effects)
		return err
	})
}
