package cataloge2e

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type acceptedReturnPublicationProbe struct {
	runforkexecution.SelectedContractReplayPersistence
	selected unclaimedFanOutReplyStore
	done     chan error
	mu       sync.Mutex
	seen     bool
}

func (p *acceptedReturnPublicationProbe) CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (result bus.CommittedSelectedForkEvent, err error) {
	if len(request.Commit.ReplyReturns) == 0 {
		return p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	}
	p.mu.Lock()
	already := p.seen
	p.seen = true
	p.mu.Unlock()
	if already {
		return result, fmt.Errorf("accepted return executed its publication more than once")
	}
	defer func() { p.done <- err }()
	retained := request.Commit.ReplyReturns[0].Record.Normalized()
	read := func() error {
		actual, err := p.selected.LoadReplyContext(ctx, retained.ID)
		if err != nil || !reflect.DeepEqual(actual, retained) {
			return fmt.Errorf("publication changed the retained terminal row: actual=%+v error=%v", actual, err)
		}
		return nil
	}
	if err := read(); err != nil {
		return result, err
	}
	proof, err := storetest.ProveSelectedReplyReturnPublicationRollback(ctx, p.selected, request)
	want := storetest.SelectedReplyReturnPublicationCounts{Events: 1, Deliveries: len(request.Commit.DeliveryRoutes), Lineages: 1, Scopes: 1}
	if err != nil || proof.Drafted != want || proof.AfterRollback != (storetest.SelectedReplyReturnPublicationCounts{}) {
		return result, fmt.Errorf("real accepted-return late-SQL rollback: proof=%+v error=%v", proof, err)
	}
	if err := read(); err != nil {
		return result, err
	}
	bad := request
	bad.Commit.ReplyReturns = append([]pinrouting.ReplyReturnContinuation(nil), request.Commit.ReplyReturns...)
	bad.Commit.ReplyReturns[0].Record = retained.Normalized()
	bad.Commit.ReplyReturns[0].Record.AcceptedReplyEventID = uuid.NewString()
	refused, refusal := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, bad)
	if refusal == nil || refused.AppendOutcome != bus.EventAppendOutcomeUnknown {
		return result, fmt.Errorf("altered terminal evidence did not refuse atomically: result=%+v error=%v", refused, refusal)
	}
	counts, err := storetest.ReadSelectedReplyReturnPublicationCounts(ctx, p.selected, request)
	if err != nil || counts != (storetest.SelectedReplyReturnPublicationCounts{}) {
		return result, fmt.Errorf("hostile publication leaked facts: counts=%+v error=%v", counts, err)
	}
	if err := read(); err != nil {
		return result, err
	}
	result, err = p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	if err != nil || result.AppendOutcome != bus.EventAppendInserted {
		return result, fmt.Errorf("original publication after rollback: result=%+v error=%v", result, err)
	}
	before, err := storetest.ReadSelectedReplyReturnPublicationCounts(ctx, p.selected, request)
	if err != nil || before != want {
		return result, fmt.Errorf("committed accepted-return evidence: counts=%+v error=%v", before, err)
	}
	repeated, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	if err != nil || repeated.AppendOutcome != bus.EventAppendExactDuplicate || len(repeated.DeliveryHandoffs) != 0 {
		return result, fmt.Errorf("exact whole-publication duplicate: result=%+v error=%v", repeated, err)
	}
	after, err := storetest.ReadSelectedReplyReturnPublicationCounts(ctx, p.selected, request)
	if err != nil || after != before {
		return result, fmt.Errorf("duplicate publication changed durable cardinalities: before=%+v after=%+v error=%v", before, after, err)
	}
	if err := read(); err != nil {
		return result, err
	}
	return result, nil
}
