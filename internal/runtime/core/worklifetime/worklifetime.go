package worklifetime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrAdmissionFenced = errors.New("work admission is fenced")
	ErrRetired         = errors.New("work occurrence is retired")
	ErrAlreadySettled  = errors.New("work lease is already settled")
)

type gateState uint8

const (
	gateOpen gateState = iota
	gateFenced
	gateRetired
)

// gate is the only process-local work counter. It is deliberately private:
// callers operate through one of the fixed occurrence types below.
type gate struct {
	mu               sync.Mutex
	state            gateState
	active           uint64
	activeTransient  uint64
	drained          chan struct{}
	transientDrained chan struct{}
	ctx              context.Context
	cancel           context.CancelCauseFunc
}

func newGate(parent context.Context) *gate {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	drained := make(chan struct{})
	close(drained)
	transientDrained := make(chan struct{})
	close(transientDrained)
	return &gate{ctx: ctx, cancel: cancel, drained: drained, transientDrained: transientDrained}
}

type Lease struct {
	mu        sync.Mutex
	gate      *gate
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stop      func() bool
	parent    *Lease
	transient bool
	settled   bool
}

func (g *gate) begin(parent context.Context) (*Lease, error) {
	return g.beginClass(parent, true)
}

func (g *gate) beginStanding(parent context.Context) (*Lease, error) {
	return g.beginClass(parent, false)
}

func (g *gate) beginClass(parent context.Context, transient bool) (*Lease, error) {
	if g == nil {
		return nil, errors.New("work occurrence is required")
	}
	if parent == nil {
		parent = context.Background()
	}
	g.mu.Lock()
	if g.ctx.Err() != nil {
		g.mu.Unlock()
		return nil, ErrRetired
	}
	switch g.state {
	case gateFenced:
		g.mu.Unlock()
		return nil, ErrAdmissionFenced
	case gateRetired:
		g.mu.Unlock()
		return nil, ErrRetired
	}
	if g.active == 0 {
		g.drained = make(chan struct{})
	}
	g.active++
	if transient {
		if g.activeTransient == 0 {
			g.transientDrained = make(chan struct{})
		}
		g.activeTransient++
	}
	g.mu.Unlock()

	ctx, cancel := context.WithCancelCause(parent)
	lease := &Lease{gate: g, ctx: ctx, cancel: cancel, transient: transient}
	lease.stop = context.AfterFunc(g.ctx, func() {
		cancel(ErrRetired)
	})
	return lease, nil
}

func (l *Lease) Context() context.Context {
	if l == nil || l.ctx == nil {
		return context.Background()
	}
	return l.ctx
}

func (l *Lease) Done() error {
	if l == nil {
		return errors.New("work lease is required")
	}
	l.mu.Lock()
	if l.settled {
		l.mu.Unlock()
		return ErrAlreadySettled
	}
	l.settled = true
	g := l.gate
	stop := l.stop
	cancel := l.cancel
	l.mu.Unlock()

	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel(nil)
	}
	var settleErr error
	if g == nil {
		settleErr = errors.New("work lease has no occurrence owner")
	} else {
		g.mu.Lock()
		if g.active == 0 {
			settleErr = errors.New("work occurrence accounting underflow")
		} else {
			g.active--
			if g.active == 0 {
				close(g.drained)
			}
			if l.transient {
				if g.activeTransient == 0 {
					settleErr = errors.Join(settleErr, errors.New("transient work occurrence accounting underflow"))
				} else {
					g.activeTransient--
					if g.activeTransient == 0 {
						close(g.transientDrained)
					}
				}
			}
		}
		g.mu.Unlock()
	}
	if l.parent != nil {
		settleErr = errors.Join(settleErr, l.parent.Done())
	}
	return settleErr
}

func (g *gate) admits() error {
	if g == nil {
		return errors.New("work occurrence is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ctx.Err() != nil {
		return ErrRetired
	}
	switch g.state {
	case gateFenced:
		return ErrAdmissionFenced
	case gateRetired:
		return ErrRetired
	default:
		return nil
	}
}

func (g *gate) fence() error {
	if g == nil {
		return errors.New("work occurrence is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == gateRetired {
		return ErrRetired
	}
	g.state = gateFenced
	return nil
}

func (g *gate) reopen() error {
	if g == nil {
		return errors.New("work occurrence is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == gateRetired {
		return ErrRetired
	}
	if g.activeTransient != 0 {
		return fmt.Errorf("work occurrence still owns %d active finite lease(s)", g.activeTransient)
	}
	g.state = gateOpen
	return nil
}

func (g *gate) retire() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.state == gateRetired {
		g.mu.Unlock()
		return
	}
	g.state = gateRetired
	cancel := g.cancel
	g.mu.Unlock()
	cancel(ErrRetired)
}

func (g *gate) wait(ctx context.Context) error {
	return g.waitClass(ctx, false)
}

func (g *gate) waitTransient(ctx context.Context) error {
	return g.waitClass(ctx, true)
}

func (g *gate) waitClass(ctx context.Context, transientOnly bool) error {
	if g == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	active := g.active
	drained := g.drained
	if transientOnly {
		active = g.activeTransient
		drained = g.transientDrained
	}
	if active == 0 {
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		active := g.activeCount()
		if transientOnly {
			active = g.activeTransientCount()
		}
		if active != 0 {
			return fmt.Errorf("wait for %d active work lease(s): %w", active, ctx.Err())
		}
		return nil
	}
}

func (g *gate) activeTransientCount() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.activeTransient
}

func (g *gate) activeCount() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

type ownedOccurrence struct {
	id          string
	gate        *gate
	parentLease *Lease
	finishMu    sync.Mutex
	finished    bool
}

func newOwnedOccurrence(parent context.Context, parentLease *Lease) *ownedOccurrence {
	return &ownedOccurrence{id: uuid.NewString(), gate: newGate(parent), parentLease: parentLease}
}

func (o *ownedOccurrence) newChild(parentLease *Lease) *ownedOccurrence {
	if o == nil || o.gate == nil {
		return nil
	}
	return newOwnedOccurrence(o.gate.ctx, parentLease)
}

func (o *ownedOccurrence) begin(ctx context.Context) (*Lease, error) {
	if o == nil {
		return nil, errors.New("work occurrence is required")
	}
	return o.gate.begin(ctx)
}

func (o *ownedOccurrence) fence() error {
	if o == nil {
		return errors.New("work occurrence is required")
	}
	return o.gate.fence()
}

func (o *ownedOccurrence) reopen() error {
	if o == nil {
		return errors.New("work occurrence is required")
	}
	return o.gate.reopen()
}

func (o *ownedOccurrence) retire() {
	if o != nil {
		o.gate.retire()
	}
}

func (o *ownedOccurrence) wait(ctx context.Context) error {
	if o == nil {
		return nil
	}
	return o.gate.wait(ctx)
}

func (o *ownedOccurrence) finish(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.retire()
	if err := o.wait(ctx); err != nil {
		return err
	}
	o.finishMu.Lock()
	defer o.finishMu.Unlock()
	if o.finished {
		return nil
	}
	if o.parentLease != nil {
		if err := o.parentLease.Done(); err != nil {
			return fmt.Errorf("settle parent occurrence lease: %w", err)
		}
	}
	o.finished = true
	return nil
}

type Process struct {
	occurrence *ownedOccurrence
}

type processContextKey struct{}

func WithProcess(ctx context.Context, process *Process) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if process == nil {
		return ctx
	}
	return context.WithValue(ctx, processContextKey{}, process)
}

func ProcessFromContext(ctx context.Context) (*Process, bool) {
	if ctx == nil {
		return nil, false
	}
	process, ok := ctx.Value(processContextKey{}).(*Process)
	return process, ok && process != nil
}

type ProcessJoinReceipt struct {
	processID string
}

func NewProcess() *Process {
	return &Process{occurrence: newOwnedOccurrence(context.Background(), nil)}
}

func (p *Process) Begin(ctx context.Context) (*Lease, error) {
	if p == nil {
		return nil, errors.New("process work owner is required")
	}
	return p.occurrence.begin(ctx)
}

func (p *Process) Fence() error {
	if p == nil {
		return errors.New("process work owner is required")
	}
	return p.occurrence.fence()
}

func (p *Process) Retire() {
	if p != nil {
		p.occurrence.retire()
	}
}

func (p *Process) Wait(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return p.occurrence.wait(ctx)
}

func (p *Process) Join(ctx context.Context) (*ProcessJoinReceipt, error) {
	if p == nil {
		return nil, errors.New("process work owner is required")
	}
	if err := p.occurrence.finish(ctx); err != nil {
		return nil, err
	}
	return &ProcessJoinReceipt{processID: p.occurrence.id}, nil
}

func (p *Process) ValidateJoinReceipt(receipt *ProcessJoinReceipt) error {
	if p == nil || receipt == nil || receipt.processID == "" || receipt.processID != p.occurrence.id {
		return errors.New("process join receipt does not belong to selected-store owner")
	}
	return nil
}

func (p *Process) ActiveCount() uint64 {
	if p == nil {
		return 0
	}
	return p.occurrence.gate.activeCount()
}

type RuntimeIdentity struct {
	RuntimeInstanceID string
	BundleHash        string
}

func (i RuntimeIdentity) validate() error {
	if strings.TrimSpace(i.RuntimeInstanceID) == "" || strings.TrimSpace(i.BundleHash) == "" {
		return errors.New("runtime occurrence requires runtime_instance_id and bundle_hash")
	}
	return nil
}

type RuntimeOccurrence struct {
	occurrence *ownedOccurrence
	identity   RuntimeIdentity
}

type ManagerRunIdentity struct {
	Generation uint64
}

func (i ManagerRunIdentity) validate() error {
	if i.Generation == 0 {
		return errors.New("manager run occurrence requires generation")
	}
	return nil
}

// ManagerRunOccurrence owns every process-local operation admitted by one
// AgentManager run. It remains distinct from the ambient runtime or standing
// occurrence so shutdown can fence and join all Manager work through one
// retained transition without losing the ambient owner's accounting.
type ManagerRunOccurrence struct {
	occurrence *ownedOccurrence
	identity   ManagerRunIdentity
}

// ManagerWorkOccurrence is the fixed projection carried by one Manager work
// context. Descendants admitted from that context retain both the Manager
// generation and its exact ambient runtime child, when one exists.
type ManagerWorkOccurrence struct {
	manager   *ManagerRunOccurrence
	companion Occurrence
}

func NewManagerRunOccurrence(ctx context.Context, parent Occurrence, identity ManagerRunIdentity) (*ManagerRunOccurrence, error) {
	if parent == nil {
		return nil, errors.New("manager run occurrence requires a parent work occurrence")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := parent.BeginStanding(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit manager run occurrence: %w", err)
	}
	return &ManagerRunOccurrence{
		occurrence: newOwnedOccurrence(parentLease.Context(), parentLease),
		identity:   identity,
	}, nil
}

// Begin admits Manager work and optionally composes the ambient standing or
// selected-fork owner into the same exactly-once settlement.
func (m *ManagerRunOccurrence) Begin(ctx context.Context, companion Occurrence) (*Lease, error) {
	return m.beginClass(ctx, companion, false)
}

// BeginStanding admits generation-long process-local work. Standing work is
// joined by retirement but does not prevent callers from observing that all
// finite operations admitted by the generation have completed.
func (m *ManagerRunOccurrence) BeginStanding(ctx context.Context) (*Lease, error) {
	return m.beginClass(ctx, nil, true)
}

func (m *ManagerRunOccurrence) beginClass(ctx context.Context, companion Occurrence, standing bool) (*Lease, error) {
	if m == nil {
		return nil, errors.New("manager run occurrence is required")
	}
	companion = normalizeManagerCompanion(m, companion)
	projection := &ManagerWorkOccurrence{manager: m, companion: companion}
	if companion == nil {
		var lease *Lease
		var err error
		if standing {
			lease, err = m.occurrence.gate.beginStanding(ctx)
		} else {
			lease, err = m.occurrence.begin(ctx)
		}
		if err != nil {
			return nil, err
		}
		lease.ctx = WithOccurrence(lease.Context(), projection)
		return lease, nil
	}
	var companionLease *Lease
	var err error
	if standing {
		companionLease, err = companion.BeginStanding(ctx)
	} else {
		companionLease, err = companion.Begin(ctx)
	}
	if err != nil {
		return nil, err
	}
	var lease *Lease
	if standing {
		lease, err = m.occurrence.gate.beginStanding(companionLease.Context())
	} else {
		lease, err = m.occurrence.begin(companionLease.Context())
	}
	if err != nil {
		_ = companionLease.Done()
		return nil, err
	}
	lease.parent = companionLease
	lease.ctx = WithOccurrence(lease.Context(), projection)
	return lease, nil
}

func normalizeManagerCompanion(manager *ManagerRunOccurrence, companion Occurrence) Occurrence {
	for companion != nil {
		current, ok := companion.(*ManagerWorkOccurrence)
		if !ok || current == nil || current.manager != manager {
			return companion
		}
		companion = current.companion
	}
	return nil
}

func (m *ManagerWorkOccurrence) Begin(ctx context.Context) (*Lease, error) {
	if m == nil || m.manager == nil {
		return nil, errors.New("manager work occurrence is required")
	}
	return m.manager.Begin(ctx, m.companion)
}

func (m *ManagerWorkOccurrence) BeginStanding(ctx context.Context) (*Lease, error) {
	if m == nil || m.manager == nil {
		return nil, errors.New("manager work occurrence is required")
	}
	return m.manager.beginClass(ctx, m.companion, true)
}

func (m *ManagerWorkOccurrence) Wait(ctx context.Context) error {
	if m == nil || m.manager == nil {
		return nil
	}
	if err := m.manager.occurrence.wait(ctx); err != nil {
		return err
	}
	if m.companion != nil {
		return m.companion.Wait(ctx)
	}
	return nil
}

func (m *ManagerWorkOccurrence) WaitForQuiescence(ctx context.Context) error {
	if m == nil || m.manager == nil {
		return nil
	}
	if err := m.manager.WaitForQuiescence(ctx); err != nil {
		return err
	}
	if m.companion != nil {
		return m.companion.WaitForQuiescence(ctx)
	}
	return nil
}

func (m *ManagerWorkOccurrence) NewRoute(ctx context.Context, identity RouteIdentity) (*RouteOccurrence, error) {
	if m == nil || m.manager == nil {
		return nil, errors.New("manager work occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	probe, err := m.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit manager route occurrence: %w", err)
	}
	if err := probe.Done(); err != nil {
		return nil, err
	}
	identity.AgentID = strings.TrimSpace(identity.AgentID)
	return &RouteOccurrence{occurrence: m.manager.occurrence.newChild(nil), identity: identity, owner: m}, nil
}

func (m *ManagerRunOccurrence) Fence() error {
	if m == nil {
		return errors.New("manager run occurrence is required")
	}
	return m.occurrence.fence()
}

func (m *ManagerRunOccurrence) Retire() {
	if m != nil {
		m.occurrence.retire()
	}
}

func (m *ManagerRunOccurrence) Wait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.occurrence.wait(ctx)
}

func (m *ManagerRunOccurrence) WaitForQuiescence(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.occurrence.gate.waitTransient(ctx)
}

func (m *ManagerRunOccurrence) RetireAndWait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.occurrence.finish(ctx)
}

func (m *ManagerRunOccurrence) Identity() ManagerRunIdentity {
	if m == nil {
		return ManagerRunIdentity{}
	}
	return m.identity
}

type occurrenceContextKey struct{}

func WithOccurrence(ctx context.Context, occurrence Occurrence) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if occurrence == nil {
		return ctx
	}
	return context.WithValue(ctx, occurrenceContextKey{}, occurrence)
}

func OccurrenceFromContext(ctx context.Context) (Occurrence, bool) {
	if ctx == nil {
		return nil, false
	}
	occurrence, ok := ctx.Value(occurrenceContextKey{}).(Occurrence)
	return occurrence, ok && occurrence != nil
}

func WithRuntimeOccurrence(ctx context.Context, occurrence *RuntimeOccurrence) context.Context {
	return WithOccurrence(ctx, occurrence)
}

func RuntimeOccurrenceFromContext(ctx context.Context) (*RuntimeOccurrence, bool) {
	occurrence, ok := ctx.Value(occurrenceContextKey{}).(*RuntimeOccurrence)
	return occurrence, ok && occurrence != nil
}

type RuntimeRetirementReceipt struct {
	occurrenceID string
}

func (p *Process) NewRuntime(ctx context.Context, identity RuntimeIdentity) (*RuntimeOccurrence, error) {
	if p == nil {
		return nil, errors.New("process work owner is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := p.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit runtime occurrence: %w", err)
	}
	return &RuntimeOccurrence{
		occurrence: p.occurrence.newChild(parentLease),
		identity: RuntimeIdentity{
			RuntimeInstanceID: strings.TrimSpace(identity.RuntimeInstanceID),
			BundleHash:        strings.TrimSpace(identity.BundleHash),
		},
	}, nil
}

func (r *RuntimeOccurrence) Begin(ctx context.Context) (*Lease, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	return r.occurrence.begin(WithOccurrence(ctx, r))
}

func (r *RuntimeOccurrence) BeginStanding(ctx context.Context) (*Lease, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	return r.occurrence.gate.beginStanding(WithOccurrence(ctx, r))
}

func (r *RuntimeOccurrence) Fence() error {
	if r == nil {
		return errors.New("runtime occurrence is required")
	}
	return r.occurrence.fence()
}

func (r *RuntimeOccurrence) Reopen() error {
	if r == nil {
		return errors.New("runtime occurrence is required")
	}
	return r.occurrence.reopen()
}

func (r *RuntimeOccurrence) Retire() {
	if r != nil {
		r.occurrence.retire()
	}
}

func (r *RuntimeOccurrence) Wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.occurrence.wait(ctx)
}

func (r *RuntimeOccurrence) WaitForQuiescence(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.occurrence.gate.waitTransient(ctx)
}

func (r *RuntimeOccurrence) RetireAndWait(ctx context.Context) (*RuntimeRetirementReceipt, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	if err := r.occurrence.finish(ctx); err != nil {
		return nil, err
	}
	return &RuntimeRetirementReceipt{occurrenceID: r.occurrence.id}, nil
}

func (r *RuntimeOccurrence) Identity() RuntimeIdentity {
	if r == nil {
		return RuntimeIdentity{}
	}
	return r.identity
}

func (r *RuntimeOccurrence) ActiveCount() uint64 {
	if r == nil {
		return 0
	}
	return r.occurrence.gate.activeCount()
}

type StandingIdentity struct {
	ServiceID  string
	RunID      string
	Generation uint64
}

func (i StandingIdentity) validate() error {
	if strings.TrimSpace(i.ServiceID) == "" || strings.TrimSpace(i.RunID) == "" || i.Generation == 0 {
		return errors.New("standing occurrence requires service_id, run_id, and durable generation")
	}
	return nil
}

type StandingOccurrence struct {
	occurrence *ownedOccurrence
	identity   StandingIdentity
}

func (r *RuntimeOccurrence) NewStanding(ctx context.Context, identity StandingIdentity) (*StandingOccurrence, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := r.BeginStanding(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit standing occurrence: %w", err)
	}
	identity.ServiceID = strings.TrimSpace(identity.ServiceID)
	identity.RunID = strings.TrimSpace(identity.RunID)
	return &StandingOccurrence{occurrence: r.occurrence.newChild(parentLease), identity: identity}, nil
}

func (s *StandingOccurrence) Begin(ctx context.Context) (*Lease, error) {
	if s == nil {
		return nil, errors.New("standing occurrence is required")
	}
	return s.occurrence.begin(WithOccurrence(ctx, s))
}

func (s *StandingOccurrence) BeginStanding(ctx context.Context) (*Lease, error) {
	if s == nil {
		return nil, errors.New("standing occurrence is required")
	}
	return s.occurrence.gate.beginStanding(WithOccurrence(ctx, s))
}

func (s *StandingOccurrence) Fence() error {
	if s == nil {
		return errors.New("standing occurrence is required")
	}
	return s.occurrence.fence()
}

func (s *StandingOccurrence) Reopen() error {
	if s == nil {
		return errors.New("standing occurrence is required")
	}
	return s.occurrence.reopen()
}

func (s *StandingOccurrence) Wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.wait(ctx)
}

func (s *StandingOccurrence) WaitForQuiescence(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.gate.waitTransient(ctx)
}

func (s *StandingOccurrence) Retire() {
	if s != nil {
		s.occurrence.retire()
	}
}

func (s *StandingOccurrence) RetireAndWait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.finish(ctx)
}

func (s *StandingOccurrence) Identity() StandingIdentity {
	if s == nil {
		return StandingIdentity{}
	}
	return s.identity
}

type RouteIdentity struct {
	RuntimeEpoch uint64
	AgentID      string
	Generation   uint64
}

func (i RouteIdentity) validate() error {
	if i.RuntimeEpoch == 0 || strings.TrimSpace(i.AgentID) == "" || i.Generation == 0 {
		return errors.New("route occurrence requires runtime_epoch, agent_id, and generation")
	}
	return nil
}

type RouteOccurrence struct {
	occurrence *ownedOccurrence
	identity   RouteIdentity
	owner      Occurrence
}

func (r *RuntimeOccurrence) NewRoute(ctx context.Context, identity RouteIdentity) (*RouteOccurrence, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if err := r.occurrence.gate.admits(); err != nil {
		return nil, fmt.Errorf("admit route occurrence: %w", err)
	}
	identity.AgentID = strings.TrimSpace(identity.AgentID)
	return &RouteOccurrence{occurrence: r.occurrence.newChild(nil), identity: identity, owner: r}, nil
}

func (s *StandingOccurrence) NewRoute(ctx context.Context, identity RouteIdentity) (*RouteOccurrence, error) {
	if s == nil {
		return nil, errors.New("standing occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if err := s.occurrence.gate.admits(); err != nil {
		return nil, fmt.Errorf("admit standing route occurrence: %w", err)
	}
	identity.AgentID = strings.TrimSpace(identity.AgentID)
	return &RouteOccurrence{occurrence: s.occurrence.newChild(nil), identity: identity, owner: s}, nil
}

func (r *RouteOccurrence) Begin(ctx context.Context) (*Lease, error) {
	if r == nil {
		return nil, errors.New("route occurrence is required")
	}
	parent, err := r.owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	lease, err := r.occurrence.begin(parent.Context())
	if err != nil {
		_ = parent.Done()
		return nil, err
	}
	lease.parent = parent
	return lease, nil
}

func (r *RouteOccurrence) RetireAndWait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.occurrence.finish(ctx)
}

func (r *RouteOccurrence) Identity() RouteIdentity {
	if r == nil {
		return RouteIdentity{}
	}
	return r.identity
}

type SelectedForkIdentity struct {
	ExecutionID string
	RunID       string
	Generation  uint64
}

func (i SelectedForkIdentity) validate() error {
	if strings.TrimSpace(i.ExecutionID) == "" || strings.TrimSpace(i.RunID) == "" || i.Generation == 0 {
		return errors.New("selected-fork occurrence requires execution_id, run_id, and generation")
	}
	return nil
}

type SelectedForkOccurrence struct {
	occurrence *ownedOccurrence
	identity   SelectedForkIdentity
}

func (r *RuntimeOccurrence) NewSelectedFork(ctx context.Context, identity SelectedForkIdentity) (*SelectedForkOccurrence, error) {
	if r == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := r.BeginStanding(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit selected-fork occurrence: %w", err)
	}
	identity.ExecutionID = strings.TrimSpace(identity.ExecutionID)
	identity.RunID = strings.TrimSpace(identity.RunID)
	return &SelectedForkOccurrence{occurrence: r.occurrence.newChild(parentLease), identity: identity}, nil
}

func (p *Process) NewSelectedFork(ctx context.Context, identity SelectedForkIdentity) (*SelectedForkOccurrence, error) {
	if p == nil {
		return nil, errors.New("process work owner is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := p.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit selected-fork occurrence: %w", err)
	}
	identity.ExecutionID = strings.TrimSpace(identity.ExecutionID)
	identity.RunID = strings.TrimSpace(identity.RunID)
	return &SelectedForkOccurrence{occurrence: p.occurrence.newChild(parentLease), identity: identity}, nil
}

func (s *SelectedForkOccurrence) Begin(ctx context.Context) (*Lease, error) {
	if s == nil {
		return nil, errors.New("selected-fork occurrence is required")
	}
	return s.occurrence.begin(WithOccurrence(ctx, s))
}

func (s *SelectedForkOccurrence) BeginStanding(ctx context.Context) (*Lease, error) {
	if s == nil {
		return nil, errors.New("selected-fork occurrence is required")
	}
	return s.occurrence.gate.beginStanding(WithOccurrence(ctx, s))
}

func (s *SelectedForkOccurrence) RetireAndWait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.finish(ctx)
}

func (s *SelectedForkOccurrence) Identity() SelectedForkIdentity {
	if s == nil {
		return SelectedForkIdentity{}
	}
	return s.identity
}

func (s *SelectedForkOccurrence) Wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.wait(ctx)
}

func (s *SelectedForkOccurrence) WaitForQuiescence(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.occurrence.gate.waitTransient(ctx)
}

func (s *SelectedForkOccurrence) NewRoute(ctx context.Context, identity RouteIdentity) (*RouteOccurrence, error) {
	if s == nil {
		return nil, errors.New("selected-fork occurrence is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	parentLease, err := s.BeginStanding(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit selected-fork route occurrence: %w", err)
	}
	identity.AgentID = strings.TrimSpace(identity.AgentID)
	return &RouteOccurrence{occurrence: s.occurrence.newChild(parentLease), identity: identity, owner: s}, nil
}
