package transactiontest

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testutil/sqlite2353"
)

type diagnostic2353Boundary struct {
	UTC       string `json:"utc"`
	ElapsedNS int64  `json:"backend_observer_elapsed_ns"`
	Phase     string `json:"phase"`
}

// This finite rolling receipt belongs to one private backend; it exposes no SQL.
type Diagnostic2353Observer struct {
	mu        sync.Mutex
	ring      [192]diagnostic2353Boundary
	next      uint64
	started   time.Time
	owner     string
	collector *Collector
	restore   func()
	stats     func() any
	stop      chan struct{}
	joined    chan struct{}
	signals   chan os.Signal
	once      sync.Once
}

func NewDiagnostic2353(slot *Slot, dialect string, stats func() any) *Diagnostic2353Observer {
	if !sqlite2353.Enabled() {
		return nil
	}
	c, restore, err := slot.Install(Options{})
	if err != nil {
		sqlite2353.Record("backend.observer_incomplete", fmt.Sprint(err))
		return nil
	}
	d := &Diagnostic2353Observer{owner: fmt.Sprintf("%s:%p", dialect, slot), started: time.Now(), collector: c, restore: restore, stats: stats, stop: make(chan struct{}), joined: make(chan struct{}), signals: make(chan os.Signal, 1)}
	c.diagnostic2353 = d
	signal.Notify(d.signals, syscall.SIGUSR2)
	go func() {
		defer close(d.joined)
		for {
			select {
			case <-d.stop:
				return
			case <-d.signals:
				d.snapshot("sample")
			}
		}
	}()
	sqlite2353.Record("backend.observer_installed", map[string]any{"owner": d.owner, "delay_ns": 0, "stats": stats()})
	return d
}

func (d *Diagnostic2353Observer) Record(phase string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.ring[d.next%uint64(len(d.ring))] = diagnostic2353Boundary{time.Now().UTC().Format(time.RFC3339Nano), time.Since(d.started).Nanoseconds(), phase}
	d.next++
	d.mu.Unlock()
}

func (d *Diagnostic2353Observer) snapshot(reason string) {
	d.mu.Lock()
	count := d.next
	start := uint64(0)
	if count > uint64(len(d.ring)) {
		start = count - uint64(len(d.ring))
	}
	spans := make([]diagnostic2353Boundary, 0, count-start)
	for i := start; i < count; i++ {
		spans = append(spans, d.ring[i%uint64(len(d.ring))])
	}
	d.mu.Unlock()
	snapshot := d.collector.Snapshot()
	active := make([]struct {
		Class ActiveClass
		Count uint64
	}, 0, len(snapshot.ActiveByClass))
	for class, count := range snapshot.ActiveByClass {
		active = append(active, struct {
			Class ActiveClass
			Count uint64
		}{class, count})
	}
	sqlite2353.Record("backend."+reason, map[string]any{"owner": d.owner, "total": snapshot.Total, "by_operation": snapshot.ByOperation, "retained": snapshot.Retained, "active": snapshot.Active, "active_classes": active, "pool": d.stats(), "boundaries": spans, "boundary_count": count, "ring_capacity": len(d.ring)})
}

func (d *Diagnostic2353Observer) Stop() {
	if d == nil {
		return
	}
	d.once.Do(func() { signal.Stop(d.signals); close(d.stop); <-d.joined; d.snapshot("joined"); d.restore() })
}

func (a *Attempt) observe2353(phase string) {
	if a != nil && a.collector != nil && a.collector.diagnostic2353 != nil {
		a.collector.diagnostic2353.Record(fmt.Sprintf("%p:%s", a, phase))
	}
}
