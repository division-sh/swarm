package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestInstanceClockOccurrenceCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cadence := range []struct {
			name string
			due  genericschedule.DueBasis
		}{
			{name: "every", due: genericschedule.EveryDue(time.Minute)},
			{name: "cron", due: genericschedule.CronDue("* * * * *")},
		} {
			for _, committed := range []bool{false, true} {
				name := backend + "/" + cadence.name + map[bool]string{false: "/rollback", true: "/commit_lost_receipt"}[committed]
				t.Run(name, func(t *testing.T) {
					owner, _, connector := newStopCommitStore(t, backend)
					selected := owner.(selectedScheduleStore)
					runID := uuid.NewString()
					ctx := selectedScheduleTestContext(t, runID)
					requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
					registerTestAuthorActivityCatalogForContext(t, owner.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
					publisher, err := newStoreTestEventBus(t, owner.(storeTestDurableEventBusStore))
					if err != nil {
						t.Fatal(err)
					}
					source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
					if err != nil {
						t.Fatal(err)
					}
					setGenericScheduleClock(t, selected, func() time.Time { return time.Now().UTC().Add(-time.Hour) })
					activation := admitGenericScheduleFixture(t, ctx, selected, genericschedule.AdmissionCommand{
						ScheduleKey: "poll", RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance, OwnerID: ".",
						EventType: "test.node_emitted", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
						ExecutionMode: executionmode.Live, Due: cadence.due,
					})
					setGenericScheduleClock(t, selected, time.Now)
					wake, err := activation.Wakeup()
					if err != nil {
						t.Fatal(err)
					}
					prepared, err := selected.PrepareGenericScheduleOccurrence(ctx, wake)
					if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
						t.Fatalf("prepare instance occurrence=%+v err=%v", prepared, err)
					}
					occurrence := prepared.Result.Occurrence
					event, err := events.NewInstancePublicationEvent(events.InstancePublicationEventInput{RunID: runID, Facts: events.EventFacts{
						ID: occurrence.EventID, Type: "test.node_emitted", Producer: events.ProducerClaim{Type: events.EventProducerInstance, ID: "."},
						Payload: []byte(`{}`), Envelope: events.EventEnvelope{FlowInstance: runID}, RoutingSource: source,
						CreatedAt: occurrence.DueAt, ExecutionMode: executionmode.Live,
					}})
					if err != nil {
						t.Fatal(err)
					}
					plans, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
					if err != nil || len(plans) != 1 {
						t.Fatalf("prepare ordinary publication=%+v err=%v", plans, err)
					}
					t.Cleanup(func() {
						if err := publisher.ReleaseEnginePublications(ctx, plans); err != nil {
							t.Error(err)
						}
					})
					command := genericschedule.CommitCommand{Activation: prepared.Result.Activation, Occurrence: occurrence, Publication: plans[0]}
					fault := errors.New("physical clock occurrence COMMIT receipt unavailable")
					var attempts atomic.Int32
					connector.arm(func(tx driver.Tx) error {
						attempts.Add(1)
						if committed {
							return errors.Join(fault, tx.Commit())
						}
						return errors.Join(fault, tx.Rollback())
					})
					result, err := selected.CommitGenericScheduleOccurrence(ctx, command)
					if !errors.Is(err, fault) || attempts.Load() != 1 || !reflect.DeepEqual(result, genericschedule.CommitResult{}) {
						t.Fatalf("uncertain outcome granted authority or retried: result=%+v attempts=%d err=%v", result, attempts.Load(), err)
					}
					stored, found, err := selected.LoadGenericScheduleActivation(ctx, activation.ID)
					if err != nil || !found {
						t.Fatalf("read durable clock after uncertain commit: found=%t err=%v", found, err)
					}
					nextDue, err := cadence.due.Next(occurrence.DueAt)
					if err != nil {
						t.Fatal(err)
					}
					if committed {
						if stored.Status != genericschedule.StatusActive || !stored.CurrentDueAt.Equal(nextDue) {
							t.Fatalf("acknowledgment loss discarded advancement=%+v", stored)
						}
					} else if !reflect.DeepEqual(stored, prepared.Result.Activation) {
						t.Fatalf("rollback consumed the occurrence: before=%+v after=%+v", prepared.Result.Activation, stored)
					}
					reader := owner.(bus.PreparedPublishEventReader)
					_, accepted, err := reader.LoadPreparedPublishEvent(ctx, occurrence.EventID)
					if err != nil || accepted != committed {
						t.Fatalf("publication/advance atomicity: accepted=%t committed=%t err=%v", accepted, committed, err)
					}
					for replay := 0; replay < 2; replay++ {
						result, err = selected.CommitGenericScheduleOccurrence(ctx, command)
						if err != nil || result.Outcome != genericschedule.CommitCommitted || result.PublicationAlreadyCommitted != (committed || replay > 0) || !result.Next.CurrentDueAt.Equal(nextDue) {
							t.Fatalf("explicit retry changed the occurrence: result=%+v err=%v", result, err)
						}
					}
					read, accepted, err := reader.LoadPreparedPublishEvent(ctx, occurrence.EventID)
					if err != nil || !accepted || read.Event.Class() != events.EventAdmissionInstancePublication || read.Event.Event().RoutingSource() != source || len(read.DeliveryRoutes) != 0 {
						t.Fatalf("exact retry lost instance authority or invented recipients=%+v err=%v", read, err)
					}
				})
			}
		}
	}
}
