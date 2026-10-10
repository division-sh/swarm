package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

func TestRunForkFanOutBarrierRetainsCapturedDue(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, overdue := range []bool{false, true} {
			name := map[bool]string{false: "sqlite", true: "postgres"}[postgres] + map[bool]string{false: "/future", true: "/overdue"}[overdue]
			t.Run(name, func(t *testing.T) {
				source, activation := runForkBarrierDueFixture(t)
				birth := activation.CurrentDueAt.Add(-time.Hour)
				if overdue {
					birth = activation.CurrentDueAt.Add(time.Hour)
				}
				owner := &runForkBarrierDueScheduleOwner{birth: birth}
				beforeSource, beforeActivation := source, activation
				beforeSummary := *source.Summary
				beforeSource.Summary = &beforeSummary
				runForkBarrierDueMutation(t, postgres, source, &activation, birth, owner)
				if len(owner.commands) != 1 || !owner.commands[0].Due.Absolute.Equal(activation.CurrentDueAt) {
					t.Fatalf("fork reset captured due to child birth: captured=%s birth=%s commands=%d", activation.CurrentDueAt, birth, len(owner.commands))
				}
				if owner.commands[0].RunID == source.Registration.IntentKey.RunID || owner.commands[0].EntityID != owner.commands[0].RunID {
					t.Fatal("retained due reused source ownership")
				}
				if !reflect.DeepEqual(beforeSource, source) || !reflect.DeepEqual(beforeActivation, activation) {
					t.Fatal("projection changed captured source evidence")
				}
				projected := source.Registration
				projected.IntentKey.RunID = owner.commands[0].RunID
				projected.EntityID = owner.commands[0].EntityID
				projected.Route = flowidentity.StoredRoute(".", projected.IntentKey.RunID, projected.IntentKey.RunID)
				projected.RoutingSource = owner.commands[0].RoutingSource
				projected.CreatedAt = birth
				expected, err := genericschedule.FanOutBarrierAdmission(projected, *source.Summary, activation.CurrentDueAt)
				if err != nil || !reflect.DeepEqual(expected, owner.commands[0]) {
					t.Fatalf("projection lost exact owner, handle, summary, mode or due: %v", err)
				}
			})
		}
	}
}

func TestRunForkFanOutBarrierRefusesUnprovenDue(t *testing.T) {
	for _, fault := range []string{"missing", "identity", "run", "entity", "payload", "hash", "due", "fired"} {
		t.Run(fault, func(t *testing.T) {
			source, activation := runForkBarrierDueFixture(t)
			captured := &activation
			switch fault {
			case "missing":
				captured = nil
			case "identity":
				activation.ID = uuid.NewString()
			case "run":
				activation.Command.RunID = uuid.NewString()
			case "entity":
				activation.Command.EntityID = uuid.NewString()
			case "payload":
				other := fanoutbarrier.Summary{Total: 2, DeadLettered: 2}
				var err error
				activation.Command, err = genericschedule.FanOutBarrierAdmission(source.Registration, other, activation.CurrentDueAt)
				if err != nil {
					t.Fatal(err)
				}
				activation.ImmutableHash, err = activation.Command.ImmutableHash()
				if err != nil {
					t.Fatal(err)
				}
			case "hash":
				activation.ImmutableHash = strings.Repeat("f", 64)
			case "due":
				activation.CurrentDueAt = activation.CurrentDueAt.Add(time.Second)
			case "fired":
				activation.Status = genericschedule.StatusFired
				activation.FiredAt, activation.AcceptedAt = activation.CurrentDueAt, activation.CurrentDueAt
				activation.CurrentEventID = genericschedule.OccurrenceEventID(activation.ID, activation.CurrentDueAt)
				activation.CurrentEventAdmittedAt = activation.CurrentDueAt
				if err := genericschedule.ValidateFanOutBarrierScheduleRelation(source, activation); err != nil {
					t.Fatalf("already-published fixture must retain a legitimate source relation: %v", err)
				}
			}
			if due, err := runForkFanOutBarrierDue(source, captured); err == nil || !due.IsZero() {
				t.Fatalf("unproven source relation supplied a child due: due=%s err=%v", due, err)
			}
		})
	}
}

func TestRunForkFanOutBarrierTerminalHistoryDoesNotRearm(t *testing.T) {
	for _, status := range []fanoutbarrier.Status{fanoutbarrier.StatusFired, fanoutbarrier.StatusOutcomeDeadLettered, fanoutbarrier.StatusSuppressedRunTerminal, fanoutbarrier.StatusSuppressedGenerationSuperseded} {
		t.Run(string(status), func(t *testing.T) {
			source, _ := runForkBarrierDueFixture(t)
			source.Status = status
			owner := &runForkBarrierDueScheduleOwner{birth: source.UpdatedAt.Add(time.Hour)}
			runForkBarrierDueMutation(t, true, source, nil, owner.birth, owner)
			if len(owner.commands) != 0 {
				t.Fatal("terminal barrier history admitted a new schedule")
			}
		})
	}
}

func runForkBarrierDueMutation(t *testing.T, postgres bool, source fanoutbarrier.Barrier, activation *genericschedule.Activation, birth time.Time, schedules GenericScheduleTxOwner) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO fan_out_obligation_barriers`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE fan_out_obligation_barriers`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()
	stop := errors.New("observe native barrier projection before rollback")
	result := mutationprotocol.RunPostgres(context.Background(), backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return materializeRunForkFanOutBarrierTx(ctx, tx, attempt, postgres, schedules, uuid.NewString(), source, activation, source.Registration.PlanRef, nil, birth)
			})
			if err != nil {
				return struct{}{}, err
			}
			return struct{}{}, stop
		})
	if result.Acknowledged() || !errors.Is(result.Err(), stop) {
		t.Fatalf("native barrier projection failed before observation: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func runForkBarrierDueFixture(t *testing.T) (fanoutbarrier.Barrier, genericschedule.Activation) {
	t.Helper()
	runID, deliveryID := uuid.NewString(), uuid.NewString()
	element := contracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "scatter/handler/items.ready/fan_out"}
	declaration, err := element.DeclarationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	plan := contracts.FanOutPlanRef{BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), ElementRef: element, SemanticDigest: "sha256:" + strings.Repeat("2", 64)}
	ref, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", "joined", declaration, plan.BundleHash, plan.SemanticDigest)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindFanOutIntent(deliveryID, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := events.NewRootRoutingSource(runID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	registration := fanoutbarrier.Registration{
		IntentKey: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: element},
		PlanRef:   plan, Handle: handle, Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
		RoutingSource: routing, ExecutionMode: executionmode.Mock, CreatedAt: at.Add(-2 * time.Hour),
	}
	summary := fanoutbarrier.Summary{Total: 2, Succeeded: 1, NoRoute: 1}
	command, err := genericschedule.FanOutBarrierAdmission(registration, summary, at)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: registration.CreatedAt, InitialDueAt: at, CurrentDueAt: at, Status: genericschedule.StatusActive}
	barrier := fanoutbarrier.Barrier{Registration: registration, Status: fanoutbarrier.StatusClosedPending, Summary: &summary, ScheduleKey: command.ScheduleKey, ScheduleActivationID: activation.ID, UpdatedAt: at}
	if err := genericschedule.ValidateFanOutBarrierScheduleRelation(barrier, activation); err != nil {
		t.Fatal(err)
	}
	return barrier, activation
}

type runForkBarrierDueScheduleOwner struct {
	birth    time.Time
	commands []genericschedule.AdmissionCommand
}

func (o *runForkBarrierDueScheduleOwner) AdmitTx(_ context.Context, _ *mutationprotocol.Attempt, command genericschedule.AdmissionCommand) (genericschedule.AdmissionResult, error) {
	o.commands = append(o.commands, command)
	hash, err := command.ImmutableHash()
	if err != nil {
		return genericschedule.AdmissionResult{}, err
	}
	due, err := command.Due.FirstDue(o.birth)
	if err != nil {
		return genericschedule.AdmissionResult{}, err
	}
	activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: o.birth, InitialDueAt: due, CurrentDueAt: due, Status: genericschedule.StatusActive}
	return genericschedule.AdmissionResult{Outcome: genericschedule.AdmissionCreated, Activation: activation}, activation.Validate()
}

func (*runForkBarrierDueScheduleOwner) CancelAdmissionTx(context.Context, *mutationprotocol.Attempt, genericschedule.AdmissionCommand, string, time.Time) (genericschedule.CancelResult, error) {
	return genericschedule.CancelResult{}, errors.New("unexpected cancellation")
}

func (*runForkBarrierDueScheduleOwner) LoadActivationTx(context.Context, *mutationprotocol.Attempt, string) (genericschedule.Activation, bool, error) {
	return genericschedule.Activation{}, false, errors.New("unexpected current source load")
}

func (*runForkBarrierDueScheduleOwner) CancelActivationTx(context.Context, *mutationprotocol.Attempt, genericschedule.CancelCommand) (genericschedule.CancelResult, error) {
	return genericschedule.CancelResult{}, errors.New("unexpected cancellation")
}
