package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestPipelineFixtureCanonicalRunSourceAdmissionOnBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.open(t)
			runner := store.engineMutations.(*recordingRuntimeMutationRunner)
			runID := runtimeRunID(ctx)
			fact := mustPipelineTestSourceArtifactFact(pipelineTestBundleHash)
			dialect := authoractivityfixture.Dialect(runner.dialect)
			rollback := errors.New("fixture admission observation rollback")
			observe := func(t *testing.T, fn func(context.Context, testRunLifecycleMutation, eventfixture.RunSourceMutation) error) {
				t.Helper()
				err := runner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
					m, err := runner.lifecycleMutation(txctx)
					if err != nil {
						return err
					}
					attempt, err := m.sourceMutation(txctx)
					if err != nil {
						return err
					}
					if err := fn(txctx, m, attempt); err != nil {
						return err
					}
					return rollback
				})
				if !errors.Is(err, rollback) {
					t.Fatalf("admission observation: %v", err)
				}
			}
			refused := func(t *testing.T, got runtimecorrelation.SourceArtifactFact, err error) {
				t.Helper()
				if err == nil || got.Validate() == nil {
					t.Fatalf("refusal returned source %q, error %v", got.BundleHash(), err)
				}
			}

			t.Run("present_is_nonminting_and_active_is_native", func(t *testing.T) {
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, attempt eventfixture.RunSourceMutation) error {
					present, err := m.RequirePresentSource(txctx, runID)
					if err != nil || !present.Matches(fact) {
						return fmt.Errorf("present source = %q, error %v", present.BundleHash(), err)
					}
					if err := attempt.RequireActiveAdmission(txctx, runID, present); err == nil {
						return errors.New("present source minted active admission")
					}
					for range 2 {
						active, err := m.RequireActiveSource(txctx, runID)
						if err != nil || !active.Matches(fact) {
							return fmt.Errorf("active source = %q, error %v", active.BundleHash(), err)
						}
						if err := attempt.RequireActiveAdmission(txctx, runID, active); err != nil {
							return err
						}
					}
					return nil
				})
			})

			t.Run("source_only_and_detached_contexts", func(t *testing.T) {
				if _, err := eventfixture.RunSource(ctx, dialect); err == nil {
					t.Fatal("source-only context supplied a native owner")
				}
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, attempt eventfixture.RunSourceMutation) error {
					source, err := m.sourceMutation(txctx)
					if err != nil {
						return err
					}
					got, err := source.RequireActive(ctx, runID)
					refused(t, got, err)
					got, err = source.RequirePresent(ctx, runID)
					refused(t, got, err)
					if err := attempt.RequireActiveAdmission(txctx, runID, fact); err == nil {
						return errors.New("detached context minted admission")
					}
					if _, err := source.RequireActive(txctx, runID); err != nil {
						return err
					}
					if err := source.Invalidate(ctx, runID); err == nil {
						return errors.New("detached context invalidated native admission")
					}
					if err := attempt.RequireActiveAdmission(txctx, runID, fact); err != nil {
						return err
					}
					return nil
				})
			})

			t.Run("missing_run", func(t *testing.T) {
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, _ eventfixture.RunSourceMutation) error {
					got, err := m.RequireActiveSource(txctx, uuid.NewString())
					refused(t, got, err)
					var missing *runtimerunlifecycle.RunNotFoundError
					if !errors.As(err, &missing) {
						return fmt.Errorf("missing run error = %v", err)
					}
					return nil
				})
			})

			t.Run("story_only_write_and_invalidation_refusal", func(t *testing.T) {
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, attempt eventfixture.RunSourceMutation) error {
					if _, err := m.RequireActiveSource(txctx, runID); err != nil {
						return err
					}
					storyOnly := ctx
					source, err := m.sourceMutation(storyOnly)
					if err != nil {
						return err
					}
					childID := uuid.NewString()
					revised := mustPipelineTestSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
					writes := []struct {
						name  string
						write func() error
					}{
						{name: "create", write: func() error {
							_, err := m.Create(storyOnly, runtimerunlifecycle.CreateRequest{RunID: childID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(), Source: fact, StartedAt: time.Now().UTC()})
							return err
						}},
						{name: "transition", write: func() error {
							_, err := m.TransitionActive(storyOnly, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: runtimerunlifecycle.StatePaused})
							return err
						}},
						{name: "terminal", write: func() error {
							_, _, err := m.MarkTerminal(storyOnly, runtimerunlifecycle.TerminalRequest{RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC()})
							return err
						}},
						{name: "fork", write: func() error {
							_, _, err := m.ForkSource(storyOnly, runtimerunlifecycle.ForkSourceRequest{RunID: runID, ContinuedAsRunID: childID, EndedAt: time.Now().UTC()})
							return err
						}},
						{name: "revision", write: func() error {
							_, err := m.ReviseSource(storyOnly, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: revised})
							return err
						}},
						{name: "invalidation", write: func() error { return source.Invalidate(storyOnly, runID) }},
						{name: "artifact_deletion", write: func() error { return source.DeleteSourceArtifactForRefusal(storyOnly, fact) }},
					}
					for _, write := range writes {
						err := write.write()
						if err == nil || !strings.Contains(err.Error(), "requires an existing native SQL frame") {
							return fmt.Errorf("story-only %s refusal = %v", write.name, err)
						}
						if err := attempt.RequireActiveAdmission(txctx, runID, fact); err != nil {
							return err
						}
					}
					snapshot, err := m.loadSnapshot(txctx, runID)
					if err != nil || snapshot.State != runtimerunlifecycle.StateRunning || snapshot.BundleHash != fact.BundleHash() {
						return fmt.Errorf("story-only write changed run: %#v, error %v", snapshot, err)
					}
					_, err = m.RequirePresentSource(txctx, childID)
					var missing *runtimerunlifecycle.RunNotFoundError
					if !errors.As(err, &missing) {
						return fmt.Errorf("story-only create left a run: %v", err)
					}
					if _, err := m.RequireActiveSource(txctx, runID); err != nil {
						return err
					}
					return nil
				})
			})

			for _, change := range []string{"pause", "source_revision", "cancelled", "forked", "artifact_deleted"} {
				t.Run(change, func(t *testing.T) {
					observe(t, func(txctx context.Context, m testRunLifecycleMutation, attempt eventfixture.RunSourceMutation) error {
						if _, err := m.RequireActiveSource(txctx, runID); err != nil {
							return err
						}
						var err error
						switch change {
						case "pause":
							_, err = m.TransitionActive(txctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: runtimerunlifecycle.StatePaused})
						case "source_revision":
							revised := mustPipelineTestSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
							_, err = m.ReviseSource(txctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: revised})
						case "cancelled":
							_, _, err = m.MarkTerminal(txctx, runtimerunlifecycle.TerminalRequest{RunID: runID, State: runtimerunlifecycle.State(change), EndedAt: time.Now().UTC()})
						case "forked":
							childID := uuid.NewString()
							if _, err = m.Create(txctx, runtimerunlifecycle.CreateRequest{RunID: childID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(), Source: fact, StartedAt: time.Now().UTC()}); err == nil {
								_, _, err = m.ForkSource(txctx, runtimerunlifecycle.ForkSourceRequest{RunID: runID, ContinuedAsRunID: childID, EndedAt: time.Now().UTC()})
							}
						case "artifact_deleted":
							var source eventfixture.RunSourceMutation
							source, err = m.sourceMutation(txctx)
							if err == nil {
								err = source.DeleteSourceArtifactForRefusal(txctx, fact)
							}
						}
						if err != nil {
							return err
						}
						if change != "artifact_deleted" && attempt.RequireActiveAdmission(txctx, runID, fact) == nil {
							return errors.New("lifecycle/source write retained warmed native admission")
						}
						got, err := m.RequireActiveSource(txctx, runID)
						if change == "pause" {
							if err != nil || !got.Matches(fact) {
								return fmt.Errorf("paused active source = %q, error %v", got.BundleHash(), err)
							}
							return attempt.RequireActiveAdmission(txctx, runID, got)
						}
						refused(t, got, err)
						if change == "source_revision" || change == "artifact_deleted" {
							var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
							if !errors.As(err, &unavailable) {
								return fmt.Errorf("missing artifact error = %v", err)
							}
						} else {
							var inactive *runtimerunlifecycle.RunNotActiveError
							if !errors.As(err, &inactive) || inactive.State != runtimerunlifecycle.State(change) {
								return fmt.Errorf("inactive run error = %v", err)
							}
						}
						return nil
					})
				})
			}

			t.Run("canceled", func(t *testing.T) {
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, _ eventfixture.RunSourceMutation) error {
					if _, err := m.RequireActiveSource(txctx, runID); err != nil {
						return err
					}
					canceled, cancel := context.WithCancel(txctx)
					cancel()
					got, err := m.RequireActiveSource(canceled, runID)
					refused(t, got, err)
					if !errors.Is(err, context.Canceled) {
						return fmt.Errorf("canceled admission error = %v", err)
					}
					source, err := m.sourceMutation(txctx)
					if err != nil {
						return err
					}
					if err := source.RequireWriteFrame(canceled); !errors.Is(err, context.Canceled) {
						return fmt.Errorf("canceled frame error = %v", err)
					}
					return nil
				})
			})

			t.Run("original_caller_canceled_with_drain_context", func(t *testing.T) {
				callerCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				checked := false
				err := runner.RunRuntimeMutationContext(callerCtx, func(txctx context.Context) error {
					m, err := runner.lifecycleMutation(txctx)
					if err != nil {
						return err
					}
					if _, err := m.RequireActiveSource(txctx, runID); err != nil {
						return err
					}
					source, err := m.sourceMutation(txctx)
					if err != nil {
						return err
					}
					cancel()
					err = source.RequireWriteFrame(context.WithoutCancel(txctx))
					if !errors.Is(err, context.Canceled) {
						return fmt.Errorf("canceled original caller frame error = %v", err)
					}
					checked = true
					return err
				})
				if !checked || !errors.Is(err, context.Canceled) {
					t.Fatalf("original cancellation checked=%t, error %v", checked, err)
				}
			})

			t.Run("foreign_ended_and_retry", func(t *testing.T) {
				foreignStore, _ := backend.open(t)
				foreign := foreignStore.engineMutations.(*recordingRuntimeMutationRunner)
				var old eventfixture.RunSourceMutation
				var endedCtx context.Context
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, _ eventfixture.RunSourceMutation) error {
					var err error
					old, err = m.sourceMutation(txctx)
					if err != nil {
						return err
					}
					endedCtx = txctx
					if _, err := old.RequireActive(txctx, runID); err != nil {
						return err
					}
					return foreign.RunRuntimeMutationContext(ctx, func(foreignCtx context.Context) error {
						got, err := old.RequireActive(foreignCtx, runID)
						refused(t, got, err)
						if !strings.Contains(err.Error(), "run admission belongs to another transaction") {
							return fmt.Errorf("foreign frame refusal = %v", err)
						}
						if err := old.Invalidate(foreignCtx, runID); err == nil || !strings.Contains(err.Error(), "run admission belongs to another transaction") {
							return fmt.Errorf("foreign invalidation refusal = %v", err)
						}
						return nil
					})
				})
				got, err := old.RequireActive(endedCtx, runID)
				refused(t, got, err)
				if err := old.Invalidate(endedCtx, runID); err == nil || !strings.Contains(err.Error(), "mutation attempt is not active") {
					t.Fatalf("ended invalidation refusal = %v", err)
				}
				observe(t, func(txctx context.Context, m testRunLifecycleMutation, attempt eventfixture.RunSourceMutation) error {
					if err := attempt.RequireActiveAdmission(txctx, runID, fact); err == nil {
						return errors.New("fresh attempt inherited prior admission")
					}
					got, err := old.RequireActive(txctx, runID)
					refused(t, got, err)
					got, err = m.RequireActiveSource(txctx, runID)
					if err != nil {
						return err
					}
					return attempt.RequireActiveAdmission(txctx, runID, got)
				})
			})
		})
	}
}
