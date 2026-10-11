# Cohort 113: stop unused raw authority in the recovery unit fixture

Binding: approved #2542/#2151 fixture authority migration. The private
startupRecoveryWorkflowPersistence is a synthetic workflow dependency, not a
selected-store constructor: it wraps startupRecoveryWorkflowOwner with its timer
reader and never consumes its SQL argument. Remove that unused parameter and all
eight arguments, including the completeRuntimeRecoveryTestDeps shared recipe.

This does not canonize the synthetic workflow owner or runtimeLogPersistenceStub.
Their real raw log writes/readback and wider diagnostic fixture migration remain
tracked. Preserve each test's existing event/delivery/manager/schedule dependency,
boot denial/allowed/degraded/fatal decision, durable logging and shutdown proof.

Nine complete finite snapshots and the actual-source oracle permit only the named
unused argument deletions; hostile timer, raw logging, failure and decision cuts
cannot normalize away. Focused race execution covers all seven explicit recovery
decision callers. Whole-candidate type checking covers the shared dependency
recipe. Downward census and unchanged guards must show no added site identities.

Value: confinement and truthful unit-fixture dependency shape; no parity expansion,
measured flake reduction, production semantic/spec change or new framework. Full
diagnostic owner migration, zero debt and integrated parent closure remain open.

Increment tooling repair: the expanded whole-candidate overlay exposed a duplicate
storetest import when an existing explicit alias is present. The existing codemod
now detects the import by exact path before adding it; default/explicit-alias
controls and candidate type checks cover this bounded repair. The two
whole-function mock runtime factory snapshot lines receive an exact unrelated-text
inventory disposition; a third or foreign snapshot does not pass. No broad guard
exemption or production constructor allowance is introduced.
