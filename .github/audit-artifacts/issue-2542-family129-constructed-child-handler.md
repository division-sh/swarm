# Cohort 129: constructed child writes and emission

Migrate the original child-entity handler witness through the existing native
WorkflowHandlerNativeFixture. The authored scoring schema, subject/name field,
node, event declaration, write expression, emit expression and original assertions
are unchanged. This is execution over a scenario_setup run, not a run-creation
witness; use the existing-run event constructor explicitly, as ruled for cohort
122. Keep the original ID, type, source, payload, routing source and clock.

Extract only the pure constructor values from seedConstructorUnitInstance. The
same CompileFlowConstructor, InitialFields, compiled initial stage and keyless
child identity supply the native activation command. The old unit consumers keep
their existing persistence path and remain tracked debt; extracting values alone
does not qualify them. No new persistence owner or fallback is introduced.

The native witness constructs the exact scoring child, checks durable revision-1
pre-state, publishes the original event with that child's exact recipient route,
claims through the selected lifecycle owner and joins its renewal heartbeat on
all exits. Its dispatch observer wraps the original planner/dispatcher before
publication. Handler execution/handoff and the original handled/one-emission,
constructed entity ID and Updated Entity label assertions remain intact.

Additional readback requires the child's field and revision advance plus complete
equality between the emitted event and the original selected persisted receipt.
The external root executes SQLite and PostgreSQL and requires exactly one native
workflow mutation, one claim and no active transaction after cleanup. The finite
codemod records the root and pure extraction; its oracle compares every original
source/handler/assertion and constructor value, and rejects changed child facts.
The native raw-authority guard now covers this root explicitly, with a negative
raw-parameter probe. No production behavior, tier, budget or compatibility change.
