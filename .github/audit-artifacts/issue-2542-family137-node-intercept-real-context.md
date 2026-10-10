# Cohort 137: node interception uses the actual run context

Three node-interception consumers wrapped their existing run context in
WithPipelinePostCommitActions with an empty OwnerAction slice. Neither production
interception nor any assertion consumes this test-private key/slice. The marker
could not confer real transaction or delivery authority and was not temporal
commit evidence. The underlying context and all legitimate authority stay intact.

The native unstamped-node verifier, targeted delivery interception and terminal
node interception now receive that exact run context directly. Actual earlier
construction/publication/claim setup, passthrough/error/log checks, zero emission,
physical event/delivery/attempt conservation and exact outcome counts remain
unchanged. Native unstamped proof executes both stores; the two remaining original
PG fixtures are regression witnesses, NOT newly native construction proof.

The native unstamped recipe is extended in place. Its workload oracle removes
only the obsolete marker/slice from the predecessor and retains the same five
execution/assertion components. Two additional complete-function recipes/oracles
permit no other changes. The broader fake transaction runner, callback collections,
timer/SQL consumers and explicit raw delivery fixtures remain parent-owned.

This is deletion of fake authority decoration, not migration to a new context
protocol or introduction of a shared transaction abstraction. No production
policy/spec, compatibility behavior, retry, skip, assertions or timing limits
change. Existing #2151/#2542 approval and tracking remain sufficient.
