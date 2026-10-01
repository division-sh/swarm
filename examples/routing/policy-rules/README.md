# Scoped Policy And Rules

Business values in `policy.yaml` remain literal. `rules.yaml` owns agent criteria
and machine equality sets; the node selects an accepted or rejected event from
the machine result. The agent has only the declared review callback tool.

```sh
swarm verify examples/routing/policy-rules
swarm serve examples/routing/policy-rules
swarm event publish work.requested --payload-json '{"left":1,"right":1}'
```

Expected: verification admits the literal values and both scoped rule sets. A
`work.requested` input reaches the review agent; its declared `review.reported`
callback runs the equality set before selecting `work.accepted` or `work.rejected`.

If verification rejects a set or citation, repair the declaration or callback;
do not move rules into policy values or bypass the granted rule identities.
