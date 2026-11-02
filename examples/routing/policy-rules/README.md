# Scoped Policy And Rules

Business values in `policy.yaml` remain literal. `rules.yaml` owns machine
equality sets; the node selects an accepted or rejected event from
the machine result. The agent has only the declared review callback tool.

```sh
swarm verify examples/routing/policy-rules
swarm serve examples/routing/policy-rules
swarm event publish work.requested --payload-json '{"left":1,"right":1}'
```

Expected: verification admits the literal values and scoped machine rule set. A
`work.requested` input reaches the review agent; its declared `review.reported`
callback runs the equality set before selecting `work.accepted` or `work.rejected`.

If verification rejects a set or callback payload, repair its declaration;
do not move machine rules into policy values or bypass their checks.
