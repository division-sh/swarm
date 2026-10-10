# Cohort 96: inbound paused acknowledgment and executable settlement witnesses

GitHub, Slack and Stripe paused-ingress journeys consume the original selected
store for all pipeline and agent settlement witnesses. Pipeline cardinality
reuses pipelinepersistence's exact event/platform/pipeline owner, without outcome
or status narrowing. Six pipeline calls preserve zero before resume and one
after resume; all durable marker, payload, pending delivery and channel assertions
remain unchanged. The existing 19-recipe inbound propagation family is updated,
not duplicated, and pins all other setup/workload statements.

The agent helper was an obsolete interpreter: it counted agent rows in
event_receipts. Authoritative platform-spec.yaml's platform_pipeline_receipt_rows
contract (line 7313), selected raw_sql_policy and canonical executable-delivery
ownership distinguish pipeline disposition from executable agent settlement.
The recursive production-writer inventory found only pipelinepersistence writers
for event_receipts, while native agent settlement writes event_delivery_attempts.
The old helper and its name are retired, not wrapped or relocated. Three callers
now consume the existing delivery settled-attempt owner with exact event/agent
role/subscriber/context. Their zero-before-resume assertion is preserved and
becomes meaningful against real persisted completion rather than dead rows.

A native both-store negative control settles a real claimed agent delivery,
proves the canonical witness changes from zero to one, then reproduces the old
counter still returning zero. That old SQL exists only as explicitly classified
negative evidence, never as a public read port or producer. Existing cohort 92
controls prove event/subscriber/node-role exclusion and cancellation/closed
authority. Three actual PostgreSQL paused webhook/resume journeys run under race.

Sibling probe: cataloge2e's Tier 12 fork readback deliberately asserts one
delivered agent obligation and zero agent-shaped platform receipt rows together
(tier12_runtime_fork_e2e_test.go's assertSelectedContractForkRuntimeRows). That is a distinct
pipeline/executable-ledger separation control, not evidence of no executable
settlement, and remains explicit raw-migration debt in the parent. No production
agent-receipt writer or additional inbound helper/caller was found.

This is legacy-reader retirement within the already approved fixture-authority
class, not a production behavior/spec migration. No new owner/framework/SQL
allowance/compatibility or architecture split. Other inbound setup/read and wider
raw-authority debt remains tracked; final parent closure is not claimed.
