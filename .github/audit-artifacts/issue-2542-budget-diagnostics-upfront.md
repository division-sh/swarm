# Budget diagnostic dependency: upfront inventory

Track: FULL. Source: signed a552cdb3a2953add81cea117bafcadddd9803c11.
This is the next batch's cut10 from ruling6165459975, not an expansion of the
completed channel-settlement checkpoint. Enumeration started11:06Z.

| Original output | Canonical domain owner | Preserved scope |
| --- | --- | --- |
| spend | internal/budgetspend | execution_mode, textual cost_usd, agent_id, transport; global LIMIT20, no new order |
| effects | backend/effectpersistence | effect_kind,state,count; exact global grouping, no added bound |
| agents | backend/agentpersistence | agent_id,lifecycle_phase,lifecycle_run_mode,turn_count; global LIMIT20 |
| deliveries | backend/delivery closed read projections | subscriber_type/id,status,reason_code,retry_count; created_at DESC LIMIT20 |
| budget_events | backend/eventrecord | exact platform.budget_threshold_crossed name and global grouping |
| notices | internal/mailboxpersistence | item_type,status,severity,count; exact global grouping |
| successful emergency-notice witness | internal/mailboxpersistence | original mailbox/source-event join, alert kind and emergency-summary prefix; six original values and NULL-to-empty entity text |
| committed live completion count | internal/budgetspend | global cost_usd>=3 AND execution_mode=live, exact multiplicity |

Two declarations in one source file: TestChannelDeliveryBudgetNoticePublicJourney
and logBudgetNoticeFailure. One direct logger call, one scanner call executed
for the six fixed projections, two positive-path scalar reads. Current retained
debt is10 findings /8 raw sites, not a100+ credit claim. The raw scanner and old
settlement diagnostic path remain needed by the isolated projection control;
this batch cannot claim their retirement. Additional compatible families may be
batched separately, with their own upfront inventories, before the next100-300
checkpoint; this internal wave gets no standalone checkpoint.

Preserve the single five-second diagnostic context, all six outputs in original
order, per-query error reporting without suppressing later sections, omission
of NULL map keys, and the existing privacy boundary. No new request/response,
credential, provider-reference or authority output. Native positive polling
retains its20-second deadline,20ms sleep, exact NoRows distinction, all provenance
and managed-provider assertions, and actual channel delivery.

Lifetime: reuse the existing harness-owned channel observation by a narrow
borrowed budget facet over the same concrete instance, never a new raw pool or
coordinator. The facet cannot close the parent; foreign fabricated facets are
rejected. No changes to channel_observation.go or its historical full-file pin
are required. Each original query remains an independent selected read cut.

History: one existing recipe/transition for the public journey, with its earlier
channel-anchor source record; the logger is one append. No existing debt
signature pair is touched or moved. Preserve original Before, metadata/order
and the complete historical source body; only the exact current endpoint may
advance. Whole-file/hash census found no budget-file pin outside this exact
channel-anchor declaration. Public-source ownership enumeration shows no file
overlap with #2617; runtimepersistence construction migrations stay untouched.

Proof plan: original public journey on both stores under race; execute all six
real diagnostic ports; error/cancel/foreign/closed-owner and NULL/bound/grouping
controls; finite exact-query/caller/source-history preservation. Run only the
affected roots internally; census, real ratchet and heavy quality checks once
at the eventual combined batch checkpoint. No timeout/budget/guard relaxation,
new accounting exception, push or PR is authorized by this inventory.
# Internal wave completion

Focused native both-store race proof is green (35.907s), including all six
nonempty diagnostic sections after genuine managed spend. Independent reads,
absence, cancellation, foreign/nil facets and closed-parent controls are green
(32.483s). The event bridge is in the existing test_event_support.go; the touched
event-boundary guard is green (1.172s), without an allowance. Finite transform
controls and affected source ingress controls are green. Ten findings/eight raw
sites are forecast, not yet credited. Per the orchestrator's batching directive,
no additional per-wave census/history pass is run; its single append composes
with the next selected-reader batch at the combined checkpoint.
