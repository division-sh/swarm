# Native Target Current-State Reload Cohort

Predecessorbb9782511. The complete target reload root consumes the original native
artifact/source/run fixture, acknowledged construction, prepareDeliveryTargetApplication,
existing selected mutateE owner, pipelineEngineStateRepo.LoadState and opaque
LoadWorkflowInstance. First old upsert is native construction; the second old
fixture upsert is a typed marker-only update AFTER target preparation through
the existing mutation owner. It does not reconstruct/reset construction or
overwrite unrelated state. Exact original target, entity, fields, qualified
gate, active lifecycle and current marker are retained; source version is canonical.

All original temporal cuts and assertions remain. An application prepared with
marker=durable must reload marker=current and both local/qualified approved gate
facts. Changing the first snapshot's local gate/marker must not affect the second
read. The final stored carrier retains only the qualified approved gate and
marker=current. Native probe requires exactly one mutation and zero claim commits.
No event is published or executed by this observation root, and no store-reopen
or public readiness claim is made. The only new fixture consumer is the original
root/external both-store entry; all other consumers are unchanged.

| Manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Target reload uses raw reconstruction/repeated fixture upserts | reproduced and fixed | TestDeliveryTargetApplicationReloadsCurrentScopedStateOnSQLiteAndPostgres, both stores under race; actual native construction, preparation-before-update, one native mutation and zero claims |
| Prepared application freezes stale fields or loses scoped gate facts | execution-proven through the same corrected path | Same exact root, current marker and both approved/./approved facts from production LoadState after update |
| Detached execution snapshot mutates later/durable state | execution-proven through the same corrected path | Same exact root, hostile first snapshot mutations, second read and original durable local-alias exclusion/qualified gate/current marker checks |
| Rewrite loses temporal update or read assertions / regains raw authority | execution-proven through the same corrected path | Independent targetReload AST oracle and hostile gate/field/detached-read edits, finite120 recipes and actual combined overlay, protected original/renamed root and raw-parameter negative |

Exact root PASS9.178s under race on both stores, no skip/failure. Complete120-recipe
suite PASS33.681s, current application inert. Fresh downward ratchet, ACTUAL registry
root,16-child census and hostile controls PASS68.006s; registry12558. All78 structural
roots PASS. Partition PASS32.564s and timing contract PASS1.457s retain all six
venue/tier memberships and exact complements. No new failure, production defect,
spec ambiguity, retry/skip/guard/decoder waiver or timing relaxation.

Debt14683->14678 findings;10894->10890 raw-operation sites: five/four removed,
ZERO added identities/multiplicity growth. Collector,67 excluded uncertainties
and12558 registry facts unchanged; no new SQL/permissions. Raw openHandler tail
is11 callers, still including target faults, composition-target conflict and
compiled/supported/lifecycle families. Their ownership remains #2542/#2151 debt,
not proved by the same helper. Existing watchlist sufficient, no new issue or
framework/compatibility surface. Authoritative selected-runtime raw-SQL policy
records this current-state/detached-snapshot component contract. Receipts:
increment7-reload-* in the existing20261007 local directory. No tier/PR/global
closure claim; zero debt, strict guards, getters, SQLite fork and final full remain due.
