# Standing Root: Additive Constructor-Consumer Probe

Not closure or merge readiness. The earlier #2438 handoff's compile drift is
repaired without restoring the retired rootSingleton flag. The parked dual-store
provider proof now exercises the single canonical keyless standing-root form as
`TestProviderSelectedRootStandingBootBothStores`, registered in local canaries
and the existing serveapp-other-late unit with both backend children required.
Authentication, exact local/connected recipients, target ownership, persisted
source, and duplicate-publication assertions are unchanged.

Actual managed receipt: `/tmp/agent-e-2496-root-provider-proof.log`, RED, 2.197s.
SQLite and PostgreSQL both refuse before HTTP admission at
`ResolveStandingTargetDeclarations` -> `ResolveFlowSingleton(".")` with
`INVALID-SINGLETON: flow . singleton is unavailable: schema not found`.
This is the unchanged historical refusal, not a new attachment failure. No
production code was changed to obtain this result.

The full path is source -> standing declaration validation -> standing service
generation selection -> O2 construction -> attachment -> authenticated webhook
publication -> normal delivery -> public/persisted readback. The first gate is
still a child-only singleton interpreter. Its bootverify sibling is
`standingActivatedFlow`, which also uses ResolveFlowSingleton. A's contained
collection/coordinator validation is a different semantic contract and must not
be relaxed by replacing this standing constructor predicate.

There is a further coordinate seam after that first gate: standingTargetPlans
uses StandingForService -> Derive, producing the static authored root path/entity,
whereas R5.1 root construction requires the exact selected generation run as the
root instance and entity. The standing transaction owns the actual generation;
predicting generation one or simply dropping singleton validation is insufficient.
Retain service_id as service identity and consume the selected generation run's
typed canonical root construction under the existing standing mutation owner.
Restart/reset must retain this distinction, not introduce another root lifetime.

Proposed disposition: bounded migration of these named standing construction
consumers under P25/P33-P36, consuming O6 for keyless eligibility and O2 for the
exact selected run. This must also repair platform-spec.yaml's stale #2438
standing-root deferral only after executed proof. Please confirm this precise
standing-root coordinate disposition; no child coordinator or process-generation
ownership/signature change is proposed. Until it is settled, the new root-provider
proof remains RED and full #2496 closure cannot be claimed. Header-reader and P11
requests remain separately identified, not rolled into a new framework.

## Eligibility Migration Receipt

The approved P25/P33-P36 migration now replaces the runtime declaration and
bootverify singleton predicates with O6's CompileFlowConstructor(source, flowID,
"") and its eligibility result. The standing declaration grammar in
platform-spec.yaml is updated in the same change. Contained collection
coordinator checks remain untouched. This is not permission to guess a
standing generation's root coordinate or to claim complete standing execution.

Both disk-loaded and reconstructed artifacts are covered for a keyless root,
a keyed-root refusal and an unassigned initial-reader refusal. Actual focused
receipts, race count3: TestResolveStandingTargetDeclarationsConsumesRootConstructor
PASS3.649s; TestStandingActivatedFlowConsumesConstructorEligibility PASS3.885s.
The initial root declaration probe was RED0.148s before the migration.
The complete public provider journey remains a separate required proof; its
pre-migration RED2.197s receipt above is not erased or called a pass.
