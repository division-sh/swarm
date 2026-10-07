# #2566 Remainder Repair And Source-Order Guard Addendum

This is a bounded implementation amendment under Gate D 6019841561 and the
user's repair instruction. No new issue, validator, source owner, permission
surface, compatibility path or old-store machinery is introduced.

## Failed Qualification Retained

At ad56d940de74f3b215e085b3c7da137177486351, the approved continuation stops:
19 units pass, serveapp-other fails, 26 do not start. Its 51 required roots
account for 49 pass and two fail. All started workers join; no supplement ran.
The earlier shutdown cancellation receipt remains red and distinct; its exact
cause and inheritance remain unproven under #2353. No exception is extended to
these two failures, and the SHA-bound exception is not automatically transferred
to the replacement head.

## Shared Owners And Repairs

- Existing EventBus CheckAPIEventPublishRecipientPlan performs exact endpoint,
  payload and read-only recipient preflight before finite eligibility. The API
  reuses its existing checker/mapping through checkEventPublicationRecipientPlan;
  missing checking capability fails closed. Existing-run recipient/target
  checks remain, and permanent replay still returns before fresh admission.
  No new payload validator or alternate algorithm. The authoritative run.start
  spec now explicitly states typed admission before finite eligibility.
- TestAudit2277StoppedRunReadinessRestart delegates its metadata-only revision to
  the existing canonicalrouting owner. The closed operation changes the name
  bytes only. The old whole-schema map decode/re-encode is deleted.
- The same operation family existed in the mailbox receiver fixture. Its closed
  gate constructor preserves waiting, active, done order instead of sorting
  active to the front. The old map reconstruction is deleted; all original
  mailbox refusal, retained-state, restart and no-mutation assertions remain.

## Guard Gap And Sibling Classification

The original corpus/input goldens did not execute the stopped-run test's later
source mutation. A complete positive-bundle construction guard likewise did
not detect this isolated re-encoding. This is a real missed consumer/guard
manifestation, raised on #2566 at comment 6042617258.

Post-mutation generated goldens now load, persist and reconstruct both actual
closed variants, independently checking entry, full stage order and finals.
A byte-diff proof permits only the metadata name replacement. A small scoped
consumer guard rejects raw YAML imports in the two actual stage-bearing
mutation consumers, including aliased/dot imports, so moving back to raw
schema parsing there fails the local sweep. JSON/data serialization remains
a positive control. No universal Go/YAML taint or runtime historical-intent
framework is claimed.

Correction to the initial sibling note: semanticview.receiverInstanceSource
rewrites the stage-free root schema from terminal-retirement/root-schema.yaml,
not worker-flow/schema.yaml. Its nested child is authored separately; the
worker's idle/complete order is not re-encoded. It is therefore a different
semantic concept with this concrete source-path proof, not a third fixed
stage-order manifestation. Other inspected ingress/root map mutations use
stage-free schemas; accumulator event/type/node, tool-input, config and JSON
serialization are separate concepts. The existing broader grammar parents
remain open; no watchlist node or new issue is needed.

## Proof Matrix

| Manifestation | Coverage | Exact proof |
| --- | --- | --- |
| Malformed payload masked by finite refusal | reproduced and fixed | Original TestA2PortfolioPublicTypedIngressRefusesBeforeMutationBothStores; new TestFiniteRunStartPayloadAdmissionPrecedesEligibilityBothStores, 48 cells across both stores, finite/service, event/event+data and six malformed shapes, with all durable counts unchanged |
| Valid service payload or feed incorrectly admitted | execution-proven through the same corrected path | Original TestFiniteRunStartRefusesServiceBeforeMutationBothStores; unchanged event/event+data/feed, repeat and no-effects assertions |
| Replay/conflict or publication-failure semantics changed | execution-proven through the same corrected path | Original TestFiniteRunStartReceiptReplayAndConflictBothStores / TestOperatorRunStartHandlersFailClosedBeforePersistence; post-admission failure doubles explicitly expose preflight; original missing-checker double remains unchanged |
| Restart metadata revision changes entry | reproduced and fixed | Original TestAudit2277StoppedRunReadinessRestart on both stores; TestStageOrderedSourceRenameChangesOnlyMetadata; actual post-mutation loaded/persisted/retained entry goldens |
| Mailbox schema revision changes receiver entry | reproduced and fixed | Original TestSelectedForkMailboxControlRefusalsBothStores; actual closed receiver-gate post-mutation goldens |
| Consumer can restore raw source reconstruction | reproduced and fixed | TestStageOrderedFixtureConsumersUseClosedMutationOwner / TestStageOrderedFixtureConsumerGuardRejectsReparsing, including aliased and dot-import hostile controls |
| Sorting changes a reachable entry without structural failure | execution-proven through the same corrected path | Original TestRewrite2566EntryGoldenRejectsSortedDumpWithoutStranding |

Count-one served repair roots pass together on both stores (13.115s).
New payload-precedence race/count-three passes (33.892s). Order guard plus
actual generated/retained goldens pass race/count-three (32.807s).
All 78 structural guard roots and vet pass. The committed script still replays
417 exact files and 553 independent entry decisions byte-idempotently;
only its authoritative spec output hash/prose edit is refreshed.

The oversized API and served combined race/count-three commands time out at
their unchanged 300s/180s aggregate caps. They show active tests, not an
assertion failure or reported race; they remain failed evidence, not green.
Replacement per-family commands retain repetitions, assertions and original
timeouts, and use managed admission rather than overlapping unscheduled runs.

The first full sweep incorrectly exported the census-only base variable into
serve tests; strict environment admission correctly rejects it. That invocation
is red; the corrected sweep does not export it. A separate SQL/type inventory
failure exposed the diagnostic overlay copy left as executable .go scratch;
it is retained as .go.txt, not made a classified production seam. The wrong
initial ratchet package invocation was cancelled before execution and earns no
credit; the actual internal/store ratchet is required.

Final managed sweep, actual persistence ratchet and partitioned preservation
receipts are separate outstanding/updated evidence, not inferred from these
focused passes. No server2 run starts without the next explicit handoff.
CI full / Local lifecycle plus the same 13 supplements remain unchanged.
Reviewer-d must reassess affected-proof carry-forward at the new head;
composed A/E proof and hosted full remain merge obligations.
