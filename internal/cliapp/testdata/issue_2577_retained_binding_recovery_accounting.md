# #2577 Retained Binding Recovery Accounting

Binding gate: issue comment `6104945833`, N13/N19/N34/N65/N66.

The two existing teardown fingerprints change for named semantic reasons:

| Predicate owner | Delta | Preserved proof |
| --- | --- | --- |
| DestructiveService.Unbind | Reads the canonical retained identity/revision, not an unavailable retired SDK connection. No generic stale-error bypass. | Exact expected revision, replay, selected teardown reservation and non-destructive cleanup remain unchanged; complete channelonboarding race controls and selected-store identity contracts cover those fences. |
| DestructiveService.retainedCredentialEvidence | Reads retained cleanup evidence without requiring provider execution. Session keys never become credentials. | Scope filtering and exact nonempty credential evidence remain unchanged; credential cleanup/rotation and retained-lifecycle controls pass on both stores. |

`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` refreshed only those two body hashes.
The independent owner list, scanner, audit rows and required proof families are
unchanged. This does not authorize an absent binding, SDK work, proof reuse,
automatic pairing, logout replay or legacy-store behavior.
