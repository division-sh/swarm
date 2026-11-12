# Compiled Channel Provider Boundary

This is the R130(a) boundary, not a plugin API. The one compile-time installation
table is `serveapp.installedChannelProviders`: immutable facts plus registration,
deployment-connection and runtime-connection factories. This lower package has
only value definitions and codecs, no installed rows or registration API.
Bindings grant no authority and expose no constructor or mutator. Packs cannot
install Go callbacks. CLI and serve use the same composition through the existing
injected registration role; cliapp does not import serveapp.

## Ownership

The provider supplies genuine readiness, its private backing/lifetime, exact
account and destination codecs, authenticated intake, retained capture/ACK,
accepted operations and truthful receipts. Telegram HTTP/polling and WhatsApp SDK
remain concrete implementations; neither is a fallback for the other.

The existing common owners retain principal/challenge/window/consent, exact
operation/source/generation/credential/session admission, binding/proof, journal
launch and settlement, text/reply/card/action meaning, statuses and joined cleanup.
In particular, readiness is not human identity and SDK retry state is not send
authority. The existing deployment-versus-learned claimant confirmation policy
stays in the CLI owner, not in the binding or inferred from session transport.

The account/input readers and connection role project existing methods only.
Private account/input seals remain at their current issuers. Publication backing
comes from the original retained reader, not an independently supplied store;
native execution must use the exact state associated with its account issuer.
WhatsApp's `whatsapp_capture` key, fingerprint, private schema and selected-receipt
reconciliation stay unchanged, including private SQLite backing with PostgreSQL.

Known-provider validation receives one selected fact value, including config
validation before factory construction. The one native constructor that decodes
providers from retained rows receives a non-enumerating facts-only lookup. That
owner uses the same lookup on reads and writes and never forwards it. Missing,
unknown or mismatched facts cannot acquire registration authority. Private
readback values are not serialized or persisted and cannot replace native checks.

Custody is unchanged: restrictive exclusively owned per-connection directories,
ownership/symlink refusals and provider-only decoding; no encryption, copy,
export, migration or fork-clone claim. Ordinary retirement drains/fences and
retains pairing; only explicit logout removes the device. Failed cleanup retains
responsibility and successor fences. Development state remains ephemeral.

## Selection Inventory

The IDs below refer to E's 58-locus source inventory in issue #2338 comment
6700559556. This is an explanation of ownership, not an executable census.

| Before loci | Current owner |
| --- | --- |
| C01, C03-C04, C08-C15 | Installed declaration/intake/codec/presentation facts, including the landed Telegram bot address/link and registration copy, consumed by existing CLI owners. |
| C02, C05-C07 | Existing common confirmation, claimant wording and continuation policy; preserve automatic confirmation for WhatsApp and deployment Telegram, without generalizing it to session transport. |
| O01-O07 | Existing declaration, account, scope, logout and native-activity validators consume exact installed facts; their authority predicates remain. |
| S01-S10 | Existing platform assembly selects the HTTP/native role once; concrete Telegram HTTP/polling and native connection methods own protocol work. |
| S11-S19 | Existing holder/account/current-credential/source owners consume installed transport and presentation facts. |
| P01 | Common issuer consumes the private original-account reader; concrete SDK account/occurrence/failure projection remains provider work. |
| P02-P07 | Concrete WhatsApp QR, private SDK construction, plan qualification and send implementation; exact finite native-target refusals remain necessary. |
| P08-P09, D01-D03 | Concrete authenticated Telegram polling/webhook protocol, credential seal and exact receipt-before-ACK. |
| D04 | Original admitted provider selects the capture codec/provenance key, not a caller-selected alternate backing. |
| D05-D08 | Existing finite native operation/category/effect catalog; its provider projection is in that catalog and execution compares the selected binding's exact target, never an arbitrary executable string. |

## Third Provider / Discord Paper Check

A third adapter must still supply its packs/embedded inventory, one compiled
installation entry, its tests/docs/private persistence adapter as needed, and
finite native catalog entries if it introduces new operations/category/effects.
Protocol implementation belongs behind the existing private roles in the sealed
native subtree. A session adapter's implementation files can reuse the private
account/input issuers and executor there; no issuer is exported to create a new
standalone plugin package. The executor consumes a prepared protocol send while
retaining currentness, destination, journal launch and settlement. No routine
edit to common identity, binding, journal or selected-store algorithms is allowed;
an additional installation table or common authority method is a design return.

Discord is a paper check only: Gateway readiness/resume plus a genuine bot
credential/private state; opaque account/channel/message IDs; authenticated DM or
explicitly authorized shared-conversation claims; accepted HTTPS text/reply sends
and exact receipt projection; truthful disconnect/logout/join outcomes. No QR
fiction, dummy token, invented revocation, real adapter or live test is included.
The existing seven-bit vector suffices: card rendering, reply references, text
actions and inbox listing true; buttons, edit and acknowledgment false for this
minimal mapping, not a claim those features are absent from Discord. No public
interface field, operation, spelling, default or limit is added.

Paper construction: the installation entry supplies the new adapter's existing
registration/deployment/runtime role factories and plan qualifier. Its adapter
files supply the real account/occurrence reader, retained capture reader and
prepared send/activity projection; exact state identity still joins the account
issuer to execution. Its pack and embedded inventory supply the existing vector
and declarations; any new operation touches the finite native catalog. Tests,
docs and provider-private backing remain the fourth touch class. No common
identity, input-seal, journal or selected-store method needs a provider branch.
