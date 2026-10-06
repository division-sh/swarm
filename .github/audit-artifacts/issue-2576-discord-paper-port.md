# Discord Paper Port: N68 / N73

This is a descriptor and protocol transcript, not an installed Discord provider.
The test-only channel manifest compiles against the existing neutral interface.
No Discord parser, Gateway implementation, credential, or live interoperability
claim is introduced. Session execution remains explicitly unavailable in PR1.

## Declaration And Owners

`internal/packs/testdata/discord-paper-port/channel.yaml` declares session ingress
and an ordinary outbound non-idempotent HTTP operation. Its vector requires
English card text, real replies, text actions and Inbox; buttons, edit and callback
acknowledgment are false. No Telegram menu/launcher operations are fabricated.
The provider may add supported optional operations later without changing the
interface. Native callback acknowledgment is not principal notice acknowledgment.

The real bot token is an admitted credential, not a stand-in for session state.
`ProviderAuthoritySession` carries that seal, stable connection UUID, exact bot
account, admission UUID and revision. Connection occurrence/health is separate.
The bot account is not the operator account. Readiness never grants human access.
An eventual installed adapter consumes the existing onboarding responsibility,
credential snapshot, activation, normalized-publication and effect owners.
No declaration-key fallback, resealing, unsigned external endpoint or independent
effect ledger is needed. An uninstalled adapter is rejected before mutation.

## Protocol Boundary

The provider receives authenticated Gateway dispatches and sends through HTTPS.
It must qualify the configured DM/guild intents and actual content permissions;
empty/redacted content cannot complete Inbox or a claim. Transport readiness is
bound to the bot identity, admission, session occurrence and successful health
observation. Resuming an inbound sequence permits duplicate inbound capture,
not replay of an uncertain outbound send. Missing token, revoked permission,
invalid session and disconnect are explicit readiness failures, not identity.

For outbound messages the compiled text budget is 2,000 characters. The adapter
supplies `allowed_mentions: {parse: []}` and does not infer recipients from text.
Provider references are exact message IDs joined with the admitted bot/channel;
the source/reference/control budget is enforced before any send. HTTP failure
before launch differs from acceptance without a receipt. The latter remains
uncertain; it grants neither automatic resend nor fabricated acknowledgment.

These provider prerequisites follow the official
[Gateway contract](https://docs.discord.com/developers/events/gateway) and
[message contract](https://docs.discord.com/developers/resources/message).

## Human And Inbox Transcript

| Step | Direct Conversation | Guild Conversation | Existing Owner |
| --- | --- | --- | --- |
| 1 | API principal reserves connect; token/session qualified | Same, with guild/channel identity | Onboarding + provider authority |
| 2 | Operator sends the one-time challenge | Same exact sender, group disclosure and consent | Verified human claim, not first sender |
| 3 | Real confirmation receipt binds bot/channel/human | Same; other guild members gain no HITL permission | Binding + activation |
| 4 | Unquoted `/inbox`, no previous bot receipt needed | Same command; only claimed human admitted | Neutral Inbox entry |
| 5 | Current card/notice text with stable reference/control words | Visible group output as disclosed | Immutable render + real delivery receipt |
| 6 | Human genuinely replies with a displayed control word | Same exact human/channel/receipt | Reply-action transfer + shared mutation fence |
| 7 | Input start returns a current prompt; exact typed reply advances | Other members' replies do not advance it | Atomic draft/input-prompt owner |
| 8 | Notice `Acknowledge` settles through principal authority | Same human-only mutation | Notice acknowledgment owner |
| 9 | Restart retains binding and historical receipt; health requalifies | Same; no sender/room inference | Exact store/readiness/recovery owners |

No token-scoped fabricated API principal, slash-command callback or Telegram
native setting is required for any step. Existing both-store text-only journeys
prove these shared paths; this paper port is not an account-free Discord E2E.

## N73 Raw-To-Normalized Reference Matrix

Only an actual reply (`message.type=19`, default reference type, exact message ID
in the same channel) can supply `reply_id`. An absent reference type uses Discord's
documented DEFAULT value, not a guessed channel rule. The adapter must reject
contradictions before producing a normalized authorized event.

| Raw Observation | Normalized Result | Subsequent Authority |
| --- | --- | --- |
| Genuine direct/guild reply to bot receipt | Exact `reply_id` | Must still join stored bot/channel/human/receipt/control |
| Genuine reply, reference type omitted | Same documented DEFAULT semantics | Same exact join |
| Genuine reply, `referenced_message` absent | Unknown fetched content; exact reference ID only | Receipt join, never quoted content |
| Explicit null `referenced_message` | Deleted; no reply authority | Refusal/teaching, no card or draft mutation |
| Forward reference (`type=1`) or snapshot | No `reply_id` | Forwarded words are not controls |
| Crosspost/reference on non-reply message | No `reply_id` | No control authority |
| Thread starter/context-menu reference | No `reply_id` | No control authority |
| Missing ID, foreign channel, conflicting IDs | No authorized reply event | Fail closed at provider admission |
| Unknown reference/message type | No authorized reply event | Typed unsupported input, not ordinary reply |
| Unquoted `Retire` | Ordinary text, no `reply_id` | No action transfer |
| Bot/webhook-generated message claiming human | No human claim/control event | No operator identity |
| Content unavailable or over 2,000 characters | No admitted text fact | No Inbox/claim success |

`TestDiscordPaperPortNormalizedReplyAndReferenceTranscripts` consumes the declared
normalized outputs, transparently not a second raw interpreter. Real-store
`reply_matrix` proves foreign quote/account/conversation rejection, exact original
source evidence, rollback and repeat transfer. The public baseline proves stale
card/prompt refusal and customer isolation on both stores. These shared joins
must not be bypassed by any eventual Discord adapter.

## Closure Limit

N68/N73 establish that Discord needs no additional channel-interface field or
Telegram operation. They do not prove a Discord Gateway implementation, live
permissions, network delivery or session recovery. #2577 realizes the session
contract for WhatsApp; adding Discord later requires its own provider protocol
and account qualification, not a neutral-interface redesign.
