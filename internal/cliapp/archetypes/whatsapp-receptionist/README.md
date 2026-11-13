# WhatsApp receptionist

Run `swarm channel connect whatsapp` here. Scan the
terminal QR from WhatsApp's Linked Devices screen, then confirm the linked
account. Replace `<number>` in `policy.yaml` with that account's international
number, including `+`, then run `swarm serve`. A customer can
send a message to that number and receive the same text back once.

This flow has no human approval step, so it never challenges a customer
or asks for an operator code. The generated `.swarm/swarm.yaml` is the
deployment configuration. Its API token is stored separately; keep the
entire `.swarm` directory private and out of source control.

After restarting `swarm serve`, the retained pairing reconnects. To remove
this linked device, run `swarm channel logout whatsapp`.

Connect works while serve runs. WhatsApp receives live for the account's
current flow holder; no serve restart is needed to pick up the connection.

`swarm serve --dev` uses invocation-only session state in a private temporary
directory, so a dev restart requires pairing again. Use plain `swarm serve`
to retain pairing across restarts.

The session provider does not pace sends. Flows must pace themselves;
high-rate sending risks WhatsApp account restrictions or a ban.

## Beta limitations

Messages sent while setup or serve is not receiving may never reach Swarm.
We have not established provider redelivery.

This echo scaffold handles new supported text, not edits/revokes. An earlier
echo is not withdrawn. Swarm makes no read-receipt call; the delivered/read
indicators shown by your phone are not qualified.

During operator setup, a five-digit customer message that is not the open code
can print `error: bad code` on the setup terminal; it still follows the flow's
normal customer route and receives no platform error reply. Wrong-code notices
coalesce between polls; they are not an attempt count or a delivery guarantee.

Typing from another linked device does not take over the conversation: those
self echoes are discarded, and the receptionist keeps answering. The owner can
read the chat in their own WhatsApp app. For intervention, an integrating app
must use its owner-agent relay; this echo starter does not implement that relay.

Treat an inbound `external_account` as an opaque delivered sender ID, not a
phone number. An app can bind it to a role through its own code ceremony, but
must establish its stability for the same sender across the supported message
kinds and reconnect first. The provider accepts phone-number and LID forms and
does not normalize them into one identity. Mocked messages do not establish
live stability; do not add a phone lookup or silently merge those IDs.

Inbound message, edit and revoke events may also carry `sender_phone` as a
canonical E.164 string. It is absent when unknown, never an empty string.
The value comes from the delivered sender, its phone-form alternate, or the
already-learned local map, in that order; there is no remote lookup or retry.
The first successful capture freezes this optional value, including absence.
Later knowledge does not enrich a pending or published message. This field is
ordinary flow data that can be handed to an agent, not sender identity or proof.

In `swarm test` scenarios, `whatsapp.send_text` returns a deterministic
schema-valid response without pairing or sending a real message. A `derive`
scenario can override that response with `connector_responses`; mocked
failures are not supported.
