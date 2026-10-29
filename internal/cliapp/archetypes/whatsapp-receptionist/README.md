# WhatsApp receptionist

Run `swarm channel connect whatsapp` here before starting serve. Scan the
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

Connect with serve stopped. Restart serve when a new holder appears: a
registration or account assignment made while serve is running is not
automatically admitted into that running process.

`swarm serve --dev` uses invocation-only session state in a private temporary
directory, so a dev restart requires pairing again. Use plain `swarm serve`
to retain pairing across restarts.

The session provider does not pace sends. Flows must pace themselves;
high-rate sending risks WhatsApp account restrictions or a ban.

In `swarm test` scenarios, `whatsapp.send_text` returns a deterministic
schema-valid response without pairing or sending a real message. A `derive`
scenario can override that response with `connector_responses`; mocked
failures are not supported.
