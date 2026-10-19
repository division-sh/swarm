# WhatsApp receptionist

Run `swarm serve` here. In another terminal in this directory, run
`swarm channel connect whatsapp`. Scan the terminal QR from WhatsApp's
Linked Devices screen, then confirm the linked account. A customer can
send a message to that number and receive the same text back once.

This flow has no human approval step, so it never challenges a customer
or asks for an operator code. The generated `.swarm/swarm.yaml` is the
deployment configuration. Its API token is stored separately; keep the
entire `.swarm` directory private and out of source control.

After restarting `swarm serve`, the retained pairing reconnects. To remove
this linked device, run `swarm channel logout whatsapp`.

`swarm serve --dev` uses invocation-only session state in a private temporary
directory, so a dev restart requires pairing again. Use plain `swarm serve`
to retain pairing across restarts.

The session provider does not pace sends. Flows must pace themselves;
high-rate sending risks WhatsApp account restrictions or a ban.

Authored `connector_responses` cannot mock `whatsapp.send_text`, including
failures: it is an in-process session operation, not an HTTP connector.
