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
