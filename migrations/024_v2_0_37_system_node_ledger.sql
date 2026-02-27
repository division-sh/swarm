-- v2.0.37: add idempotency ledger for runtime system nodes.
CREATE TABLE IF NOT EXISTS system_node_ledger (
    event_id        UUID NOT NULL REFERENCES events(id),
    node_id         TEXT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, node_id)
);
