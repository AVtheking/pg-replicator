CREATE TABLE todos (
    id         SERIAL PRIMARY KEY,
    title      TEXT NOT NULL,
    completed  BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Send full old-row values on UPDATE/DELETE so the sync engine can
-- evaluate shape where-clauses against the previous state of a row.
ALTER TABLE todos REPLICA IDENTITY FULL;

CREATE PUBLICATION sync_pub FOR TABLE todos;

INSERT INTO todos (title, completed) VALUES
    ('Build replication consumer', false),
    ('Decode pgoutput messages', false),
    ('Ship the sync engine', false);
