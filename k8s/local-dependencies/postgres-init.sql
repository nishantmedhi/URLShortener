CREATE TABLE IF NOT EXISTS links (
    code text PRIMARY KEY,
    destination text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz
);

CREATE TABLE IF NOT EXISTS clicks (
    event_id uuid PRIMARY KEY,
    code text NOT NULL REFERENCES links(code) ON DELETE CASCADE,
    clicked_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS clicks_code_clicked_at_idx ON clicks(code, clicked_at DESC);
