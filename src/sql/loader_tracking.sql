-- Additive loader bookkeeping. Does not alter observations, predictions or DQ.
-- A file receipt is committed in the SAME transaction as its observation inserts.
CREATE TABLE IF NOT EXISTS aircraft_loaded_files (
    source_key TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    mtime_ns BIGINT NOT NULL,
    rows_read BIGINT NOT NULL CHECK (rows_read >= 0),
    rows_inserted BIGINT NOT NULL CHECK (rows_inserted >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_key, relative_path)
);

CREATE TABLE IF NOT EXISTS aircraft_load_state (
    source_key TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision > 0),
    last_committed_at TIMESTAMPTZ NOT NULL,
    last_file TEXT NOT NULL
);
