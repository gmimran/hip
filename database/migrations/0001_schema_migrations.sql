CREATE TABLE IF NOT EXISTS hip.schema_migrations (
    version VARCHAR(100) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_ms INTEGER NOT NULL CHECK (execution_ms >= 0)
);
