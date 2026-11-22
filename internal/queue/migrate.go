package queue

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaVersion = 1

// The original queue used SQLite's default user_version of zero.
const migrateToV1 = `
CREATE TABLE IF NOT EXISTS terminal_sequences (
    terminal_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL CHECK (last_seq > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS queued_transactions (
    transaction_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    amount_minor INTEGER NOT NULL CHECK (amount_minor > 0),
    currency TEXT NOT NULL,
    created_at TEXT NOT NULL,
    seq INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    UNIQUE (terminal_id, seq)
) STRICT;

ALTER TABLE queued_transactions ADD COLUMN sent_at TEXT;
ALTER TABLE queued_transactions ADD COLUMN result_status TEXT;
ALTER TABLE queued_transactions ADD COLUMN result_reason TEXT;
ALTER TABLE queued_transactions ADD COLUMN processed_at TEXT CHECK (
    (result_status IS NULL AND result_reason IS NULL AND processed_at IS NULL)
    OR (result_status IS NOT NULL AND processed_at IS NOT NULL AND (
        (result_status = 'success' AND result_reason IS NULL)
        OR (result_status = 'failed' AND result_reason IS NOT NULL AND result_reason <> '')
    ))
);

CREATE INDEX queued_transactions_unsent ON queued_transactions (terminal_id, seq)
    WHERE sent_at IS NULL AND result_status IS NULL;
PRAGMA user_version = 1;
`

func migrate(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read queue schema version: %w", err)
	}
	if version == schemaVersion {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("unsupported queue schema version %d", version)
	}

	// Recheck under the write lock in case another process migrated first.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin queue migration: %w", err)
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("recheck queue schema version: %w", err)
	}
	switch version {
	case schemaVersion:
		return nil
	case 0:
		if _, err := tx.ExecContext(ctx, migrateToV1); err != nil {
			return fmt.Errorf("migrate queue to version 1: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit queue migration: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported queue schema version %d", version)
	}
}
