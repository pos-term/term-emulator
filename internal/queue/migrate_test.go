package queue

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
)

func TestMigrateOriginalQueue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	legacy := createLegacyQueue(t, path)
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	const openers = 8
	start := make(chan struct{})
	var group sync.WaitGroup
	for range openers {
		group.Go(func() {
			<-start
			q, err := Open(t.Context(), path)
			if err != nil {
				t.Errorf("open queue during concurrent migration: %v", err)
				return
			}
			defer q.Close()
			if _, err := q.Enqueue(t.Context(), "term-001", 1); err != nil {
				t.Errorf("enqueue during concurrent migration: %v", err)
			}
		})
	}
	close(start)
	group.Wait()

	q := openQueue(t, path)
	want := Transaction{
		SchemaVersion: 1,
		TransactionID: "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10",
		TerminalID:    "term-001",
		AmountMinor:   15050,
		Currency:      "RUB",
		CreatedAt:     "2026-09-19T10:15:30.123Z",
		Seq:           42,
	}
	payments := listPayments(t, q, want.TerminalID)
	if len(payments) != 1+openers || payments[0] != (Payment{Transaction: want}) {
		t.Fatalf("migrated payments: %+v", payments)
	}
	for i, payment := range payments {
		if wantSeq := want.Seq + int64(i); payment.Seq != wantSeq {
			t.Fatalf("sequence after migration: got %d, want %d", payment.Seq, wantSeq)
		}
	}
	checkNextUnsent(t, q, want)
	if other := enqueue(t, q, "term-002"); other.Seq != 8 {
		t.Fatalf("sequence for second terminal after migration: got %d, want 8", other.Seq)
	}
}

func TestMigrationRollsBackOnFailure(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "queue.db")
	legacy := createLegacyQueue(t, path)
	if _, err := legacy.ExecContext(ctx, `CREATE INDEX queued_transactions_unsent ON queued_transactions (seq)`); err != nil {
		t.Fatal(err)
	}
	if q, err := Open(ctx, path); err == nil {
		q.Close()
		t.Fatal("expected migration to fail on index name collision")
	}
	var version int
	if err := legacy.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		t.Fatalf("queue schema version after failed migration: version=%d, err=%v", version, err)
	}
	if _, err := legacy.ExecContext(ctx, `DROP INDEX queued_transactions_unsent`); err != nil {
		t.Fatal(err)
	}
	q := openQueue(t, path)
	if payments := listPayments(t, q, "term-001"); len(payments) != 1 || payments[0].Seq != 42 {
		t.Fatalf("payments after migration retry: %+v", payments)
	}
}

func createLegacyQueue(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// Schema and insert layout from the first terminal CLI.
	_, err = db.ExecContext(t.Context(), `
		CREATE TABLE terminal_sequences (
			terminal_id TEXT PRIMARY KEY,
			last_seq INTEGER NOT NULL CHECK (last_seq > 0)
		) STRICT;
		CREATE TABLE queued_transactions (
			transaction_id TEXT PRIMARY KEY,
			terminal_id TEXT NOT NULL,
			amount_minor INTEGER NOT NULL CHECK (amount_minor > 0),
			currency TEXT NOT NULL,
			created_at TEXT NOT NULL,
			seq INTEGER NOT NULL,
			schema_version INTEGER NOT NULL,
			UNIQUE (terminal_id, seq)
		) STRICT;
		INSERT INTO terminal_sequences VALUES ('term-001', 42), ('term-002', 7);
		INSERT INTO queued_transactions VALUES (
			'0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10', 'term-001', 15050, 'RUB',
			'2026-09-19T10:15:30.123Z', 42, 1
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
