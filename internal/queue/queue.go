package queue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

const schema = `
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
`

// Transaction contains the fields of a pos.transactions.v1 message.
type Transaction struct {
	SchemaVersion int    `json:"schema_version"`
	TransactionID string `json:"transaction_id"`
	TerminalID    string `json:"terminal_id"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
	CreatedAt     string `json:"created_at"`
	Seq           int64  `json:"seq"`
}

type Queue struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Queue, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve queue path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create queue directory: %w", err)
	}
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: "_busy_timeout=5000"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open queue: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize queue: %w", err)
	}
	return &Queue{db: db}, nil
}

func (q *Queue) Close() error {
	return q.db.Close()
}

// Enqueue commits the payment and its sequence number together.
func (q *Queue) Enqueue(ctx context.Context, terminalID string, amountMinor int64) (Transaction, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Transaction{}, fmt.Errorf("generate transaction ID: %w", err)
	}
	payment := Transaction{
		SchemaVersion: 1,
		TransactionID: id.String(),
		TerminalID:    terminalID,
		AmountMinor:   amountMinor,
		Currency:      "RUB",
		CreatedAt:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return Transaction{}, fmt.Errorf("begin queue write: %w", err)
	}
	defer tx.Rollback()

	err = tx.QueryRowContext(ctx, `
		INSERT INTO terminal_sequences (terminal_id, last_seq) VALUES (?, 1)
		ON CONFLICT (terminal_id) DO UPDATE SET last_seq = last_seq + 1
		RETURNING last_seq`, terminalID).Scan(&payment.Seq)
	if err != nil {
		return Transaction{}, fmt.Errorf("allocate sequence: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO queued_transactions
			(transaction_id, terminal_id, amount_minor, currency, created_at, seq, schema_version)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, payment.TransactionID, payment.TerminalID, payment.AmountMinor,
		payment.Currency, payment.CreatedAt, payment.Seq, payment.SchemaVersion)
	if err != nil {
		return Transaction{}, fmt.Errorf("queue payment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Transaction{}, fmt.Errorf("commit queue write: %w", err)
	}
	return payment, nil
}

func (q *Queue) List(ctx context.Context, terminalID string) ([]Transaction, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT transaction_id, terminal_id, amount_minor, currency, created_at, seq, schema_version
		FROM queued_transactions WHERE terminal_id = ? ORDER BY seq`, terminalID)
	if err != nil {
		return nil, fmt.Errorf("read queue: %w", err)
	}
	defer rows.Close()

	var payments []Transaction
	for rows.Next() {
		var payment Transaction
		if err := rows.Scan(&payment.TransactionID, &payment.TerminalID, &payment.AmountMinor,
			&payment.Currency, &payment.CreatedAt, &payment.Seq, &payment.SchemaVersion); err != nil {
			return nil, fmt.Errorf("read payment: %w", err)
		}
		payments = append(payments, payment)
	}
	return payments, rows.Err()
}
