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

const timestampLayout = "2006-01-02T15:04:05.000Z"

const transactionColumns = `transaction_id, terminal_id, amount_minor, currency, created_at, seq, schema_version`

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

// Payment adds locally stored delivery information to the original transaction.
type Payment struct {
	Transaction
	SentAt *string `json:"sent_at,omitempty"`
	Result *Result `json:"result,omitempty"`
}

// Result is the final decision received from the payment processor.
type Result struct {
	Status      string  `json:"status"`
	Reason      *string `json:"reason"`
	ProcessedAt string  `json:"processed_at"`
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
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: "_busy_timeout=5000&_txlock=immediate"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open queue: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
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
		CreatedAt:     time.Now().UTC().Format(timestampLayout),
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

func (q *Queue) List(ctx context.Context, terminalID string) ([]Payment, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT `+transactionColumns+`, sent_at, result_status, result_reason, processed_at
		FROM queued_transactions WHERE terminal_id = ? ORDER BY seq`, terminalID)
	if err != nil {
		return nil, fmt.Errorf("read queue: %w", err)
	}
	defer rows.Close()

	var payments []Payment
	for rows.Next() {
		var payment Payment
		var result Result
		var status, processedAt sql.NullString
		if err := rows.Scan(append(payment.scanFields(), &payment.SentAt, &status, &result.Reason, &processedAt)...); err != nil {
			return nil, fmt.Errorf("read payment: %w", err)
		}
		if status.Valid {
			result.Status = status.String
			result.ProcessedAt = processedAt.String
			payment.Result = &result
		}
		payments = append(payments, payment)
	}
	return payments, rows.Err()
}

// NextUnsent returns the oldest payment still needing delivery for this terminal.
func (q *Queue) NextUnsent(ctx context.Context, terminalID string) (Transaction, bool, error) {
	var payment Transaction
	err := q.db.QueryRowContext(ctx, `
		SELECT `+transactionColumns+` FROM queued_transactions
		WHERE terminal_id = ? AND sent_at IS NULL AND result_status IS NULL
		ORDER BY seq LIMIT 1`, terminalID).Scan(payment.scanFields()...)
	if err == sql.ErrNoRows {
		return Transaction{}, false, nil
	}
	if err != nil {
		return Transaction{}, false, fmt.Errorf("read next unsent payment: %w", err)
	}
	return payment, true, nil
}

// MarkSent records the first observed Kafka acknowledgement independently of the result.
func (q *Queue) MarkSent(ctx context.Context, terminalID, transactionID string) error {
	_, err := q.db.ExecContext(ctx, `
		UPDATE queued_transactions SET sent_at = ?
		WHERE terminal_id = ? AND transaction_id = ? AND sent_at IS NULL`,
		time.Now().UTC().Format(timestampLayout), terminalID, transactionID)
	if err != nil {
		return fmt.Errorf("mark payment sent: %w", err)
	}
	return nil
}

// SaveResult reports whether it stored the first final result for a local payment.
func (q *Queue) SaveResult(ctx context.Context, terminalID, transactionID string, result Result) (bool, error) {
	updated, err := q.db.ExecContext(ctx, `
		UPDATE queued_transactions SET result_status = ?, result_reason = ?, processed_at = ?
		WHERE terminal_id = ? AND transaction_id = ? AND result_status IS NULL`,
		result.Status, result.Reason, result.ProcessedAt, terminalID, transactionID)
	if err != nil {
		return false, fmt.Errorf("save payment result: %w", err)
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check saved payment result: %w", err)
	}
	return count == 1, nil
}

func (payment *Transaction) scanFields() []any {
	return []any{&payment.TransactionID, &payment.TerminalID, &payment.AmountMinor,
		&payment.Currency, &payment.CreatedAt, &payment.Seq, &payment.SchemaVersion}
}
