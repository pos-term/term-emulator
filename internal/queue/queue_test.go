package queue

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDeliverySurvivesRestart(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "queue.db")
	q := openQueue(t, path)
	first := enqueue(t, q, "term-001")
	second := enqueue(t, q, "term-001")
	other := enqueue(t, q, "term-002")
	checkNextUnsent(t, q, first)

	// A crash after Kafka acknowledges a record can leave it unmarked locally.
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = openQueue(t, path)
	checkNextUnsent(t, q, first)
	if err := q.MarkSent(ctx, first.TerminalID, first.TransactionID); err != nil {
		t.Fatal(err)
	}
	checkNextUnsent(t, q, second)
	checkNextUnsent(t, q, other)

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = openQueue(t, path)
	checkNextUnsent(t, q, second)
	payments := listPayments(t, q, first.TerminalID)
	if len(payments) != 2 || payments[0].Transaction != first || payments[0].SentAt == nil || payments[0].Result != nil {
		t.Fatalf("expected first payment to be awaiting a result, got %+v", payments)
	}
	if _, err := time.Parse(timestampLayout, *payments[0].SentAt); err != nil {
		t.Fatal(err)
	}
	result := Result{Status: "success", ProcessedAt: "2026-11-21T10:15:31.005Z"}
	if saved, err := q.SaveResult(ctx, first.TerminalID, first.TransactionID, result); err != nil || !saved {
		t.Fatalf("save result for previously acknowledged payment: saved=%v, err=%v", saved, err)
	}
	got := listPayments(t, q, first.TerminalID)[0]
	if !reflect.DeepEqual(got.SentAt, payments[0].SentAt) || !reflect.DeepEqual(got.Result, &result) {
		t.Fatalf("payment after result: %+v", got)
	}
}

func TestResultBeforeAcknowledgement(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "queue.db")
	q := openQueue(t, path)
	payment := enqueue(t, q, "term-001")
	reason := "LIMIT_EXCEEDED"
	result := Result{Status: "failed", Reason: &reason, ProcessedAt: "2026-11-21T10:15:31.005Z"}

	if saved, err := q.SaveResult(ctx, "term-002", payment.TransactionID, result); err != nil || saved {
		t.Fatalf("save result addressed to another terminal: saved=%v, err=%v", saved, err)
	}
	checkNextUnsent(t, q, payment)
	if saved, err := q.SaveResult(ctx, payment.TerminalID, payment.TransactionID, result); err != nil || !saved {
		t.Fatalf("save result before ack: saved=%v, err=%v", saved, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = openQueue(t, path)
	if _, found, err := q.NextUnsent(ctx, payment.TerminalID); err != nil || found {
		t.Fatalf("resend after an early result: found=%v, err=%v", found, err)
	}
	beforeAck := listPayments(t, q, payment.TerminalID)[0]
	if beforeAck.SentAt != nil || !reflect.DeepEqual(beforeAck.Result, &result) {
		t.Fatalf("payment with result and no ack: %+v", beforeAck)
	}
	if err := q.MarkSent(ctx, payment.TerminalID, payment.TransactionID); err != nil {
		t.Fatal(err)
	}
	for _, replay := range []Result{result, {Status: "success", ProcessedAt: result.ProcessedAt}} {
		if saved, err := q.SaveResult(ctx, payment.TerminalID, payment.TransactionID, replay); err != nil || saved {
			t.Fatalf("save repeated result: saved=%v, err=%v", saved, err)
		}
	}
	got := listPayments(t, q, payment.TerminalID)[0]
	if got.SentAt == nil || !reflect.DeepEqual(got.Result, &result) {
		t.Fatalf("payment after ack and repeated results: %+v", got)
	}
}

func TestInvalidResultLeavesPaymentUnsent(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.db"))
	payment := enqueue(t, q, "term-001")
	for _, status := range []string{"pending", "failed"} {
		result := Result{Status: status, ProcessedAt: "2026-11-21T10:15:31.005Z"}
		if saved, err := q.SaveResult(t.Context(), payment.TerminalID, payment.TransactionID, result); err == nil || saved {
			t.Fatalf("save invalid result %s: saved=%v, err=%v", status, saved, err)
		}
	}
	checkNextUnsent(t, q, payment)
}

func openQueue(t *testing.T, path string) *Queue {
	t.Helper()
	q, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func enqueue(t *testing.T, q *Queue, terminalID string) Transaction {
	t.Helper()
	payment, err := q.Enqueue(t.Context(), terminalID, 15050)
	if err != nil {
		t.Fatal(err)
	}
	return payment
}

func listPayments(t *testing.T, q *Queue, terminalID string) []Payment {
	t.Helper()
	payments, err := q.List(t.Context(), terminalID)
	if err != nil {
		t.Fatal(err)
	}
	return payments
}

func checkNextUnsent(t *testing.T, q *Queue, want Transaction) {
	t.Helper()
	got, found, err := q.NextUnsent(t.Context(), want.TerminalID)
	if err != nil || !found || got != want {
		t.Fatalf("next unsent payment: got %+v, found=%v, err=%v; want %+v", got, found, err, want)
	}
}
