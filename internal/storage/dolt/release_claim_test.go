package dolt

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

func TestReleaseClaim(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const (
		id        = "test-release-claim"
		otherID   = "test-release-other"
		holder    = "zagcode-attempt-111111111111111111111111"
		successor = "zagcode-attempt-222222222222222222222222"
		actor     = "tracker-test"
	)
	operation := "tracker-release:" + strings.Repeat("a", 64)
	stale := "tracker-release:" + strings.Repeat("b", 64)

	createAndClaim := func(id string) {
		t.Helper()
		issue := &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
		if err := store.CreateIssue(ctx, issue, actor); err != nil {
			t.Fatalf("create issue: %v", err)
		}
		if err := store.ClaimIssue(ctx, id, holder); err != nil {
			t.Fatalf("claim issue: %v", err)
		}
	}
	get := func(id string) *types.Issue {
		t.Helper()
		issue, err := store.GetIssue(ctx, id)
		if err != nil {
			t.Fatalf("get issue: %v", err)
		}
		return issue
	}
	eventCount := func() int {
		t.Helper()
		events, err := store.GetEvents(ctx, id, 0)
		if err != nil {
			t.Fatalf("get events: %v", err)
		}
		return len(events)
	}
	refuse := func(id, assignee, op string) {
		t.Helper()
		if err := store.ReleaseClaim(ctx, id, assignee, op, actor); err == nil {
			t.Fatalf("expected release refusal for %q, %q, %q", id, assignee, op)
		}
	}

	createAndClaim(id)
	before := get(id)
	if before.StartedAt == nil {
		t.Fatal("claim did not set started_at")
	}
	refuse("test-release-cla", holder, operation)
	refuse(strings.ToUpper(id), holder, operation)
	refuse(id, "", operation)
	refuse(id, holder, "tracker-release:invalid")
	if err := store.ReleaseClaim(ctx, id, holder, operation, actor); err != nil {
		t.Fatalf("release claim: %v", err)
	}
	if got := get(id); got.Status != types.StatusOpen || got.Assignee != "" ||
		got.StartedAt == nil || !got.StartedAt.Equal(*before.StartedAt) || got.ClosedAt != nil {
		t.Fatalf("release did not reopen and unassign without changing lifecycle timestamps: %+v", got)
	}
	var committedEvents int
	if err := store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events AS OF 'HEAD' WHERE issue_id = ? AND event_type = ? AND actor = ?",
		id, types.EventStatusChanged, actor).Scan(&committedEvents); err != nil || committedEvents != 1 {
		t.Fatalf("committed release audit events = %d, err = %v; want 1", committedEvents, err)
	}
	var committedReceipt string
	if err := store.db.QueryRowContext(ctx, "SELECT value FROM metadata AS OF 'HEAD' WHERE `key` = ?",
		"_release_claim_v1:"+operation).Scan(&committedReceipt); err != nil || !strings.Contains(committedReceipt, operation) {
		t.Fatalf("release did not commit the receipt: %q, %v", committedReceipt, err)
	}

	if err := store.ClaimIssue(ctx, id, successor); err != nil {
		t.Fatalf("claim for successor: %v", err)
	}
	createAndClaim(otherID)
	beforeReplay := get(id)
	beforeEvents := eventCount()
	refuse(id, holder, stale)
	refuse(id, successor, operation)
	refuse(otherID, holder, operation)
	if err := store.ReleaseClaim(ctx, id, holder, operation, "different-audit-actor"); err != nil {
		t.Fatalf("replay after successor: %v", err)
	}
	if got := get(id); got.Status != types.StatusInProgress || got.Assignee != successor || !got.UpdatedAt.Equal(beforeReplay.UpdatedAt) {
		t.Fatalf("replay or stale request changed successor's claim: %+v", got)
	}
	if got := eventCount(); got != beforeEvents {
		t.Fatalf("replay or refusal changed audit history: got %d events, want %d", got, beforeEvents)
	}
	if got := get(otherID); got.Status != types.StatusInProgress || got.Assignee != holder {
		t.Fatalf("conflicting tuple changed another issue: %+v", got)
	}

	if err := store.CloseIssue(ctx, id, "done", actor, ""); err != nil {
		t.Fatalf("close issue: %v", err)
	}
	beforeEvents = eventCount()
	refuse(id, successor, stale)
	if got := get(id); got.Status != types.StatusClosed || got.ClosedAt == nil {
		t.Fatalf("release changed closed issue: %+v", got)
	}
	if got := eventCount(); got != beforeEvents {
		t.Fatalf("refusal changed closed issue audit: got %d events, want %d", got, beforeEvents)
	}
}

func TestReleaseClaimConcurrentOperations(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const (
		id     = "test-release-contended"
		holder = "zagcode-attempt-111111111111111111111111"
		actor  = "tracker-test"
	)
	op1 := "tracker-release:" + strings.Repeat("a", 64)
	op2 := "tracker-release:" + strings.Repeat("b", 64)
	issue := &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if err := store.CreateIssue(ctx, issue, actor); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := store.ClaimIssue(ctx, id, holder); err != nil {
		t.Fatalf("claim issue: %v", err)
	}

	begin := func() *sql.Tx {
		t.Helper()
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin release transaction: %v", err)
		}
		return tx
	}
	tx1 := begin()
	defer func() { _ = tx1.Rollback() }()
	tx2 := begin()
	defer func() { _ = tx2.Rollback() }()

	stage := func(tx *sql.Tx, op string) {
		t.Helper()
		result, err := issueops.ReleaseClaimInTx(ctx, tx, id, holder, op, actor)
		if err != nil || !result.Recorded || result.Updated == nil {
			t.Fatalf("stage release from original snapshot: result = %v, err = %v", result, err)
		}
		// Make the issue rows byte-for-byte equal regardless of wall time,
		// as they would be if both updates reached Dolt in the same second.
		if _, err := tx.ExecContext(ctx, "UPDATE issues SET updated_at = '2025-01-02 03:04:05' WHERE id = ?", id); err != nil {
			t.Fatalf("normalize release timestamps: %v", err)
		}
	}
	stage(tx1, op1)
	stage(tx2, op2)

	if err := tx1.Commit(); err != nil {
		t.Fatalf("first release commit: %v", err)
	}
	if err := tx2.Commit(); err == nil {
		t.Fatal("second release committed from the same original snapshot")
	}

	var receipts int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM metadata WHERE `key` IN (?, ?)",
		"_release_claim_v1:"+op1, "_release_claim_v1:"+op2).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("release receipts = %d, err = %v; want 1", receipts, err)
	}
	events, err := store.GetEvents(ctx, id, 0)
	if err != nil {
		t.Fatalf("read release audit: %v", err)
	}
	var releases int
	for _, event := range events {
		if event.EventType == types.EventStatusChanged && event.Actor == actor {
			releases++
		}
	}
	if releases != 1 {
		t.Fatalf("release audit events = %d; want 1", releases)
	}
	if changed, err := store.ReleaseClaimWithResult(ctx, id, holder, op1, actor); err != nil || changed {
		t.Fatalf("winning receipt replay: changed = %v, err = %v; want false, nil", changed, err)
	}
	if err := store.ReleaseClaim(ctx, id, holder, op2, actor); err == nil {
		t.Fatal("losing operation replay incorrectly returned a receipt")
	}
}

func TestReleaseClaimRefusalLeavesTrackerUnchanged(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const (
		holder    = "zagcode-attempt-111111111111111111111111"
		successor = "zagcode-attempt-222222222222222222222222"
		actor     = "tracker-test"
	)
	for _, tc := range []struct {
		name   string
		reason storage.ReleaseClaimRefusalReason
		hex    string
	}{
		{"closed", storage.ReleaseClaimNotInProgress, "c"},
		{"reassigned", storage.ReleaseClaimHolderChanged, "d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "test-release-" + tc.name
			op := "tracker-release:" + strings.Repeat(tc.hex, 64)
			issue := &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
			if err := store.CreateIssue(ctx, issue, actor); err != nil {
				t.Fatalf("create issue: %v", err)
			}
			if err := store.ClaimIssue(ctx, id, holder); err != nil {
				t.Fatalf("claim issue: %v", err)
			}
			if tc.reason == storage.ReleaseClaimNotInProgress {
				if err := store.CloseIssue(ctx, id, "done", actor, ""); err != nil {
					t.Fatalf("close issue: %v", err)
				}
			} else if err := store.UpdateIssue(ctx, id, map[string]interface{}{"assignee": successor}, actor); err != nil {
				t.Fatalf("reassign issue: %v", err)
			}

			before, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatalf("read issue before refusal: %v", err)
			}
			beforeEvents, err := store.GetEvents(ctx, id, 0)
			if err != nil {
				t.Fatalf("read audit before refusal: %v", err)
			}
			beforeCommit, err := store.GetCurrentCommit(ctx)
			if err != nil {
				t.Fatalf("read commit before refusal: %v", err)
			}
			changed, err := store.ReleaseClaimWithResult(ctx, id, holder, op, actor)
			var refusal *storage.ReleaseClaimRefusal
			if changed || !errors.As(err, &refusal) || refusal.Reason != tc.reason {
				t.Fatalf("release = (%v, %v); want refusal %s", changed, err, tc.reason)
			}
			after, err := store.GetIssue(ctx, id)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Errorf("refusal changed issue: before = %+v, after = %+v, err = %v", before, after, err)
			}
			afterEvents, err := store.GetEvents(ctx, id, 0)
			if err != nil || !reflect.DeepEqual(beforeEvents, afterEvents) {
				t.Errorf("refusal changed audit: before = %+v, after = %+v, err = %v", beforeEvents, afterEvents, err)
			}
			if afterCommit, err := store.GetCurrentCommit(ctx); err != nil || afterCommit != beforeCommit {
				t.Errorf("refusal changed Dolt HEAD: before = %s, after = %s, err = %v", beforeCommit, afterCommit, err)
			}
			var records int
			if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM metadata WHERE `key` = ?",
				"_release_claim_v1:"+op).Scan(&records); err != nil || records != 0 {
				t.Errorf("refusal metadata rows = %d, err = %v; want 0", records, err)
			}

			// A refusal leaves the operation ID available if the predicate later matches.
			if err := store.UpdateIssue(ctx, id, map[string]interface{}{
				"assignee": holder,
				"status":   types.StatusInProgress,
			}, actor); err != nil {
				t.Fatalf("restore matching claim: %v", err)
			}
			if changed, err := store.ReleaseClaimWithResult(ctx, id, holder, op, actor); err != nil || !changed {
				t.Fatalf("retry of the same operation = (%v, %v); want new release", changed, err)
			}
			released, err := store.GetIssue(ctx, id)
			if err != nil || released.Status != types.StatusOpen || released.Assignee != "" {
				t.Fatalf("retry did not release issue: %+v, err = %v", released, err)
			}

			if err := store.ReleaseClaim(ctx, id, successor, op, actor); err == nil || errors.As(err, &refusal) {
				t.Fatalf("successful retry did not bind the operation to its tuple: %v", err)
			}
		})
	}
}
