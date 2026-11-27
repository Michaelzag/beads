package dolt

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

func guardToken(ctx context.Context, t *testing.T, store *DoltStore, id string) string {
	t.Helper()
	token, err := store.GetMetadata(ctx, storage.CloseGuardKey(id))
	if err != nil {
		t.Fatalf("read guard token for %s: %v", id, err)
	}
	return token
}

type trackerState struct {
	issue  *types.Issue
	events []*types.Event
	head   string
	guard  string
}

func readTrackerState(ctx context.Context, t *testing.T, store *DoltStore, id string) trackerState {
	t.Helper()
	issue, err := store.GetIssue(ctx, id)
	if err != nil {
		t.Fatalf("read issue: %v", err)
	}
	events, err := store.GetEvents(ctx, id, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	head, err := store.GetCurrentCommit(ctx)
	if err != nil {
		t.Fatalf("read commit: %v", err)
	}
	return trackerState{issue: issue, events: events, head: head, guard: guardToken(ctx, t, store, id)}
}

func requirePreconditionFailed(t *testing.T, err error, field, expected, actual string) {
	t.Helper()
	var failed *storage.UpdatePreconditionFailed
	if !errors.As(err, &failed) {
		t.Fatalf("err = %v; want precondition failure", err)
	}
	if failed.Field != field || failed.Expected != expected || failed.Actual != actual {
		t.Fatalf("precondition failure = %+v; want field=%s expected=%q actual=%q", failed, field, expected, actual)
	}
}

func TestUpdateIssueConditional(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const id, actor = "test-cas", "cas-test"
	if err := store.CreateIssue(ctx, &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, actor); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	created := guardToken(ctx, t, store, id)
	if created == "" {
		t.Fatal("create did not set a guard token")
	}

	// A title edit does not invalidate a status precondition.
	if err := store.UpdateIssue(ctx, id, map[string]interface{}{"title": "renamed"}, actor); err != nil {
		t.Fatalf("title update: %v", err)
	}
	if got := guardToken(ctx, t, store, id); got != created {
		t.Fatalf("title update changed guard %q -> %q", created, got)
	}

	// Both preconditions match: the update and a fresh guard are written.
	if err := store.UpdateIssueConditional(ctx, id, map[string]interface{}{"status": string(types.StatusInProgress)}, actor,
		storage.UpdatePrecondition{Status: strPtr("open"), Guard: strPtr(created)}); err != nil {
		t.Fatalf("matching conditional update: %v", err)
	}
	matched := readTrackerState(ctx, t, store, id)
	if matched.issue.Status != types.StatusInProgress {
		t.Fatalf("status after match = %s; want in_progress", matched.issue.Status)
	}
	if matched.guard == "" || matched.guard == created {
		t.Fatalf("guard after match = %q; want a fresh token (was %q)", matched.guard, created)
	}

	for _, tc := range []struct {
		name, field, expected, actual string
		cond                          storage.UpdatePrecondition
	}{
		{"status mismatch", "status", "open", "in_progress", storage.UpdatePrecondition{Status: strPtr("open"), Guard: strPtr(matched.guard)}},
		{"guard mismatch", "guard", created, matched.guard, storage.UpdatePrecondition{Status: strPtr("in_progress"), Guard: strPtr(created)}},
		{"status only mismatch", "status", "blocked", "in_progress", storage.UpdatePrecondition{Status: strPtr("blocked")}},
		{"guard only mismatch", "guard", created, matched.guard, storage.UpdatePrecondition{Guard: strPtr(created)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := readTrackerState(ctx, t, store, id)
			err := store.UpdateIssueConditional(ctx, id, map[string]interface{}{"status": string(types.StatusClosed), "notes": "must not land"}, actor, tc.cond)
			requirePreconditionFailed(t, err, tc.field, tc.expected, tc.actual)
			if after := readTrackerState(ctx, t, store, id); !reflect.DeepEqual(before, after) {
				t.Fatalf("refusal wrote: before = %+v, after = %+v", before, after)
			}
		})
	}

	err := store.UpdateIssueConditional(ctx, id, map[string]interface{}{"status": "closed"}, actor, storage.UpdatePrecondition{Status: strPtr("open")})
	if err == nil || err.Error() != "precondition_failed: status is in_progress, expected open" {
		t.Fatalf("refusal message = %v", err)
	}

	// An issue with no guard token (written before guards existed) matches "".
	if _, err := store.db.ExecContext(ctx, "DELETE FROM metadata WHERE `key` = ?", storage.CloseGuardKey(id)); err != nil {
		t.Fatalf("remove guard: %v", err)
	}
	if err := store.UpdateIssueConditional(ctx, id, map[string]interface{}{"notes": "first guarded write"}, actor,
		storage.UpdatePrecondition{Guard: strPtr("")}); err != nil {
		t.Fatalf("conditional update expecting no guard: %v", err)
	}
	fresh := guardToken(ctx, t, store, id)
	if fresh == "" {
		t.Fatal("a conditional write that changes no guarded field must still set the guard")
	}
	requirePreconditionFailed(t, store.UpdateIssueConditional(ctx, id, map[string]interface{}{"notes": "again"}, actor,
		storage.UpdatePrecondition{Guard: strPtr("")}), "guard", "", fresh)
}

// A stale reader's status and updated_at can both still match after another
// writer's same-second write; the guard token cannot.
func TestUpdateIssueConditionalSameSecondStaleToken(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const actor = "cas-test"
	for _, tc := range []struct {
		name   string
		writes []map[string]interface{}
	}{
		{"status rewritten unchanged", []map[string]interface{}{{"status": "open"}}},
		{"status changed and restored", []map[string]interface{}{{"status": "in_progress"}, {"status": "open"}}},
		{"assignee changed", []map[string]interface{}{{"assignee": "someone-else"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "test-stale-" + tc.name
			if err := store.CreateIssue(ctx, &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, actor); err != nil {
				t.Fatalf("create issue: %v", err)
			}
			read, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatalf("read issue: %v", err)
			}
			token := guardToken(ctx, t, store, id)

			for _, w := range tc.writes {
				if err := store.UpdateIssue(ctx, id, w, "other-writer"); err != nil {
					t.Fatalf("plain update %v: %v", w, err)
				}
			}
			// Land the other writer in the reader's second.
			if _, err := store.db.ExecContext(ctx, "UPDATE issues SET updated_at = ? WHERE id = ?", read.UpdatedAt, id); err != nil {
				t.Fatalf("normalize updated_at: %v", err)
			}
			current, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatalf("read issue: %v", err)
			}
			if current.Status != read.Status || !current.UpdatedAt.Equal(read.UpdatedAt) {
				t.Fatalf("fixture: status/updated_at = %s/%v; want the stale read's %s/%v", current.Status, current.UpdatedAt, read.Status, read.UpdatedAt)
			}

			before := readTrackerState(ctx, t, store, id)
			if before.guard == token {
				t.Fatal("plain write left the guard token unchanged")
			}
			err = store.UpdateIssueConditional(ctx, id, map[string]interface{}{"status": "closed"}, actor,
				storage.UpdatePrecondition{Status: strPtr(string(read.Status)), Guard: strPtr(token)})
			requirePreconditionFailed(t, err, "guard", token, before.guard)
			if after := readTrackerState(ctx, t, store, id); !reflect.DeepEqual(before, after) {
				t.Fatalf("stale CAS wrote: before = %+v, after = %+v", before, after)
			}
		})
	}
}

func countActorEvents(ctx context.Context, t *testing.T, store *DoltStore, id, actor string) int {
	t.Helper()
	events, err := store.GetEvents(ctx, id, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	n := 0
	for _, e := range events {
		if e.Actor == actor {
			n++
		}
	}
	return n
}

// Two transactions from the same snapshot write byte-identical issue rows, so
// only the guard cell can make Dolt refuse the second commit.
func TestUpdateIssueConditionalOverlappingTransactions(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const id, actor = "test-cas-overlap", "cas-writer"
	if err := store.CreateIssue(ctx, &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, "setup"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	token := guardToken(ctx, t, store, id)
	cond := storage.UpdatePrecondition{Status: strPtr("open"), Guard: strPtr(token)}

	stage := func() *sql.Tx {
		t.Helper()
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := issueops.UpdateIssueConditionalInTx(ctx, tx, id, map[string]interface{}{"status": "in_progress"}, actor, cond); err != nil {
			t.Fatalf("stage conditional update: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE issues SET updated_at = '2025-01-02 03:04:05', started_at = '2025-01-02 03:04:05' WHERE id = ?", id); err != nil {
			t.Fatalf("normalize timestamps: %v", err)
		}
		return tx
	}
	tx1 := stage()
	defer func() { _ = tx1.Rollback() }()
	tx2 := stage()
	defer func() { _ = tx2.Rollback() }()

	if err := tx1.Commit(); err != nil {
		t.Fatalf("first conditional commit: %v", err)
	}
	if err := tx2.Commit(); err == nil {
		t.Fatal("second conditional update committed from the same snapshot")
	}
	if n := countActorEvents(ctx, t, store, id, actor); n != 1 {
		t.Fatalf("conditional update events = %d; want 1", n)
	}
}

// Concurrent conditional updates from the same prior state: exactly one wins,
// every other one is refused, and the winner's write is the one that stands.
func TestUpdateIssueConditionalConcurrent(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	const id, actor = "test-cas-race", "cas-writer"
	if err := store.CreateIssue(ctx, &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, "setup"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	token := guardToken(ctx, t, store, id)

	targets := []string{"in_progress", "blocked"}
	errs := make([]error, len(targets))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = store.UpdateIssueConditional(ctx, id, map[string]interface{}{"status": target, "notes": "writer " + target}, actor,
				storage.UpdatePrecondition{Status: strPtr("open"), Guard: strPtr(token)})
		}()
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner >= 0 {
				t.Fatalf("both conditional updates succeeded")
			}
			winner = i
			continue
		}
		var failed *storage.UpdatePreconditionFailed
		if !errors.As(err, &failed) {
			t.Fatalf("loser error = %v; want precondition failure", err)
		}
	}
	if winner < 0 {
		t.Fatalf("no conditional update succeeded: %v", errs)
	}
	issue, err := store.GetIssue(ctx, id)
	if err != nil {
		t.Fatalf("read issue: %v", err)
	}
	if string(issue.Status) != targets[winner] || issue.Notes != "writer "+targets[winner] {
		t.Fatalf("issue = %s/%q; want the winner's %s", issue.Status, issue.Notes, targets[winner])
	}
	if n := countActorEvents(ctx, t, store, id, actor); n != 1 {
		t.Fatalf("conditional update events = %d; want 1", n)
	}
}
