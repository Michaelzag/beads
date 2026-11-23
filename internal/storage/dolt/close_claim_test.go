package dolt

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

const (
	closeTestActor      = "tracker-test"
	closeTestHolder     = "zagcode-attempt-111111111111111111111111"
	closeTestAcceptance = "  Keep Café UTF-8.\nLine two.\n"
	closeTestReason     = "zagcom-handoff:cd4b525e-19cc-4077-9edc-af77521e0042"
)

func closeTestIntent(id string, n int) storage.CloseClaimIntent {
	return storage.CloseClaimIntent{
		WorkItemID:                id,
		ExpectedAssignee:          closeTestHolder,
		ExpectedAcceptanceDigest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(closeTestAcceptance))),
		ExpectedRepoLabelsDigest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("repo:Alpha\nrepo:beta"))),
		OperationID:               fmt.Sprintf("tracker-close:%064x", n),
		CloseReason:               closeTestReason,
	}
}

func createCloseTestIssue(t *testing.T, ctx context.Context, store *DoltStore, id string) {
	t.Helper()
	issue := &types.Issue{
		ID: id, Title: id, AcceptanceCriteria: closeTestAcceptance,
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		Labels: []string{"repo:beta", "other:ignored", "repo:Alpha"},
	}
	if err := store.CreateIssue(ctx, issue, closeTestActor); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := store.ClaimIssue(ctx, id, closeTestHolder); err != nil {
		t.Fatalf("claim issue: %v", err)
	}
}

func TestConditionalCloseRecordsOperationReceipt(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	const (
		execute = storage.CloseClaimExecute
		lookup  = storage.CloseClaimLookup
		refuse  = storage.CloseClaimRefuseIfMissing
	)

	head := func(t *testing.T) string {
		t.Helper()
		got, err := store.GetCurrentCommit(ctx)
		if err != nil {
			t.Fatalf("read HEAD: %v", err)
		}
		return got
	}
	assertReceipt := func(t *testing.T, intent storage.CloseClaimIntent, mode storage.CloseClaimMode, outcome storage.CloseClaimOutcome, refusal storage.CloseClaimRefusalReason) storage.CloseClaimReceipt {
		t.Helper()
		want := storage.CloseClaimReceipt{Outcome: outcome, CloseClaimIntent: intent, Reason: refusal}
		got, err := store.CloseClaim(ctx, intent, closeTestActor, mode, nil)
		if err != nil || got != want {
			t.Fatalf("close claim mode=%d: got (%+v, %v), want %+v", mode, got, err, want)
		}
		return got
	}
	assertCommitted := func(t *testing.T, receipt storage.CloseClaimReceipt) {
		t.Helper()
		var value string
		if err := store.db.QueryRowContext(ctx, "SELECT value FROM metadata AS OF 'HEAD' WHERE `key` = ?", "_close_claim_v1:"+receipt.OperationID).Scan(&value); err != nil {
			t.Fatalf("read receipt at HEAD: %v", err)
		}
		var got struct {
			storage.CloseClaimReceipt
			Actor   string `json:"actor"`
			WriteID string `json:"write_id"`
		}
		if err := json.Unmarshal([]byte(value), &got); err != nil || got.CloseClaimReceipt != receipt || got.Actor != closeTestActor || got.WriteID == "" {
			t.Fatalf("committed receipt = %s (%v), want %+v with actor and write ID", value, err, receipt)
		}
	}

	t.Run("success and lost reply", func(t *testing.T) {
		intent := closeTestIntent("test-close-success", 1)
		createCloseTestIssue(t, ctx, store, intent.WorkItemID)
		before := head(t)
		assertReceipt(t, intent, lookup, storage.CloseClaimMissing, "")
		if got := head(t); got != before {
			t.Fatalf("missing lookup advanced HEAD: %s -> %s", before, got)
		}
		issue, err := store.GetIssue(ctx, intent.WorkItemID)
		if err != nil || issue.Status != types.StatusInProgress {
			t.Fatalf("lookup changed issue: %+v (%v)", issue, err)
		}
		receipt := assertReceipt(t, intent, execute, storage.CloseClaimClosed, "")
		assertCommitted(t, receipt)
		after := head(t)
		var status, reason, session string
		if err := store.db.QueryRowContext(ctx, "SELECT status, close_reason, closed_by_session FROM issues AS OF 'HEAD' WHERE id = ?", intent.WorkItemID).
			Scan(&status, &reason, &session); err != nil || status != "closed" || reason != intent.CloseReason || session != intent.OperationID {
			t.Fatalf("close at HEAD = (%q, %q, %q, %v)", status, reason, session, err)
		}
		var events int
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events AS OF 'HEAD' WHERE issue_id = ? AND event_type = ? AND actor = ?",
			intent.WorkItemID, types.EventClosed, closeTestActor).Scan(&events); err != nil || events != 1 {
			t.Fatalf("close events at HEAD = %d (%v), want 1", events, err)
		}
		// Treat the initial response as lost: all modes return the exact
		// committed receipt without mutating the issue, audit or HEAD.
		assertReceipt(t, intent, execute, storage.CloseClaimClosed, "")
		assertReceipt(t, intent, lookup, storage.CloseClaimClosed, "")
		assertReceipt(t, intent, refuse, storage.CloseClaimClosed, "")
		for _, change := range []func(*storage.CloseClaimIntent){
			func(i *storage.CloseClaimIntent) { i.WorkItemID += "x" },
			func(i *storage.CloseClaimIntent) { i.ExpectedAssignee += "x" },
			func(i *storage.CloseClaimIntent) { i.ExpectedAcceptanceDigest = i.ExpectedRepoLabelsDigest },
			func(i *storage.CloseClaimIntent) { i.ExpectedRepoLabelsDigest = i.ExpectedAcceptanceDigest },
			func(i *storage.CloseClaimIntent) { i.CloseReason = "zagcom-handoff:cd4b525e-19cc-4077-9edc-af77521e0043" },
		} {
			other := intent
			change(&other)
			for _, mode := range []storage.CloseClaimMode{execute, lookup, refuse} {
				if got, err := store.CloseClaim(ctx, other, closeTestActor, mode, nil); err == nil || got.Outcome != "" {
					t.Fatalf("wrong tuple exposed a receipt: %+v (%v)", got, err)
				}
			}
		}
		for _, mode := range []storage.CloseClaimMode{execute, lookup, refuse} {
			if got, err := store.CloseClaim(ctx, intent, "other-actor", mode, nil); err == nil || got.Outcome != "" {
				t.Fatalf("wrong actor exposed a receipt: %+v (%v)", got, err)
			}
		}
		if got := head(t); got != after {
			t.Fatalf("replay advanced HEAD: %s -> %s", after, got)
		}
	})

	t.Run("ordinary close with same reason", func(t *testing.T) {
		intent := closeTestIntent("test-close-unrecorded", 2)
		createCloseTestIssue(t, ctx, store, intent.WorkItemID)
		if err := store.CloseIssue(ctx, intent.WorkItemID, intent.CloseReason, closeTestActor, "ordinary-session"); err != nil {
			t.Fatalf("ordinary close: %v", err)
		}
		assertReceipt(t, intent, lookup, storage.CloseClaimMissing, "")
		assertCommitted(t, assertReceipt(t, intent, refuse, storage.CloseClaimRefused, storage.CloseClaimClosedByOther))
	})

	t.Run("open blocker refusal survives unblocking", func(t *testing.T) {
		intent := closeTestIntent("test-close-blocked", 3)
		blocker := "test-close-blocker"
		createCloseTestIssue(t, ctx, store, intent.WorkItemID)
		createCloseTestIssue(t, ctx, store, blocker)
		if err := store.AddDependency(ctx, &types.Dependency{IssueID: intent.WorkItemID, DependsOnID: blocker, Type: types.DepBlocks}, closeTestActor); err != nil {
			t.Fatalf("add blocker: %v", err)
		}
		assertCommitted(t, assertReceipt(t, intent, execute, storage.CloseClaimRefused, storage.CloseClaimBlocked))
		if err := store.CloseIssue(ctx, blocker, "ordinary close of blocker", closeTestActor, ""); err != nil {
			t.Fatalf("clear blocker: %v", err)
		}
		assertReceipt(t, intent, execute, storage.CloseClaimRefused, storage.CloseClaimBlocked)
		assertReceipt(t, intent, lookup, storage.CloseClaimRefused, storage.CloseClaimBlocked)
		intent.OperationID = closeTestIntent(intent.WorkItemID, 4).OperationID
		assertReceipt(t, intent, execute, storage.CloseClaimClosed, "")
	})

	t.Run("exact ID and claim predicates", func(t *testing.T) {
		intent := closeTestIntent("test-close-custody", 5)
		createCloseTestIssue(t, ctx, store, intent.WorkItemID)
		for n, change := range []func(*storage.CloseClaimIntent){
			func(i *storage.CloseClaimIntent) { i.ExpectedAssignee = "other-holder" },
			func(i *storage.CloseClaimIntent) { i.ExpectedAcceptanceDigest = i.ExpectedRepoLabelsDigest },
			func(i *storage.CloseClaimIntent) { i.ExpectedRepoLabelsDigest = i.ExpectedAcceptanceDigest },
		} {
			wrong := closeTestIntent(intent.WorkItemID, 6+n)
			change(&wrong)
			assertCommitted(t, assertReceipt(t, wrong, execute, storage.CloseClaimRefused, storage.CloseClaimCustodyMismatch))
		}
		for n, id := range []string{"test-close-cust", strings.ToUpper(intent.WorkItemID)} {
			wrong := closeTestIntent(id, 9+n)
			assertCommitted(t, assertReceipt(t, wrong, execute, storage.CloseClaimRefused, storage.CloseClaimNotFound))
		}
		assertReceipt(t, intent, execute, storage.CloseClaimClosed, "")
	})

	t.Run("policy refusal if no prior receipt", func(t *testing.T) {
		intent := closeTestIntent("test-close-policy", 11)
		createCloseTestIssue(t, ctx, store, intent.WorkItemID)
		receipt := assertReceipt(t, intent, refuse, storage.CloseClaimRefused, storage.CloseClaimClosurePolicyRefused)
		assertCommitted(t, receipt)
		assertReceipt(t, intent, execute, storage.CloseClaimRefused, storage.CloseClaimClosurePolicyRefused)
		assertReceipt(t, intent, lookup, storage.CloseClaimRefused, storage.CloseClaimClosurePolicyRefused)
		issue, err := store.GetIssue(ctx, intent.WorkItemID)
		if err != nil || issue.Status != types.StatusInProgress {
			t.Fatalf("policy refusal changed issue: %+v (%v)", issue, err)
		}
	})
}

func TestConditionalCloseConcurrentOperations(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()

	for n, tc := range []struct {
		name           string
		firstOrdinary  bool
		secondOrdinary bool
		sameOperation  bool
		firstRefuses   bool
		secondRefuses  bool
	}{
		{name: "ordinary wins", firstOrdinary: true},
		{name: "conditional wins", secondOrdinary: true},
		{name: "different operation IDs"},
		{name: "same operation ID", sameOperation: true},
		{name: "close wins over policy refusal", sameOperation: true, secondRefuses: true},
		{name: "policy refusal wins over close", sameOperation: true, firstRefuses: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := fmt.Sprintf("test-contended-close-%d", n)
			createCloseTestIssue(t, ctx, store, id)
			first, second := closeTestIntent(id, 100+n*2), closeTestIntent(id, 101+n*2)
			if tc.sameOperation {
				second = first
			}
			stage := func(intent storage.CloseClaimIntent, ordinary, refuses bool) *sql.Tx {
				t.Helper()
				tx, err := store.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("begin candidate transaction: %v", err)
				}
				t.Cleanup(func() { _ = tx.Rollback() })
				if ordinary {
					if _, err := issueops.CloseIssueInTx(ctx, tx, id, intent.CloseReason, closeTestActor, "ordinary-session"); err != nil {
						t.Fatalf("stage ordinary close: %v", err)
					}
				} else {
					mode, want := storage.CloseClaimExecute, storage.CloseClaimClosed
					if refuses {
						mode, want = storage.CloseClaimRefuseIfMissing, storage.CloseClaimRefused
					}
					result, err := issueops.CloseClaimInTx(ctx, tx, intent, closeTestActor, mode, nil)
					if err != nil || !result.Recorded || result.Receipt.Outcome != want {
						t.Fatalf("stage conditional close: %+v (%v)", result, err)
					}
				}
				if refuses {
					return tx
				}
				// Remove timestamp differences: the session and/or receipt write
				// ID must force a conflict even if Dolt stores the same second.
				if _, err := tx.ExecContext(ctx, "UPDATE issues SET updated_at = '2025-01-02 03:04:05', closed_at = '2025-01-02 03:04:05' WHERE id = ?", id); err != nil {
					t.Fatalf("normalize close timestamps: %v", err)
				}
				return tx
			}
			commit := func(tx *sql.Tx, ordinary, refuses bool) error {
				var tables []string
				if !refuses {
					tables = append(tables, "issues", "events")
				}
				if !ordinary {
					tables = append(tables, "metadata")
				}
				for _, table := range tables {
					if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', 'test: close race')"); err != nil {
					return err
				}
				return tx.Commit()
			}
			tx1 := stage(first, tc.firstOrdinary, tc.firstRefuses)
			tx2 := stage(second, tc.secondOrdinary, tc.secondRefuses)
			if err := commit(tx1, tc.firstOrdinary, tc.firstRefuses); err != nil {
				t.Fatalf("commit winner: %v", err)
			}
			if err := commit(tx2, tc.secondOrdinary, tc.secondRefuses); err == nil {
				t.Fatal("second candidate committed from the same original snapshot")
			}
			_ = tx2.Rollback()

			var events int
			wantEvents := 1
			if tc.firstRefuses {
				wantEvents = 0
			}
			if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events AS OF 'HEAD' WHERE issue_id = ? AND event_type = ?", id, types.EventClosed).Scan(&events); err != nil || events != wantEvents {
				t.Fatalf("close events at HEAD = %d (%v), want %d", events, err, wantEvents)
			}
			wantFirst := storage.CloseClaimClosed
			if tc.firstOrdinary {
				wantFirst = storage.CloseClaimMissing
			} else if tc.firstRefuses {
				wantFirst = storage.CloseClaimRefused
			}
			firstReceipt, err := store.CloseClaim(ctx, first, closeTestActor, storage.CloseClaimLookup, nil)
			if err != nil || firstReceipt.Outcome != wantFirst {
				t.Fatalf("first lookup: %+v (%v), want %s", firstReceipt, err, wantFirst)
			}
			if !tc.sameOperation {
				loser, err := store.CloseClaim(ctx, second, closeTestActor, storage.CloseClaimLookup, nil)
				if err != nil || loser.Outcome != storage.CloseClaimMissing {
					t.Fatalf("loser receipt lookup: %+v (%v)", loser, err)
				}
				loser, err = store.CloseClaim(ctx, second, closeTestActor, storage.CloseClaimExecute, nil)
				if err != nil || loser.Outcome != storage.CloseClaimRefused || loser.Reason != storage.CloseClaimClosedByOther {
					t.Fatalf("loser retry: %+v (%v)", loser, err)
				}
			}
		})
	}
}
