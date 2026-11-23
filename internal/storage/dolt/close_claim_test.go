package dolt

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
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

// runCloseTestOverlap starts and stages both candidates before publishing either
// one. The callbacks use production mutations; the test never writes the guard.
func runCloseTestOverlap(t *testing.T, ctx context.Context, store *DoltStore, id string, first, second func(*sql.Tx) []string) {
	t.Helper()
	begin := func() *sql.Tx {
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin candidate transaction: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		return tx
	}
	key := fmt.Sprintf("_close_guard_v1:%x", sha256.Sum256([]byte(id)))
	guard := func(tx *sql.Tx) string {
		var value string
		if err := tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key` = ?", key).Scan(&value); err != nil {
			t.Fatalf("read guard for %s: %v", id, err)
		}
		if nonce, err := uuid.Parse(value); err != nil || nonce.Version() != 4 {
			t.Fatalf("guard for %s is not a random UUID: %q", id, value)
		}
		return value
	}
	stage := func(tx *sql.Tx, tables []string) {
		for _, table := range tables {
			if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
				t.Fatalf("stage %s for overlap commit: %v", table, err)
			}
		}
	}
	commit := func(tx *sql.Tx, tables []string) error {
		if len(tables) != 0 {
			if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', 'test: close race')"); err != nil {
				return err
			}
		}
		return tx.Commit()
	}

	tx1, tx2 := begin(), begin()
	base := guard(tx1)
	if got := guard(tx2); got != base {
		t.Fatalf("candidates did not start from the same guard: %q != %q", base, got)
	}
	firstTables, secondTables := first(tx1), second(tx2)
	stage(tx1, firstTables)
	stage(tx2, secondTables)
	winner, loser := guard(tx1), guard(tx2)
	if winner == base || loser == base || winner == loser {
		t.Fatalf("expected distinct guard writes from %q, got %q and %q", base, winner, loser)
	}
	if err := commit(tx1, firstTables); err != nil {
		t.Fatalf("commit winner: %v", err)
	}
	if err := commit(tx2, secondTables); !isSerializationError(err) {
		t.Fatalf("second candidate from the same guard: want serialization conflict, got %v", err)
	}
	_ = tx2.Rollback()

	var got string
	if err := store.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key` = ?", key).Scan(&got); err != nil || got != winner {
		t.Fatalf("working guard = %q (%v), want winner %q", got, err, winner)
	}
}

func stageCloseTestClaim(t *testing.T, ctx context.Context, tx *sql.Tx, intent storage.CloseClaimIntent, mode storage.CloseClaimMode, outcome storage.CloseClaimOutcome) []string {
	t.Helper()
	result, err := issueops.CloseClaimInTx(ctx, tx, intent, closeTestActor, mode, nil)
	if err != nil || !result.Recorded || result.Receipt.Outcome != outcome {
		t.Fatalf("stage conditional close: %+v (%v), want recorded %s", result, err, outcome)
	}
	if outcome == storage.CloseClaimClosed {
		return []string{"metadata", "issues", "events"}
	}
	return []string{"metadata"}
}

func assertCloseTestNoReceipt(t *testing.T, ctx context.Context, store *DoltStore, intent storage.CloseClaimIntent) {
	t.Helper()
	for _, query := range []string{
		"SELECT COUNT(*) FROM metadata WHERE `key` = ?",
		"SELECT COUNT(*) FROM metadata AS OF 'HEAD' WHERE `key` = ?",
	} {
		var count int
		if err := store.db.QueryRowContext(ctx, query, "_close_claim_v1:"+intent.OperationID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("losing receipt %s via %q: count = %d (%v), want 0", intent.OperationID, query, count, err)
		}
	}
}

func assertCloseTestStale(t *testing.T, ctx context.Context, store *DoltStore, intent storage.CloseClaimIntent, reason storage.CloseClaimRefusalReason) {
	t.Helper()
	assertCloseTestNoReceipt(t, ctx, store, intent)
	want := storage.CloseClaimReceipt{Outcome: storage.CloseClaimRefused, CloseClaimIntent: intent, Reason: reason}
	got, err := store.CloseClaim(ctx, intent, closeTestActor, storage.CloseClaimExecute, nil)
	if err != nil || got != want {
		t.Fatalf("close retry on fresh snapshot: %+v (%v), want %+v", got, err, want)
	}
	got, err = store.CloseClaim(ctx, intent, closeTestActor, storage.CloseClaimLookup, nil)
	if err != nil || got != want {
		t.Fatalf("durable refusal lookup: %+v (%v), want %+v", got, err, want)
	}
}

func TestConditionalCloseConcurrentOperations(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()

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
			ctx, cancel := testContext(t)
			defer cancel()
			id := fmt.Sprintf("test-contended-close-%d", n)
			createCloseTestIssue(t, ctx, store, id)
			first, second := closeTestIntent(id, 100+n*2), closeTestIntent(id, 101+n*2)
			if tc.sameOperation {
				second = first
			}
			stage := func(intent storage.CloseClaimIntent, ordinary, refuses bool, session string) func(*sql.Tx) []string {
				return func(tx *sql.Tx) []string {
					t.Helper()
					var tables []string
					if ordinary {
						if _, err := issueops.CloseIssueInTx(ctx, tx, id, intent.CloseReason, closeTestActor, session); err != nil {
							t.Fatalf("stage ordinary close: %v", err)
						}
						tables = []string{"issues", "events", "metadata"}
					} else {
						mode, want := storage.CloseClaimExecute, storage.CloseClaimClosed
						if refuses {
							mode, want = storage.CloseClaimRefuseIfMissing, storage.CloseClaimRefused
						}
						tables = stageCloseTestClaim(t, ctx, tx, intent, mode, want)
					}
					if tc.firstOrdinary || tc.secondOrdinary {
						// Both candidates use the conditional operation ID as the
						// close session and the same reason. Make the rest of the
						// close row equal too, so Dolt cannot rely on it to conflict.
						if _, err := tx.ExecContext(ctx, "UPDATE issues SET updated_at = '2025-01-02 03:04:05', closed_at = '2025-01-02 03:04:05' WHERE id = ?", id); err != nil {
							t.Fatalf("normalize close timestamps: %v", err)
						}
					}
					return tables
				}
			}
			runCloseTestOverlap(t, ctx, store, id, stage(first, tc.firstOrdinary, tc.firstRefuses, second.OperationID),
				stage(second, tc.secondOrdinary, tc.secondRefuses, first.OperationID))

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
			switch {
			case tc.sameOperation:
				// The winner's response might be lost. Re-execution must return
				// its exact receipt without advancing HEAD or closing again.
				head, err := store.GetCurrentCommit(ctx)
				if err != nil {
					t.Fatalf("read winner HEAD: %v", err)
				}
				if got, err := store.CloseClaim(ctx, second, closeTestActor, storage.CloseClaimExecute, nil); err != nil || got != firstReceipt {
					t.Fatalf("same-operation retry: %+v (%v), want %+v", got, err, firstReceipt)
				}
				if got, err := store.GetCurrentCommit(ctx); err != nil || got != head {
					t.Fatalf("same-operation retry HEAD: %s (%v), want %s", got, err, head)
				}
			case tc.secondOrdinary:
				// An ordinary close may retry in a fresh transaction. It
				// must not change the already committed conditional receipt.
				if err := store.CloseIssue(ctx, id, second.CloseReason, closeTestActor, first.OperationID); err != nil {
					t.Fatalf("ordinary close on fresh snapshot: %v", err)
				}
				if got, err := store.CloseClaim(ctx, first, closeTestActor, storage.CloseClaimLookup, nil); err != nil || got != firstReceipt {
					t.Fatalf("receipt after ordinary retry: %+v (%v), want %+v", got, err, firstReceipt)
				}
			default:
				assertCloseTestStale(t, ctx, store, second, storage.CloseClaimClosedByOther)
			}
		})
	}
}

func TestConditionalCloseConcurrentPredicateWrites(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()

	type mutation func(context.Context, *sql.Tx, string, string) ([]string, error)
	update := func(field string, value any, onRelated bool) mutation {
		return func(ctx context.Context, tx *sql.Tx, id, related string) ([]string, error) {
			if onRelated {
				id = related
			}
			_, err := issueops.UpdateIssueInTx(ctx, tx, id, map[string]interface{}{field: value}, closeTestActor)
			return []string{"issues", "events", "metadata"}, err
		}
	}
	label := func(add, batch bool) mutation {
		return func(ctx context.Context, tx *sql.Tx, id, _ string) ([]string, error) {
			if batch {
				// Exercise the batch methods (and their actual dirty-table
				// tracking). Both handles can read this regular-only fixture
				// through the same SQL session for the wisp routing check.
				bulk := &doltTransaction{regularTx: tx, ignoredTx: tx, store: store}
				var err error
				if add {
					err = bulk.AddLabel(ctx, id, "repo:new", closeTestActor)
				} else {
					err = bulk.RemoveLabel(ctx, id, "repo:beta", closeTestActor)
				}
				dirty := bulk.dirty.DirtyTables()
				if err == nil && (!dirty["labels"] || !dirty["metadata"]) {
					return nil, fmt.Errorf("batch label writer did not mark labels and metadata dirty: %v", dirty)
				}
				var tables []string
				for table := range dirty {
					tables = append(tables, table)
				}
				return tables, err
			}
			var err error
			if add {
				err = issueops.AddLabelInTx(ctx, tx, "", "", id, "repo:new", closeTestActor)
			} else {
				err = issueops.RemoveLabelInTx(ctx, tx, "", "", id, "repo:beta", closeTestActor)
			}
			// Ordinary AddLabel/RemoveLabel publish only the working set.
			return nil, err
		}
	}
	dependency := func(kind types.DependencyType) mutation {
		return func(ctx context.Context, tx *sql.Tx, id, related string) ([]string, error) {
			if kind == types.DepParentChild {
				id, related = related, id
			}
			err := issueops.AddDependencyInTx(ctx, tx, &types.Dependency{IssueID: id, DependsOnID: related, Type: kind}, closeTestActor, issueops.AddDependencyOpts{})
			return []string{"dependencies", "issues", "metadata"}, err
		}
	}
	release := func(ctx context.Context, tx *sql.Tx, id, _ string) ([]string, error) {
		result, err := issueops.ReleaseClaimInTx(ctx, tx, id, closeTestHolder, fmt.Sprintf("tracker-release:%064x", 1), closeTestActor)
		if err == nil && (!result.Recorded || result.Updated == nil || result.RefusalReason != "") {
			err = fmt.Errorf("expected newly recorded release, got %+v", result)
		}
		return []string{"issues", "events", "metadata"}, err
	}

	const (
		custody = storage.CloseClaimCustodyMismatch
		blocked = storage.CloseClaimBlocked
		policy  = storage.CloseClaimClosurePolicyRefused
	)
	for n, tc := range []struct {
		name         string
		write        mutation
		want         storage.CloseClaimRefusalReason
		link         types.DependencyType
		existingLink bool
	}{
		{name: "conditional release", write: release, want: custody},
		{name: "assignee", write: update("assignee", "another-holder", false), want: custody},
		{name: "acceptance", write: update("acceptance_criteria", "Edited acceptance", false), want: custody},
		{name: "add repo label", write: label(true, false), want: custody},
		{name: "remove repo label", write: label(false, false), want: custody},
		{name: "batch add repo label", write: label(true, true), want: custody},
		{name: "batch remove repo label", write: label(false, true), want: custody},
		{name: "insert blocker edge", write: dependency(types.DepBlocks), want: blocked, link: types.DepBlocks},
		{name: "reopen blocker", write: update("status", types.StatusOpen, true), want: blocked, link: types.DepBlocks, existingLink: true},
		{name: "insert epic child", write: dependency(types.DepParentChild), want: policy, link: types.DepParentChild},
		{name: "reopen epic child", write: update("status", types.StatusOpen, true), want: policy, link: types.DepParentChild, existingLink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := testContext(t)
			defer cancel()
			id := fmt.Sprintf("test-close-predicate-%d", n)
			intent := closeTestIntent(id, 200+n)
			createCloseTestIssue(t, ctx, store, id)
			related := id + "-related"
			if tc.link != "" {
				createCloseTestIssue(t, ctx, store, related)
				if tc.link == types.DepParentChild {
					if err := store.UpdateIssue(ctx, id, map[string]interface{}{"issue_type": string(types.TypeEpic)}, closeTestActor); err != nil {
						t.Fatalf("make target an epic: %v", err)
					}
				}
				if tc.existingLink {
					// An existing closed child or blocker lets the conditional
					// candidate pass before the other transaction reopens it.
					if err := store.CloseIssue(ctx, related, "fixture: initially closed", closeTestActor, ""); err != nil {
						t.Fatalf("close related issue: %v", err)
					}
					dep := &types.Dependency{IssueID: id, DependsOnID: related, Type: tc.link}
					if tc.link == types.DepParentChild {
						dep.IssueID, dep.DependsOnID = related, id
					}
					if err := store.AddDependency(ctx, dep, closeTestActor); err != nil {
						t.Fatalf("link initially closed issue: %v", err)
					}
				}
			}
			runCloseTestOverlap(t, ctx, store, id, func(tx *sql.Tx) []string {
				tables, err := tc.write(ctx, tx, id, related)
				if err != nil {
					t.Fatalf("stage predicate writer: %v", err)
				}
				return tables
			}, func(tx *sql.Tx) []string {
				return stageCloseTestClaim(t, ctx, tx, intent, storage.CloseClaimExecute, storage.CloseClaimClosed)
			})
			assertCloseTestStale(t, ctx, store, intent, tc.want)
			var events int
			if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events AS OF 'HEAD' WHERE issue_id = ? AND event_type = ?", id, types.EventClosed).Scan(&events); err != nil || events != 0 {
				t.Fatalf("losing close events at HEAD = %d (%v), want 0", events, err)
			}
		})
	}
}
