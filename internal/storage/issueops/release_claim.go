package issueops

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

var (
	releaseAssigneePattern  = regexp.MustCompile(`^zagcode-attempt-[0-9a-f]{24}$`)
	releaseOperationPattern = regexp.MustCompile(`^tracker-release:[0-9a-f]{64}$`)
)

type releaseClaimReceipt struct {
	WorkItemID       string                            `json:"work_item_id"`
	ExpectedAssignee string                            `json:"expected_assignee"`
	OperationID      string                            `json:"operation_id"`
	RefusalReason    storage.ReleaseClaimRefusalReason `json:"refusal_reason,omitempty"`
}

// ReleaseClaimResult describes the operation's outcome within a transaction.
// Recorded is false for replay; Updated is nonnil only for a new issue mutation.
type ReleaseClaimResult struct {
	Recorded      bool
	Updated       *UpdateResult
	RefusalReason storage.ReleaseClaimRefusalReason
}

// ReleaseClaimInTx conditionally releases the expected claim and records its
// outcome. Callers must commit before reporting a definitive refusal.
func ReleaseClaimInTx(ctx context.Context, tx *sql.Tx, id, expectedAssignee, operationID, actor string) (ReleaseClaimResult, error) {
	if id == "" || id != strings.TrimSpace(id) {
		return ReleaseClaimResult{}, fmt.Errorf("release requires an exact issue ID")
	}
	if !releaseAssigneePattern.MatchString(expectedAssignee) {
		return ReleaseClaimResult{}, fmt.Errorf("expected assignee must be a canonical tracker attempt handle")
	}
	if !releaseOperationPattern.MatchString(operationID) {
		return ReleaseClaimResult{}, fmt.Errorf("operation ID must be a canonical tracker release ID")
	}

	const receiptPrefix = "_release_claim_v1:"
	key := receiptPrefix + operationID
	want := releaseClaimReceipt{WorkItemID: id, ExpectedAssignee: expectedAssignee, OperationID: operationID}
	var value string
	err := tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key` = ?", key).Scan(&value)
	switch {
	case err == nil:
		var got releaseClaimReceipt
		if err := json.Unmarshal([]byte(value), &got); err != nil {
			return ReleaseClaimResult{}, fmt.Errorf("decode release receipt: %w", err)
		}
		reason := got.RefusalReason
		got.RefusalReason = ""
		if got != want {
			return ReleaseClaimResult{}, fmt.Errorf("operation ID is already bound to a different release")
		}
		switch reason {
		case "", storage.ReleaseClaimHolderChanged, storage.ReleaseClaimNotInProgress:
			return ReleaseClaimResult{RefusalReason: reason}, nil
		default:
			return ReleaseClaimResult{}, fmt.Errorf("release receipt contains an unknown refusal reason")
		}
	case !errors.Is(err, sql.ErrNoRows):
		return ReleaseClaimResult{}, fmt.Errorf("read release receipt: %w", err)
	}

	result, err := updateIssueInTx(ctx, tx, id, map[string]interface{}{
		"status":   types.StatusOpen,
		"assignee": "",
	}, actor, true, expectedAssignee)
	if err != nil {
		return ReleaseClaimResult{}, err
	}

	// Dolt can merge identical issue-row writes from concurrent snapshots.
	// Serialize both refusals and releases on the same canonical issue key.
	// The exact ID was validated by updateIssueInTx before either outcome.
	issueKey := fmt.Sprintf("_release_claim_issue_v1:%x", sha256.Sum256([]byte(id)))
	if err := SetMetadataInTx(ctx, tx, issueKey, operationID); err != nil {
		return ReleaseClaimResult{}, fmt.Errorf("record release contention: %w", err)
	}

	// Never upsert. Distinct write IDs also prevent the Dolt merge from
	// coalescing simultaneous identical inserts but keeping both audit events.
	if _, err := tx.ExecContext(ctx, "INSERT INTO metadata (`key`, value) VALUES (?, "+
		"JSON_OBJECT('work_item_id', ?, 'expected_assignee', ?, 'operation_id', ?, 'refusal_reason', ?, 'write_id', UUID()))",
		key, id, expectedAssignee, operationID, result.releaseRefusal); err != nil {
		return ReleaseClaimResult{}, fmt.Errorf("insert release receipt: %w", err)
	}
	if result.releaseRefusal != "" {
		return ReleaseClaimResult{Recorded: true, RefusalReason: result.releaseRefusal}, nil
	}
	return ReleaseClaimResult{Recorded: true, Updated: result}, nil
}
