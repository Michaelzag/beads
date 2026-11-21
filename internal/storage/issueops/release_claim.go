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

	"github.com/steveyegge/beads/internal/types"
)

var (
	releaseAssigneePattern  = regexp.MustCompile(`^zagcode-attempt-[0-9a-f]{24}$`)
	releaseOperationPattern = regexp.MustCompile(`^tracker-release:[0-9a-f]{64}$`)
)

type releaseClaimReceipt struct {
	WorkItemID       string `json:"work_item_id"`
	ExpectedAssignee string `json:"expected_assignee"`
	OperationID      string `json:"operation_id"`
}

// ReleaseClaimInTx releases the expected claim and records its receipt in the
// same transaction. A nil result with no error is a read-only receipt replay.
func ReleaseClaimInTx(ctx context.Context, tx *sql.Tx, id, expectedAssignee, operationID, actor string) (*UpdateResult, error) {
	if id == "" || id != strings.TrimSpace(id) {
		return nil, fmt.Errorf("release requires an exact issue ID")
	}
	if !releaseAssigneePattern.MatchString(expectedAssignee) {
		return nil, fmt.Errorf("expected assignee must be a canonical tracker attempt handle")
	}
	if !releaseOperationPattern.MatchString(operationID) {
		return nil, fmt.Errorf("operation ID must be a canonical tracker release ID")
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
			return nil, fmt.Errorf("decode release receipt: %w", err)
		}
		if got != want {
			return nil, fmt.Errorf("operation ID is already bound to a different release")
		}
		return nil, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("read release receipt: %w", err)
	}

	result, err := updateIssueInTx(ctx, tx, id, map[string]interface{}{
		"status":   types.StatusOpen,
		"assignee": "",
	}, actor, true, expectedAssignee)
	if err != nil {
		return nil, err
	}

	// Dolt can merge identical issue-row writes from concurrent snapshots.
	// Different operations releasing the same issue must instead conflict on
	// a shared key, even when both writes have the same updated_at.
	issueKey := fmt.Sprintf("_release_claim_issue_v1:%x", sha256.Sum256([]byte(result.OldIssue.ID)))
	if err := SetMetadataInTx(ctx, tx, issueKey, operationID); err != nil {
		return nil, fmt.Errorf("record release contention: %w", err)
	}

	// Never upsert. Distinct write IDs also prevent the Dolt merge from
	// coalescing simultaneous identical inserts but keeping both audit events.
	if _, err := tx.ExecContext(ctx, "INSERT INTO metadata (`key`, value) VALUES (?, "+
		"JSON_OBJECT('work_item_id', ?, 'expected_assignee', ?, 'operation_id', ?, 'write_id', UUID()))",
		key, id, expectedAssignee, operationID); err != nil {
		return nil, fmt.Errorf("insert release receipt: %w", err)
	}
	return result, nil
}
