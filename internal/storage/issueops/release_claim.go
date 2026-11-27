package issueops

import (
	"context"
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
// Recorded is true only for a new success receipt; Updated describes its issue mutation.
type ReleaseClaimResult struct {
	Recorded      bool
	Updated       *UpdateResult
	RefusalReason storage.ReleaseClaimRefusalReason
}

// ReleaseClaimInTx conditionally releases the expected claim and records its
// success. A new refusal leaves the transaction unchanged.
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

	if result.releaseRefusal != "" {
		return ReleaseClaimResult{RefusalReason: result.releaseRefusal}, nil
	}

	// Never upsert. Distinct write IDs also prevent the Dolt merge from
	// coalescing simultaneous identical inserts but keeping both audit events.
	if _, err := tx.ExecContext(ctx, "INSERT INTO metadata (`key`, value) VALUES (?, "+
		"JSON_OBJECT('work_item_id', ?, 'expected_assignee', ?, 'operation_id', ?, 'refusal_reason', ?, 'write_id', UUID()))",
		key, id, expectedAssignee, operationID, result.releaseRefusal); err != nil {
		return ReleaseClaimResult{}, fmt.Errorf("insert release receipt: %w", err)
	}
	return ReleaseClaimResult{Recorded: true, Updated: result}, nil
}
