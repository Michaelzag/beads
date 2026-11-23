package issueops

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/validation"
)

const closeClaimReceiptPrefix = "_close_claim_v1:"

var (
	closeClaimDigestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	closeClaimOperationPattern = regexp.MustCompile(`^tracker-close:[0-9a-f]{64}$`)
)

type closeClaimRecord struct {
	storage.CloseClaimReceipt
	Actor   string `json:"actor"`
	WriteID string `json:"write_id"`
}

// CloseClaimResult holds a receipt and whether it was newly recorded in the
// transaction. Only a newly recorded closed result changes an issue.
type CloseClaimResult struct {
	Receipt  storage.CloseClaimReceipt
	Recorded bool
}

// ValidateCloseClaimIntent checks the shared CLI and storage input contract.
func ValidateCloseClaimIntent(intent storage.CloseClaimIntent) error {
	if id := intent.WorkItemID; id == "" || id != strings.TrimSpace(id) || !utf8.ValidString(id) {
		return fmt.Errorf("close-claim requires an exact issue ID without surrounding whitespace")
	}
	if intent.ExpectedAssignee == "" || !utf8.ValidString(intent.ExpectedAssignee) {
		return fmt.Errorf("--expected-assignee must be nonempty UTF-8")
	}
	if !closeClaimDigestPattern.MatchString(intent.ExpectedAcceptanceDigest) || !closeClaimDigestPattern.MatchString(intent.ExpectedRepoLabelsDigest) {
		return fmt.Errorf("expected digests must be sha256: followed by 64 lowercase hexadecimal digits")
	}
	if !closeClaimOperationPattern.MatchString(intent.OperationID) {
		return fmt.Errorf("--operation-id must be tracker-close: followed by 64 lowercase hexadecimal digits")
	}
	reasonUUID, ok := strings.CutPrefix(intent.CloseReason, "zagcom-handoff:")
	id, err := uuid.Parse(reasonUUID)
	if !ok || err != nil || id == uuid.Nil || id.String() != reasonUUID {
		return fmt.Errorf("--reason must be zagcom-handoff: followed by a canonical lowercase non-nil UUID")
	}
	return nil
}

// CloseClaimInTx checks the committed receipt first. If none exists, a write
// request evaluates and (if eligible) closes the issue in the same transaction
// as the new receipt. The caller must commit before using a new receipt.
func CloseClaimInTx(ctx context.Context, tx *sql.Tx, intent storage.CloseClaimIntent, actor string, mode storage.CloseClaimMode, checkGate func(*types.Issue) error) (CloseClaimResult, error) {
	if err := ValidateCloseClaimIntent(intent); err != nil {
		return CloseClaimResult{}, err
	}
	if actor == "" || !utf8.ValidString(actor) {
		return CloseClaimResult{}, fmt.Errorf("close-claim requires an actor")
	}

	key := closeClaimReceiptPrefix + intent.OperationID
	var value string
	err := tx.QueryRowContext(ctx, "SELECT value FROM metadata AS OF 'HEAD' WHERE `key` = ?", key).Scan(&value)
	switch {
	case err == nil:
		var record closeClaimRecord
		if err := json.Unmarshal([]byte(value), &record); err != nil {
			return CloseClaimResult{}, fmt.Errorf("decode close receipt: %w", err)
		}
		if record.CloseClaimIntent != intent || record.Actor != actor {
			return CloseClaimResult{}, fmt.Errorf("operation ID is already bound to a different close")
		}
		if record.WriteID == "" || !validCloseClaimReceipt(record.CloseClaimReceipt) {
			return CloseClaimResult{}, fmt.Errorf("invalid committed close receipt")
		}
		return CloseClaimResult{Receipt: record.CloseClaimReceipt}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return CloseClaimResult{}, fmt.Errorf("read committed close receipt: %w", err)
	}
	switch mode {
	case storage.CloseClaimExecute, storage.CloseClaimRefuseIfMissing:
	case storage.CloseClaimLookup:
		return CloseClaimResult{Receipt: storage.CloseClaimReceipt{Outcome: storage.CloseClaimMissing, CloseClaimIntent: intent}}, nil
	default:
		return CloseClaimResult{}, fmt.Errorf("unrecognized close-claim mode")
	}

	refusal, err := checkCloseClaimInTx(ctx, tx, intent, mode, checkGate)
	if err != nil {
		return CloseClaimResult{}, err
	}
	receipt := storage.CloseClaimReceipt{Outcome: storage.CloseClaimRefused, CloseClaimIntent: intent, Reason: refusal}
	if refusal == "" {
		result, err := CloseIssueInTx(ctx, tx, intent.WorkItemID, intent.CloseReason, actor, intent.OperationID)
		if err != nil {
			return CloseClaimResult{}, err
		}
		if result.IsWisp {
			return CloseClaimResult{}, fmt.Errorf("issue resolved to an unversioned wisp during close")
		}
		receipt.Outcome = storage.CloseClaimClosed
	}

	// Different operations on one exact item must contend, even if at least
	// one only writes a refusal. Different writes of the same operation must
	// also conflict rather than merge as equal rows under Dolt MVCC.
	issueKey := fmt.Sprintf("_close_claim_issue_v1:%x", sha256.Sum256([]byte(intent.WorkItemID)))
	if err := SetMetadataInTx(ctx, tx, issueKey, intent.OperationID); err != nil {
		return CloseClaimResult{}, fmt.Errorf("record close contention: %w", err)
	}
	writeID, err := uuid.NewRandom()
	if err != nil {
		return CloseClaimResult{}, fmt.Errorf("generate close receipt write ID: %w", err)
	}
	encoded, err := json.Marshal(closeClaimRecord{CloseClaimReceipt: receipt, Actor: actor, WriteID: writeID.String()})
	if err != nil {
		return CloseClaimResult{}, fmt.Errorf("encode close receipt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO metadata (`key`, value) VALUES (?, ?)", key, string(encoded)); err != nil {
		return CloseClaimResult{}, fmt.Errorf("insert close receipt: %w", err)
	}
	return CloseClaimResult{Receipt: receipt, Recorded: true}, nil
}

func validCloseClaimReceipt(receipt storage.CloseClaimReceipt) bool {
	switch receipt.Outcome {
	case storage.CloseClaimClosed:
		return receipt.Reason == ""
	case storage.CloseClaimRefused:
		switch receipt.Reason {
		case storage.CloseClaimClosedByOther, storage.CloseClaimCustodyMismatch, storage.CloseClaimBlocked, storage.CloseClaimClosurePolicyRefused, storage.CloseClaimNotFound:
			return true
		}
	}
	return false
}

func checkCloseClaimInTx(ctx context.Context, tx *sql.Tx, intent storage.CloseClaimIntent, mode storage.CloseClaimMode, checkGate func(*types.Issue) error) (storage.CloseClaimRefusalReason, error) {
	id := intent.WorkItemID
	issue, err := GetIssueInTx(ctx, tx, id)
	if errors.Is(err, storage.ErrNotFound) {
		// GetIssueInTx tolerates missing tables. A missing primary issues
		// table is a storage failure, not a definite absent issue.
		exists, err := optionalTableExistsInTx(ctx, tx, "issues")
		if err != nil {
			return "", fmt.Errorf("check issues table: %w", err)
		}
		if !exists {
			return "", fmt.Errorf("close-claim requires the issues table")
		}
		return storage.CloseClaimNotFound, nil
	}
	if err != nil {
		return "", err
	}
	// SQL collation may have matched a different spelling of the ID.
	if issue.ID != id {
		return storage.CloseClaimNotFound, nil
	}
	if issue.Status == types.StatusClosed {
		return storage.CloseClaimClosedByOther, nil
	}
	if issue.Status != types.StatusInProgress || issue.Assignee != intent.ExpectedAssignee {
		return storage.CloseClaimCustodyMismatch, nil
	}
	if digestCloseClaimText(issue.AcceptanceCriteria) != intent.ExpectedAcceptanceDigest {
		return storage.CloseClaimCustodyMismatch, nil
	}
	var repoLabels []string
	for _, label := range issue.Labels {
		if strings.HasPrefix(label, "repo:") {
			repoLabels = append(repoLabels, label)
		}
	}
	slices.Sort(repoLabels)
	if digestCloseClaimText(strings.Join(repoLabels, "\n")) != intent.ExpectedRepoLabelsDigest {
		return storage.CloseClaimCustodyMismatch, nil
	}
	if mode == storage.CloseClaimRefuseIfMissing {
		return storage.CloseClaimClosurePolicyRefused, nil
	}
	if err := validation.Chain(validation.NotTemplate(), validation.NotPinned(false))(id, issue); err != nil {
		return storage.CloseClaimClosurePolicyRefused, nil
	}
	wisps, err := WispIDSetInTx(ctx, tx, []string{id})
	if err != nil {
		return "", fmt.Errorf("check close-claim wisp routing: %w", err)
	}
	if len(wisps) != 0 {
		// Wisps and their events cannot be committed to HEAD with the receipt.
		return storage.CloseClaimClosurePolicyRefused, nil
	}
	if issue.IssueType == types.TypeEpic {
		children, err := GetDependentsWithMetadataInTx(ctx, tx, id)
		if err != nil {
			return "", fmt.Errorf("check epic children: %w", err)
		}
		for _, child := range children {
			if child.DependencyType == types.DepParentChild && child.Status != types.StatusClosed {
				return storage.CloseClaimClosurePolicyRefused, nil
			}
		}
	}
	if issue.IssueType == "gate" {
		if checkGate == nil {
			return "", fmt.Errorf("close-claim requires a gate policy checker")
		}
		if err := checkGate(issue); err != nil {
			return storage.CloseClaimClosurePolicyRefused, nil
		}
	}
	blocked, blockers, err := IsBlockedInTx(ctx, tx, id)
	if err != nil {
		return "", fmt.Errorf("check close blockers: %w", err)
	}
	if blocked && len(blockers) > 0 {
		return storage.CloseClaimBlocked, nil
	}
	return "", nil
}

func digestCloseClaimText(value string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(value)))
}
