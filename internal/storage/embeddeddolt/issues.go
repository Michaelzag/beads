//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// ClaimIssue atomically claims an issue using compare-and-swap semantics.
// Delegates SQL work to issueops; EmbeddedDolt auto-commits the transaction.
func (s *EmbeddedDoltStore) ClaimIssue(ctx context.Context, id string, actor string) error {
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := issueops.ClaimIssueInTx(ctx, tx, id, actor)
		return err
	})
}

// ReleaseClaim releases the expected claim or returns its existing receipt.
func (s *EmbeddedDoltStore) ReleaseClaim(ctx context.Context, id, expectedAssignee, operationID, actor string) error {
	_, err := s.ReleaseClaimWithResult(ctx, id, expectedAssignee, operationID, actor)
	return err
}

// ReleaseClaimWithResult reports whether a successful release changed the issue.
func (s *EmbeddedDoltStore) ReleaseClaimWithResult(ctx context.Context, id, expectedAssignee, operationID, actor string) (bool, error) {
	var result issueops.ReleaseClaimResult
	err := s.withConn(ctx, true, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.ReleaseClaimInTx(ctx, tx, id, expectedAssignee, operationID, actor)
		if err != nil || !result.Recorded {
			return err
		}

		tables := []string{"metadata"}
		if result.Updated != nil {
			// A wisp status change may also update a regular issue's blocker state.
			tables = append(tables, "issues")
			if !result.Updated.IsWisp {
				tables = append(tables, "events")
			}
		}
		for _, table := range tables {
			if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
				return fmt.Errorf("dolt add %s: %w", table, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?)", fmt.Sprintf("bd: release claim %s", id)); err != nil {
			return fmt.Errorf("dolt commit release claim: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if result.RefusalReason != "" {
		return false, &storage.ReleaseClaimRefusal{Reason: result.RefusalReason}
	}
	return result.Updated != nil, nil
}

// ClaimReadyIssue atomically claims the first ready issue matching filter.
func (s *EmbeddedDoltStore) ClaimReadyIssue(ctx context.Context, filter types.WorkFilter, actor string) (*types.Issue, error) {
	var claimed *types.Issue
	err := s.withConn(ctx, true, func(tx *sql.Tx) error {
		var err error
		claimed, err = issueops.ClaimReadyIssueInTx(ctx, tx, filter, actor)
		return err
	})
	return claimed, err
}

// UpdateIssue updates fields on an issue.
// Delegates SQL work to issueops; EmbeddedDolt auto-commits the transaction.
func (s *EmbeddedDoltStore) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	// Validate metadata against schema before routing.
	if rawMeta, ok := updates["metadata"]; ok {
		metadataStr, err := storage.NormalizeMetadataValue(rawMeta)
		if err != nil {
			return fmt.Errorf("invalid metadata: %w", err)
		}
		if err := issueops.ValidateMetadataIfConfigured(json.RawMessage(metadataStr)); err != nil {
			return err
		}
	}

	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := issueops.UpdateIssueInTx(ctx, tx, id, updates, actor)
		return err
	})
}

// UpdateIssueConditional applies updates only if cond holds in the writing
// transaction, replacing the issue's guard token in the same transaction.
func (s *EmbeddedDoltStore) UpdateIssueConditional(ctx context.Context, id string, updates map[string]interface{}, actor string, cond storage.UpdatePrecondition) error {
	if rawMeta, ok := updates["metadata"]; ok {
		metadataStr, err := storage.NormalizeMetadataValue(rawMeta)
		if err != nil {
			return fmt.Errorf("invalid metadata: %w", err)
		}
		if err := issueops.ValidateMetadataIfConfigured(json.RawMessage(metadataStr)); err != nil {
			return err
		}
	}

	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := issueops.UpdateIssueConditionalInTx(ctx, tx, id, updates, actor, cond)
		return err
	})
}

// GetGuardedIssue reads an issue, its dependencies and its guard token in one
// read transaction, routed as UpdateIssueConditional routes.
func (s *EmbeddedDoltStore) GetGuardedIssue(ctx context.Context, id string) (*storage.GuardedIssue, error) {
	var result *storage.GuardedIssue
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.GetGuardedIssueInTx(ctx, tx, id)
		return err
	})
	return result, err
}

// ReopenIssue reopens a closed issue, setting status to open and clearing
// closed_at and defer_until. If reason is non-empty, it is recorded as a comment.
// Wraps UpdateIssue; EmbeddedDolt auto-commits the transaction.
func (s *EmbeddedDoltStore) ReopenIssue(ctx context.Context, id string, reason string, actor string) error {
	updates := map[string]interface{}{
		"status":      string(types.StatusOpen),
		"defer_until": nil,
	}
	if err := s.UpdateIssue(ctx, id, updates, actor); err != nil {
		return err
	}
	if reason != "" {
		if err := s.AddComment(ctx, id, actor, reason); err != nil {
			return fmt.Errorf("reopen comment: %w", err)
		}
	}
	return nil
}

// UpdateIssueType changes the issue_type field of an issue.
// Wraps UpdateIssue; EmbeddedDolt auto-commits the transaction.
func (s *EmbeddedDoltStore) UpdateIssueType(ctx context.Context, id string, issueType string, actor string) error {
	return s.UpdateIssue(ctx, id, map[string]interface{}{"issue_type": issueType}, actor)
}

// CloseIssue closes an issue with a reason.
// Delegates SQL work to issueops; EmbeddedDolt auto-commits the transaction.
func (s *EmbeddedDoltStore) CloseIssue(ctx context.Context, id string, reason string, actor string, session string) error {
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := issueops.CloseIssueInTx(ctx, tx, id, reason, actor, session)
		return err
	})
}

// IsBlocked checks if an issue is blocked by active dependencies.
func (s *EmbeddedDoltStore) IsBlocked(ctx context.Context, issueID string) (bool, []string, error) {
	var blocked bool
	var blockers []string
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		var err error
		blocked, blockers, err = issueops.IsBlockedInTx(ctx, tx, issueID)
		return err
	})
	return blocked, blockers, err
}

// GetNewlyUnblockedByClose finds issues that become unblocked when closedIssueID is closed.
func (s *EmbeddedDoltStore) GetNewlyUnblockedByClose(ctx context.Context, closedIssueID string) ([]*types.Issue, error) {
	var result []*types.Issue
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.GetNewlyUnblockedByCloseInTx(ctx, tx, closedIssueID)
		return err
	})
	return result, err
}
