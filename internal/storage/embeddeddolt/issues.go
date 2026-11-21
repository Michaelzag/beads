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
	var changed bool
	err := s.withConn(ctx, true, func(tx *sql.Tx) error {
		result, err := issueops.ReleaseClaimInTx(ctx, tx, id, expectedAssignee, operationID, actor)
		if err != nil || result == nil {
			return err
		}

		tables := []string{"metadata"}
		if !result.IsWisp {
			tables = append(tables, "issues", "events")
		}
		for _, table := range tables {
			if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
				return fmt.Errorf("dolt add %s: %w", table, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?)", fmt.Sprintf("bd: release claim %s", id)); err != nil {
			return fmt.Errorf("dolt commit release claim: %w", err)
		}
		changed = true
		return nil
	})
	return err == nil && changed, err
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
