package dolt

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// CloseClaim closes the expected claim or returns its committed receipt.
func (s *DoltStore) CloseClaim(ctx context.Context, intent storage.CloseClaimIntent, actor string, mode storage.CloseClaimMode, checkGate func(*types.Issue) error) (storage.CloseClaimReceipt, error) {
	var result issueops.CloseClaimResult
	run := s.withRetryTx
	if mode == storage.CloseClaimLookup {
		run = s.withReadTx
	}
	err := run(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.CloseClaimInTx(ctx, tx, intent, actor, mode, checkGate)
		if err != nil || !result.Recorded {
			return err
		}

		tables := []string{"metadata"}
		if result.Receipt.Outcome == storage.CloseClaimClosed {
			tables = append(tables, "issues", "events")
		}
		for _, table := range tables {
			if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
				return fmt.Errorf("dolt add %s: %w", table, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?, '--author', ?)",
			fmt.Sprintf("bd: close claim %s", intent.WorkItemID), s.commitAuthorString()); err != nil {
			return fmt.Errorf("dolt commit close claim: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.CloseClaimReceipt{}, err
	}
	return result.Receipt, nil
}
