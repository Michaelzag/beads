//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// OpenForCloseClaim uses an already-provisioned store without schema or
// workspace initialization. The protocol itself opens the SQL transaction.
func OpenForCloseClaim(beadsDir, database, branch string) (*EmbeddedDoltStore, error) {
	if !validIdentifier.MatchString(database) {
		return nil, fmt.Errorf("embeddeddolt: invalid database name: %q", database)
	}
	dataDir, err := cacheKey(beadsDir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(filepath.Join(dataDir, database, ".dolt")); err != nil {
		return nil, fmt.Errorf("embeddeddolt: open provisioned database: %w", err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("embeddeddolt: provisioned database lacks a .dolt directory")
	}
	return &EmbeddedDoltStore{dataDir: dataDir, beadsDir: filepath.Dir(dataDir), database: database, branch: branch}, nil
}

// CloseClaim closes the expected claim or returns its committed receipt.
func (s *EmbeddedDoltStore) CloseClaim(ctx context.Context, intent storage.CloseClaimIntent, actor string, mode storage.CloseClaimMode, checkGate func(*types.Issue) error) (storage.CloseClaimReceipt, error) {
	var result issueops.CloseClaimResult
	err := s.withConn(ctx, mode != storage.CloseClaimLookup, func(tx *sql.Tx) error {
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
		if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?)", fmt.Sprintf("bd: close claim %s", intent.WorkItemID)); err != nil {
			return fmt.Errorf("dolt commit close claim: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.CloseClaimReceipt{}, err
	}
	return result.Receipt, nil
}
