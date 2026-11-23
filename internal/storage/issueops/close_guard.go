package issueops

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// BumpCloseGuardsInTx makes overlapping writers of close predicates contend on
// the same metadata cell per exact issue ID. Values must differ even for retries
// of the same operation, since Dolt can merge identical writes.
func BumpCloseGuardsInTx(ctx context.Context, tx *sql.Tx, ids ...string) error {
	if len(ids) > 1 {
		ids = slices.Clone(ids)
		slices.Sort(ids)
		ids = slices.Compact(ids)
	}
	for _, id := range ids {
		nonce, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("generate close guard for %s: %w", id, err)
		}
		key := fmt.Sprintf("_close_guard_v1:%x", sha256.Sum256([]byte(id)))
		if err := SetMetadataInTx(ctx, tx, key, nonce.String()); err != nil {
			return fmt.Errorf("bump close guard for %s: %w", id, err)
		}
	}
	return nil
}

// BumpStatusCloseGuardsInTx guards the changed issue and affected blocker
// readers, including direct parents whose epic-close policy reads child status.
// Call it while the pre-mutation parent edges are still visible.
func BumpStatusCloseGuardsInTx(ctx context.Context, tx *sql.Tx, id string, affectedIssues, affectedWisps []string) error {
	ids, err := statusCloseGuardIDsInTx(ctx, tx, id, affectedIssues, affectedWisps)
	if err != nil {
		return err
	}
	return BumpCloseGuardsInTx(ctx, tx, ids...)
}

//nolint:gosec // G201: dependency table names are hardcoded.
func statusCloseGuardIDsInTx(ctx context.Context, tx *sql.Tx, id string, affectedIssues, affectedWisps []string) ([]string, error) {
	ids := make([]string, 0, 1+len(affectedIssues)+len(affectedWisps))
	ids = append(ids, id)
	ids = append(ids, affectedIssues...)
	ids = append(ids, affectedWisps...)
	for _, depTable := range []string{"dependencies", "wisp_dependencies"} {
		target := depTargetExpr("d")
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
			SELECT COALESCE(p.id, w.id, %s)
			FROM %s d
			LEFT JOIN issues p ON p.id = %s
			LEFT JOIN wisps w ON w.id = %s
			WHERE d.issue_id = ? AND d.type = 'parent-child'`, target, depTable, target, target), id)
		if err != nil {
			if optionalBlockedTable(depTable) && isTableNotExistError(err) {
				continue
			}
			return nil, fmt.Errorf("read close guard parents of %s: %w", id, err)
		}
		for rows.Next() {
			var parent string
			if err := rows.Scan(&parent); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan close guard parent of %s: %w", id, err)
			}
			ids = append(ids, parent)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read close guard parent rows of %s: %w", id, err)
		}
	}

	// IsBlockedInTx also reads the status of the spawner itself when
	// describing a waits-for edge. Cache recomputation only follows children.
	var issueWaiters, wispWaiters []string
	if err := loadWaitersOnSpawnerIDsInTx(ctx, tx, []string{id}, &issueWaiters, make(map[string]bool), &wispWaiters, make(map[string]bool)); err != nil {
		return nil, err
	}
	ids = append(ids, issueWaiters...)
	ids = append(ids, wispWaiters...)
	return ids, nil
}

// BumpDependencyCloseGuardsInTx guards both endpoints and the blocker readers
// discovered before and after a dependency mutation.
func BumpDependencyCloseGuardsInTx(ctx context.Context, tx *sql.Tx, source, target string, beforeIssues, beforeWisps, afterIssues, afterWisps []string) error {
	ids := make([]string, 0, 2+len(beforeIssues)+len(beforeWisps)+len(afterIssues)+len(afterWisps))
	ids = append(ids, source, target)
	ids = append(ids, beforeIssues...)
	ids = append(ids, beforeWisps...)
	ids = append(ids, afterIssues...)
	ids = append(ids, afterWisps...)
	return BumpCloseGuardsInTx(ctx, tx, ids...)
}

// closeGuardIssueIDInTx resolves the exact spelling of an endpoint selected
// by normal SQL collation. A dangling edge may be removed without an issue row.
//
//nolint:gosec // G201: issueTable comes from WispTableRouting.
func closeGuardIssueIDInTx(ctx context.Context, tx *sql.Tx, issueTable, id string) (string, error) {
	var exactID string
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE id = ?`, issueTable), id).Scan(&exactID)
	if err == sql.ErrNoRows {
		return id, nil
	}
	if err != nil {
		return "", fmt.Errorf("read exact close guard issue ID for %s: %w", id, err)
	}
	return exactID, nil
}
