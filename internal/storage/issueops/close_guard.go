package issueops

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

type edgeQueryRunner interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type closeGuardWriter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type closeGuardReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type closeGuardRunner interface {
	closeGuardWriter
	closeGuardReader
}

// BumpCloseGuardsInTx makes overlapping writers of close predicates contend on
// the same metadata cell per exact issue ID. Values must differ even for retries
// of the same operation, since Dolt can merge identical writes.
func BumpCloseGuardsInTx(ctx context.Context, tx closeGuardWriter, ids ...string) error {
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
		key := storage.CloseGuardKey(id)
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
	ids, err := appendCloseGuardIDsInTx(ctx, tx, ids, "issues", affectedIssues, issueWaiters)
	if err != nil {
		return nil, err
	}
	return appendCloseGuardIDsInTx(ctx, tx, ids, "wisps", affectedWisps, wispWaiters)
}

// BumpDependencyCloseGuardsInTx guards both endpoints and the blocker readers
// discovered before and after a dependency mutation.
func BumpDependencyCloseGuardsInTx(ctx context.Context, tx closeGuardRunner, source, target string, beforeIssues, beforeWisps, afterIssues, afterWisps []string) error {
	ids := make([]string, 0, 2+len(beforeIssues)+len(beforeWisps)+len(afterIssues)+len(afterWisps))
	ids = append(ids, source, target)
	ids, err := appendCloseGuardIDsInTx(ctx, tx, ids, "issues", beforeIssues, afterIssues)
	if err != nil {
		return err
	}
	ids, err = appendCloseGuardIDsInTx(ctx, tx, ids, "wisps", beforeWisps, afterWisps)
	if err != nil {
		return err
	}
	return BumpCloseGuardsInTx(ctx, tx, ids...)
}

// BumpEdgeCloseGuardsInTx guards exact edge endpoints and predicate readers.
// The SQL runner must be bound to the same transaction as the edge write.
func BumpEdgeCloseGuardsInTx(ctx context.Context, tx interface {
	edgeQueryRunner
	closeGuardRunner
}, source, target string, sourceIsWisp bool, targetKind DepTargetKind, depType types.DependencyType) error {
	sourceTable, _, _, _ := WispTableRouting(sourceIsWisp)
	source, err := closeGuardIssueIDInTx(ctx, tx, sourceTable, source)
	if err != nil {
		return err
	}
	if targetKind != DepTargetExternal {
		targetTable, _, _, _ := WispTableRouting(targetKind == DepTargetWisp)
		target, err = closeGuardIssueIDInTx(ctx, tx, targetTable, target)
		if err != nil {
			return err
		}
	}
	affected := AffectedByDepChangeInTx
	if sourceIsWisp {
		affected = AffectedByDepChangeForWispInTx
	}
	issues, wisps, err := affected(ctx, tx, source, target, depType)
	if err != nil {
		return err
	}
	return BumpDependencyCloseGuardsInTx(ctx, tx, source, target, issues, wisps, nil, nil)
}

// appendCloseGuardIDsInTx resolves graph-derived IDs in their source table.
// Batch indexed point lookups preserve the column's collation without changing
// the input spelling of a dangling ID.
//
//nolint:gosec // G201: issueTable is one of two hardcoded constants.
func appendCloseGuardIDsInTx(ctx context.Context, tx closeGuardReader, ids []string, issueTable string, groups ...[]string) ([]string, error) {
	graphIDs := slices.Concat(groups...)
	if len(graphIDs) == 0 {
		return ids, nil
	}
	slices.Sort(graphIDs)
	lookup := fmt.Sprintf("(SELECT id FROM %s WHERE id = ?)", issueTable)
	for batch := range slices.Chunk(slices.Compact(graphIDs), queryBatchSize) {
		args := make([]any, len(batch))
		exactIDs := make([]sql.NullString, len(batch))
		dest := make([]any, len(batch))
		for i, id := range batch {
			args[i], dest[i] = id, &exactIDs[i]
		}
		query := "SELECT " + strings.Repeat(lookup+", ", len(batch)-1) + lookup
		if err := tx.QueryRowContext(ctx, query, args...).Scan(dest...); err != nil {
			return nil, fmt.Errorf("resolve close guard IDs from %s: %w", issueTable, err)
		}
		for i, id := range batch {
			if exactIDs[i].Valid {
				id = exactIDs[i].String
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// closeGuardIssueIDInTx resolves the exact spelling of an endpoint selected
// by normal SQL collation. A dangling edge may be removed without an issue row.
//
//nolint:gosec // G201: issueTable comes from WispTableRouting.
func closeGuardIssueIDInTx(ctx context.Context, tx closeGuardReader, issueTable, id string) (string, error) {
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
