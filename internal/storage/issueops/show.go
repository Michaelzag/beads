package issueops

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// GetShowDetailsInTx loads exact local matches, keyed by the original input.
// Leave non-exact and cross-table matches to the CLI's routed resolver.
func GetShowDetailsInTx(ctx context.Context, tx *sql.Tx, inputs []string) (map[string]storage.ShowDetail, error) {
	inputs = slices.Clone(inputs)
	slices.Sort(inputs)
	inputs = slices.Compact(inputs)
	result := make(map[string]storage.ShowDetail, len(inputs))
	// Bound SQL size while fitting the common parent + 200 children request.
	for batch := range slices.Chunk(inputs, 256) {
		issues, err := showExactIDs(ctx, tx, "issues", batch)
		if err != nil {
			return nil, err
		}
		wisps, err := showExactIDs(ctx, tx, "wisps", batch)
		if err != nil {
			return nil, err
		}
		for _, plane := range []struct {
			ids, other []sql.NullString
			wisp       bool
		}{
			{issues, wisps, false}, {wisps, issues, true},
		} {
			canonical := make(map[string]*storage.ShowDetail)
			var ids []string
			for i, id := range plane.ids {
				if id.Valid && !plane.other[i].Valid && canonical[id.String] == nil {
					canonical[id.String] = &storage.ShowDetail{}
					ids = append(ids, id.String)
				}
			}
			if len(ids) == 0 {
				continue
			}
			if err := loadShowDetails(ctx, tx, ids, canonical, plane.wisp); err != nil {
				return nil, err
			}
			for i, id := range plane.ids {
				if id.Valid && !plane.other[i].Valid {
					result[batch[i]] = *canonical[id.String]
				}
			}
		}
	}
	return result, nil
}

// As in the close-guard endpoint lookup, each scalar subquery is an indexed
// equality. Keeping the result position lets SQL match aliases using the
// column's collation without trying to reproduce that collation in Go.
//
//nolint:gosec // table is issues or wisps.
func showExactIDs(ctx context.Context, tx *sql.Tx, table string, ids []string) ([]sql.NullString, error) {
	lookup := "(SELECT id FROM " + table + " WHERE id = ?)"
	_, args := buildSQLInClause(ids)
	idsOut := make([]sql.NullString, len(ids))
	dest := make([]any, len(ids))
	for i := range dest {
		dest[i] = &idsOut[i]
	}
	query := "SELECT " + strings.Repeat(lookup+", ", len(ids)-1) + lookup
	return idsOut, tx.QueryRowContext(ctx, query, args...).Scan(dest...)
}

//nolint:gosec // table and column names are fixed schema constants.
func loadShowDetails(ctx context.Context, tx *sql.Tx, ids []string, details map[string]*storage.ShowDetail, isWisp bool) error {
	issueTable, labelTable, _, _ := WispTableRouting(isWisp)
	in, args := buildSQLInClause(ids)
	where := " WHERE i.id IN (" + in + ")"
	issueFrom := " FROM " + issueTable + " i"
	if err := scanShowRows(ctx, tx, "SELECT "+IssueSelectColumns+issueFrom+where, args, func(rows *sql.Rows) error {
		issue, err := ScanIssueFrom(rows)
		if err == nil {
			details[issue.ID].Issue = issue
		}
		return err
	}); err != nil {
		return err
	}
	// Project the owning ID through a primary-key lookup, not a Go map
	// keyed by the auxiliary row's spelling. Keep the outer lookup bounded
	// to the requested IDs too, so both sides use indexes.
	ownerID := "(SELECT id FROM " + issueTable + " WHERE id = a.issue_id)"
	auxWhere := " WHERE a.issue_id IN (" + in + ")"
	if err := scanShowRows(ctx, tx, "SELECT "+ownerID+", a.label FROM "+labelTable+" a"+
		auxWhere+" ORDER BY a.label", args, func(rows *sql.Rows) error {
		var id, label string
		if err := rows.Scan(&id, &label); err != nil {
			return err
		}
		detail := details[id]
		detail.Issue.Labels = append(detail.Issue.Labels, label)
		return nil
	}); err != nil {
		return err
	}

	guardKeys := make([]string, len(ids))
	guards := make(map[string]*storage.ShowDetail, len(ids))
	for i, id := range ids {
		key := storage.CloseGuardKey(id)
		guardKeys[i], guards[key] = key, details[id]
	}
	guardIn, guardArgs := buildSQLInClause(guardKeys)
	if err := scanShowRows(ctx, tx, "SELECT `key`, value FROM metadata WHERE `key` IN ("+guardIn+")", guardArgs, func(rows *sql.Rows) error {
		var key, token string
		if err := rows.Scan(&key, &token); err != nil {
			return err
		}
		detail := guards[key]
		if detail == nil {
			return fmt.Errorf("noncanonical guard key: %q", key)
		}
		detail.GuardToken = token
		return nil
	}); err != nil {
		return err
	}

	type edge struct{ source, target, kind string }
	var edges []edge
	targets := make(map[string]struct{})
	for _, table := range []string{"dependencies", "wisp_dependencies"} {
		if err := scanShowRows(ctx, tx, "SELECT "+ownerID+", "+depTargetExpr("a")+", a.type FROM "+table+" a"+
			auxWhere, args, func(rows *sql.Rows) error {
			var e edge
			if err := rows.Scan(&e.source, &e.target, &e.kind); err != nil {
				return err
			}
			edges = append(edges, e)
			targets[e.target] = struct{}{}
			details[e.source].DependencyCount++
			return nil
		}); err != nil {
			return err
		}
	}
	if len(targets) > 0 {
		ids := make([]string, 0, len(targets))
		for id := range targets {
			ids = append(ids, id)
		}
		issues, err := GetIssuesByIDsInTx(ctx, tx, ids, nil)
		if err != nil {
			return err
		}
		byID := make(map[string]*types.Issue, len(issues))
		for _, issue := range issues {
			byID[issue.ID] = issue
		}
		for _, e := range edges {
			// The single-ID read also omits dangling/external targets and
			// matches hydrated targets by exact stored spelling.
			if issue := byID[e.target]; issue != nil {
				d := details[e.source]
				d.Dependencies = append(d.Dependencies, &types.IssueWithDependencyMetadata{
					Issue: *issue, DependencyType: types.DependencyType(e.kind),
				})
			}
		}
	}

	// Split COALESCE as in issueCountsQuery, but count every edge type.
	var lookups []string
	var countArgs []any
	for _, table := range []string{"dependencies", "wisp_dependencies"} {
		guards := ""
		for _, col := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
			lookups = append(lookups, fmt.Sprintf("SELECT %s AS target FROM %s WHERE %s IN (%s)%s", col, table, col, in, guards))
			countArgs = append(countArgs, args...)
			guards += " AND " + col + " IS NULL"
		}
	}
	// The legacy COALESCE comparison against a bound parameter is bytewise
	// on Dolt. Use index scopes to find candidates, but do not fold spellings
	// when aggregating their counts.
	if err := scanShowRows(ctx, tx, "SELECT BINARY target, count(*) FROM ("+
		strings.Join(lookups, " UNION ALL ")+") d GROUP BY BINARY target",
		countArgs, func(rows *sql.Rows) error {
			var id string
			var count int64
			if err := rows.Scan(&id, &count); err != nil {
				return err
			}
			if detail := details[id]; detail != nil {
				detail.DependentCount = count
			}
			return nil
		}); err != nil {
		return err
	}
	// Show's legacy comment count queries only comments, even for wisps.
	return scanShowRows(ctx, tx, "SELECT "+ownerID+", a.n FROM (SELECT issue_id, count(*) AS n FROM comments"+
		" WHERE issue_id IN ("+in+") GROUP BY issue_id) a", args, func(rows *sql.Rows) error {
		var id string
		var count int64
		if err := rows.Scan(&id, &count); err != nil {
			return err
		}
		details[id].CommentCount = count
		return nil
	})
}

func scanShowRows(ctx context.Context, tx *sql.Tx, query string, args []any, scan func(*sql.Rows) error) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
