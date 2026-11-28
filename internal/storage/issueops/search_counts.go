package issueops

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/steveyegge/beads/internal/types"
)

func SearchIssuesWithCountsInTx(ctx context.Context, tx *sql.Tx, query string, filter types.IssueFilter) ([]*types.IssueWithCounts, error) {
	limit := filter.Limit

	wispDepsExist, err := optionalTableExistsInTx(ctx, tx, "wisp_dependencies")
	if err != nil {
		return nil, fmt.Errorf("search issues with counts: wisp dependency probe: %w", err)
	}

	if filter.Ephemeral != nil && *filter.Ephemeral {
		empty, probeErr := wispsTableEmptyOrMissingInTx(ctx, tx)
		if probeErr != nil {
			return nil, fmt.Errorf("search issues with counts: ephemeral wisp probe: %w", probeErr)
		}
		if empty || !wispDepsExist {
			return nil, nil
		}
		wisps, err := runFilterSearchQueryInTx(ctx, tx, query, filter, WispsFilterTables, true)
		if err != nil {
			return nil, err
		}
		return finishSearchIssuesWithCounts(wisps, limit), nil
	}

	out, err := runFilterSearchQueryInTx(ctx, tx, query, filter, IssuesFilterTables, wispDepsExist)
	if err != nil {
		return nil, err
	}

	empty, probeErr := wispsTableEmptyOrMissingInTx(ctx, tx)
	if probeErr != nil {
		return nil, fmt.Errorf("search issues with counts: wisp probe: %w", probeErr)
	}
	if empty {
		return finishSearchIssuesWithCounts(out, limit), nil
	}
	if !wispDepsExist {
		return finishSearchIssuesWithCounts(out, limit), nil
	}

	wisps, err := runFilterSearchQueryInTx(ctx, tx, query, filter, WispsFilterTables, true)
	if err != nil {
		if isTableNotExistError(err) {
			return finishSearchIssuesWithCounts(out, limit), nil
		}
		return nil, err
	}
	if len(wisps) == 0 {
		return finishSearchIssuesWithCounts(out, limit), nil
	}

	seen := make(map[string]struct{}, len(out))
	for _, iwc := range out {
		if iwc != nil && iwc.Issue != nil {
			seen[iwc.Issue.ID] = struct{}{}
		}
	}
	for _, w := range wisps {
		if w == nil || w.Issue == nil {
			continue
		}
		if _, dup := seen[w.Issue.ID]; dup {
			return nil, fmt.Errorf("search issues with counts: id %q exists in both issues and wisps", w.Issue.ID)
		}
		out = append(out, w)
	}
	return finishSearchIssuesWithCounts(out, limit), nil
}

func runFilterSearchQueryInTx(ctx context.Context, tx *sql.Tx, query string, filter types.IssueFilter, tables FilterTables, includeWispReverseDeps bool) ([]*types.IssueWithCounts, error) {
	whereClauses, args, err := BuildIssueFilterClauses(query, filter, tables)
	if err != nil {
		return nil, err
	}
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + joinAnd(whereClauses)
	}
	const orderBy = "ORDER BY i.priority ASC, i.created_at DESC, i.id ASC"
	ids, err := queryIssueCandidatesInTx(ctx, tx, tables, whereSQL, orderBy, args, filter.Limit, false)
	if err != nil {
		return nil, err
	}
	return hydrateIssueCountsInTx(ctx, tx, tables, ids, includeWispReverseDeps, filter.SkipLabels)
}

func hydrateIssueCountsInTx(ctx context.Context, tx *sql.Tx, tables FilterTables, ids []string, includeWispReverseDeps, skipLabels bool) ([]*types.IssueWithCounts, error) {
	var out []*types.IssueWithCounts
	for batch := range slices.Chunk(ids, queryBatchSize) {
		query, args := issueCountsQuery(tables, batch, includeWispReverseDeps, skipLabels)
		items, err := scanIssueCountsInTx(ctx, tx, tables, query, args)
		if err != nil {
			return nil, err
		}
		byID := make(map[string]*types.IssueWithCounts, len(items))
		for _, item := range items {
			byID[item.Issue.ID] = item
		}
		for _, id := range batch {
			if item := byID[id]; item != nil {
				out = append(out, item)
			}
		}
	}
	return out, nil
}

//nolint:gosec // G201: SQL fragments use only fixed names and placeholders.
func issueCountsQuery(tables FilterTables, ids []string, includeWispReverseDeps, skipLabels bool) (string, []any) {
	placeholders, batchArgs := buildSQLInClause(ids)
	var args []any
	scope := func(column string) string {
		args = append(args, batchArgs...)
		return column + " IN (" + placeholders + ")"
	}

	labelsSelect := "l.labels_json AS labels_json"
	labelsJoin := fmt.Sprintf(`
		LEFT JOIN (
			SELECT issue_id, JSON_ARRAYAGG(label) AS labels_json
			FROM %s WHERE %s
			GROUP BY issue_id
		) l ON l.issue_id = i.id`, tables.Labels, scope("issue_id"))
	if skipLabels {
		labelsSelect = "NULL AS labels_json"
		labelsJoin = ""
		args = nil
	}

	dependencyCountScope := scope("issue_id")
	// Split COALESCE into indexed target lookups. The null guards preserve its
	// precedence, including for pre-existing rows with multiple targets set.
	var reverseBlockers []string
	depTables := []string{"dependencies"}
	if includeWispReverseDeps {
		depTables = append(depTables, "wisp_dependencies")
	}
	for _, table := range depTables {
		nullGuards := ""
		for _, col := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
			reverseBlockers = append(reverseBlockers, fmt.Sprintf(
				"SELECT %s AS dep_id FROM %s WHERE type = 'blocks' AND %s%s",
				col, table, scope(col), nullGuards))
			nullGuards += " AND " + col + " IS NULL"
		}
	}

	searchSQL := fmt.Sprintf(`
		SELECT %s,
			%s,
			COALESCE(dc.cnt, 0) AS dep_count,
			COALESCE(rc.cnt, 0) AS rdep_count,
			COALESCE(cc.cnt, 0) AS comment_count,
			pc.parent_id     AS parent_id,
			d.deps_json      AS deps_json
		FROM %s i
		%s
		LEFT JOIN (
			SELECT issue_id, COUNT(*) AS cnt
			FROM %s
			WHERE type = 'blocks' AND %s
			GROUP BY issue_id
		) dc ON dc.issue_id = i.id
		LEFT JOIN (
			SELECT dep_id, COUNT(*) AS cnt FROM (
				%s
			) all_blockers GROUP BY dep_id
		) rc ON rc.dep_id = i.id
		LEFT JOIN (
			SELECT issue_id, COUNT(*) AS cnt
			FROM %s WHERE %s
			GROUP BY issue_id
		) cc ON cc.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id,
			       MIN(COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external)) AS parent_id
			FROM %s
			WHERE type = 'parent-child' AND %s
			GROUP BY issue_id
		) pc ON pc.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id, JSON_ARRAYAGG(%s) AS deps_json
			FROM %s WHERE %s
			GROUP BY issue_id
		) d ON d.issue_id = i.id
		WHERE %s
	`,
		readyWorkIssueColumns,
		labelsSelect,
		tables.Main,
		labelsJoin,
		tables.Dependencies,
		dependencyCountScope,
		strings.Join(reverseBlockers, " UNION ALL "),
		tables.Comments,
		scope("issue_id"),
		tables.Dependencies,
		scope("issue_id"),
		readyWorkDepJSONObject,
		tables.Dependencies,
		scope("issue_id"),
		scope("i.id"),
	)
	return searchSQL, args
}

func scanIssueCountsInTx(ctx context.Context, tx *sql.Tx, tables FilterTables, searchSQL string, args []any) ([]*types.IssueWithCounts, error) {
	rows, err := tx.QueryContext(ctx, searchSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("search count %s: %w", tables.Main, err)
	}
	defer func() { _ = rows.Close() }()

	var out []*types.IssueWithCounts
	for rows.Next() {
		iwc, scanErr := scanReadyWorkRowWithCounts(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, iwc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("search count %s: rows: %w", tables.Main, err)
	}
	return out, nil
}

func finishSearchIssuesWithCounts(items []*types.IssueWithCounts, limit int) []*types.IssueWithCounts {
	sortSearchIssuesWithCounts(items)
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func sortSearchIssuesWithCounts(items []*types.IssueWithCounts) {
	if len(items) <= 1 {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a == nil || a.Issue == nil {
			return false
		}
		if b == nil || b.Issue == nil {
			return true
		}
		if a.Issue.Priority != b.Issue.Priority {
			return a.Issue.Priority < b.Issue.Priority
		}
		if !a.Issue.CreatedAt.Equal(b.Issue.CreatedAt) {
			return a.Issue.CreatedAt.After(b.Issue.CreatedAt)
		}
		return a.Issue.ID < b.Issue.ID
	})
}

func joinAnd(clauses []string) string {
	switch len(clauses) {
	case 0:
		return ""
	case 1:
		return clauses[0]
	}
	total := 0
	for _, c := range clauses {
		total += len(c)
	}
	total += 5 * (len(clauses) - 1)
	buf := make([]byte, 0, total)
	for i, c := range clauses {
		if i > 0 {
			buf = append(buf, " AND "...)
		}
		buf = append(buf, c...)
	}
	return string(buf)
}
