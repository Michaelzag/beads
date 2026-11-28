package issueops

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

type readyWorkOrder struct {
	sql  string
	args []interface{}
}

func readyWorkPageSize(limit int) int {
	if limit <= 0 {
		return 0
	}
	const minPageSize = 100
	if limit < minPageSize {
		return minPageSize
	}
	return limit
}

func buildReadyWorkOrder(policy types.SortPolicy) readyWorkOrder {
	switch policy {
	case types.SortPolicyOldest:
		return readyWorkOrder{sql: "ORDER BY created_at ASC, id ASC"}
	case types.SortPolicyPriority:
		return readyWorkOrder{sql: "ORDER BY priority ASC, created_at DESC, id ASC"}
	case types.SortPolicyHybrid, "":
		recentCutoff := time.Now().UTC().Add(-48 * time.Hour)
		return readyWorkOrder{
			sql: `ORDER BY
			CASE WHEN created_at >= ? THEN 0 ELSE 1 END ASC,
			CASE WHEN created_at >= ? THEN priority ELSE 999 END ASC,
			created_at ASC, id ASC`,
			args: []interface{}{recentCutoff, recentCutoff},
		}
	default:
		return readyWorkOrder{sql: "ORDER BY priority ASC, created_at DESC, id ASC"}
	}
}

func buildReadyWorkPredicates(ctx context.Context, tx *sql.Tx, filter types.WorkFilter, tables FilterTables) (string, []any, error) {
	var statusClause string
	if filter.Status != "" {
		statusClause = "status = ?"
	} else {
		statusClause = "status IN ('open', 'in_progress')"
	}
	whereClauses := []string{
		statusClause,
		"(pinned = 0 OR pinned IS NULL)",
		"is_blocked = 0",
	}
	if !filter.IncludeEphemeral {
		whereClauses = append(whereClauses, "(ephemeral = 0 OR ephemeral IS NULL)")
	}
	var args []interface{}
	if filter.Status != "" {
		args = append(args, string(filter.Status))
	}

	if filter.Priority != nil {
		whereClauses = append(whereClauses, "priority = ?")
		args = append(args, *filter.Priority)
	}
	if filter.Type != "" {
		whereClauses = append(whereClauses, "issue_type = ?")
		args = append(args, filter.Type)
	} else {
		excludeTypes := readyWorkExcludeTypes(filter.ExcludeTypes)
		placeholders := make([]string, len(excludeTypes))
		for i, t := range excludeTypes {
			placeholders[i] = "?"
			args = append(args, string(t))
		}
		whereClauses = append(whereClauses, fmt.Sprintf("issue_type NOT IN (%s)", strings.Join(placeholders, ",")))
	}
	if filter.Unassigned {
		whereClauses = append(whereClauses, "(assignee IS NULL OR assignee = '')")
	} else if filter.Assignee != nil {
		whereClauses = append(whereClauses, "assignee = ?")
		args = append(args, *filter.Assignee)
	}

	if !filter.IncludeDeferred {
		whereClauses = append(whereClauses, "(defer_until IS NULL OR defer_until <= UTC_TIMESTAMP())")
	}

	if len(filter.Labels) > 0 {
		for _, label := range filter.Labels {
			whereClauses = append(whereClauses, fmt.Sprintf("id IN (SELECT issue_id FROM %s WHERE label = ?)", tables.Labels))
			args = append(args, label)
		}
	}
	if len(filter.ExcludeLabels) > 0 {
		placeholders := make([]string, len(filter.ExcludeLabels))
		for i, label := range filter.ExcludeLabels {
			placeholders[i] = "?"
			args = append(args, label)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("id NOT IN (SELECT issue_id FROM %s WHERE label IN (%s))", tables.Labels, strings.Join(placeholders, ", ")))
	}

	// Parent filtering: return all transitive descendants of parentID.
	// GH#3396: previously was a one-hop subquery against dependencies, so
	// grandchildren were silently dropped despite the help text and
	// WorkFilter.ParentID godoc both promising "descendants (recursive)".
	if filter.ParentID != nil {
		parentID := *filter.ParentID
		descendantIDs, descErr := GetDescendantIDsInTx(ctx, tx, parentID, 0)
		if descErr != nil {
			return "", nil, fmt.Errorf("get parent descendants: %w", descErr)
		}
		parentClauses := []string{fmt.Sprintf("(id LIKE CONCAT(?, '.%%') AND id NOT IN (SELECT issue_id FROM %s WHERE type = 'parent-child'))", tables.Dependencies)}
		args = append(args, parentID)
		for start := 0; start < len(descendantIDs); start += queryBatchSize {
			end := start + queryBatchSize
			if end > len(descendantIDs) {
				end = len(descendantIDs)
			}
			placeholders, batchArgs := buildSQLInClause(descendantIDs[start:end])
			parentClauses = append(parentClauses, fmt.Sprintf("id IN (%s)", placeholders))
			args = append(args, batchArgs...)
		}
		whereClauses = append(whereClauses, "("+strings.Join(parentClauses, " OR ")+")")
	}

	if filter.MoleculeID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(id IN (SELECT issue_id FROM %s WHERE type = 'parent-child' AND %s = ?) OR (id LIKE CONCAT(?, '.%%') AND id NOT IN (SELECT issue_id FROM %s WHERE type = 'parent-child')))", tables.Dependencies, DepTargetExpr, tables.Dependencies))
		args = append(args, filter.MoleculeID, filter.MoleculeID)
	}

	if filter.HasMetadataKey != "" {
		if err := storage.ValidateMetadataKey(filter.HasMetadataKey); err != nil {
			return "", nil, err
		}
		whereClauses = append(whereClauses, "JSON_EXTRACT(metadata, ?) IS NOT NULL")
		args = append(args, storage.JSONMetadataPath(filter.HasMetadataKey))
	}

	if len(filter.MetadataFields) > 0 {
		metaKeys := make([]string, 0, len(filter.MetadataFields))
		for k := range filter.MetadataFields {
			metaKeys = append(metaKeys, k)
		}
		sort.Strings(metaKeys)
		for _, k := range metaKeys {
			if err := storage.ValidateMetadataKey(k); err != nil {
				return "", nil, err
			}
			whereClauses = append(whereClauses, "JSON_UNQUOTE(JSON_EXTRACT(metadata, ?)) = ?")
			args = append(args, storage.JSONMetadataPath(k), filter.MetadataFields[k])
		}
	}

	return "WHERE " + strings.Join(whereClauses, " AND "), args, nil
}

//nolint:gosec // G201: whereSQL/orderBySQL built from hardcoded strings and ? placeholders
func GetReadyWorkInTx(
	ctx context.Context,
	tx *sql.Tx,
	filter types.WorkFilter,
) ([]*types.Issue, error) {
	issueIDs, err := queryReadyWorkCandidatesInTx(ctx, tx, filter, IssuesFilterTables)
	if err != nil {
		return nil, err
	}

	issues, err := GetIssuesByIDsInTx(ctx, tx, issueIDs, nil)
	if err != nil {
		return nil, fmt.Errorf("get ready work: fetch issues: %w", err)
	}
	issueMap := make(map[string]*types.Issue, len(issues))
	for _, iss := range issues {
		issueMap[iss.ID] = iss
	}
	ordered := make([]*types.Issue, 0, len(issueIDs))
	for _, id := range issueIDs {
		if iss, ok := issueMap[id]; ok {
			ordered = append(ordered, iss)
		}
	}

	wisps, wErr := getReadyWispsInTx(ctx, tx, filter)
	if wErr != nil {
		return nil, wErr
	}
	if len(wisps) > 0 {
		ordered, err = mergeReadyWisps(ordered, wisps, filter)
		if err != nil {
			return nil, err
		}
	}

	return ordered, nil
}

func mergeReadyWisps(ordered []*types.Issue, wisps []*types.Issue, filter types.WorkFilter) ([]*types.Issue, error) {
	seen := make(map[string]struct{}, len(ordered))
	for _, issue := range ordered {
		seen[issue.ID] = struct{}{}
	}
	for _, wisp := range wisps {
		if _, exists := seen[wisp.ID]; exists {
			return nil, fmt.Errorf("ready work id %q exists in both issues and wisps", wisp.ID)
		}
		ordered = append(ordered, wisp)
	}
	sortReadyIssues(ordered, filter.SortPolicy)
	if filter.Limit > 0 && len(ordered) > filter.Limit {
		ordered = ordered[:filter.Limit]
	}
	return ordered, nil
}

func getReadyWispsInTx(ctx context.Context, tx *sql.Tx, filter types.WorkFilter) ([]*types.Issue, error) {
	empty, err := wispsTableEmptyOrMissingInTx(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("search wisps (ready work): probe: %w", err)
	}
	if empty {
		return nil, nil
	}

	wispFilter := readyWorkWispIssueFilter(filter)
	if filter.Limit <= 0 {
		wispFilter.Limit = 0
		wisps, err := searchTableInTx(ctx, tx, "", wispFilter, WispsFilterTables)
		if err != nil {
			if isTableNotExistError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("search wisps (ready work): %w", err)
		}
		return filterReadyWispsInTx(ctx, tx, filter, wisps)
	}

	pageSize := readyWorkPageSize(filter.Limit)
	orderBy := buildReadyWorkOrder(filter.SortPolicy)
	ready := make([]*types.Issue, 0, filter.Limit)
	for offset := 0; len(ready) < filter.Limit; offset += pageSize {
		pageIDs, err := queryReadyWispIssueIDPage(ctx, tx, wispFilter, !filter.IncludeDeferred, orderBy, pageSize, offset)
		if err != nil {
			if isTableNotExistError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("search wisps (ready work): %w", err)
		}
		if len(pageIDs) == 0 {
			break
		}

		pageWisps, err := getWispIssuesByIDsInOrderInTx(ctx, tx, pageIDs)
		if err != nil {
			return nil, fmt.Errorf("search wisps (ready work): %w", err)
		}
		pageReady, err := filterReadyWispsInTx(ctx, tx, filter, pageWisps)
		if err != nil {
			return nil, err
		}
		for _, wisp := range pageReady {
			ready = append(ready, wisp)
			if len(ready) >= filter.Limit {
				break
			}
		}
		if len(pageIDs) < pageSize {
			break
		}
	}
	return ready, nil
}

func queryReadyWispIssueIDPage(ctx context.Context, tx *sql.Tx, filter types.IssueFilter, excludeDeferred bool, orderBy readyWorkOrder, limit, offset int) ([]string, error) {
	fromSQL, labelWhere, labelArgs, labelDriven, filterForClauses := buildLabelDrivenSearch(filter, WispsFilterTables)
	whereClauses, args, err := BuildIssueFilterClauses("", filterForClauses, WispsFilterTables)
	if err != nil {
		return nil, err
	}
	if len(labelWhere) > 0 {
		whereClauses = append(labelWhere, whereClauses...)
		args = append(labelArgs, args...)
	}
	if excludeDeferred {
		whereClauses = append(whereClauses, "(defer_until IS NULL OR defer_until <= UTC_TIMESTAMP())")
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	selectSQL := "SELECT "
	if labelDriven {
		selectSQL = "SELECT DISTINCT "
	}
	args = append(args, orderBy.args...)
	//nolint:gosec // G201: SQL fragments are fixed table/column names and parameterized filters; limit/offset are ints.
	query := fmt.Sprintf(`%sid FROM %s %s %s LIMIT %d OFFSET %d`,
		selectSQL, fromSQL, whereSQL, orderBy.sql, limit, offset)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search wisps: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("search wisps: scan id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("search wisps: rows: %w", err)
	}
	return ids, nil
}

func getWispIssuesByIDsInOrderInTx(ctx context.Context, tx *sql.Tx, ids []string) ([]*types.Issue, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	wispSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wispSet[id] = struct{}{}
	}
	issues, err := GetIssuesByIDsInTx(ctx, tx, ids, wispSet)
	if err != nil {
		return nil, err
	}
	issueMap := make(map[string]*types.Issue, len(issues))
	for _, issue := range issues {
		issueMap[issue.ID] = issue
	}
	ordered := make([]*types.Issue, 0, len(ids))
	for _, id := range ids {
		if issue, ok := issueMap[id]; ok {
			ordered = append(ordered, issue)
		}
	}
	return ordered, nil
}

func readyWorkExcludeTypes(extra []types.IssueType) []types.IssueType {
	excludeTypes := []types.IssueType{
		types.IssueType("merge-request"),
		types.TypeGate,
		types.TypeMolecule,
		types.TypeMessage,
		types.IssueType("agent"),
		types.IssueType("role"),
		types.IssueType("rig"),
	}
	seen := make(map[types.IssueType]bool, len(excludeTypes)+len(extra))
	for _, t := range excludeTypes {
		seen[t] = true
	}
	for _, t := range extra {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		excludeTypes = append(excludeTypes, t)
	}
	return excludeTypes
}

func readyWorkWispIssueFilter(filter types.WorkFilter) types.IssueFilter {
	pinnedFalse := false
	wispFilter := types.IssueFilter{
		Priority:       filter.Priority,
		Labels:         filter.Labels,
		LabelsAny:      filter.LabelsAny,
		ExcludeLabels:  filter.ExcludeLabels,
		Limit:          filter.Limit,
		MolType:        filter.MolType,
		WispType:       filter.WispType,
		Pinned:         &pinnedFalse,
		MetadataFields: filter.MetadataFields,
		HasMetadataKey: filter.HasMetadataKey,
	}
	if filter.Status != "" {
		s := filter.Status
		wispFilter.Status = &s
	} else {
		wispFilter.Statuses = []types.Status{types.StatusOpen, types.StatusInProgress}
	}
	if filter.Type != "" {
		t := types.IssueType(filter.Type)
		wispFilter.IssueType = &t
	} else {
		wispFilter.ExcludeTypes = readyWorkExcludeTypes(filter.ExcludeTypes)
	}
	if filter.Unassigned {
		wispFilter.NoAssignee = true
	} else if filter.Assignee != nil {
		wispFilter.Assignee = filter.Assignee
	}
	if filter.MoleculeID != "" {
		moleculeID := filter.MoleculeID
		wispFilter.ParentID = &moleculeID
	}
	if !filter.IncludeEphemeral {
		ephFalse := false
		wispFilter.Ephemeral = &ephFalse
	}
	return wispFilter
}

func filterReadyWispsInTx(ctx context.Context, tx *sql.Tx, filter types.WorkFilter, wisps []*types.Issue) ([]*types.Issue, error) {
	if len(wisps) == 0 {
		return wisps, nil
	}

	wispIDs := make([]string, 0, len(wisps))
	for _, wisp := range wisps {
		wispIDs = append(wispIDs, wisp.ID)
	}

	excluded := make(map[string]struct{})
	if filter.ParentID != nil {
		parentID := *filter.ParentID
		descendantIDs, err := GetDescendantIDsInTx(ctx, tx, parentID, 0)
		if err != nil {
			return nil, fmt.Errorf("get wisp parent descendants: %w", err)
		}
		descendantSet := make(map[string]struct{}, len(descendantIDs))
		for _, id := range descendantIDs {
			descendantSet[id] = struct{}{}
		}
		parentedSet, err := getParentedIDSetInTx(ctx, tx, wispIDs)
		if err != nil {
			return nil, err
		}
		for _, wisp := range wisps {
			if _, ok := descendantSet[wisp.ID]; ok {
				continue
			}
			if strings.HasPrefix(wisp.ID, parentID+".") {
				if _, hasParent := parentedSet[wisp.ID]; !hasParent {
					continue
				}
			}
			excluded[wisp.ID] = struct{}{}
		}
	}

	if !filter.IncludeDeferred {
		now := time.Now().UTC()
		for _, wisp := range wisps {
			if wisp.DeferUntil != nil && wisp.DeferUntil.After(now) {
				excluded[wisp.ID] = struct{}{}
			}
		}
		deferredChildIDs, err := getChildrenOfDeferredParentsInTx(ctx, tx, wispIDs)
		if err != nil {
			return nil, fmt.Errorf("get ready work: compute deferred parent children: %w", err)
		}
		for id := range deferredChildIDs {
			excluded[id] = struct{}{}
		}
	}

	for start := 0; start < len(wispIDs); start += queryBatchSize {
		end := start + queryBatchSize
		if end > len(wispIDs) {
			end = len(wispIDs)
		}
		placeholders, args := buildSQLInClause(wispIDs[start:end])
		//nolint:gosec // G201: only IN-clause placeholders are formatted in.
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
			SELECT id FROM wisps WHERE id IN (%s) AND is_blocked = 1
		`, placeholders), args...)
		if err != nil {
			return nil, fmt.Errorf("get ready work: filter blocked wisps: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan blocked wisp: %w", err)
			}
			excluded[id] = struct{}{}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("blocked wisp rows: %w", err)
		}
	}

	ready := wisps[:0]
	for _, wisp := range wisps {
		if wisp.Pinned {
			continue
		}
		if _, skip := excluded[wisp.ID]; skip {
			continue
		}
		ready = append(ready, wisp)
	}
	return ready, nil
}

func sortReadyIssues(issues []*types.Issue, policy types.SortPolicy) {
	recentCutoff := time.Now().UTC().Add(-48 * time.Hour)
	sort.SliceStable(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		switch policy {
		case types.SortPolicyOldest:
			return issueCreatedBefore(a, b)
		case types.SortPolicyPriority:
			return issuePriorityBefore(a, b)
		case types.SortPolicyHybrid, "":
			aRecent := !a.CreatedAt.Before(recentCutoff)
			bRecent := !b.CreatedAt.Before(recentCutoff)
			if aRecent != bRecent {
				return aRecent
			}
			if aRecent && a.Priority != b.Priority {
				return a.Priority < b.Priority
			}
			return issueCreatedBefore(a, b)
		default:
			return issuePriorityBefore(a, b)
		}
	})
}

func issuePriorityBefore(a, b *types.Issue) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return a.ID < b.ID
}

func issueCreatedBefore(a, b *types.Issue) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.ID < b.ID
}

func queryReadyWorkCandidatesInTx(ctx context.Context, tx *sql.Tx, filter types.WorkFilter, tables FilterTables) ([]string, error) {
	where, args, err := buildReadyWorkPredicates(ctx, tx, filter, tables)
	if err != nil {
		return nil, err
	}
	order := buildReadyWorkOrder(filter.SortPolicy)
	return queryIssueCandidatesInTx(ctx, tx, tables, where, order.sql, append(args, order.args...), filter.Limit, !filter.IncludeDeferred)
}

// queryIssueCandidatesInTx selects IDs in result order. Deferred-parent
// exclusion must run before the limit, so keep paging when it rejects a row.
//
//nolint:gosec // G201: fragments are fixed names, placeholders and integer limits.
func queryIssueCandidatesInTx(ctx context.Context, tx *sql.Tx, tables FilterTables, whereSQL, orderBySQL string, args []any, limit int, excludeDeferredParents bool) ([]string, error) {
	query := fmt.Sprintf("SELECT i.id FROM %s i %s %s", tables.Main, whereSQL, orderBySQL)
	if !excludeDeferredParents {
		if limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", limit)
		}
		return queryReadyIssueIDPage(ctx, tx, query, args)
	}

	pageSize := readyWorkPageSize(limit)
	var out []string
	for offset := 0; ; offset += pageSize {
		pageQuery := query
		if pageSize > 0 {
			pageQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", pageSize, offset)
		}
		page, err := queryReadyIssueIDPage(ctx, tx, pageQuery, args)
		if err != nil {
			return nil, err
		}
		deferred, err := getChildrenOfDeferredParentsInTx(ctx, tx, page)
		if err != nil {
			return nil, fmt.Errorf("get ready work: compute deferred parent children: %w", err)
		}
		for _, id := range page {
			if _, skip := deferred[id]; !skip {
				out = append(out, id)
				if limit > 0 && len(out) == limit {
					return out, nil
				}
			}
		}
		if pageSize == 0 || len(page) < pageSize {
			return out, nil
		}
	}
}

func queryReadyIssueIDPage(ctx context.Context, tx *sql.Tx, query string, args []interface{}) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	var issueIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("get ready work: scan id: %w", err)
		}
		issueIDs = append(issueIDs, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get ready work: rows: %w", err)
	}
	return issueIDs, nil
}

// getChildrenOfDeferredParentsInTx checks only the supplied candidate IDs,
// preserving both dependency sources and both parent target types.
func getChildrenOfDeferredParentsInTx(ctx context.Context, tx *sql.Tx, candidateIDs []string) (map[string]struct{}, error) {
	deferred := make(map[string]struct{})
	for batch := range slices.Chunk(candidateIDs, queryBatchSize) {
		issueParents, wispParents := make(map[string][]string), make(map[string][]string)
		for _, depTable := range []string{"dependencies", "wisp_dependencies"} {
			if err := loadCandidateParentsInTx(ctx, tx, depTable, batch, issueParents, wispParents); err != nil {
				if depTable == "wisp_dependencies" && isTableNotExistError(err) {
					continue
				}
				return nil, fmt.Errorf("deferred parents: candidate edges from %s: %w", depTable, err)
			}
		}
		for _, source := range []struct {
			table    string
			children map[string][]string
		}{{"issues", issueParents}, {"wisps", wispParents}} {
			parentIDs := make([]string, 0, len(source.children))
			for parent := range source.children {
				parentIDs = append(parentIDs, parent)
			}
			for parents := range slices.Chunk(parentIDs, queryBatchSize) {
				query, args := futureDeferredParentsQuery(source.table, parents)
				ids, err := queryReadyIssueIDPage(ctx, tx, query, args)
				if err != nil {
					if source.table == "wisps" && isTableNotExistError(err) {
						break
					}
					return nil, fmt.Errorf("deferred parents: lookup in %s: %w", source.table, err)
				}
				for _, parent := range ids {
					for _, child := range source.children[parent] {
						deferred[child] = struct{}{}
					}
				}
			}
		}
	}
	return deferred, nil
}

func loadCandidateParentsInTx(ctx context.Context, tx *sql.Tx, table string, ids []string, issueParents, wispParents map[string][]string) error {
	query, args := candidateParentEdgesQuery(table, ids)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var child string
		var issueParent, wispParent sql.NullString
		if err := rows.Scan(&child, &issueParent, &wispParent); err != nil {
			return err
		}
		if issueParent.Valid {
			issueParents[issueParent.String] = append(issueParents[issueParent.String], child)
		}
		if wispParent.Valid {
			wispParents[wispParent.String] = append(wispParents[wispParent.String], child)
		}
	}
	return rows.Err()
}

//nolint:gosec // G201: names are fixed and IDs are bound.
func candidateParentEdgesQuery(table string, ids []string) (string, []any) {
	placeholders, args := buildSQLInClause(ids)
	return fmt.Sprintf(`SELECT issue_id, depends_on_issue_id, depends_on_wisp_id FROM %s
		WHERE issue_id IN (%s) AND type = 'parent-child'`, table, placeholders), args
}

// Keep the parent lookup separate so Dolt cannot reorder a join into an
// unindexed scan of future-deferred parents.
//
//nolint:gosec // G201: names are fixed and IDs are bound.
func futureDeferredParentsQuery(table string, ids []string) (string, []any) {
	placeholders, args := buildSQLInClause(ids)
	return fmt.Sprintf(`SELECT id FROM %s WHERE id IN (%s) AND defer_until > UTC_TIMESTAMP()`, table, placeholders), args
}

//nolint:gosec // G201: depTable is hardcoded to "dependencies" or "wisp_dependencies"
func getParentedIDSetInTx(ctx context.Context, tx *sql.Tx, issueIDs []string) (map[string]struct{}, error) {
	parented := make(map[string]struct{})
	if len(issueIDs) == 0 {
		return parented, nil
	}
	for _, depTable := range []string{"dependencies", "wisp_dependencies"} {
		for start := 0; start < len(issueIDs); start += queryBatchSize {
			end := start + queryBatchSize
			if end > len(issueIDs) {
				end = len(issueIDs)
			}
			placeholders, args := buildSQLInClause(issueIDs[start:end])
			query := fmt.Sprintf(`
				SELECT issue_id FROM %s
				WHERE type = 'parent-child' AND issue_id IN (%s)
			`, depTable, placeholders)
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				if depTable == "wisp_dependencies" && isTableNotExistError(err) {
					break
				}
				return nil, fmt.Errorf("get parented IDs from %s: %w", depTable, err)
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					_ = rows.Close()
					return nil, fmt.Errorf("get parented IDs: scan: %w", err)
				}
				parented[id] = struct{}{}
			}
			_ = rows.Close()
			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("get parented IDs: rows from %s: %w", depTable, err)
			}
		}
	}
	return parented, nil
}

// buildSQLInClause builds a parameterized IN clause from a slice of IDs.
func buildSQLInClause(ids []string) (string, []interface{}) {
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return strings.Join(placeholders, ","), args
}
