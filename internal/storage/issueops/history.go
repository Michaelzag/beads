package issueops

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// Bound both the ancestry work and the keyed diff work in each statement.
// A sparse issue may require many windows, but never one unbounded sort.
const historyWindowSize = 256

type historyCommit struct {
	hash, committer  string
	date, authorDate time.Time
	height           uint64
	parents          []string
}

type historyEdge struct {
	Pos    int
	Child  string
	Parent string
}

// HistoryInTx returns committed revisions of an issue in ancestry order,
// newest first. A nonpositive limit returns all revisions.
func HistoryInTx(ctx context.Context, tx *sql.Tx, issueID string, limit int) ([]*storage.HistoryEntry, error) {
	var entries []*storage.HistoryEntry
	var database, head string
	if err := tx.QueryRowContext(ctx, "SELECT DATABASE(), HASHOF('HEAD')").Scan(&database, &head); err != nil {
		return nil, fmt.Errorf("resolve issue history HEAD: %w", err)
	}
	base, _, _ := strings.Cut(database, "/")
	// Pin the diff's target schema independently of uncommitted changes.
	qualified := "`" + strings.ReplaceAll(base+"/"+head, "`", "``") + "`"
	indexed, err := hasIndexedHistoryDiff(ctx, tx, qualified)
	if err != nil {
		return nil, fmt.Errorf("check history issue index: %w", err)
	}
	if !indexed {
		qualified = "" // Nonstandard PK order: compare indexed snapshots directly.
	}
	frontier := map[string]struct{}{head: {}}
	for len(frontier) > 0 {
		page, err := historyPage(ctx, tx, frontier)
		if err != nil {
			return nil, fmt.Errorf("read issue history commits: %w", err)
		}
		var edges []historyEdge
		for pos, commit := range page {
			delete(frontier, commit.hash)
			for _, parent := range commit.parents {
				frontier[parent] = struct{}{}
				edges = append(edges, historyEdge{pos, commit.hash, parent})
			}
		}
		remaining := len(page)
		if limit > 0 {
			remaining = min(remaining, limit-len(entries))
		}
		revisions := make(map[int]bool)
		for batch := range slices.Chunk(edges, historyWindowSize) {
			// A merge may contribute to both batches. Let SQL return one
			// extra row if the boundary splits the last seen commit.
			batchLimit := remaining - len(revisions)
			if revisions[batch[0].Pos] {
				batchLimit++
			}
			items, err := historyDiffs(ctx, tx, qualified, issueID, batch, batchLimit)
			if err != nil {
				return nil, fmt.Errorf("read issue history revisions: %w", err)
			}
			maps.Copy(revisions, items)
			if len(revisions) >= remaining {
				break
			}
		}
		for pos, commit := range page {
			if revisions[pos] {
				snapshot, err := historySnapshot(ctx, tx, issueID, commit.hash)
				if err != nil {
					return nil, fmt.Errorf("read issue snapshot: %w", err)
				}
				if snapshot == nil {
					return nil, fmt.Errorf("missing snapshot for %q at %s", issueID, commit.hash)
				}
				issue, err := decodeHistoryIssue(snapshot)
				if err != nil {
					return nil, fmt.Errorf("decode issue snapshot: %w", err)
				}
				entries = append(entries, &storage.HistoryEntry{
					CommitHash: commit.hash,
					Committer:  commit.committer,
					CommitDate: commit.date,
					Issue:      issue,
				})
				if limit > 0 && len(entries) == limit {
					return entries, nil
				}
			}
		}
	}
	return entries, nil
}

// DOLT_LOG (the table function) traverses lazily by descending commit height.
// Each page resumes from the parents not yet emitted, not an OFFSET or a
// date cursor. An ancestor must have strictly smaller height than its child.
func historyPage(ctx context.Context, tx *sql.Tx, frontier map[string]struct{}) ([]historyCommit, error) {
	roots := slices.Sorted(maps.Keys(frontier))
	var page []historyCommit
	for batch := range slices.Chunk(roots, historyWindowSize) {
		args := make([]any, 0, len(batch))
		for _, root := range batch {
			args = append(args, root)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT commit_hash, committer, date, author_date, commit_order, parents
			FROM DOLT_LOG(`+strings.Repeat("?, ", len(batch))+`'--parents', '--decorate=no')
			LIMIT ?`, append(args, historyWindowSize)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var commit historyCommit
			var parents string
			if err := rows.Scan(&commit.hash, &commit.committer, &commit.date, &commit.authorDate, &commit.height, &parents); err != nil {
				rows.Close()
				return nil, err
			}
			if parents != "" {
				commit.parents = strings.Split(parents, ", ")
			}
			page = append(page, commit)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		// Only very wide frontiers need more than one query. Merge their
		// highest commits at each step, retaining native order on ties.
		if len(roots) > historyWindowSize {
			slices.SortStableFunc(page, func(a, b historyCommit) int {
				switch {
				case a.height > b.height:
					return -1
				case a.height < b.height:
					return 1
				default:
					return b.authorDate.Compare(a.authorDate)
				}
			})
			seen := make(map[string]bool)
			page = slices.DeleteFunc(page, func(commit historyCommit) bool {
				duplicate := seen[commit.hash]
				seen[commit.hash] = true
				return duplicate
			})
			page = page[:min(len(page), historyWindowSize)]
		}
	}
	if len(page) == 0 {
		return nil, fmt.Errorf("no commits returned for nonempty ancestry frontier")
	}
	return page, nil
}

func hasIndexedHistoryDiff(ctx context.Context, tx *sql.Tx, qualified string) (bool, error) {
	//nolint:gosec // qualified is an escaped SQL identifier.
	rows, err := tx.QueryContext(ctx, "SHOW INDEX FROM "+qualified+".issues WHERE Key_name = 'PRIMARY' AND Seq_in_index = 1 AND Column_name = 'id'")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

func historyDiffs(ctx context.Context, tx *sql.Tx, qualified, issueID string, edges []historyEdge, limit int) (map[int]bool, error) {
	if qualified == "" {
		return historySnapshotDiffs(ctx, tx, issueID, edges, limit)
	}
	encoded, err := json.Marshal(edges)
	if err != nil {
		return nil, err
	}
	// The composite Dolt index probes a single ID on each exact parent edge.
	// Keep one ordinal per commit before applying the limit; the snapshot
	// is later read from its source schema, not cast to the HEAD schema.
	revisions, err := readHistoryDiffs(ctx, tx, qualified, string(encoded), issueID, limit)
	if err != nil {
		// Even compatible historical endpoints can be unconvertible to
		// HEAD's target key when a primary key lost columns.
		if strings.Contains(err.Error(), "could not map primary key column") {
			return historySnapshotDiffs(ctx, tx, issueID, edges, limit)
		}
		return nil, err
	}
	warnings, err := tx.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return nil, err
	}
	var incompatible []historyEdge
	for warnings.Next() {
		var level, message, parent, child string
		var code int
		if err := warnings.Scan(&level, &code, &message); err != nil {
			warnings.Close()
			return nil, err
		}
		const warning = "cannot render full diff between commits %s and %s due to primary key set change"
		if n, err := fmt.Sscanf(message, warning, &parent, &child); n == 2 && err == nil {
			for _, edge := range edges {
				if edge.Child == child && edge.Parent == parent {
					incompatible = append(incompatible, edge)
					break
				}
			}
		}
	}
	warnings.Close()
	if err := warnings.Err(); err != nil {
		return nil, err
	}
	// The native diff cannot compare incompatible primary keys. Probe the
	// two exact snapshots instead; a dropped table has no after-image.
	for _, edge := range incompatible {
		if revisions[edge.Pos] {
			continue
		}
		changed, err := historyAcrossSchemaChange(ctx, tx, issueID, edge)
		if err != nil {
			return nil, err
		}
		if changed {
			revisions[edge.Pos] = true
		}
	}
	return revisions, nil
}

func historySnapshotDiffs(ctx context.Context, tx *sql.Tx, issueID string, edges []historyEdge, limit int) (map[int]bool, error) {
	revisions := make(map[int]bool)
	for _, edge := range edges {
		if revisions[edge.Pos] {
			continue
		}
		changed, err := historyAcrossSchemaChange(ctx, tx, issueID, edge)
		if err != nil {
			return nil, err
		}
		if changed {
			revisions[edge.Pos] = true
			if len(revisions) == limit {
				break
			}
		}
	}
	return revisions, nil
}

func readHistoryDiffs(ctx context.Context, tx *sql.Tx, qualified, edgesJSON, issueID string, limit int) (map[int]bool, error) {
	//nolint:gosec // qualified is an escaped SQL identifier.
	rows, err := tx.QueryContext(ctx, `
		SELECT /*+ LOOKUP_JOIN(e,d) */ DISTINCT e.pos
		FROM JSON_TABLE(?, '$[*]' COLUMNS (
			pos INT PATH '$.Pos',
			child_hash VARCHAR(32) PATH '$.Child',
			parent_hash VARCHAR(32) PATH '$.Parent'
		)) e
		JOIN `+qualified+`.dolt_commit_diff_issues d ON d.to_commit = e.child_hash
			AND d.from_commit = e.parent_hash AND d.to_id = ?
		ORDER BY e.pos
		LIMIT ?`, edgesJSON, issueID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	revisions := make(map[int]bool)
	for rows.Next() {
		var pos int
		if err := rows.Scan(&pos); err != nil {
			return nil, err
		}
		revisions[pos] = true
	}
	return revisions, rows.Err()
}

func historyAcrossSchemaChange(ctx context.Context, tx *sql.Tx, issueID string, edge historyEdge) (bool, error) {
	after, err := historySnapshot(ctx, tx, issueID, edge.Child)
	if err != nil || after == nil {
		return false, err
	}
	before, err := historySnapshot(ctx, tx, issueID, edge.Parent)
	return err == nil && !maps.Equal(before, after), err
}

// Read all fields, not just the CLI projection: a commit that changes a field
// the CLI doesn't display is still a revision, even across a PK change.
func historySnapshot(ctx context.Context, tx *sql.Tx, issueID, ref string) (map[string]sql.NullString, error) {
	if err := ValidateRef(ref); err != nil {
		return nil, err
	}
	//nolint:gosec // ValidateRef ensures ref is safe to use as a literal.
	rows, err := tx.QueryContext(ctx, "SELECT * FROM issues AS OF '"+ref+"' WHERE id = ? LIMIT 1", issueID)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil || !rows.Next() {
		if err == nil {
			err = rows.Err()
		}
		return nil, err
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range cols {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	snapshot := make(map[string]sql.NullString, len(cols))
	for i, col := range cols {
		snapshot[col] = values[i]
	}
	return snapshot, rows.Err()
}

func decodeHistoryIssue(fields map[string]sql.NullString) (*types.Issue, error) {
	value := func(key string) string { return fields[key].String }
	issue := &types.Issue{
		ID: value("id"), Title: value("title"), Description: value("description"),
		Design: value("design"), AcceptanceCriteria: value("acceptance_criteria"), Notes: value("notes"),
		Status: types.Status(value("status")), IssueType: types.IssueType(value("issue_type")),
		Assignee: value("assignee"), Owner: value("owner"), CreatedBy: value("created_by"),
		CreatedAt: ParseTimeString(value("created_at")), UpdatedAt: ParseTimeString(value("updated_at")),
		CloseReason: value("close_reason"), Pinned: value("pinned") == "1", MolType: types.MolType(value("mol_type")),
	}
	var err error
	if issue.Priority, err = strconv.Atoi(value("priority")); err != nil {
		return nil, err
	}
	if fields["estimated_minutes"].Valid {
		mins, err := strconv.Atoi(value("estimated_minutes"))
		if err != nil {
			return nil, err
		}
		issue.EstimatedMinutes = &mins
	}
	if fields["closed_at"].Valid {
		closedAt := ParseTimeString(value("closed_at"))
		issue.ClosedAt = &closedAt
	}
	return issue, nil
}
