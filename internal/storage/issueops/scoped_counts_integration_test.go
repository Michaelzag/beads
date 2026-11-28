package issueops

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage/schema"
	"github.com/steveyegge/beads/internal/types"
)

// BEADS_TEST_SCOPED_COUNTS_DSN must name an isolated Dolt server on which the
// account can create databases. The test creates and drops its own database.
func TestScopedIssueCountsOnDolt(t *testing.T) {
	dsn := os.Getenv("BEADS_TEST_SCOPED_COUNTS_DSN")
	if dsn == "" {
		t.Skip("set BEADS_TEST_SCOPED_COUNTS_DSN to an isolated Dolt server")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.MultiStatements = true
	cfg.InterpolateParams = true
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx := context.Background()
	database := fmt.Sprintf("bd_scoped_counts_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+database); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = database
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatal(err)
	}

	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	const parent = "hyd-root"
	var childIDs []string
	exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,defer_until) VALUES
		('hyd-root','parent','','','','',NULL), ('hyd-hold','deferred','','','','','2099-01-01'),
		('hyd-linked','linked','','','','',NULL)`)
	for i := 1; i <= 28; i++ {
		id := fmt.Sprintf("hyd-root.%02d", i)
		childIDs = append(childIDs, id)
		exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES (?,?,'','','','')`, id, id)
		exec(`INSERT INTO dependencies (id,issue_id,type,depends_on_issue_id,created_by) VALUES (?,?, 'parent-child', ?, 'fixture')`, "root-"+id, id, parent)
		exec(`INSERT INTO labels (issue_id,label) VALUES (?, 'fixture')`, id)
		exec(`INSERT INTO comments (issue_id,author,text) VALUES (?, 'fixture', 'comment')`, id)
	}
	exec(`INSERT INTO dependencies (id,issue_id,type,depends_on_issue_id,created_by) VALUES
		('z-hold','hyd-root.01','parent-child','hyd-hold','fixture'),
		('a-block','hyd-root.02','blocks','hyd-linked','fixture'),
		('z-reverse','hyd-linked','blocks','hyd-root.02','fixture')`)
	// Explicitly use the production query builders in the scale check. TREE
	// gives bounded index ranges and join keys; Dolt does not report runtime
	// rows examined for these queries.
	checkPlans := func(tx *sql.Tx) []int {
		t.Helper()
		var accesses []int
		check := func(query string, args []any) {
			t.Helper()
			rows, err := tx.QueryContext(ctx, "EXPLAIN FORMAT=TREE "+query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var lines []string
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatal(err)
				}
				lines = append(lines, line)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			plan := strings.Join(lines, "\n")
			if strings.Contains(plan, "─ Table\n") || strings.Contains(plan, "TableScan") || !strings.Contains(plan, "IndexedTableAccess(") {
				t.Fatalf("expected only indexed table reads, got:\n%s", plan)
			}
			accesses = append(accesses, strings.Count(plan, "IndexedTableAccess("))
		}
		for _, tables := range []FilterTables{IssuesFilterTables, WispsFilterTables} {
			q, args := issueCountsQuery(tables, childIDs, true, false)
			check(q, args)
		}
		for _, tables := range []FilterTables{IssuesFilterTables, WispsFilterTables} {
			q, args := candidateParentEdgesQuery(tables.Dependencies, childIDs)
			check(q, args)
			q, args = futureDeferredParentsQuery(tables.Main, []string{parent, "hyd-hold"})
			check(q, args)
		}
		return accesses
	}
	read := func() (listJSON, readyJSON string, plans []int) {
		t.Helper()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		parentID := parent
		list, err := SearchIssuesWithCountsInTx(ctx, tx, "", types.IssueFilter{ParentID: &parentID})
		if err != nil {
			t.Fatal(err)
		}
		ready, err := GetReadyWorkWithCountsInTx(ctx, tx, types.WorkFilter{ParentID: &parentID, SortPolicy: types.SortPolicyOldest, Limit: 27})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 28 || len(ready) != 27 || ready[0].ID != "hyd-root.02" {
			t.Fatalf("got %d list rows, %d ready rows; want 28 list, 27 ready starting at .02", len(list), len(ready))
		}
		item := ready[0]
		if len(item.Labels) != 1 || item.CommentCount != 1 || item.DependencyCount != 1 || item.DependentCount != 1 || item.Parent == nil || *item.Parent != parent || len(item.Dependencies) != 2 || item.Dependencies[0].DependsOnID != "hyd-linked" {
			t.Fatalf("wrong hydration for %s: %+v, deps %v", item.ID, item, item.Dependencies)
		}
		legacy := legacyCountsForFixture(t, ctx, tx)
		// All fixture rows share the same priority. Ready differs from list
		// only by the direct child of the deferred alternate parent.
		legacyReady := slices.DeleteFunc(slices.Clone(legacy), func(item *types.IssueWithCounts) bool {
			return item.ID == "hyd-root.01"
		})
		encode := func(value []*types.IssueWithCounts) string {
			b, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			return string(b)
		}
		sortIssuesWithCountsByPolicy(legacyReady, types.SortPolicyOldest)
		if encode(list) != encode(legacy) || encode(ready) != encode(legacyReady) {
			t.Fatal("scoped list or ready JSON differs from unscoped aggregation")
		}
		return encode(list), encode(ready), checkPlans(tx)
	}
	listBefore, readyBefore, plansBefore := read()
	for start := 0; start < 5000; start += 100 {
		var issues, labels, comments, deps []string
		var args []any
		for i := start; i < start+100; i++ {
			id := fmt.Sprintf("hyd-noise-%04d", i)
			args = append(args, id)
			issues = append(issues, "(?,'noise','','','','')")
			labels = append(labels, "(?,'noise')")
			comments = append(comments, "(?,'fixture','noise')")
			kind := "parent-child"
			if i%2 != 0 {
				kind = "blocks"
			}
			deps = append(deps, "(?,'hyd-hold','"+kind+"','fixture')")
		}
		exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES `+strings.Join(issues, ","), args...)
		exec(`INSERT INTO labels (issue_id,label) VALUES `+strings.Join(labels, ","), args...)
		exec(`INSERT INTO comments (issue_id,author,text) VALUES `+strings.Join(comments, ","), args...)
		exec(`INSERT INTO dependencies (issue_id,depends_on_issue_id,type,created_by) VALUES `+strings.Join(deps, ","), args...)
	}
	listAfter, readyAfter, plansAfter := read()
	if listBefore != listAfter || readyBefore != readyAfter {
		t.Fatal("unrelated rows changed scoped counts JSON")
	}
	if !slices.Equal(plansBefore, plansAfter) {
		t.Fatalf("indexed accesses changed with unrelated rows: %v -> %v", plansBefore, plansAfter)
	}
	t.Logf("after adding 5000 unrelated issues: unchanged list/ready JSON, indexed accesses before %v after %v", plansBefore, plansAfter)
}

// legacyCountsForFixture is the pre-scoping aggregation, restricted to the
// fixture only at the outer issue join. Keep the inner scans unscoped.
func legacyCountsForFixture(t *testing.T, ctx context.Context, tx *sql.Tx) []*types.IssueWithCounts {
	t.Helper()
	sqlText := fmt.Sprintf(`
		SELECT %s, l.labels_json, COALESCE(dc.cnt,0), COALESCE(rc.cnt,0),
			COALESCE(cc.cnt,0), pc.parent_id, d.deps_json
		FROM issues i
		LEFT JOIN (SELECT issue_id, JSON_ARRAYAGG(label) AS labels_json FROM labels GROUP BY issue_id) l ON l.issue_id = i.id
		LEFT JOIN (SELECT issue_id, COUNT(*) AS cnt FROM dependencies WHERE type = 'blocks' GROUP BY issue_id) dc ON dc.issue_id = i.id
		LEFT JOIN (
			SELECT dep_id, COUNT(*) AS cnt FROM (
				SELECT COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external) AS dep_id
				FROM dependencies WHERE type = 'blocks'
				UNION ALL
				SELECT COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external) AS dep_id
				FROM wisp_dependencies WHERE type = 'blocks'
			) all_blockers GROUP BY dep_id
		) rc ON rc.dep_id = i.id
		LEFT JOIN (SELECT issue_id, COUNT(*) AS cnt FROM comments GROUP BY issue_id) cc ON cc.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id, MIN(COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external)) AS parent_id
			FROM dependencies WHERE type = 'parent-child' GROUP BY issue_id
		) pc ON pc.issue_id = i.id
		LEFT JOIN (SELECT issue_id, JSON_ARRAYAGG(%s) AS deps_json FROM dependencies GROUP BY issue_id) d ON d.issue_id = i.id
		WHERE i.id LIKE 'hyd-root.%%'
		ORDER BY i.priority ASC, i.created_at DESC, i.id ASC
	`, readyWorkIssueColumns, strings.Replace(readyWorkDepJSONObject, "'row_id', id,", "", 1))
	items, err := scanIssueCountsInTx(ctx, tx, IssuesFilterTables, sqlText, nil)
	if err != nil {
		t.Fatal(err)
	}
	return items
}
