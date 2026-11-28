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
func newScopedCountsDolt(t *testing.T, ctx context.Context, collation string) *sql.Conn {
	t.Helper()
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
	t.Cleanup(func() { _ = admin.Close() })
	database := fmt.Sprintf("bd_scoped_counts_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database+" CHARACTER SET utf8mb4 COLLATE "+collation); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+database); err != nil {
			t.Error(err)
		}
	})
	cfg.DBName = database
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestScopedIssueCountsOnDolt(t *testing.T) {
	ctx := context.Background()
	conn := newScopedCountsDolt(t, ctx, "utf8mb4_unicode_ci")

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
		('z-hold','hyd-ROOT.01','parent-child','hyd-HOLD','fixture'),
		('a-block','hyd-root.02','blocks','hyd-linked','fixture'),
		('z-reverse','hyd-linked','blocks','hyd-root.02','fixture')`)
	// Explicitly use the production query builders in the scale check. TREE
	// gives bounded index ranges and join keys; Dolt does not report runtime
	// rows examined for these queries.
	checkPlans := func(tx *sql.Tx) []int {
		t.Helper()
		var accesses []int
		check := func(query string, args []any, deferred bool) {
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
			if deferred && (!strings.Contains(plan, "keys: i.id") || !strings.Contains(plan, "keys: dep.depends_on_")) {
				t.Fatalf("deferred parent must be looked up via candidate edge keys, got:\n%s", plan)
			}
			accesses = append(accesses, strings.Count(plan, "IndexedTableAccess("))
		}
		for _, tables := range []FilterTables{IssuesFilterTables, WispsFilterTables} {
			q, args := issueCountsQuery(tables, childIDs, true, false)
			check(q, args, false)
		}
		for _, tables := range []FilterTables{IssuesFilterTables, WispsFilterTables} {
			for _, deps := range []string{"dependencies", "wisp_dependencies"} {
				for _, parents := range []string{"issues", "wisps"} {
					q, args := deferredParentChildrenQuery(tables.Main, deps, parents, childIDs)
					check(q, args, true)
				}
			}
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

func seedReadyParentFixture(t *testing.T, ctx context.Context, conn *sql.Conn) (string, []string) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	const root = "ready-root"
	const unrelated = "ready-noise-root"
	exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES
		(?,?,'','','',''), (?,?,'','','',''),
		('ready-root.dotted','implicit issue','','','',''),
		('ready-root.moved','explicitly outside','','','',''),
		('ready-cross-plane','issue below wisp','','','',''),
		('ready-external','external target column','','','','')`, root, root, unrelated, unrelated)
	exec(`INSERT INTO wisps (id,title,description,design,acceptance_criteria,notes,ephemeral) VALUES
		('ready-wisp','explicit wisp','','','','',0),
		('ready-root.wisp','implicit wisp','','','','',0),
		('ready-root.other-wisp','explicitly outside','','','','',0)`)
	exec(`INSERT INTO wisp_dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES
		('ready-wisp','parent-child',?,'fixture'),
		('ready-root.other-wisp','parent-child',?,'fixture')`, root, unrelated)
	exec(`INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES
		('ready-root.moved','parent-child',?,'fixture')`, unrelated)
	exec(`INSERT INTO dependencies (issue_id,type,depends_on_wisp_id,created_by) VALUES
		('ready-cross-plane','parent-child','ready-wisp','fixture')`)
	exec(`INSERT INTO dependencies (issue_id,type,depends_on_external,created_by) VALUES
		('ready-external','parent-child',?,'fixture')`, root)
	want := []string{"ready-root.dotted", "ready-cross-plane", "ready-external", "ready-wisp", "ready-root.wisp"}
	tip := "READY-ROOT"
	for i := range 24 {
		child := fmt.Sprintf("ready-depth-%02d", i)
		exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES (?,?,'','','','')`, child, child)
		exec(`INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES (?, 'parent-child', ?, 'fixture')`, child, tip)
		tip = child
		want = append(want, child)
	}
	exec(`INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES (?, 'parent-child', ?, 'fixture')`, root, tip)
	for start := 0; start < 5000; start += 100 {
		var args []any
		for i := start; i < start+100; i++ {
			args = append(args, fmt.Sprintf("ready-noise-%04d", i))
		}
		exec(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?,'noise','','','',''),", len(args)), ","), args...)
		exec(`INSERT INTO dependencies (issue_id,depends_on_issue_id,type,created_by) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?,'ready-noise-root','parent-child','fixture'),", len(args)), ","), args...)
	}
	return root, want
}

func TestReadyParentDescendantsOnDolt(t *testing.T) {
	ctx := context.Background()
	conn := newScopedCountsDolt(t, ctx, "utf8mb4_unicode_ci")
	parent, want := seedReadyParentFixture(t, ctx, conn)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// The leaf is reachable directly and via the middle node. Only the
	// path through the middle node reaches the depth limit.
	if _, err := tx.ExecContext(ctx, `INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES
		('diamond-root','root','','','',''), ('diamond-mid','mid','','','',''), ('diamond-leaf','leaf','','','','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES
		('diamond-mid','parent-child','diamond-root','fixture'),
		('diamond-leaf','parent-child','diamond-root','fixture'),
		('diamond-leaf','parent-child','diamond-mid','fixture')`); err != nil {
		t.Fatal(err)
	}
	if _, err := GetDescendantIDsInTx(ctx, tx, "diamond-root", 2); err == nil || !strings.Contains(err.Error(), "reached max depth 2") {
		t.Fatalf("bounded diamond traversal: got %v, want a depth limit error", err)
	}

	filter := types.WorkFilter{ParentID: &parent, Limit: 100}
	got, err := GetReadyWorkWithCountsInTx(ctx, tx, filter)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, issue := range got {
		ids = append(ids, issue.ID)
	}
	slices.Sort(ids)
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Fatalf("ready IDs = %v, want %v", ids, want)
	}
	filter.Limit = 1
	issues, total, err := GetReadyWorkWithTotalInTx(ctx, tx, filter)
	if err != nil || len(issues) != 1 || total != len(want) {
		t.Fatalf("limited plain ready: %d issues, total %d, err %v; want 1 issue, total %d", len(issues), total, err, len(want))
	}
	wide := make([]string, queryBatchSize)
	for i := range wide {
		wide[i] = fmt.Sprintf("ready-noise-%04d", i)
	}
	for _, frontier := range [][]string{{parent}, {"ready-wisp", "ready-depth-00"}, wide} {
		for _, table := range []string{"dependencies", "wisp_dependencies"} {
			for _, target := range descendantTargetColumns {
				query, args := descendantChildrenQuery(table, target, frontier, parent)
				checkDescendantLookupPlan(t, ctx, tx, query, args, table, target, frontier)
			}
		}
	}
}

func checkDescendantLookupPlan(t *testing.T, ctx context.Context, tx *sql.Tx, query string, args []any, table, target string, frontier []string) {
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
	if !strings.Contains(plan, "IndexedTableAccess("+table+")") ||
		!strings.Contains(plan, "index: ["+table+".type,"+table+"."+target+"]") {
		t.Fatalf("expected indexed target lookup, got:\n%s", plan)
	}
	// Dolt wraps long range lists inside the tree formatter.
	ranges := strings.Join(strings.Fields(strings.ReplaceAll(plan, "│", "")), "")
	for _, id := range frontier {
		if !strings.Contains(ranges, "["+id+","+id+"]") {
			t.Fatalf("expected an index point range for %s, got:\n%s", id, plan)
		}
	}
	t.Logf("%s.%s: %d indexed target point ranges", table, target, len(frontier))
}

func TestReadyParentDistinctIDsOnDolt(t *testing.T) {
	ctx := context.Background()
	conn := newScopedCountsDolt(t, ctx, "utf8mb4_0900_bin")
	if _, err := conn.ExecContext(ctx, `INSERT INTO issues (id,title,description,design,acceptance_criteria,notes) VALUES
		('case-root','root','','','',''), ('case-A','a','','','',''), ('case-a','b','','','',''),
		('comma-root','root','','','',''), ('comma-x,comma-y','x','','','',''), ('comma-y','y','','','','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES
		('case-A','parent-child','case-root','fixture'), ('case-a','parent-child','case-A','fixture'),
		('comma-x,comma-y','parent-child','comma-root','fixture'), ('comma-y','parent-child','comma-x,comma-y','fixture')`); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, tc := range []struct {
		root string
		want []string
	}{
		{"case-root", []string{"case-A", "case-a"}},
		{"comma-root", []string{"comma-x,comma-y", "comma-y"}},
	} {
		got, err := GetDescendantIDsInTx(ctx, tx, tc.root, 0)
		slices.Sort(got)
		if err != nil || !slices.Equal(got, tc.want) {
			t.Fatalf("%q: got descendants %v, err %v; want %v", tc.root, got, err, tc.want)
		}
	}
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
