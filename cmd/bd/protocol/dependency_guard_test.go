package protocol

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/doltutil"
	"github.com/steveyegge/beads/internal/storage/domain"
	graphdb "github.com/steveyegge/beads/internal/storage/domain/db"
	"github.com/steveyegge/beads/internal/types"
)

func TestProtocol_DependencyGuard(t *testing.T) {
	if testDoltPort == 3307 {
		t.Fatalf("refusing to initialize a test database on port %d", testDoltPort)
	}
	newFixture := func(t *testing.T) (*workspace, *sql.DB) {
		t.Helper()
		w := newWorkspace(t, "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(testDoltPort))
		cfg, err := configfile.Load(filepath.Join(w.dir, ".beads"))
		if err != nil || cfg == nil || !cfg.IsDoltServerMode() || cfg.DoltDatabase == "" {
			t.Fatalf("cannot identify disposable workspace database: %v", err)
		}
		dsn := doltutil.ServerDSN{Host: "127.0.0.1", Port: testDoltPort, User: "root", Database: cfg.DoltDatabase}
		db, err := sql.Open("mysql", dsn.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return w, db
	}
	checkRefused := func(t *testing.T, w *workspace, db *sql.DB, id, stale string) {
		t.Helper()
		before := w.showJSON(id)
		current := guardOf(t, before)
		if current == stale {
			t.Errorf("edge mutation left %s's guard unchanged", id)
		}
		type snapshot struct {
			head   string
			events int
			dirty  int
		}
		read := func() snapshot {
			t.Helper()
			var s snapshot
			err := db.QueryRowContext(t.Context(), `SELECT HASHOF('HEAD'),
				(SELECT COUNT(*) FROM events WHERE issue_id = ?),
				(SELECT COUNT(*) FROM dolt_status)`, id).Scan(&s.head, &s.events, &s.dirty)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		state := read()
		out, stderr, code := w.runCAS("update", id, "--status", "in_progress", "--if-guard", stale, "--json")
		var got map[string]any
		err := json.Unmarshal([]byte(out), &got)
		if code != 3 || err != nil || got["outcome"] != "refused" || got["reason"] != "precondition_failed" ||
			got["field"] != "guard" || got["work_item_id"] != id || got["expected"] != stale || got["actual"] != current {
			t.Errorf("want stale guard refusal for %s, got exit %d: %s\n%s", id, code, out, stderr)
		}
		if after := w.showJSON(id); !reflect.DeepEqual(before, after) {
			t.Errorf("refused CAS changed issue: before=%v after=%v", before, after)
		}
		if after := read(); after != state {
			t.Errorf("refused CAS changed Dolt state: before=%+v after=%+v", state, after)
		}
	}
	assertEdge := func(t *testing.T, db *sql.DB, source, target string, kind types.DependencyType, want int) {
		t.Helper()
		var count int
		err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM dependencies
			WHERE issue_id = ? AND depends_on_issue_id = ? AND type = ?`, source, target, kind).Scan(&count)
		if err != nil || count != want {
			t.Fatalf("edge %s -> %s (%s): count=%d, want %d: %v", source, target, kind, count, want, err)
		}
	}

	t.Run("batch_delete", func(t *testing.T) {
		w, db := newFixture(t)
		parent := w.create("Deleted parent", "--type", "epic")
		child := w.create("Surviving child", "--parent", parent)
		related := w.create("Surviving related issue")
		w.run("dep", "add", related, parent, "--type", string(types.DepRelated))
		childGuard := guardOf(t, w.showJSON(child))
		relatedGuard := guardOf(t, w.showJSON(related))
		assertEdge(t, db, child, parent, types.DepParentChild, 1)
		assertEdge(t, db, related, parent, types.DepRelated, 1)

		w.run("delete", parent, "--force")

		assertEdge(t, db, child, parent, types.DepParentChild, 0)
		assertEdge(t, db, related, parent, types.DepRelated, 0)
		checkRefused(t, w, db, child, childGuard)
		checkRefused(t, w, db, related, relatedGuard)
	})

	t.Run("graph_create", func(t *testing.T) {
		w, db := newFixture(t)
		child := w.create("Existing child")
		parent := w.create("Existing parent", "--type", "epic")
		waiter := w.create("Existing waiter")
		w.run("dep", "add", waiter, parent, "--type", string(types.DepWaitsFor))
		childGuard := guardOf(t, w.showJSON(child))
		parentGuard := guardOf(t, w.showJSON(parent))
		waiterGuard := guardOf(t, w.showJSON(waiter))
		assertEdge(t, db, child, parent, types.DepParentChild, 0)

		// Exercise the proxied CLI's graph use case and SQL repositories in
		// a real transaction against the same database used by show/update.
		ctx := t.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		graph := domain.NewIssueUseCase(graphdb.NewIssueSQLRepository(tx), graphdb.NewDependencySQLRepository(tx),
			graphdb.NewLabelSQLRepository(tx), graphdb.NewChildCounterSQLRepository(tx),
			graphdb.NewCommentSQLRepository(tx), graphdb.NewConfigSQLRepository(tx))
		_, err = graph.ApplyIssueGraph(ctx, domain.GraphPlan{
			Nodes: []domain.GraphNode{{Key: "extra", Issue: &types.Issue{Title: "New graph node", Priority: 2, IssueType: types.TypeTask}}},
			Edges: []domain.GraphEdge{{FromID: child, ToID: parent, Type: types.DepParentChild}},
		}, "test")
		if err != nil {
			t.Fatalf("apply graph: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit graph: %v", err)
		}

		assertEdge(t, db, child, parent, types.DepParentChild, 1)
		checkRefused(t, w, db, child, childGuard)
		checkRefused(t, w, db, parent, parentGuard)
		checkRefused(t, w, db, waiter, waiterGuard)
	})
}
