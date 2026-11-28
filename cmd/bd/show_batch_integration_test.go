package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage"
)

// BEADS_TEST_SHOW_BATCH_DSN must point to a dedicated, idle Dolt server.
// BEADS_TEST_SHOW_BEFORE and BEADS_TEST_SHOW_AFTER are absolute binary paths.
func TestShowBatchOnDolt(t *testing.T) {
	dsn, before, after := os.Getenv("BEADS_TEST_SHOW_BATCH_DSN"), os.Getenv("BEADS_TEST_SHOW_BEFORE"), os.Getenv("BEADS_TEST_SHOW_AFTER")
	if dsn == "" || before == "" || after == "" {
		t.Skip("set BEADS_TEST_SHOW_BATCH_DSN, BEADS_TEST_SHOW_BEFORE and BEADS_TEST_SHOW_AFTER")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	host, port, ok := strings.Cut(cfg.Addr, ":")
	if cfg.Net != "tcp" || !ok {
		t.Fatal("expected a TCP DSN (IPv4 or hostname)")
	}
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	cfg.DBName = fmt.Sprintf("bd_show_batch_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + cfg.DBName + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + cfg.DBName); err != nil {
			t.Error(err)
		}
	}()

	dir := t.TempDir()
	run := func(bin string, args ...string) (string, string, int, time.Duration) {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(),
			"BEADS_DOLT_SERVER_HOST="+host, "BEADS_DOLT_SERVER_PORT="+port,
			"BEADS_DOLT_PORT="+port, "BEADS_DOLT_SERVER_DATABASE="+cfg.DBName,
			"BEADS_DOLT_SERVER_USER="+cfg.User, "BEADS_DOLT_PASSWORD="+cfg.Passwd,
			"BEADS_DOLT_SHARED_SERVER=", "BEADS_DOLT_SOCKET=", "BEADS_DB=", "BD_DB=",
		)
		cmd.Dir = dir
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		start := time.Now()
		err := cmd.Run()
		elapsed := time.Since(start)
		code := 0
		if failure, ok := err.(*exec.ExitError); ok {
			code = failure.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return out.String(), stderr.String(), code, elapsed
	}
	if out, stderr, code, _ := run(after, "init", "--prefix", "batchshow", "--database", cfg.DBName,
		"--server", "--external", "--server-host", host, "--server-port", port, "--server-user", cfg.User,
		"--skip-agents", "--skip-hooks", "--non-interactive", "--quiet"); code != 0 {
		t.Fatalf("bd init: exit %d: %s\n%s", code, out, stderr)
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seed := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	const parent, wisp = "batchshow-parent", "batchshow-wisp"
	insertIssue := func(id, issueType string) {
		t.Helper()
		seed(`INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,issue_type)
			VALUES (?,?,?,'','','',?)`, id, id, strings.Repeat("description ", 64), issueType)
	}
	insertIssue(parent, "epic")
	var ids = []string{parent}
	for i := 1; i <= 200; i++ {
		id := fmt.Sprintf("batchshow-parent.%03d", i)
		insertIssue(id, "task")
		ids = append(ids, id)
		seed(`INSERT INTO dependencies (issue_id,type,depends_on_issue_id,created_by) VALUES (?,'parent-child',?,'fixture')`, id, parent)
		seed(`INSERT INTO labels (issue_id,label) VALUES (?, 'trailing'), (?, 'before')`, id, strings.ToUpper(id))
		seed(`INSERT INTO comments (issue_id,author,text) VALUES (?, 'fixture', 'comment')`, strings.ToUpper(id))
	}
	insertIssue("batchshow-café", "task")
	seed(`INSERT INTO wisps (id,title,ephemeral) VALUES (?, 'fixture wisp', 1)`, wisp)
	seed(`INSERT INTO wisp_labels (issue_id,label) VALUES (?, 'wisp-label')`, strings.ToUpper(wisp))
	seed(`INSERT INTO wisp_comments (issue_id,author,text) VALUES (?, 'fixture', 'ignored by legacy count')`, wisp)
	seed(`INSERT INTO dependencies (id,issue_id,type,depends_on_wisp_id,created_by) VALUES ('000-extra',?, 'blocks',?, 'fixture')`, strings.ToUpper(ids[1]), wisp)
	// Cross-prefix dependencies can also hydrate targets from this store.
	insertIssue("other-local", "task")
	seed(`INSERT INTO dependencies (id,issue_id,type,depends_on_external,created_by) VALUES ('000-cross',?, 'relates-to','other-local','fixture')`, ids[1])
	seed(`INSERT INTO dependencies (id,issue_id,type,depends_on_issue_id,created_by) VALUES ('zzz-block',?, 'blocks',?, 'fixture')`, ids[1], ids[2])
	seed(`INSERT INTO wisp_dependencies (id,issue_id,type,depends_on_issue_id) VALUES
		('00', ?, 'blocks', ?), ('01', ?, 'parent-child', ?)`, wisp, ids[2], strings.ToUpper(wisp), parent)
	seed(`INSERT INTO dependencies (issue_id,type,depends_on_external,created_by) VALUES (?, 'relates-to', ?, 'fixture')`, ids[2], strings.ToUpper(parent))
	seed("INSERT INTO metadata (`key`,value) VALUES (?, 'example-guard'), (?, 'wisp-guard')", storage.CloseGuardKey(ids[1]), storage.CloseGuardKey(wisp))

	compare := func(t *testing.T, args []string, wantExit int) (time.Duration, time.Duration) {
		t.Helper()
		cli := append([]string{"show", "--readonly", "--json"}, args...)
		oldOut, oldErr, oldCode, oldTime := run(before, cli...)
		newOut, newErr, newCode, newTime := run(after, cli...)
		if oldCode != wantExit || newCode != oldCode || newErr != oldErr {
			t.Fatalf("old exit %d and stderr %q; new exit %d and stderr %q; want exit %d", oldCode, oldErr, newCode, newErr, wantExit)
		}
		oldOut = normalizeShowDependencyOrder(t, oldOut)
		if newOut != oldOut {
			i := 0
			for i < min(len(oldOut), len(newOut)) && oldOut[i] == newOut[i] {
				i++
			}
			t.Fatalf("JSON differs at byte %d: old %q, new %q", i, oldOut[i:min(i+200, len(oldOut))], newOut[i:min(i+200, len(newOut))])
		}
		return oldTime, newTime
	}
	t.Run("deterministic-dependency-order", func(t *testing.T) {
		type edge struct {
			Type string `json:"dependency_type"`
			ID   string `json:"id"`
		}
		want := []edge{{"blocks", ids[2]}, {"blocks", wisp}, {"parent-child", parent}, {"relates-to", "other-local"}}
		for _, args := range [][]string{{ids[1]}, {ids[1], parent}} {
			out, stderr, code, _ := run(after, append([]string{"show", "--readonly", "--json"}, args...)...)
			if code != 0 {
				t.Fatalf("show %v: exit %d: %s", args, code, stderr)
			}
			var issues []struct {
				Dependencies []edge `json:"dependencies"`
			}
			if err := json.Unmarshal([]byte(out), &issues); err != nil {
				t.Fatal(err)
			}
			if len(issues) != len(args) || !slices.Equal(issues[0].Dependencies, want) {
				t.Fatalf("show %v dependency order: got %+v, want %+v", args, issues, want)
			}
		}
	})
	t.Run("mixed-and-aliases", func(t *testing.T) {
		compare(t, []string{strings.ToUpper(ids[1]), "batchshow-missing", wisp, "BATCHSHOW-WISP", "BATCHSHOW-CAFE", "parent", ids[1], "--id", wisp}, 0)
	})
	t.Run("all-missing", func(t *testing.T) {
		compare(t, []string{"batchshow-missing", "batchshow-no-such-id"}, 1)
	})
	t.Run("plane-collision", func(t *testing.T) {
		insertIssue("batchshow-collision", "task")
		seed(`INSERT INTO wisps (id,title) VALUES ('batchshow-COLLISION', 'collision')`)
		compare(t, []string{parent, "batchshow-collision"}, 1)
	})

	questions := func() uint64 {
		t.Helper()
		var name string
		var count uint64
		if err := admin.QueryRow("SHOW GLOBAL STATUS LIKE 'Questions'").Scan(&name, &count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	countQueries := func(t *testing.T, bin string, ids []string) uint64 {
		t.Helper()
		start := questions()
		_, stderr, code, _ := run(bin, append([]string{"show", "--readonly", "--json"}, ids...)...)
		if code != 0 {
			t.Fatalf("show: exit %d: %s", code, stderr)
		}
		return questions() - start - 1 // Exclude the status query itself.
	}
	t.Run("parent-and-200-children", func(t *testing.T) {
		compare(t, ids, 0) // Warm each binary once before measuring.
		// Choose two arguments with the same hydration shape: at least one
		// child, an outgoing edge and targets in both issue and wisp tables.
		small, large := countQueries(t, after, ids[:2]), countQueries(t, after, ids)
		oldLarge := countQueries(t, before, ids)
		t.Logf("Dolt Questions for 2 and 201 args: head %d and %d; base at 201 args: %d", small, large, oldLarge)
		if large != small || large >= 50 {
			t.Fatalf("expected constant question count under 50, got %d and %d", small, large)
		}

		var oldTimes, newTimes []time.Duration
		for range 5 {
			oldTime, newTime := compare(t, ids, 0)
			oldTimes, newTimes = append(oldTimes, oldTime), append(newTimes, newTime)
		}
		slices.Sort(oldTimes)
		slices.Sort(newTimes)
		oldTime, newTime := oldTimes[2], newTimes[2]
		t.Logf("201 args, median of 5 paired invocations: before %v, after %v (%.2fx)", oldTime, newTime, float64(oldTime)/float64(newTime))
		if oldTime < 5*newTime {
			t.Fatal("expected at least 5x speedup")
		}
	})
}

// Normalize only the legacy dependency arrays. The rest of the JSON,
// including field spelling, whitespace and issue order, is compared as bytes.
func normalizeShowDependencyOrder(t *testing.T, out string) string {
	t.Helper()
	array := regexp.MustCompile(`(?ms)^    "dependencies": (\[.*?^    \])`)
	const prefix = `    "dependencies": `
	return array.ReplaceAllStringFunc(out, func(field string) string {
		var deps []json.RawMessage
		if err := json.Unmarshal([]byte(strings.TrimPrefix(field, prefix)), &deps); err != nil {
			t.Fatal(err)
		}
		type edge struct {
			Type string `json:"dependency_type"`
			ID   string `json:"id"`
		}
		key := func(raw json.RawMessage) string {
			var e edge
			if err := json.Unmarshal(raw, &e); err != nil {
				t.Fatal(err)
			}
			return e.Type + "\x00" + e.ID
		}
		slices.SortFunc(deps, func(a, b json.RawMessage) int { return strings.Compare(key(a), key(b)) })
		data, err := json.Marshal(deps)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := json.Indent(&buf, data, "    ", "  "); err != nil {
			t.Fatal(err)
		}
		return prefix + buf.String()
	})
}
