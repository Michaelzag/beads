//go:build cgo

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
	"github.com/stretchr/testify/require"
)

// TestServerHistory uses an explicitly supplied, disposable Dolt server and a
// fresh database. The same fixture optionally times the pre-fix binary.
func TestServerHistory(t *testing.T) {
	port := os.Getenv("BEADS_TEST_HISTORY_DOLT_PORT")
	if port == "" {
		t.Skip("set BEADS_TEST_HISTORY_DOLT_PORT to a disposable Dolt server")
	}
	bd := buildEmbeddedBD(t)
	dir := t.TempDir()
	initGitRepoAt(t, dir)
	database := fmt.Sprintf("history_test_%d", time.Now().UnixNano())
	run := func(binary string, args ...string) ([]byte, time.Duration) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = dir
		cmd.Env = bdEnv(dir)
		start := time.Now()
		stdout, stderr, err := runCommandBuffers(t, cmd)
		elapsed := time.Since(start)
		require.NoErrorf(t, err, "bd %s (%v): %s\n%s", strings.Join(args, " "), elapsed, stdout.String(), stderr.String())
		return stdout.Bytes(), elapsed
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Net, cfg.Addr = "root", "tcp", "127.0.0.1:"+port
	cfg.ParseTime = true
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
		if err != nil {
			t.Error(err)
		}
		_ = admin.Close()
	})
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	run(bd, "init", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", port, "--prefix", "hist", "--database", database)
	cfg.DBName = database
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	db.SetMaxOpenConns(1) // Keep branch checkout session-local in the merge case.
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	execSQL := func(query string, args ...any) {
		t.Helper()
		_, err := db.ExecContext(ctx, query, args...)
		require.NoError(t, err)
	}
	create := func(title string) string {
		t.Helper()
		out, _ := run(bd, "create", title, "--json")
		var issue types.Issue
		require.NoError(t, json.Unmarshal(out, &issue))
		require.NotEmpty(t, issue.ID)
		return issue.ID
	}
	commit := func(message, date string) string {
		t.Helper()
		var hash string
		err := db.QueryRowContext(ctx, "CALL DOLT_COMMIT('-Am', ?, '--date', ?)", message, date).Scan(&hash)
		require.NoError(t, err)
		return hash
	}
	other := create("Unrelated issue")
	id := create("Target issue")
	type revision struct {
		hash, status, notes string
		date, updatedAt     time.Time
	}
	var revisions []revision
	record := func(status, notes string) {
		t.Helper()
		var hash string
		var date time.Time
		require.NoError(t, db.QueryRowContext(ctx, "SELECT commit_hash, date FROM dolt_log ORDER BY commit_order DESC LIMIT 1").Scan(&hash, &date))
		out, _ := run(bd, "show", id, "--json")
		var issues []types.Issue
		require.NoError(t, json.Unmarshal(out, &issues))
		require.Len(t, issues, 1)
		require.False(t, date.Equal(issues[0].UpdatedAt), "fixture must distinguish commit date from issue updated_at")
		revisions = append(revisions, revision{hash, status, notes, date, issues[0].UpdatedAt})
	}
	update := func(args ...string) {
		t.Helper()
		run(bd, append([]string{"update", id}, args...)...)
	}
	filler := func(i int) {
		t.Helper()
		execSQL("UPDATE issues SET title = ? WHERE id = ?", fmt.Sprintf("Unrelated revision %d", i), other)
		execSQL("CALL DOLT_COMMIT('-Am', 'unrelated issue revision')")
	}
	check := func(out []byte, expected []revision) []storage.HistoryEntry {
		t.Helper()
		var entries []storage.HistoryEntry
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, len(expected))
		for i, want := range expected {
			entry := entries[i]
			require.NotNil(t, entry.Issue)
			require.Equal(t, id, entry.Issue.ID)
			require.Equal(t, want.hash, entry.CommitHash)
			require.Truef(t, want.date.Equal(entry.CommitDate), "entry %d: commit date %v, want %v", i, entry.CommitDate, want.date)
			require.Truef(t, want.updatedAt.Equal(entry.Issue.UpdatedAt), "entry %d: updated_at %v, want %v", i, entry.Issue.UpdatedAt, want.updatedAt)
			require.Equal(t, want.status, string(entry.Issue.Status))
			require.Equal(t, want.notes, entry.Issue.Notes)
		}
		return entries
	}

	record("open", "")
	filler(0)
	update("--priority", "1")
	record("open", "")
	filler(1)
	const note = "decision: preserve separate issue and commit timestamps"
	update("--append-notes", note)
	record("open", note)
	filler(2)
	update("--status", "in_progress")
	record("in_progress", note)
	filler(3)
	want := []revision{revisions[3], revisions[2], revisions[1], revisions[0]}
	t.Run("unlimited", func(t *testing.T) {
		all, _ := run(bd, "history", id, "--json")
		check(all, want)
		limited, _ := run(bd, "history", id, "--limit", "2", "--json")
		var allJSON, limitJSON []json.RawMessage
		require.NoError(t, json.Unmarshal(all, &allJSON))
		require.NoError(t, json.Unmarshal(limited, &limitJSON))
		require.Len(t, limitJSON, 2)
		for i := range limitJSON {
			require.True(t, bytes.Equal(allJSON[i], limitJSON[i]))
		}
	})

	t.Run("select_only", func(t *testing.T) {
		// Scope the account and its grants to this disposable fixture.
		user := fmt.Sprintf("history_ro_%d", time.Now().UnixNano())
		account := "'" + user + "'@'%'"
		_, err := admin.ExecContext(ctx, "CREATE USER "+account)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := admin.Exec("DROP USER " + account)
			require.NoError(t, err)
		})
		_, err = admin.ExecContext(ctx, "GRANT SELECT ON `"+database+"`.* TO "+account)
		require.NoError(t, err)

		args := []string{"history", id, "--limit", "3", "--json"}
		baseline := bd
		if before := os.Getenv("BEADS_TEST_HISTORY_BEFORE_BINARY"); before != "" {
			baseline = before
		}
		expected, _ := run(baseline, args...)
		versionPath := filepath.Join(dir, ".beads", localVersionFile)
		require.NoError(t, os.WriteFile(versionPath, []byte("0.56.0\n"), 0600))

		runAsReader := func(args ...string) (bytes.Buffer, bytes.Buffer, error) {
			t.Helper()
			callCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(callCtx, bd, args...)
			cmd.Dir = dir
			cmd.Env = append(bdEnv(dir), "BEADS_DOLT_SERVER_USER="+user)
			return runCommandBuffers(t, cmd)
		}
		out, stderr, err := runAsReader(args...)
		require.NoError(t, err, "SELECT-only history: %s\n%s", out.String(), stderr.String())
		check(out.Bytes(), want[:3])
		require.JSONEq(t, string(expected), out.String())
		version, err := os.ReadFile(versionPath)
		require.NoError(t, err)
		require.Equal(t, "0.56.0\n", string(version), "history must not write version metadata")
		t.Log("SELECT-only history returned the same three revisions as the baseline")

		out, stderr, err = runAsReader("update", id, "--title", "write must be denied")
		require.Error(t, err)
		failure := strings.ToLower(out.String() + stderr.String())
		require.Contains(t, failure, "denied")
		require.Contains(t, failure, user)
		t.Logf("SELECT-only update was denied: %s", strings.TrimSpace(failure))
	})

	t.Run("long", func(t *testing.T) {
		for i := 4; i < 2000; i++ {
			filler(i)
		}
		var n int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_log").Scan(&n))
		require.GreaterOrEqual(t, n, 2000)
		t.Logf("Dolt commits: %d", n)
		args := []string{"history", id, "--limit", "3", "--json"}
		if before := os.Getenv("BEADS_TEST_HISTORY_BEFORE_BINARY"); before != "" {
			out, elapsed := run(before, args...)
			t.Logf("Before: bd %s: %v\n%s", strings.Join(args, " "), elapsed, out)
		}
		out, elapsed := run(bd, args...)
		t.Logf("After: bd %s: %v\n%s", strings.Join(args, " "), elapsed, out)
		require.Less(t, elapsed, 5*time.Second)
		entries := check(out, want[:3])
		require.Contains(t, entries[1].Issue.Notes, note)
		require.NotContains(t, entries[2].Issue.Notes, note)
	})

	t.Run("equal_commit_dates", func(t *testing.T) {
		tiedID := create("Issue with tied commit dates")
		const date = "2020-01-02T12:00:00Z"
		execSQL("UPDATE issues SET priority = 1 WHERE id = ?", tiedID)
		previous := commit("set priority", date)
		execSQL("UPDATE issues SET notes = CONCAT(notes, ?) WHERE id = ?", note, tiedID)
		appendHash := commit("append notes", date)
		for range 3 {
			out, _ := run(bd, "history", tiedID, "--limit", "1", "--json")
			var entries []storage.HistoryEntry
			require.NoError(t, json.Unmarshal(out, &entries))
			require.Len(t, entries, 1)
			require.Equal(t, appendHash, entries[0].CommitHash)
			require.Equal(t, note, entries[0].Issue.Notes)
		}
		out, _ := run(bd, "history", tiedID, "--limit", "2", "--json")
		var entries []storage.HistoryEntry
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, 2)
		require.Equal(t, appendHash, entries[0].CommitHash)
		require.Equal(t, previous, entries[1].CommitHash)
		require.True(t, entries[0].CommitDate.Equal(entries[1].CommitDate))
		require.Empty(t, entries[1].Issue.Notes)
	})

	t.Run("merge", func(t *testing.T) {
		mergeID := create("Issue changed on both sides of a fork")
		execSQL("CALL DOLT_BRANCH('side')")
		execSQL("UPDATE issues SET notes = 'main edit' WHERE id = ?", mergeID)
		main := commit("main edit", "2020-01-02T12:00:00Z")
		execSQL("CALL DOLT_CHECKOUT('side')")
		execSQL("UPDATE issues SET priority = 1 WHERE id = ?", mergeID)
		side := commit("side edit", "2020-01-03T12:00:00Z")
		// Keep one merge parent reachable beyond the first history window.
		for i := range 260 {
			filler(i + 2000)
		}
		execSQL("CALL DOLT_CHECKOUT('main')")
		execSQL("CALL DOLT_MERGE('--no-ff', 'side')")
		var head string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT HASHOF('HEAD')").Scan(&head))
		out, _ := run(bd, "history", mergeID, "--limit", "3", "--json")
		var entries []storage.HistoryEntry
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, 3)
		require.Equal(t, head, entries[0].CommitHash)
		require.Equal(t, "main edit", entries[0].Issue.Notes)
		require.Equal(t, 1, entries[0].Issue.Priority)
		require.ElementsMatch(t, []string{main, side}, []string{entries[1].CommitHash, entries[2].CommitHash})
	})

	t.Run("schema_breaks", func(t *testing.T) {
		schemaID := create("Issue spanning a table recreation")
		execSQL("UPDATE issues SET notes = 'before PK change' WHERE id = ?", schemaID)
		before := commit("before PK change", "2020-01-01T12:00:00Z")
		execSQL("SET FOREIGN_KEY_CHECKS=0")
		execSQL("CREATE UNIQUE INDEX history_test_issue_id ON issues(id)")
		execSQL("ALTER TABLE issues DROP PRIMARY KEY, ADD PRIMARY KEY (id, priority)")
		execSQL("UPDATE issues SET notes = 'during PK change' WHERE id = ?", schemaID)
		pkChange := commit("during PK change", "2020-01-02T12:00:00Z")
		execSQL("CREATE TABLE saved_issues LIKE issues")
		execSQL("INSERT INTO saved_issues SELECT * FROM issues WHERE id = ?", schemaID)
		rows, err := db.QueryContext(ctx, `SELECT DISTINCT TABLE_NAME, CONSTRAINT_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = 'issues'`)
		require.NoError(t, err)
		var drops []string
		quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
		for rows.Next() {
			var table, constraint string
			require.NoError(t, rows.Scan(&table, &constraint))
			drops = append(drops, "ALTER TABLE "+quote(table)+" DROP FOREIGN KEY "+quote(constraint))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		for _, drop := range drops {
			execSQL(drop)
		}
		execSQL("DROP TABLE issues")
		commit("drop issues", "2020-01-03T12:00:00Z")
		execSQL("CREATE TABLE issues LIKE saved_issues")
		execSQL("INSERT INTO issues SELECT * FROM saved_issues")
		execSQL("UPDATE issues SET notes = 'after recreation' WHERE id = ?", schemaID)
		recreated := commit("recreate issues", "2020-01-04T12:00:00Z")
		out, _ := run(bd, "history", schemaID, "--limit", "3", "--json")
		var entries []storage.HistoryEntry
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, 3)
		require.Equal(t, recreated, entries[0].CommitHash)
		require.Equal(t, "after recreation", entries[0].Issue.Notes)
		require.Equal(t, pkChange, entries[1].CommitHash)
		require.Equal(t, "during PK change", entries[1].Issue.Notes)
		require.Equal(t, before, entries[2].CommitHash)
		require.Equal(t, "before PK change", entries[2].Issue.Notes)

		execSQL("ALTER TABLE issues DROP PRIMARY KEY, ADD PRIMARY KEY (priority, id)")
		execSQL("UPDATE issues SET notes = 'non-leading ID' WHERE id = ?", schemaID)
		commit("move ID out of the PK prefix", "2020-01-05T12:00:00Z")
		execSQL("UPDATE issues SET notes = 'non-leading ID edit' WHERE id = ?", schemaID)
		nonLeading := commit("edit without a leading ID", "2020-01-06T12:00:00Z")
		out, _ = run(bd, "history", schemaID, "--limit", "1", "--json")
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, 1)
		require.Equal(t, nonLeading, entries[0].CommitHash)

		execSQL("ALTER TABLE issues DROP PRIMARY KEY, ADD PRIMARY KEY (id)")
		execSQL("UPDATE issues SET notes = 'after contraction' WHERE id = ?", schemaID)
		contracted := commit("restore PK", "2020-01-07T12:00:00Z")
		out, _ = run(bd, "history", schemaID, "--limit", "2", "--json")
		require.NoError(t, json.Unmarshal(out, &entries))
		require.Len(t, entries, 2)
		require.Equal(t, contracted, entries[0].CommitHash)
		require.Equal(t, nonLeading, entries[1].CommitHash)

		execSQL("DROP TABLE issues") // WORKING only; HEAD still has its own schema.
		working, _ := run(bd, "history", schemaID, "--limit", "2", "--json")
		require.Equal(t, string(out), string(working))
	})
}
