package protocol

import (
	"database/sql"
	"encoding/json"
	"maps"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/doltutil"
)

func TestProtocol_ReleaseClaimReceipt(t *testing.T) {
	t.Parallel()
	if testDoltPort == 3307 {
		t.Fatalf("refusing to initialize a test database on port %d", testDoltPort)
	}
	w := newWorkspace(t, "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(testDoltPort))
	id := w.create("Conditional release test issue")
	holder := "zagcode-attempt-" + strings.Repeat("a", 24)
	op := "tracker-release:" + strings.Repeat("b", 64)
	w.run("update", id, "--claim", "--actor", holder)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"missing assignee", []string{id, "--operation-id", op}},
		{"missing operation", []string{id, "--expected-assignee", holder}},
		{"wrong assignee", []string{id, "--expected-assignee", "zagcode-attempt-" + strings.Repeat("c", 24), "--operation-id", op}},
		{"ordinary assignee", []string{id, "--expected-assignee", "operator", "--operation-id", op}},
		{"abbreviated ID", []string{id[:len(id)-1], "--expected-assignee", holder, "--operation-id", op}},
		{"read-only", []string{id, "--expected-assignee", holder, "--operation-id", op, "--readonly"}},
		{"dry-run", []string{id, "--expected-assignee", holder, "--operation-id", op, "--dry-run"}},
	} {
		args := append([]string{"release-claim", "--json"}, tc.args...)
		out, code := w.runExpectError(args...)
		if code != 1 || strings.Contains(out, `"released"`) {
			t.Errorf("%s: expected refusal without a released receipt, got exit %d: %s", tc.name, code, out)
		}
	}
	if got := w.showJSON(id)["assignee"]; got != holder {
		t.Fatalf("assignee after refusals = %v, want %q", got, holder)
	}

	want := map[string]string{
		"outcome":           "released",
		"work_item_id":      id,
		"expected_assignee": holder,
		"operation_id":      op,
	}
	head := func() string {
		cmd := exec.Command(w.bd, "vc", "status", "--json")
		cmd.Dir, cmd.Env = w.dir, w.env()
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("read disposable workspace status: %v", err)
		}
		var status struct {
			Commit string `json:"commit"`
		}
		if err := json.Unmarshal(out, &status); err != nil || status.Commit == "" || status.Commit == "(unknown)" {
			t.Fatalf("cannot read Dolt HEAD: %s (%v)", out, err)
		}
		return status.Commit
	}
	cfg, err := configfile.Load(filepath.Join(w.dir, ".beads"))
	if err != nil || cfg == nil || !cfg.IsDoltServerMode() || cfg.DoltDatabase == "" {
		t.Fatalf("cannot identify disposable workspace database: %v", err)
	}
	if testDoltPort <= 0 || testDoltPort == 3307 {
		t.Fatalf("refusing direct fixture write on port %d", testDoltPort)
	}
	dsn := doltutil.ServerDSN{Host: "127.0.0.1", Port: testDoltPort, User: "root", Database: cfg.DoltDatabase}
	db, err := sql.Open("mysql", dsn.String())
	if err != nil {
		t.Fatalf("open Dolt fixture client: %v", err)
	}
	defer db.Close()
	ctx := t.Context()
	assertPending := func() {
		var dirty int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status WHERE table_name = 'release_replay_fixture' AND NOT staged").Scan(&dirty); err != nil || dirty != 1 {
			t.Fatalf("want one unstaged fixture table, got %d (%v)", dirty, err)
		}
		var marker string
		if err := db.QueryRowContext(ctx, "SELECT marker FROM release_replay_fixture WHERE id = 1").Scan(&marker); err != nil || marker != "pending" {
			t.Fatalf("pending marker = %q (%v), want %q", marker, err, "pending")
		}
	}
	beforeRelease := head()
	var afterRelease string
	for _, attempt := range []string{"initial", "replay"} {
		if attempt == "replay" {
			cliIssue := w.showJSON(id)
			var sqlID, title, status string
			if err := db.QueryRowContext(ctx, "SELECT id, title, status FROM issues WHERE id = ?", id).Scan(&sqlID, &title, &status); err != nil {
				t.Fatalf("read issue %q through the fixture client: %v", id, err)
			}
			if sqlID != id || cliIssue["id"] != sqlID || cliIssue["title"] != title || cliIssue["status"] != status {
				t.Fatalf("CLI and fixture client disagree on the issue: CLI=%v SQL=(%q, %q, %q)", cliIssue, sqlID, title, status)
			}

			// Create genuine working-set changes without the CLI's create-time commit.
			if _, err := db.ExecContext(ctx, `SET @@session.dolt_transaction_commit = 0;
				CREATE TABLE release_replay_fixture (id INT PRIMARY KEY, marker VARCHAR(16));
				INSERT INTO release_replay_fixture VALUES (1, 'pending')`); err != nil {
				t.Fatalf("write pending Dolt fixture: %v", err)
			}
			if got := head(); got != afterRelease {
				t.Fatalf("writing pending fixture unexpectedly advanced Dolt HEAD: %s -> %s", afterRelease, got)
			}
			assertPending()
		}
		cmd := exec.Command(w.bd, "release-claim", id, "--expected-assignee", holder, "--operation-id", op, "--json", "--dolt-auto-commit", "on")
		cmd.Dir = w.dir
		cmd.Env = append(w.env(), "BD_JSON_ENVELOPE=1")
		out, err := cmd.Output()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				t.Fatalf("%s release: %v\n%s", attempt, err, exitErr.Stderr)
			}
			t.Fatalf("%s release: %v", attempt, err)
		}
		var got map[string]string
		if err := json.Unmarshal(out, &got); err != nil || !maps.Equal(got, want) {
			t.Fatalf("%s receipt = %s (%v), want %v", attempt, out, err, want)
		}
		if attempt == "initial" {
			afterRelease = head()
			if afterRelease == beforeRelease {
				t.Fatalf("initial release did not advance Dolt HEAD with auto-commit on: %s", afterRelease)
			}
		} else {
			if got := head(); got != afterRelease {
				t.Fatalf("replayed release committed unrelated pending work: %s -> %s", afterRelease, got)
			}
			assertPending()
		}
	}
}
