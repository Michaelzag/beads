package protocol

import (
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// runCAS runs bd and returns stdout, stderr and the exit code.
func (w *workspace) runCAS(args ...string) (string, string, int) {
	w.t.Helper()
	cmd := exec.Command(w.bd, args...)
	cmd.Dir, cmd.Env = w.dir, w.env()
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err == nil {
		return string(out), stderr.String(), 0
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		w.t.Fatalf("bd %s: %v", strings.Join(args, " "), err)
	}
	return string(out), stderr.String(), exitErr.ExitCode()
}

func guardOf(t *testing.T, issue map[string]any) string {
	t.Helper()
	token, ok := issue["guard_token"].(string)
	if !ok || token == "" {
		t.Fatalf("show --json guard_token = %#v; want a token", issue["guard_token"])
	}
	return token
}

// TestProtocol_ConditionalUpdate asserts that update --if-status/--if-guard
// writes only when both preconditions match, and that a refusal writes
// nothing and exits 3 with a machine-readable result.
func TestProtocol_ConditionalUpdate(t *testing.T) {
	t.Parallel()
	if testDoltPort == 3307 {
		t.Fatalf("refusing to initialize a test database on port %d", testDoltPort)
	}
	w := newWorkspace(t, "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(testDoltPort))
	id := w.create("Conditional update test issue")
	initial := w.showJSON(id)
	token := guardOf(t, initial)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no write", []string{"update", id, "--if-guard", token}},
		{"claim", []string{"update", id, "--claim", "--if-guard", token}},
		{"labels", []string{"update", id, "--status", "blocked", "--add-label", "x", "--if-guard", token}},
		{"two ids", []string{"update", id, id, "--status", "blocked", "--if-guard", token}},
		{"implicit undefer", []string{"update", id, "--defer", "", "--if-guard", token}},
	} {
		if _, _, code := w.runCAS(tc.args...); code != 1 {
			t.Errorf("%s: exit %d; want usage failure 1", tc.name, code)
		}
	}

	assertRefused := func(field, expected, actual string, args ...string) {
		t.Helper()
		before := w.showJSON(id)
		stdout, stderr, code := w.runCAS(append([]string{"update", id, "--status", "closed", "--json"}, args...)...)
		if code != 3 {
			t.Fatalf("exit %d; want 3\nstdout=%s\nstderr=%s", code, stdout, stderr)
		}
		wantMsg := "precondition_failed: " + field + " is " + actual + ", expected " + expected
		if !strings.Contains(stderr, wantMsg) {
			t.Fatalf("stderr = %q; want %q", stderr, wantMsg)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(stdout), &got); err != nil {
			t.Fatalf("refusal JSON %q: %v", stdout, err)
		}
		want := map[string]any{"outcome": "refused", "reason": "precondition_failed", "work_item_id": id, "field": field, "expected": expected, "actual": actual}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("refusal %s = %#v; want %#v (full: %s)", k, got[k], v, stdout)
			}
		}
		after := w.showJSON(id)
		for _, k := range []string{"status", "updated_at", "guard_token", "notes", "closed_at"} {
			if before[k] != after[k] {
				t.Fatalf("refusal changed %s: %#v -> %#v", k, before[k], after[k])
			}
		}
	}
	assertRefused("status", "in_progress", "open", "--if-status", "in_progress", "--if-guard", token)
	assertRefused("guard", "stale-token", token, "--if-status", "open", "--if-guard", "stale-token")

	// Both match: the write lands and the token changes.
	w.run("update", id, "--status", "in_progress", "--if-status", "open", "--if-guard", token)
	matched := w.showJSON(id)
	if matched["status"] != "in_progress" {
		t.Fatalf("status after matching update = %v", matched["status"])
	}
	next := guardOf(t, matched)
	if next == token {
		t.Fatal("matching update left guard_token unchanged")
	}
	assertRefused("guard", token, next, "--if-guard", token)

	// Concurrent conditional updates from the same prior state.
	targets := []string{"blocked", "deferred"}
	codes := make([]int, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, codes[i] = w.runCAS("update", id, "--status", target, "--if-status", "in_progress", "--if-guard", next)
		}()
	}
	wg.Wait()
	winner := -1
	for i, code := range codes {
		switch code {
		case 0:
			if winner >= 0 {
				t.Fatalf("both concurrent conditional updates succeeded: %v", codes)
			}
			winner = i
		case 3:
		default:
			t.Fatalf("concurrent conditional update exit codes = %v; want one 0 and one 3", codes)
		}
	}
	if winner < 0 {
		t.Fatalf("no concurrent conditional update succeeded: %v", codes)
	}
	if got := w.showJSON(id)["status"]; got != targets[winner] {
		t.Fatalf("status = %v; want the winner's %s", got, targets[winner])
	}
}
