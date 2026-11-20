package protocol

import (
	"encoding/json"
	"maps"
	"os/exec"
	"strings"
	"testing"
)

func TestProtocol_ReleaseClaimReceipt(t *testing.T) {
	t.Parallel()
	w := newWorkspace(t)
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
	for _, attempt := range []string{"initial", "replay"} {
		cmd := exec.Command(w.bd, "release-claim", id, "--expected-assignee", holder, "--operation-id", op, "--json")
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
	}
}
