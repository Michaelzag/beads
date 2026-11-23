package protocol

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func TestProtocol_CloseClaimReceipt(t *testing.T) {
	t.Parallel()
	if testDoltPort == 3307 {
		t.Fatalf("refusing to initialize a test database on port %d", testDoltPort)
	}
	w := newWorkspace(t, "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(testDoltPort))
	const (
		acceptance = "Record the complete receipt, including UTF-8: Café."
		holder     = "zagcode-attempt-aaaaaaaaaaaaaaaaaaaaaaaa"
		reason     = "zagcom-handoff:cd4b525e-19cc-4077-9edc-af77521e0042"
	)
	digest := func(s string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(s))) }
	id := w.create("Conditional close test issue", "--acceptance", acceptance, "--labels", "repo:beta,ignored,repo:Alpha")
	w.run("update", id, "--claim", "--actor", holder)
	op := "tracker-close:" + strings.Repeat("a", 64)
	acceptanceDigest, labelsDigest := digest(acceptance), digest("repo:Alpha\nrepo:beta")
	args := func(assignee, operation, closeReason string) []string {
		return []string{"close-claim", id, "--json", "--dolt-auto-commit", "on",
			"--expected-assignee", assignee, "--operation-id", operation, "--reason", closeReason,
			"--expected-acceptance-digest", acceptanceDigest, "--expected-repo-labels-digest", labelsDigest}
	}
	assertReceipt := func(assignee, operation, outcome, refusal string, flags ...string) {
		t.Helper()
		cmd := exec.Command(w.bd, append(args(assignee, operation, reason), flags...)...)
		cmd.Dir = w.dir
		cmd.Env = append(w.env(), "BD_JSON_ENVELOPE=1", "BEADS_ACTOR=close-protocol-test")
		out, err := cmd.Output()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				t.Fatalf("expected exit 0 for %s, got %v: %s", outcome, err, exitErr.Stderr)
			}
			t.Fatalf("run close claim: %v", err)
		}
		want := map[string]string{
			"outcome":                     outcome,
			"work_item_id":                id,
			"expected_assignee":           assignee,
			"expected_acceptance_digest":  acceptanceDigest,
			"expected_repo_labels_digest": labelsDigest,
			"operation_id":                operation,
			"close_reason":                reason,
		}
		if refusal != "" {
			want["reason"] = refusal
		}
		var got map[string]string
		if err := json.Unmarshal(out, &got); err != nil || !maps.Equal(got, want) {
			t.Fatalf("receipt = %s (%v), want %v", out, err, want)
		}
	}

	assertReceipt(holder, op, "missing", "", "--receipt-only", "--readonly")
	assertReceipt("other-holder", "tracker-close:"+strings.Repeat("b", 64), "refused", "custody_mismatch")
	for _, invalid := range [][]string{
		append(args(holder, op, reason), "--force"),
		append(args(holder, op, reason), "--receipt-only", "--refuse-if-missing"),
		append(args(holder, op, reason), "--readonly"),
		args(holder, op, "zagcom-handoff:00000000-0000-0000-0000-000000000000"),
	} {
		if out, code := w.runExpectError(invalid...); code != 1 || strings.Contains(out, `"outcome"`) {
			t.Fatalf("expected non-receipt failure, got exit %d: %s", code, out)
		}
	}
	assertReceipt(holder, op, "closed", "")
	assertReceipt(holder, op, "closed", "", "--receipt-only")
	assertReceipt(holder, op, "closed", "", "--refuse-if-missing")
	assertReceipt(holder, "tracker-close:"+strings.Repeat("c", 64), "refused", "closed_by_other")
}
