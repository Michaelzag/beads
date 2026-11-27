//go:build cgo

package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func snapshotParent(snap *showSnapshot) string {
	for _, dep := range snap.dependencies {
		if dep.DependencyType == types.DepParentChild {
			return dep.ID
		}
	}
	return ""
}

// A reparent between show's guard read and its parent read must not let a
// conditional update pair a fresh parent with the token it was read under.
func TestShowSnapshotGuardCoversParentRead(t *testing.T) {
	// Not parallel: showAfterGuardRead is package state.
	s := newTestStore(t, filepath.Join(t.TempDir(), ".beads", "beads.db"))
	ctx := context.Background()
	const actor = "show-guard-test"

	for _, id := range []string{"test-child", "test-parent-a", "test-parent-b"} {
		if err := s.CreateIssue(ctx, &types.Issue{ID: id, Title: id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, actor); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	reparent := func(from, to string) {
		t.Helper()
		if err := s.RemoveDependency(ctx, "test-child", from, actor); err != nil {
			t.Fatalf("remove parent %s: %v", from, err)
		}
		if err := s.AddDependency(ctx, &types.Dependency{IssueID: "test-child", DependsOnID: to, Type: types.DepParentChild}, actor); err != nil {
			t.Fatalf("add parent %s: %v", to, err)
		}
	}
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "test-child", DependsOnID: "test-parent-a", Type: types.DepParentChild}, actor); err != nil {
		t.Fatalf("add parent: %v", err)
	}

	// The reparent lands after the token read and before the parent read.
	showAfterGuardRead = func() { reparent("test-parent-a", "test-parent-b") }
	snap, err := readShowSnapshot(ctx, s, "test-child")
	showAfterGuardRead = func() {}
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if got := snapshotParent(snap); got != "test-parent-b" {
		t.Fatalf("snapshot parent = %q; want the post-reparent parent", got)
	}
	if snap.guardToken == nil {
		t.Fatal("snapshot has no guard token")
	}
	current, err := s.GetMetadata(ctx, storage.CloseGuardKey("test-child"))
	if err != nil {
		t.Fatalf("read guard: %v", err)
	}
	if current == *snap.guardToken {
		t.Fatal("reparent did not replace the child's guard token")
	}
	// The token predates the parent it was returned with, so a CAS refuses.
	err = s.UpdateIssueConditional(ctx, "test-child", map[string]interface{}{"status": "in_progress"}, actor,
		storage.UpdatePrecondition{Status: new(string(snap.issue.Status)), Guard: snap.guardToken})
	var failed *storage.UpdatePreconditionFailed
	if !errors.As(err, &failed) || failed.Field != "guard" {
		t.Fatalf("CAS with a token older than the returned parent = %v; want guard precondition failure", err)
	}
	if issue, err := s.GetIssue(ctx, "test-child"); err != nil || issue.Status != types.StatusOpen {
		t.Fatalf("refused CAS changed the issue: %+v, err = %v", issue, err)
	}

	// Without an interleaved writer the token matches the returned parent.
	snap, err = readShowSnapshot(ctx, s, "test-child")
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if got := snapshotParent(snap); got != "test-parent-b" {
		t.Fatalf("snapshot parent = %q; want test-parent-b", got)
	}
	if err := s.UpdateIssueConditional(ctx, "test-child", map[string]interface{}{"status": "in_progress"}, actor,
		storage.UpdatePrecondition{Status: new(string(snap.issue.Status)), Guard: snap.guardToken}); err != nil {
		t.Fatalf("CAS with a consistent snapshot: %v", err)
	}
}
