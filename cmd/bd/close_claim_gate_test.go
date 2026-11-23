package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

func TestCloseClaimWorkflowDiscoveryErrors(t *testing.T) {
	original := discoverRunIDByWorkflowNameFunc
	t.Cleanup(func() { discoverRunIDByWorkflowNameFunc = original })
	gate := &types.Issue{AwaitID: "build"}
	discoverRunIDByWorkflowNameFunc = func(string) (string, error) {
		return "", errors.New("GitHub unavailable")
	}
	if _, _, _, err := checkGHRunWithErrors(gate, false, true); err == nil {
		t.Fatal("unavailable workflow lookup must remain unknown")
	}
	discoverRunIDByWorkflowNameFunc = func(string) (string, error) {
		return "", noWorkflowRunsError{workflow: "build"}
	}
	resolved, _, reason, err := checkGHRunWithErrors(gate, false, true)
	if err != nil || resolved || reason == "" {
		t.Fatalf("no runs must be unsatisfied: resolved=%v reason=%q err=%v", resolved, reason, err)
	}
}

func TestCloseClaimGitHubFailuresRemainUnknown(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\necho not found >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	if _, _, _, err := checkGHRunStatusWithErrors("123", true); err == nil {
		t.Fatal("failed run lookup must remain unknown")
	}
	if _, _, _, err := checkGHPRWithErrors(&types.Issue{AwaitID: "123"}, true); err == nil {
		t.Fatal("failed PR lookup must remain unknown")
	}
	if err := os.WriteFile(gh, []byte("#!/bin/sh\necho '{}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := checkGHRunStatusWithErrors("123", true); err == nil {
		t.Fatal("missing run status must remain unknown")
	}
	if _, _, _, err := checkGHPRWithErrors(&types.Issue{AwaitID: "123"}, true); err == nil {
		t.Fatal("missing PR state must remain unknown")
	}
}
