package main

import (
	"errors"
	"fmt"
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

func TestCloseClaimWorkflowListNullAndEmpty(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	t.Setenv("PATH", dir)
	for _, tc := range []struct {
		response string
		wantErr  bool
	}{
		{"null", true},
		{"[]", false},
	} {
		t.Run(tc.response, func(t *testing.T) {
			if err := os.WriteFile(gh, []byte("#!/bin/sh\necho '"+tc.response+"'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			gate := &types.Issue{AwaitID: "build"}
			resolved, _, _, err := checkGHRunWithErrors(gate, false, true)
			if (err != nil) != tc.wantErr || resolved {
				t.Fatalf("strict lookup: resolved=%v err=%v", resolved, err)
			}
			resolved, _, _, err = checkGHRunWithErrors(gate, false, false)
			if err != nil || resolved {
				t.Fatalf("ordinary lookup: resolved=%v err=%v", resolved, err)
			}
		})
	}
}

func TestCloseClaimWorkflowConclusions(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	t.Setenv("PATH", dir)
	for _, tc := range []struct {
		conclusion string
		wantErr    bool
		resolved   bool
	}{
		{"", true, false},
		{"unrecognized", true, false},
		{"success", false, true},
		{"failure", false, false},
		{"cancelled", false, false},
		{"canceled", false, false},
		{"skipped", false, false},
		{"action_required", false, false},
		{"neutral", false, false},
		{"stale", false, false},
		{"startup_failure", false, false},
		{"timed_out", false, false},
	} {
		t.Run(tc.conclusion, func(t *testing.T) {
			output := fmt.Sprintf(`{"status":"completed","conclusion":%q,"name":"build"}`, tc.conclusion)
			if err := os.WriteFile(gh, []byte("#!/bin/sh\necho '"+output+"'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			resolved, _, _, err := checkGHRunStatusWithErrors("123", true)
			if (err != nil) != tc.wantErr || resolved != tc.resolved {
				t.Fatalf("strict conclusion %q: resolved=%v err=%v", tc.conclusion, resolved, err)
			}
			if tc.conclusion == "skipped" {
				resolved, _, _, err = checkGHRunStatusWithErrors("123", false)
				if err != nil || !resolved {
					t.Fatalf("ordinary skipped conclusion: resolved=%v err=%v", resolved, err)
				}
			}
		})
	}
}
