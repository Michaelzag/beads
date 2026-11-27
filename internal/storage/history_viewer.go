package storage

import (
	"context"

	"github.com/steveyegge/beads/internal/types"
)

// HistoryViewer provides time-travel queries and diffs.
type HistoryViewer interface {
	// History returns committed issue revisions, newest first. A nonpositive limit returns all.
	History(ctx context.Context, issueID string, limit int) ([]*HistoryEntry, error)
	AsOf(ctx context.Context, issueID string, ref string) (*types.Issue, error)
	Diff(ctx context.Context, fromRef, toRef string) ([]*DiffEntry, error)
}
