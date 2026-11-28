package issueops

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/steveyegge/beads/internal/types"
)

func deferredParentLookupRegex(depTable, parentTable string) string {
	return `FROM issues i\s+LEFT JOIN ` + depTable + ` dep.*LEFT JOIN ` + parentTable + ` p.*WHERE i\.id IN `
}

func beginMockTx(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *sql.Tx) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	return db, mock, tx
}

func TestBuildSQLInClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		ids              []string
		wantPlaceholders string
		wantArgs         []interface{}
	}{
		{
			name:             "single ID",
			ids:              []string{"42"},
			wantPlaceholders: "?",
			wantArgs:         []interface{}{"42"},
		},
		{
			name:             "multiple IDs",
			ids:              []string{"1", "2", "3"},
			wantPlaceholders: "?,?,?",
			wantArgs:         []interface{}{"1", "2", "3"},
		},
		{
			name:             "empty slice",
			ids:              []string{},
			wantPlaceholders: "",
			wantArgs:         []interface{}{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotPlaceholders, gotArgs := buildSQLInClause(tt.ids)

			if gotPlaceholders != tt.wantPlaceholders {
				t.Errorf("placeholders = %q, want %q", gotPlaceholders, tt.wantPlaceholders)
			}

			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args length = %d, want %d", len(gotArgs), len(tt.wantArgs))
			}

			for i := range gotArgs {
				if gotArgs[i] != tt.wantArgs[i] {
					t.Errorf("args[%d] = %v, want %v", i, gotArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestGetReadyWorkInTx_PropagatesDeferredParentChildError(t *testing.T) {
	t.Parallel()

	_, mock, tx := beginMockTx(t)
	childErr := errors.New("dolt transient dependency read failure")
	mock.ExpectQuery(`SELECT i\.id FROM issues i`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("candidate"))
	mock.ExpectQuery(deferredParentLookupRegex("dependencies", "issues")).
		WithArgs("candidate").WillReturnError(childErr)

	_, err := GetReadyWorkInTx(
		context.Background(),
		tx,
		types.WorkFilter{},
	)
	if err == nil {
		t.Fatal("expected deferred parent child error")
	}
	if !errors.Is(err, childErr) {
		t.Fatalf("expected wrapped deferred parent child error, got %v", err)
	}
	if !strings.Contains(err.Error(), "compute deferred parent children") {
		t.Fatalf("expected deferred parent child context, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestReadyParentWalkOnceWithCounts(t *testing.T) {
	t.Parallel()
	_, mock, tx := beginMockTx(t)
	const root = "ready-root"
	parent := root
	mock.ExpectQuery(`SELECT 1 FROM wisp_dependencies LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"1"}))
	for _, table := range []string{"dependencies", "wisp_dependencies"} {
		for _, target := range descendantTargetColumns {
			mock.ExpectQuery(`SELECT issue_id FROM ` + table + ` WHERE type = 'parent-child' AND \(` + target + ` = \?\)`).
				WithArgs(root, root).WillReturnRows(sqlmock.NewRows([]string{"issue_id"}))
		}
	}
	mock.ExpectQuery(`SELECT i\.id FROM issues i`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(`SELECT 1 FROM wisps LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT i\.id FROM wisps i`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	if _, err := GetReadyWorkWithCountsInTx(context.Background(), tx, types.WorkFilter{ParentID: &parent}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadStatusByIDInTxErrorsOnIssueWispCollision(t *testing.T) {
	t.Parallel()

	_, mock, tx := beginMockTx(t)
	mock.ExpectQuery("SELECT id, status FROM issues").
		WithArgs("dup-id").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow("dup-id", types.StatusOpen))
	mock.ExpectQuery("SELECT id, status FROM wisps").
		WithArgs("dup-id").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow("dup-id", types.StatusClosed))

	_, err := loadStatusByIDInTx(context.Background(), tx, []string{"dup-id"})
	if err == nil {
		t.Fatal("expected duplicate issue/wisp status error")
	}
	if !strings.Contains(err.Error(), `id "dup-id" exists in both issues and wisps`) {
		t.Fatalf("error = %v, want duplicate issue/wisp context", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMergeReadyWispsErrorsOnIssueWispCollision(t *testing.T) {
	t.Parallel()

	_, err := mergeReadyWisps(
		[]*types.Issue{{ID: "dup-id", Status: types.StatusOpen}},
		[]*types.Issue{{ID: "dup-id", Status: types.StatusClosed}},
		types.WorkFilter{},
	)
	if err == nil {
		t.Fatal("expected duplicate issue/wisp ready-work error")
	}
	if !strings.Contains(err.Error(), `id "dup-id" exists in both issues and wisps`) {
		t.Fatalf("error = %v, want duplicate issue/wisp context", err)
	}
}

func TestGetChildrenOfDeferredParentsInTx_ReturnsChildrenFromBothDependencyTables(t *testing.T) {
	t.Parallel()

	_, mock, tx := beginMockTx(t)
	candidates := []string{
		"child-from-dependencies-issues",
		"child-from-dependencies-wisps",
		"child-from-wisp-dependencies-issues",
		"child-from-wisp-dependencies-wisps",
	}
	cols := []string{"child_id", "parent_id"}
	mock.ExpectQuery(deferredParentLookupRegex("dependencies", "issues")).
		WithArgs(candidates[0], candidates[1], candidates[2], candidates[3]).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(candidates[0], "issue-parent").AddRow("not-deferred", nil))
	mock.ExpectQuery(deferredParentLookupRegex("dependencies", "wisps")).
		WithArgs(candidates[0], candidates[1], candidates[2], candidates[3]).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(candidates[1], "wisp-parent"))
	mock.ExpectQuery(deferredParentLookupRegex("wisp_dependencies", "issues")).
		WithArgs(candidates[0], candidates[1], candidates[2], candidates[3]).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(candidates[2], "issue-parent"))
	mock.ExpectQuery(deferredParentLookupRegex("wisp_dependencies", "wisps")).
		WithArgs(candidates[0], candidates[1], candidates[2], candidates[3]).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(candidates[3], "wisp-parent"))

	got, err := getChildrenOfDeferredParentsInTx(context.Background(), tx, "issues", candidates)
	if err != nil {
		t.Fatalf("getChildrenOfDeferredParentsInTx: %v", err)
	}
	want := map[string]struct{}{candidates[0]: {}, candidates[1]: {}, candidates[2]: {}, candidates[3]: {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("children = %v, want %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestGetChildrenOfDeferredParentsInTx_NoCandidatesSkipsSQL(t *testing.T) {
	t.Parallel()

	_, mock, tx := beginMockTx(t)
	got, err := getChildrenOfDeferredParentsInTx(context.Background(), tx, "issues", nil)
	if err != nil {
		t.Fatalf("getChildrenOfDeferredParentsInTx: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("children = %v, want empty", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestGetChildrenOfDeferredParentsInTx_IgnoresMissingWispDependenciesTable(t *testing.T) {
	t.Parallel()

	_, mock, tx := beginMockTx(t)
	cols := []string{"child_id", "parent_id"}
	mock.ExpectQuery(deferredParentLookupRegex("dependencies", "issues")).
		WillReturnRows(sqlmock.NewRows(cols).AddRow("child-from-dependencies-issues", "issue-parent"))
	mock.ExpectQuery(deferredParentLookupRegex("dependencies", "wisps")).
		WillReturnRows(sqlmock.NewRows(cols).AddRow("child-from-dependencies-wisps", "wisp-parent"))
	mock.ExpectQuery(deferredParentLookupRegex("wisp_dependencies", "issues")).
		WillReturnError(errors.New("table wisp_dependencies does not exist"))

	got, err := getChildrenOfDeferredParentsInTx(context.Background(), tx, "issues", []string{"child-from-dependencies-issues", "child-from-dependencies-wisps"})
	if err != nil {
		t.Fatalf("getChildrenOfDeferredParentsInTx: %v", err)
	}
	want := map[string]struct{}{"child-from-dependencies-issues": {}, "child-from-dependencies-wisps": {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("children = %v, want %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
