package fix

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// getDatabasePath returns the actual database directory path, respecting dolt_data_dir.
// When dolt_data_dir is configured (e.g. ext4 redirect for WSL), the database lives
// outside .beads/dolt/ — this function resolves the correct location.
func getDatabasePath(beadsDir string) string {
	cfg, err := configfile.Load(beadsDir)
	if err != nil || cfg == nil {
		return filepath.Join(beadsDir, "dolt") // fallback to default
	}
	return cfg.DatabasePath(beadsDir)
}

// OrphanedDependencies removes dependencies pointing to non-existent issues.
// If verbose is true, prints each removed dependency; otherwise shows only summary.
func OrphanedDependencies(path string, verbose bool) error {
	beadsDir, err := resolvedWorkspaceBeadsDir(path)
	if err != nil {
		return err
	}

	db, err := openDoltDB(beadsDir)
	if err != nil {
		fmt.Printf("  Orphaned dependencies fix skipped (%v)\n", err)
		return nil
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Find orphaned dependencies (exclude external: cross-rig tracking refs, #1593)
	//nolint:gosec // G202: fixDependencyUnionSQL returns a fixed internal SELECT fragment.
	query := `
		SELECT d.dep_table, d.id, d.issue_id, d.depends_on_id, d.type, d.target_is_issue, d.target_is_wisp
		FROM (` + fixDependencyUnionSQL() + `) d
		WHERE NOT EXISTS (SELECT 1 FROM issues i WHERE i.id = d.depends_on_id)
		  AND NOT EXISTS (SELECT 1 FROM wisps w WHERE w.id = d.depends_on_id)
		  AND d.depends_on_id NOT LIKE 'external:%'
	`
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query orphaned dependencies: %w", err)
	}
	defer rows.Close()

	type orphan struct {
		depTable, id, issueID, dependsOnID, depType string
		targetIsIssue, targetIsWisp                 bool
	}
	var orphans []orphan

	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.depTable, &o.id, &o.issueID, &o.dependsOnID, &o.depType, &o.targetIsIssue, &o.targetIsWisp); err == nil {
			orphans = append(orphans, o)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	_ = rows.Close()

	if len(orphans) == 0 {
		fmt.Println("  No orphaned dependencies to fix")
		return nil
	}

	// Delete orphaned dependencies
	// Uses explicit transaction so writes persist when @@autocommit is OFF
	// (e.g. Dolt server started with --no-auto-commit).
	showIndividual := verbose || len(orphans) < 20
	var removed int
	for _, o := range orphans {
		var result sql.Result
		var err error
		switch o.depTable {
		case "dependencies":
			result, err = tx.Exec("DELETE FROM dependencies WHERE id = ?", o.id)
		case "wisp_dependencies":
			result, err = tx.Exec("DELETE FROM wisp_dependencies WHERE id = ?", o.id)
		default:
			fmt.Printf("  Warning: skipped orphaned dependency from unexpected table %s\n", o.depTable)
			continue
		}
		if err != nil {
			fmt.Printf("  Warning: failed to remove %s→%s: %v\n", o.issueID, o.dependsOnID, err)
		} else {
			if err := guardDeletedDependency(tx, result, o.depTable, o.issueID, o.dependsOnID, o.depType, o.targetIsIssue, o.targetIsWisp); err != nil {
				return err
			}
			removed++
			if showIndividual {
				fmt.Printf("  Removed orphaned dependency: %s→%s\n", o.issueID, o.dependsOnID)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit orphaned dependency removals: %w", err)
	}

	// Commit changes in Dolt
	_, _ = db.Exec("CALL DOLT_COMMIT('-Am', 'doctor: remove orphaned dependencies')") // Best effort: commit advisory; schema fix already applied in-memory

	fmt.Printf("  Fixed %d orphaned dependency reference(s)\n", removed)
	return nil
}

// ChildParentDependencies removes child→parent blocking dependencies.
// These often indicate a modeling mistake (deadlock: child waits for parent, parent waits for children).
// Requires explicit opt-in via --fix-child-parent flag since some workflows may use these intentionally.
// If verbose is true, prints each removed dependency; otherwise shows only summary.
func ChildParentDependencies(path string, verbose bool) error {
	beadsDir, err := resolvedWorkspaceBeadsDir(path)
	if err != nil {
		return err
	}

	db, err := openDoltDB(beadsDir)
	if err != nil {
		fmt.Printf("  Child-parent dependencies fix skipped (%v)\n", err)
		return nil
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Find child→parent BLOCKING dependencies where issue_id starts with depends_on_id + "."
	// Only matches blocking types (blocks, conditional-blocks, waits-for) that cause deadlock.
	// Excludes 'parent-child' type which is a legitimate structural hierarchy relationship.
	//nolint:gosec // G202: fixDependencyUnionSQL returns a fixed internal SELECT fragment.
	query := `
		SELECT d.dep_table, d.id, d.issue_id, d.depends_on_id, d.type, d.target_is_issue, d.target_is_wisp
		FROM (` + fixDependencyUnionSQL() + `) d
		WHERE d.issue_id LIKE CONCAT(d.depends_on_id, '.%')
		  AND d.type IN ('blocks', 'conditional-blocks', 'waits-for')
	`
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query child-parent dependencies: %w", err)
	}
	defer rows.Close()

	type badDep struct {
		depTable, id, issueID, dependsOnID, depType string
		targetIsIssue, targetIsWisp                 bool
	}
	var badDeps []badDep

	for rows.Next() {
		var d badDep
		if err := rows.Scan(&d.depTable, &d.id, &d.issueID, &d.dependsOnID, &d.depType, &d.targetIsIssue, &d.targetIsWisp); err == nil {
			badDeps = append(badDeps, d)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	_ = rows.Close()

	if len(badDeps) == 0 {
		fmt.Println("  No child→parent dependencies to fix")
		return nil
	}

	// Delete child→parent blocking dependencies (preserving parent-child type)
	// Uses explicit transaction so writes persist when @@autocommit is OFF
	// (e.g. Dolt server started with --no-auto-commit).
	showIndividual := verbose || len(badDeps) < 20
	var removed int
	for _, d := range badDeps {
		var result sql.Result
		var err error
		switch d.depTable {
		case "dependencies":
			result, err = tx.Exec("DELETE FROM dependencies WHERE id = ?", d.id)
		case "wisp_dependencies":
			result, err = tx.Exec("DELETE FROM wisp_dependencies WHERE id = ?", d.id)
		default:
			fmt.Printf("  Warning: skipped child→parent dependency from unexpected table %s\n", d.depTable)
			continue
		}
		if err != nil {
			fmt.Printf("  Warning: failed to remove %s→%s: %v\n", d.issueID, d.dependsOnID, err)
		} else {
			if err := guardDeletedDependency(tx, result, d.depTable, d.issueID, d.dependsOnID, d.depType, d.targetIsIssue, d.targetIsWisp); err != nil {
				return err
			}
			removed++
			if showIndividual {
				fmt.Printf("  Removed child→parent dependency: %s→%s\n", d.issueID, d.dependsOnID)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit dependency removals: %w", err)
	}

	// Commit changes in Dolt
	_, _ = db.Exec("CALL DOLT_COMMIT('-Am', 'doctor: remove child-parent dependency anti-patterns')") // Best effort: commit advisory; schema fix already applied in-memory

	fmt.Printf("  Fixed %d child→parent dependency anti-pattern(s)\n", removed)
	return nil
}

func guardDeletedDependency(tx *sql.Tx, result sql.Result, table, source, target, depType string, targetIsIssue, targetIsWisp bool) error {
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	kind := issueops.DepTargetExternal
	if targetIsIssue {
		kind = issueops.DepTargetIssue
	} else if targetIsWisp {
		kind = issueops.DepTargetWisp
	}
	return issueops.BumpEdgeCloseGuardsInTx(context.Background(), tx, source, target, table == "wisp_dependencies", kind, types.DependencyType(depType))
}

// openDoltDB opens a Dolt database connection via MySQL protocol.
// Delegates to openFixDB for DSN construction (timeout + password support).
func openDoltDB(beadsDir string) (*sql.DB, error) {
	cfg, err := configfile.Load(beadsDir)
	if err != nil || cfg == nil {
		return nil, fmt.Errorf("no database configuration found")
	}

	db, err := openFixDB(beadsDir, cfg)
	if err != nil {
		return nil, fmt.Errorf("dolt server connection failed: %w", err)
	}

	// Verify the connection actually works
	if err := db.Ping(); err != nil {
		_ = db.Close() // Best effort cleanup
		return nil, fmt.Errorf("dolt server not reachable: %w", err)
	}

	return db, nil
}
