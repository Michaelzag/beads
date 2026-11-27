package fix

// Keep in sync with issueops.DepTargetExpr.
const fixDependencyTargetExpr = "COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external)"

const fixDependencyTargetFlags = "depends_on_issue_id IS NOT NULL AS target_is_issue, depends_on_wisp_id IS NOT NULL AS target_is_wisp"

func fixDependencyUnionSQL() string {
	return `
		SELECT 'dependencies' AS dep_table, issue_id, ` + fixDependencyTargetExpr + ` AS depends_on_id, type, ` + fixDependencyTargetFlags + `
		FROM dependencies
		UNION ALL
		SELECT 'wisp_dependencies' AS dep_table, issue_id, ` + fixDependencyTargetExpr + ` AS depends_on_id, type, ` + fixDependencyTargetFlags + `
		FROM wisp_dependencies`
}
