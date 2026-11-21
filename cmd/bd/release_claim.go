package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

type claimReleaser interface {
	ReleaseClaimWithResult(ctx context.Context, id, expectedAssignee, operationID, actor string) (changed bool, err error)
}

var (
	releaseClaimAssigneePattern  = regexp.MustCompile(`^zagcode-attempt-[0-9a-f]{24}$`)
	releaseClaimOperationPattern = regexp.MustCompile(`^tracker-release:[0-9a-f]{64}$`)
)

var releaseClaimCmd = &cobra.Command{
	Use:          "release-claim ID --expected-assignee HOLDER --operation-id OP",
	GroupID:      "issues",
	Short:        "Release a tracker claim held by the expected assignee",
	SilenceUsage: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.ExactArgs(1)(cmd, args); err != nil {
			return err
		}
		if id := strings.TrimSpace(args[0]); id == "" || id != args[0] {
			return fmt.Errorf("a full issue ID without surrounding whitespace is required")
		}
		expectedAssignee, _ := cmd.Flags().GetString("expected-assignee")
		if !releaseClaimAssigneePattern.MatchString(expectedAssignee) {
			return fmt.Errorf("--expected-assignee must be zagcode-attempt- followed by 24 lowercase hexadecimal digits")
		}
		operationID, _ := cmd.Flags().GetString("operation-id")
		if !releaseClaimOperationPattern.MatchString(operationID) {
			return fmt.Errorf("--operation-id must be tracker-release: followed by 64 lowercase hexadecimal digits")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		CheckReadonly("release-claim")

		releaser, ok := store.(claimReleaser)
		if !ok {
			return fmt.Errorf("configured store does not support release-claim")
		}

		expectedAssignee, _ := cmd.Flags().GetString("expected-assignee")
		operationID, _ := cmd.Flags().GetString("operation-id")
		changed, err := releaser.ReleaseClaimWithResult(rootCtx, args[0], expectedAssignee, operationID, actor)
		if err != nil {
			return fmt.Errorf("releasing claim on %s: %w", args[0], err)
		}
		commandDidWrite.Store(changed)
		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		// Finish the normal write lifecycle before emitting any success receipt.
		cmd.Root().PersistentPostRun(cmd, args)

		if jsonOutput {
			expectedAssignee, _ := cmd.Flags().GetString("expected-assignee")
			operationID, _ := cmd.Flags().GetString("operation-id")
			outputJSONRaw(map[string]string{
				"outcome":           "released",
				"work_item_id":      args[0],
				"expected_assignee": expectedAssignee,
				"operation_id":      operationID,
			})
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Released claim on %s\n", args[0])
		}
	},
}

func init() {
	releaseClaimCmd.Flags().String("expected-assignee", "", "Exact tracker attempt assignee (required)")
	releaseClaimCmd.Flags().String("operation-id", "", "Tracker release operation ID (required)")
	releaseClaimCmd.ValidArgsFunction = issueIDCompletion
	rootCmd.AddCommand(releaseClaimCmd)
}
