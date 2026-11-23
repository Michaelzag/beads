package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

var closeClaimReceipt storage.CloseClaimReceipt

var closeClaimCmd = &cobra.Command{
	Use:          "close-claim ID --expected-assignee HOLDER --expected-acceptance-digest sha256:HEX --expected-repo-labels-digest sha256:HEX --operation-id tracker-close:HEX --reason REASON [--receipt-only | --refuse-if-missing]",
	GroupID:      "issues",
	Short:        "Conditionally close a tracker claim and record its outcome",
	Hidden:       true,
	SilenceUsage: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.ExactArgs(1)(cmd, args); err != nil {
			return err
		}
		return issueops.ValidateCloseClaimIntent(closeClaimIntentFromFlags(cmd, args[0]))
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		receiptOnly, _ := cmd.Flags().GetBool("receipt-only")
		refuseIfMissing, _ := cmd.Flags().GetBool("refuse-if-missing")
		mode := storage.CloseClaimExecute
		if receiptOnly {
			mode = storage.CloseClaimLookup
		} else {
			CheckReadonly("close-claim")
			if refuseIfMissing {
				mode = storage.CloseClaimRefuseIfMissing
			}
		}
		closer, ok := storage.UnwrapStore(store).(storage.CloseClaimStore)
		if !ok {
			return fmt.Errorf("configured store does not support close-claim")
		}

		// The helper sees the in-transaction issue. Workflow discovery
		// must not update await_id on a separate connection.
		receipt, err := closer.CloseClaim(rootCtx, closeClaimIntentFromFlags(cmd, args[0]), actor, mode, func(issue *types.Issue) error {
			return checkGateSatisfactionWithRunIDPersistence(issue, false)
		})
		if err != nil {
			return fmt.Errorf("closing claim on %s: %w", args[0], err)
		}
		closeClaimReceipt = receipt
		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		// The store already committed any new receipt. Finish root cleanup
		// before emitting an outcome, including a refusal or missing lookup.
		cmd.Root().PersistentPostRun(cmd, args)
		if jsonOutput {
			outputJSONRaw(closeClaimReceipt)
			return nil
		}
		if closeClaimReceipt.Reason != "" {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %s (%s)\n", closeClaimReceipt.WorkItemID, closeClaimReceipt.Outcome, closeClaimReceipt.Reason)
			return err
		}
		_, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", closeClaimReceipt.WorkItemID, closeClaimReceipt.Outcome)
		return err
	},
}

func closeClaimIntentFromFlags(cmd *cobra.Command, id string) storage.CloseClaimIntent {
	assignee, _ := cmd.Flags().GetString("expected-assignee")
	acceptance, _ := cmd.Flags().GetString("expected-acceptance-digest")
	repoLabels, _ := cmd.Flags().GetString("expected-repo-labels-digest")
	operationID, _ := cmd.Flags().GetString("operation-id")
	reason, _ := cmd.Flags().GetString("reason")
	return storage.CloseClaimIntent{
		WorkItemID:                id,
		ExpectedAssignee:          assignee,
		ExpectedAcceptanceDigest: acceptance,
		ExpectedRepoLabelsDigest:  repoLabels,
		OperationID:               operationID,
		CloseReason:               reason,
	}
}

func init() {
	closeClaimCmd.Flags().String("expected-assignee", "", "Exact expected assignee (required)")
	closeClaimCmd.Flags().String("expected-acceptance-digest", "", "Digest of the exact expected acceptance text (required)")
	closeClaimCmd.Flags().String("expected-repo-labels-digest", "", "Digest of the exact expected repo labels (required)")
	closeClaimCmd.Flags().String("operation-id", "", "Tracker close operation ID (required)")
	closeClaimCmd.Flags().String("reason", "", "Canonical tracker handoff reason (required)")
	closeClaimCmd.Flags().Bool("receipt-only", false, "Look up a committed close receipt without writing")
	closeClaimCmd.Flags().Bool("refuse-if-missing", false, "Record a terminal policy refusal if no committed receipt exists")
	closeClaimCmd.MarkFlagsMutuallyExclusive("receipt-only", "refuse-if-missing")
	rootCmd.AddCommand(closeClaimCmd)
}
