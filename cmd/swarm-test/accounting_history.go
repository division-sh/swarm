package main

import (
	"context"
	"fmt"
	"sort"
)

// Copying an authenticated read cut preserves input availability, not execution
// authority. The frozen executor still replays the entire untrusted plan/receipt.
func accountingProofReadCuts(ctx context.Context, root, head, origin string, policy accountingPolicyObservation, args []string) ([]string, error) {
	git := func(args ...string) ([]byte, error) { return accountingGit(ctx, root, args...) }
	base, _, err := derivePreflightComparisonUsing(git, head, origin)
	if err != nil {
		return nil, err
	}
	if err = checkAccountingBase(ctx, root, base, policy); err != nil {
		return nil, err
	}
	planFile, receiptFile, err := accountingInputArguments(args)
	if err != nil {
		return nil, err
	}
	cuts := []string{base}
	if args[0] == "plan" {
		return cuts, nil
	}
	var plan accountingPlan
	if receiptFile != "" {
		var receipt accountingCertificate
		if err = readAccountingJSON(receiptFile, &receipt); err != nil {
			return nil, err
		}
		if receipt.Version != 1 || receipt.Head != head {
			return nil, fmt.Errorf("accounting receipt requests a foreign source cut")
		}
		if err = verifyAccountingPolicyHistory(ctx, root, receipt.Policy, policy); err != nil {
			return nil, err
		}
		plan = receipt.Plan
		cuts = append(cuts, receipt.Policy.Commit)
	} else if err = readAccountingJSON(planFile, &plan); err != nil {
		return nil, err
	}
	if err = admitAccountingPlanCuts(ctx, root, head, args[0], plan, policy); err != nil {
		return nil, err
	}
	cuts = append(cuts, plan.Base, plan.Head, plan.Policy.Commit)
	sort.Strings(cuts)
	return cuts, nil
}

func admitAccountingPlanCuts(ctx context.Context, root, head, operation string, plan accountingPlan, policy accountingPolicyObservation) error {
	if plan.Version != 1 || !hexIdentity(plan.Base, 40) || !hexIdentity(plan.Head, 40) || (operation == "apply" && head != plan.Head) {
		return fmt.Errorf("accounting plan requests an invalid or foreign source cut")
	}
	if err := verifyAccountingPolicyHistory(ctx, root, plan.Policy, policy); err != nil {
		return err
	}
	if err := checkAccountingBase(ctx, root, plan.Base, policy); err != nil {
		return err
	}
	for _, pair := range [][2]string{{plan.Base, plan.Head}, {plan.Head, head}} {
		if _, err := accountingGit(ctx, root, "merge-base", "--is-ancestor", pair[0], pair[1]); err != nil {
			return fmt.Errorf("accounting replay cut is missing or outside source ancestry: %w", err)
		}
	}
	return nil
}
