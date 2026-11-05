package mab

import (
	"fmt"
)

// ApplyWeakMABPrior applies an already-built weak donor prior to a fresh target
// policy.
//
// The operation only changes the target's decision state:
//   - UCB1 keeps coupled prior weight/reward separate from Count/SumRewards/Real*;
//   - UCB1Decoupled keeps reward prior weight, exploration prior weight and
//     reward sum separate from Count/SumRewards/Real*;
//   - LinUCB adds the prior contribution to live A and b while keeping it
//     separate from RealAContribution/RealBContribution.
//
// Consequently, a received prior can influence the target's early decisions
// without ever being re-exported as real target knowledge.
func ApplyWeakMABPrior(target Policy, prior WeakMABPrior) error {
	if target == nil {
		return fmt.Errorf("target MAB policy cannot be nil")
	}

	if err := validateWeakMABPriorForApplication(prior); err != nil {
		return err
	}

	if target.GetType() != prior.Policy {
		return fmt.Errorf("weak prior policy %q does not match target policy %q", prior.Policy, target.GetType())
	}

	switch typed := target.(type) {
	case *UCB1Bandit:
		if typed == nil {
			return fmt.Errorf("target UCB1 policy cannot be nil")
		}
		if typed.FunctionName == prior.DonorFunctionName {
			return fmt.Errorf("weak prior donor and target function must be different")
		}
		return typed.applyWeakPrior(prior)

	case *UCB1DecoupledBandit:
		if typed == nil {
			return fmt.Errorf("target UCB1Decoupled policy cannot be nil")
		}
		if typed.FunctionName == prior.DonorFunctionName {
			return fmt.Errorf("weak prior donor and target function must be different")
		}
		return typed.applyWeakPrior(prior)

	default:
		return fmt.Errorf("unsupported target MAB policy type %T", target)
	}
}

func (b *UCB1Bandit) applyWeakPrior(prior WeakMABPrior) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.validateFreshTargetForWeakPriorLocked(); err != nil {
		return err
	}

	for arm, priorArm := range prior.Arms {
		if !priorArm.Transferred {
			continue
		}
		if _, exists := b.Arms[arm]; !exists {
			return fmt.Errorf("weak prior arm %q is not initialized in target UCB1 policy", arm)
		}
	}

	if !prior.HasPrior {
		return nil
	}

	for arm, priorArm := range prior.Arms {
		if !priorArm.Transferred {
			continue
		}

		payload := priorArm.UCB1
		stats := b.Arms[arm]
		stats.PriorObservationWeight = payload.ObservationWeight
		// Legacy UCB1 is coupled by definition. Keep the exploration weight
		// recorded for provenance, although bandit_ucb1.go intentionally uses
		// PriorObservationWeight for both roles.
		stats.PriorExplorationObservationWeight = payload.ExplorationObservationWeight
		stats.PriorRewardSum = payload.RewardSum
	}

	b.PriorDonorFunctionName = prior.DonorFunctionName
	return nil
}

func (b *UCB1Bandit) validateFreshTargetForWeakPriorLocked() error {
	if b.PriorDonorFunctionName != "" {
		return fmt.Errorf("target UCB1 policy already has a weak prior from donor %q", b.PriorDonorFunctionName)
	}

	if b.TotalCounts != 0 || b.TotalInFlight != 0 {
		return fmt.Errorf("weak prior can only be applied to a fresh UCB1 target")
	}

	for arm, stats := range b.Arms {
		if stats == nil {
			return fmt.Errorf("target UCB1 arm %q has nil state", arm)
		}

		if stats.Count != 0 ||
			stats.SumRewards != 0.0 ||
			stats.AvgReward != 0.0 ||
			stats.InFlight != 0 ||
			stats.RealCount != 0 ||
			stats.RealSumRewards != 0.0 ||
			stats.RealAvgReward != 0.0 ||
			stats.SyntheticCount != 0 ||
			stats.PriorObservationWeight != 0.0 ||
			stats.PriorExplorationObservationWeight != 0.0 ||
			stats.PriorRewardSum != 0.0 {

			return fmt.Errorf("weak prior can only be applied to a fresh UCB1 target: arm %q already contains learning state", arm)
		}
	}

	return nil
}

func (b *UCB1DecoupledBandit) applyWeakPrior(prior WeakMABPrior) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.validateFreshTargetForWeakPriorLocked(); err != nil {
		return err
	}

	for arm, priorArm := range prior.Arms {
		if !priorArm.Transferred {
			continue
		}
		if _, exists := b.Arms[arm]; !exists {
			return fmt.Errorf("weak prior arm %q is not initialized in target UCB1Decoupled policy", arm)
		}
	}

	if !prior.HasPrior {
		return nil
	}

	for arm, priorArm := range prior.Arms {
		if !priorArm.Transferred {
			continue
		}

		payload := priorArm.UCB1
		stats := b.Arms[arm]
		stats.PriorObservationWeight = payload.ObservationWeight
		stats.PriorExplorationObservationWeight = payload.ExplorationObservationWeight
		stats.PriorRewardSum = payload.RewardSum
	}

	b.PriorDonorFunctionName = prior.DonorFunctionName
	return nil
}

func (b *UCB1DecoupledBandit) validateFreshTargetForWeakPriorLocked() error {
	if b.PriorDonorFunctionName != "" {
		return fmt.Errorf("target UCB1Decoupled policy already has a weak prior from donor %q", b.PriorDonorFunctionName)
	}

	if b.TotalCounts != 0 || b.TotalInFlight != 0 {
		return fmt.Errorf("weak prior can only be applied to a fresh UCB1Decoupled target")
	}

	for arm, stats := range b.Arms {
		if stats == nil {
			return fmt.Errorf("target UCB1Decoupled arm %q has nil state", arm)
		}

		if stats.Count != 0 ||
			stats.SumRewards != 0.0 ||
			stats.AvgReward != 0.0 ||
			stats.InFlight != 0 ||
			stats.RealCount != 0 ||
			stats.RealSumRewards != 0.0 ||
			stats.RealAvgReward != 0.0 ||
			stats.SyntheticCount != 0 ||
			stats.PriorObservationWeight != 0.0 ||
			stats.PriorExplorationObservationWeight != 0.0 ||
			stats.PriorRewardSum != 0.0 {

			return fmt.Errorf("weak prior can only be applied to a fresh UCB1Decoupled target: arm %q already contains learning state", arm)
		}
	}

	return nil
}

func validateWeakMABPriorForApplication(prior WeakMABPrior) error {
	if prior.SchemaVersion != WeakMABPriorSchemaVersion {
		return fmt.Errorf("unsupported weak MAB prior schema version %d", prior.SchemaVersion)
	}

	if prior.DonorFunctionName == "" {
		return fmt.Errorf("weak prior donor function name cannot be empty")
	}

	if err := validateWeakMABPriorConfig(prior.Config); err != nil {
		return err
	}
	if err := validateWeakMABPriorConfigForPolicy(prior.Config, prior.Policy); err != nil {
		return err
	}

	switch prior.Policy {
	case UCB1, UCB1Decoupled:
		// Supported.
	default:
		return fmt.Errorf("unsupported weak prior policy %q", prior.Policy)
	}

	if prior.SourceRealObservationCount < 0 || prior.SourceExcludedSyntheticObservationCount < 0 {
		return fmt.Errorf("weak prior source observation summaries cannot be negative")
	}

	if prior.ArmCount != len(prior.Arms) {
		return fmt.Errorf("weak prior arm summary mismatch: arms=%d summary=%d", len(prior.Arms), prior.ArmCount)
	}

	var totalReal int64
	var totalSynthetic int64
	transferred := 0
	skipped := 0

	for arm, priorArm := range prior.Arms {
		if arm == "" {
			return fmt.Errorf("weak prior arm name cannot be empty")
		}

		if priorArm.SourceRealObservationCount < 0 || priorArm.SourceExcludedSyntheticObservationCount < 0 {
			return fmt.Errorf("weak prior arm %q has negative source observation counts", arm)
		}

		totalReal += priorArm.SourceRealObservationCount
		totalSynthetic += priorArm.SourceExcludedSyntheticObservationCount

		if priorArm.Transferred {
			transferred++
			if err := validateTransferredWeakPriorArm(arm, prior.Policy, prior.Config, priorArm); err != nil {
				return err
			}
			continue
		}

		skipped++
		if err := validateSkippedWeakPriorArm(arm, prior.Config, priorArm); err != nil {
			return err
		}
	}

	if totalReal != prior.SourceRealObservationCount {
		return fmt.Errorf("weak prior source real observation summary mismatch: arms=%d summary=%d", totalReal, prior.SourceRealObservationCount)
	}
	if totalSynthetic != prior.SourceExcludedSyntheticObservationCount {
		return fmt.Errorf("weak prior source synthetic observation summary mismatch: arms=%d summary=%d", totalSynthetic, prior.SourceExcludedSyntheticObservationCount)
	}
	if transferred != prior.TransferredArmCount || skipped != prior.SkippedArmCount || transferred+skipped != prior.ArmCount {
		return fmt.Errorf("weak prior transferred/skipped arm summary mismatch")
	}
	if prior.HasPrior != (transferred > 0) {
		return fmt.Errorf("weak prior has_prior does not match transferred arm count")
	}

	return nil
}

func validateTransferredWeakPriorArm(arm string, policy BanditType, config WeakMABPriorConfig, priorArm WeakMABArmPrior) error {
	if priorArm.SkipReason != "" {
		return fmt.Errorf("transferred weak prior arm %q cannot have a skip reason", arm)
	}

	weights, err := resolveWeakPriorWeights(config, policy)
	if err != nil {
		return err
	}

	rewardWeight := priorArm.AppliedEquivalentObservationWeight
	if !isFiniteNumber(rewardWeight) || rewardWeight <= 0.0 || rewardWeight > weights.reward || rewardWeight > float64(priorArm.SourceRealObservationCount) {
		return fmt.Errorf("weak prior arm %q has invalid applied reward observation weight", arm)
	}

	if priorArm.SourceRealObservationCount < config.MinRealObservationsPerArm {
		return fmt.Errorf("weak prior arm %q was transferred without sufficient real evidence", arm)
	}

	expectedScale := rewardWeight / float64(priorArm.SourceRealObservationCount)
	if !isFiniteNumber(priorArm.AttenuationScale) || !weakPriorAlmostEqual(expectedScale, priorArm.AttenuationScale) {
		return fmt.Errorf("weak prior arm %q has inconsistent attenuation scale", arm)
	}

	switch policy {
	case UCB1, UCB1Decoupled:
		if priorArm.UCB1 == nil {
			return fmt.Errorf("weak prior arm %q has invalid UCB1 payload", arm)
		}

		explorationWeight := priorArm.AppliedExplorationObservationWeight
		if !isFiniteNumber(explorationWeight) || explorationWeight <= 0.0 || explorationWeight > weights.exploration ||
			(policy == UCB1 && explorationWeight > float64(priorArm.SourceRealObservationCount)) {
			return fmt.Errorf("weak prior arm %q has invalid applied exploration observation weight", arm)
		}

		payload := priorArm.UCB1
		if !isFiniteNumber(payload.ObservationWeight) ||
			!isFiniteNumber(payload.ExplorationObservationWeight) ||
			!isFiniteNumber(payload.RewardSum) ||
			!isFiniteNumber(payload.MeanReward) ||
			!weakPriorAlmostEqual(payload.ObservationWeight, rewardWeight) ||
			!weakPriorAlmostEqual(payload.ExplorationObservationWeight, explorationWeight) {
			return fmt.Errorf("weak prior arm %q has invalid UCB1 statistics", arm)
		}

		expectedMean := payload.RewardSum / payload.ObservationWeight
		if !weakPriorAlmostEqual(expectedMean, payload.MeanReward) {
			return fmt.Errorf("weak prior arm %q has inconsistent UCB1 mean reward", arm)
		}

		if policy == UCB1 && !weakPriorAlmostEqual(payload.ObservationWeight, payload.ExplorationObservationWeight) {
			return fmt.Errorf("legacy UCB1 prior arm %q must use coupled reward/exploration weights", arm)
		}
	}

	return nil
}

func validateSkippedWeakPriorArm(arm string, config WeakMABPriorConfig, priorArm WeakMABArmPrior) error {
	if priorArm.UCB1 != nil ||
		priorArm.AppliedEquivalentObservationWeight != 0.0 ||
		priorArm.AppliedExplorationObservationWeight != 0.0 ||
		priorArm.AttenuationScale != 0.0 {
		return fmt.Errorf("skipped weak prior arm %q contains transferable payload", arm)
	}

	switch priorArm.SkipReason {
	case WeakPriorSkipNoRealObservations:
		if priorArm.SourceRealObservationCount != 0 {
			return fmt.Errorf("weak prior arm %q has inconsistent no-real-observations skip reason", arm)
		}

	case WeakPriorSkipInsufficientRealObservations:
		if priorArm.SourceRealObservationCount <= 0 || priorArm.SourceRealObservationCount >= config.MinRealObservationsPerArm {
			return fmt.Errorf("weak prior arm %q has inconsistent insufficient-evidence skip reason", arm)
		}

	default:
		return fmt.Errorf("skipped weak prior arm %q has invalid skip reason %q", arm, priorArm.SkipReason)
	}

	return nil
}
