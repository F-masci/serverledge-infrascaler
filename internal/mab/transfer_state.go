package mab

const TransferableMABKnowledgeSchemaVersion = 1

// TransferableMABKnowledge is a read-only snapshot of the learning state that
// can be considered for transfer to another function.
//
// In-flight selections and synthetic fallback rewards are deliberately excluded
// from the transferable statistics. The number of excluded synthetic
// observations is retained only for diagnostics and provenance.
type TransferableMABKnowledge struct {
	SchemaVersion                     int                                 `json:"schema_version"`
	FunctionName                      string                              `json:"function_name"`
	Policy                            BanditType                          `json:"policy"`
	HasRealKnowledge                  bool                                `json:"has_real_knowledge"`
	RealObservationCount              int64                               `json:"real_observation_count"`
	ExcludedSyntheticObservationCount int64                               `json:"excluded_synthetic_observation_count"`
	Arms                              map[string]TransferableArmKnowledge `json:"arms"`
}

// TransferableArmKnowledge contains the transferable reward statistics
// associated with a UCB1-family arm.
type TransferableArmKnowledge struct {
	RealObservationCount              int64                         `json:"real_observation_count"`
	ExcludedSyntheticObservationCount int64                         `json:"excluded_synthetic_observation_count"`
	UCB1                              *TransferableUCB1ArmKnowledge `json:"ucb1,omitempty"`
}

// TransferableUCB1ArmKnowledge contains only reward statistics produced by
// accepted real execution feedback.
type TransferableUCB1ArmKnowledge struct {
	RealSumRewards float64 `json:"real_sum_rewards"`
	RealAvgReward  float64 `json:"real_avg_reward"`
}

// TransferableKnowledge returns a deep read-only snapshot of the real-feedback
// knowledge accumulated by UCB1.
func (b *UCB1Bandit) TransferableKnowledge() TransferableMABKnowledge {

	b.mu.RLock()
	defer b.mu.RUnlock()
	arms := make(map[string]TransferableArmKnowledge, len(b.Arms))

	var totalReal int64
	var totalSynthetic int64

	for arm, stats := range b.Arms {
		totalReal += stats.RealCount
		totalSynthetic += stats.SyntheticCount
		arms[arm] = TransferableArmKnowledge{
			RealObservationCount:              stats.RealCount,
			ExcludedSyntheticObservationCount: stats.SyntheticCount,
			UCB1: &TransferableUCB1ArmKnowledge{
				RealSumRewards: stats.RealSumRewards,
				RealAvgReward:  stats.RealAvgReward,
			},
		}
	}

	return TransferableMABKnowledge{
		SchemaVersion:                     TransferableMABKnowledgeSchemaVersion,
		FunctionName:                      b.FunctionName,
		Policy:                            UCB1,
		HasRealKnowledge:                  totalReal > 0,
		RealObservationCount:              totalReal,
		ExcludedSyntheticObservationCount: totalSynthetic,
		Arms:                              arms,
	}
}

// SnapshotTransferableKnowledge returns the transferable state associated with
// an already existing function bandit.
//
// Unlike GetBandit, this method deliberately does not create a new policy when
// functionName is unknown.
func (bm *BanditManager) SnapshotTransferableKnowledge(functionName string) (TransferableMABKnowledge, bool) {
	bm.mu.RLock()

	bandit, exists := bm.bandits[functionName]
	bm.mu.RUnlock()

	if !exists {
		return TransferableMABKnowledge{}, false
	}

	return transferableKnowledgeFromPolicy(bandit)
}
