package config

// CanAcceptOffload reports whether the configured scheduling policy allows
// this node to accept horizontal edge-offloaded requests.
func CanAcceptOffload() bool {
	return GetString(SCHEDULING_POLICY, "default") != "cloudonly"
}
