package chpolicy

import "testing"

// ResetForTest clears the installed policy, so the install-once check holds
// under go test -count=N. It panics outside a test binary, so production code
// can never clear the dial policy.
func ResetForTest() {
	if !testing.Testing() {
		panic("chpolicy: ResetForTest called outside a test binary")
	}
	mu.Lock()
	defer mu.Unlock()
	current, set = Policy{}, false
}
