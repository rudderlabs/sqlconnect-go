package chpolicy

// ResetForTest clears the installed policy. Tests call it so the install-once
// check holds under go test -count=N; production code never calls it.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()
	current, set = Policy{}, false
}
