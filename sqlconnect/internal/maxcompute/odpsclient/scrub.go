package odpsclient

// Scrub applies the destination's redaction patterns to free text: URLs,
// bearer tokens, credential assignments, opaque key ids, RAM principals and
// ARNs. SafeError applies them only to the diagnostic it extracts; the driver
// also scrubs its own messages, which can carry caller input.
func Scrub(text string) string {
	text = odpsURLPattern.ReplaceAllString(text, "[URL]")
	text = odpsBearerPattern.ReplaceAllString(text, "[credential]")
	text = odpsCredentialPattern.ReplaceAllString(text, "[credential]")
	text = odpsOpaqueKeyPattern.ReplaceAllString(text, "[credential]")
	text = odpsRAMPrincipal.ReplaceAllString(text, "[principal]")
	text = odpsRAMArn.ReplaceAllString(text, "[principal]")
	return text
}
