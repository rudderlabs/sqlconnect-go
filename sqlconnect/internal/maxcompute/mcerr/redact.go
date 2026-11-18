package mcerr

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// The source's redaction beyond the destination's SafeError patterns: STS
// token ids, the two forms in which MaxCompute echoes a key id, and Context
// IDs. The destination's opaque-key pattern covers LTAI, CAIS and AKIA only.
var (
	stsKeyPattern        = regexp.MustCompile(`\bSTS\.[A-Za-z0-9+/=._-]{4,}`)
	keyIDNotFoundPattern = regexp.MustCompile(`(?i)(accessKeyId\s+not\s+found:\s*)\S+`)
	keyIDQuotedPattern   = regexp.MustCompile(`(?i)(AccessKeyId\s*')[^']*(')`)
	keyIDFoundPattern    = regexp.MustCompile(`(?i)(accessKeyId\S*\s+found:\s*)\S+`)
	contextIDPattern     = regexp.MustCompile(`(?i)(Context\s+ID:\s*)[A-Za-z0-9-]+`)
	principalPattern     = regexp.MustCompile(`(?i)\bRAM\$[^\s;,'"]+`)
)

// maxMessageRunes caps an error message.
const maxMessageRunes = 512

// cutDebugInfo drops the base64 Java stack MaxCompute appends to some
// errors, and the tips that name the caller's principal.
func cutDebugInfo(text string) string {
	for _, marker := range []string{"DebugingInfo", "--->Tips:", "---> Tips:"} {
		if i := strings.Index(text, marker); i >= 0 {
			text = text[:i]
		}
	}
	return strings.TrimSpace(text)
}

// Redact removes key ids, STS ids, principals, ARNs, URLs and Context IDs
// from text, and caps it at 512 runes. It runs on every message after the
// destination's SafeError.
func Redact(text string) string {
	text = cutDebugInfo(text)
	text = stsKeyPattern.ReplaceAllString(text, "[credential]")
	text = keyIDNotFoundPattern.ReplaceAllString(text, "${1}[credential]")
	text = keyIDQuotedPattern.ReplaceAllString(text, "${1}[credential]${2}")
	text = keyIDFoundPattern.ReplaceAllString(text, "${1}[credential]")
	text = contextIDPattern.ReplaceAllString(text, "${1}[redacted]")
	text = principalPattern.ReplaceAllString(text, "[principal]")
	if utf8.RuneCountInString(text) > maxMessageRunes {
		text = string([]rune(text)[:maxMessageRunes])
	}
	return text
}
