package mcerr

import (
	"compress/flate"
	"compress/zlib"
	"errors"
)

// CodeStreamCorrupt marks a result stream that failed an integrity check.
const CodeStreamCorrupt = "MC_STREAM_CORRUPT"

// StreamCorrupt is a result stream that failed an integrity check after the
// driver's resumes ran out. The stream's bytes were damaged in flight (a record
// CRC, the record count or the stream checksum did not match, or the deflate
// stream did not decode), so it is a retryable network error, not a data
// error: reading the result again can succeed.
func StreamCorrupt(message string) *Error {
	return &Error{Category: CategoryNetwork, Code: CodeStreamCorrupt, Message: Redact(message), Retryable: true}
}

// streamIntegrityMessages are the fixed texts of the SDK v0.4.26 tunnel
// reader's integrity errors (tunnel/record_protoc_reader.go and
// tunnel/protoc_stream_reader.go). The SDK raises them with errors.New, so the
// text is the only way to tell them apart; stream_test pins them to the SDK.
var streamIntegrityMessages = map[string]bool{
	"crc value is error":                         true,
	"record count does not match":                true,
	"invalid stream":                             true,
	"expect end of stream, but not":              true,
	"checksum is invalid":                        true,
	"invalid protobuf tag":                       true,
	"failed to read a tag from the proto stream": true,
	"invalid bytes for varint":                   true,
}

// IsStreamCorrupt reports whether err is a result stream that failed an
// integrity check: one of the SDK reader's integrity errors, or a deflate
// stream that does not decode. A cut stream that merely ends early is a
// transport error (io.ErrUnexpectedEOF), not this.
func IsStreamCorrupt(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[flate.CorruptInputError](err); ok {
		return true
	}
	if errors.Is(err, zlib.ErrChecksum) || errors.Is(err, zlib.ErrHeader) {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if streamIntegrityMessages[e.Error()] {
			return true
		}
	}
	return false
}
