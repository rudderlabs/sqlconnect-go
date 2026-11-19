package mcerr

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestStreamCorruptClassification(t *testing.T) {
	for msg := range streamIntegrityMessages {
		t.Run(msg, func(t *testing.T) {
			// The SDK returns errors.New(msg), often wrapped in WithStack.
			for _, err := range []error{pkgerrors.New(msg), pkgerrors.WithStack(pkgerrors.New(msg)), fmt.Errorf("read: %w", pkgerrors.New(msg))} {
				require.True(t, IsStreamCorrupt(err))
				e := FromSDK(err)
				require.Equal(t, CategoryNetwork, e.Category)
				require.Equal(t, CodeStreamCorrupt, e.Code)
				require.True(t, e.Retryable)
				require.False(t, e.OutcomeUnknown)
				require.Contains(t, e.Message, msg)
			}
		})
	}

	t.Run("a deflate stream that does not decode", func(t *testing.T) {
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, _ = w.Write(bytes.Repeat([]byte("rudderstack "), 200))
		require.NoError(t, w.Close())
		good := buf.Bytes()

		corruptBody := append([]byte(nil), good...)
		corruptBody[len(corruptBody)/2] ^= 0xff
		corruptSum := append([]byte(nil), good...)
		corruptSum[len(corruptSum)-1] ^= 0xff

		for name, in := range map[string][]byte{
			"bad header":   []byte("not zlib at all"),
			"bad body":     corruptBody,
			"bad checksum": corruptSum,
		} {
			t.Run(name, func(t *testing.T) {
				err := readZlib(in)
				require.Error(t, err)
				require.True(t, IsStreamCorrupt(err), "%v", err)
				require.Equal(t, CodeStreamCorrupt, FromSDK(err).Code)
			})
		}

		t.Run("a cut deflate stream is a transport error, not corruption", func(t *testing.T) {
			err := readZlib(good[:len(good)/2])
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.False(t, IsStreamCorrupt(err))
			e := FromSDK(err)
			require.Equal(t, CategoryNetwork, e.Category)
			require.Empty(t, e.Code)
			require.True(t, e.Retryable)
		})
	})

	t.Run("a typed flate error", func(t *testing.T) {
		require.True(t, IsStreamCorrupt(pkgerrors.WithStack(flate.CorruptInputError(7))))
	})

	t.Run("other errors are not stream corruption", func(t *testing.T) {
		for _, err := range []error{nil, io.EOF, io.ErrUnexpectedEOF, pkgerrors.New("invalid stream id in the request"), Local(CategoryData, "crc value is error x")} {
			require.False(t, IsStreamCorrupt(err), "%v", err)
		}
	})
}

func readZlib(in []byte) error {
	r, err := zlib.NewReader(bytes.NewReader(in))
	if err != nil {
		return err
	}
	_, err = io.ReadAll(r)
	return err
}

// TestStreamIntegrityMessagesMatchTheSDK pins the matched texts to the SDK
// source: an SDK upgrade that rewords one fails here instead of turning a
// retryable corruption into a permanent unknown error.
func TestStreamIntegrityMessagesMatchTheSDK(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/aliyun/aliyun-odps-go-sdk").Output()
	if err != nil {
		t.Skipf("go list: %v", err)
	}
	dir := filepath.Join(strings.TrimSpace(string(out)), "odps", "tunnel")
	var src strings.Builder
	for _, f := range []string{"record_protoc_reader.go", "protoc_stream_reader.go"} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		require.NoError(t, err)
		src.Write(b)
	}
	for msg := range streamIntegrityMessages {
		require.Contains(t, src.String(), `errors.New("`+msg+`")`, "the SDK no longer raises %q", msg)
	}
}
