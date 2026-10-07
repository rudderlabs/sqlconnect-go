package fabric

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockColumnType struct{ databaseTypeName string }

func (m mockColumnType) DatabaseTypeName() string                       { return m.databaseTypeName }
func (m mockColumnType) DecimalSize() (precision, scale int64, ok bool) { return 0, 0, false }

func TestColumnTypeMapper(t *testing.T) {
	tests := map[string]string{
		"bigint": "int", "tinyint": "int", "decimal(28,10)": "float", "smallmoney": "float", "BIT": "boolean",
		"binary(16)": "string", "nvarchar(255)": "string", "datetime2(6)": "datetime", "smalldatetime": "datetime", "integer": "INTEGER", "geography": "GEOGRAPHY",
	}
	for raw, expected := range tests {
		require.Equal(t, expected, columnTypeMapper(mockColumnType{raw}), raw)
	}
}

func TestJSONRowMapper(t *testing.T) {
	now := time.Now()
	require.Equal(t, "text", jsonRowMapper("NVARCHAR", []byte("text")))
	require.Equal(t, "00112233-4455-6677-8899-AABBCCDDEEFF", jsonRowMapper("UNIQUEIDENTIFIER", []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}))
	require.Equal(t, 12.5, jsonRowMapper("DECIMAL", "12.5"))
	require.Equal(t, 12.5, jsonRowMapper("NUMERIC(10,2)", []byte("12.5")))
	require.Equal(t, 1.5, jsonRowMapper("SMALLMONEY", "1.5000"))
	require.Equal(t, "binary", jsonRowMapper("BINARY(16)", []byte("binary")))
	require.Equal(t, "not-a-number", jsonRowMapper("NUMERIC", "not-a-number"))
	require.Equal(t, now, jsonRowMapper("DATETIME2", now))
	require.Nil(t, jsonRowMapper("VARCHAR", nil))
}
