package driver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/stretchr/testify/require"
)

func TestConvertValueUTC(t *testing.T) {
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)
	saved := time.Local
	time.Local = kolkata
	defer func() { time.Local = saved }()

	instant := time.Unix(1763460000, 123456789) // in time.Local, as the SDK builds it
	for _, v := range []data.Data{data.Timestamp(instant), data.DateTime(instant)} {
		got, err := convertValue(v)
		require.NoError(t, err)
		require.Equal(t, time.UTC, got.(time.Time).Location())
		require.True(t, instant.Equal(got.(time.Time)))
	}
	got, err := convertValue(data.Date(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)))
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC), got)
}

func TestConvertValue(t *testing.T) {
	vc, err := data.MakeVarChar(10, "v")
	require.NoError(t, err)
	cases := []struct {
		in   data.Data
		want any
	}{
		{nil, nil},
		{data.TinyInt(1), int64(1)},
		{data.SmallInt(2), int64(2)},
		{data.Int(3), int64(3)},
		{data.BigInt(4), int64(4)},
		{data.Double(1.1), 1.1},
		{data.NewDecimal(38, 18, "12345678901234567890.123456789012345678"), "12345678901234567890.123456789012345678"},
		{data.String("s"), "s"},
		{vc, "v"},
		{data.Bool(true), true},
		{data.Binary{1, 2}, []byte{1, 2}},
		{&data.Json{Data: `{"a":1}`, Valid: true}, []byte(`{"a":1}`)},
	}
	for _, c := range cases {
		got, err := convertValue(c.in)
		require.NoError(t, err, "%#v", c.in)
		require.Equal(t, c.want, got, "%#v", c.in)
	}

	_, err = convertValue(data.VarChar{})
	require.ErrorIs(t, err, errCharOverflow, "the SDK's emptied VARCHAR is refused, not returned as an empty string")
	_, err = convertValue(data.Char{})
	require.ErrorIs(t, err, errCharOverflow)
}

func TestConvertComplex(t *testing.T) {
	arr := data.NewArrayWithType(datatype.NewArrayType(datatype.BigIntType))
	arr.UnSafeAppend(data.BigInt(1), nil, data.BigInt(3))
	got, err := convertValue(arr)
	require.NoError(t, err)
	require.Equal(t, `[1,null,3]`, string(got.([]byte)))

	st := data.NewStructWithTyp(datatype.NewStructType(
		datatype.NewStructFieldType("z", datatype.StringType),
		datatype.NewStructFieldType("a", datatype.NewDecimalType(10, 2)),
		datatype.NewStructFieldType("t", datatype.TimestampType),
	))
	require.NoError(t, st.SetField("z", data.String("last")))
	require.NoError(t, st.SetField("a", data.NewDecimal(10, 2, "1.10")))
	require.NoError(t, st.SetField("t", data.Timestamp(time.Unix(0, 5).In(time.FixedZone("x", 3600)))))
	got, err = convertValue(st)
	require.NoError(t, err)
	require.Equal(t, `{"z":"last","a":1.10,"t":"1970-01-01T00:00:00.000000005Z"}`, string(got.([]byte)), "field order, decimal digits and UTC kept")

	m := data.NewMapWithType(datatype.NewMapType(datatype.BigIntType, datatype.StringType))
	require.NoError(t, m.SafeSet(data.BigInt(2), data.String("b")))
	require.NoError(t, m.SafeSet(data.BigInt(1), data.String("a")))
	got, err = convertValue(m)
	require.NoError(t, err)
	require.Equal(t, `{"1":"a","2":"b"}`, string(got.([]byte)))
	require.True(t, json.Valid(got.([]byte)))
}

func TestCheckTunnelEndpoint(t *testing.T) {
	require.NoError(t, checkTunnelEndpoint("https://dt.me-east-1.maxcompute.aliyun.com"))
	require.NoError(t, checkTunnelEndpoint("https://dt.cn-shanghai-intranet.maxcompute.aliyun-inc.aliyuncs.com"))
	for _, bad := range []string{"http://dt.me-east-1.maxcompute.aliyun.com", "https://evil.example.com", "https://u:p@dt.aliyun.com", "https://aliyun.com.evil.io", "::"} {
		require.Error(t, checkTunnelEndpoint(bad), bad)
	}
}
