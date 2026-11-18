package driver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// convertValue converts one SDK value to a database/sql driver value:
//   - integers: int64; FLOAT, DOUBLE: float64 (never through the SDK's lossy
//     String())
//   - DECIMAL: the exact digits as a string
//   - STRING, VARCHAR, CHAR: string
//   - BOOLEAN: bool
//   - DATE: UTC midnight; DATETIME, TIMESTAMP: the instant in UTC (the SDK
//     returns the process-local zone); TIMESTAMP_NTZ: the wall time as UTC
//   - BINARY, GEOGRAPHY: []byte
//   - JSON, ARRAY, MAP, STRUCT: JSON text as []byte, built from the typed
//     values, never from the SDK's String()
//   - NULL: nil
//
//nolint:nilnil // a SQL NULL is a nil value, not a missing one
func convertValue(v data.Data) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case data.TinyInt:
		return int64(x), nil
	case data.SmallInt:
		return int64(x), nil
	case data.Int:
		return int64(x), nil
	case data.BigInt:
		return int64(x), nil
	case data.Float:
		// The shortest float32 form, so FLOAT 1.1 reads 1.1 and not
		// 1.100000023841858, as ClickHouse's Float32 does.
		f, _ := strconv.ParseFloat(strconv.FormatFloat(float64(x), 'g', -1, 32), 64)
		return f, nil
	case data.Double:
		return float64(x), nil
	case *data.Decimal:
		if x == nil || !x.Valid {
			return nil, nil
		}
		return x.Value(), nil
	case data.String:
		return string(x), nil
	case data.VarChar:
		return charData(x.Type(), x.Data())
	case data.Char:
		return charData(x.Type(), x.Data())
	case data.Bool:
		return bool(x), nil
	case data.Date:
		t := time.Time(x)
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	case data.DateTime:
		return time.Time(x).UTC(), nil
	case data.Timestamp:
		return time.Time(x).UTC(), nil
	case data.TimestampNtz:
		return time.Time(x).UTC(), nil
	case data.Binary:
		return bytes.Clone(x), nil
	case data.Geography:
		return bytes.Clone(x), nil
	case *data.Json:
		if x == nil || !x.Valid {
			return nil, nil
		}
		return []byte(x.Data), nil
	case *data.Array, *data.Map, *data.Struct:
		j, err := jsonValue(v)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(j)
		if err != nil {
			return nil, mcerr.Local(mcerr.CategoryData, "a complex value cannot be encoded as JSON")
		}
		return b, nil
	case data.IntervalDayTime, data.IntervalYearMonth:
		return x.String(), nil
	default:
		return nil, mcerr.Local(mcerr.CategoryData, fmt.Sprintf("unsupported value type %s", reflect.TypeOf(v)))
	}
}

// charData guards against an SDK defect: the protoc reader builds VARCHAR(n)
// and CHAR(n) values with a byte-length check, and on a multi-byte value
// longer than n bytes it silently returns an empty value with length 0. A
// declared length is never 0, so length 0 means the value was lost.
func charData(t datatype.DataType, s string) (any, error) {
	switch ct := t.(type) {
	case datatype.VarcharType:
		if ct.Length == 0 {
			return nil, errCharOverflow
		}
	case datatype.CharType:
		if ct.Length == 0 {
			return nil, errCharOverflow
		}
	}
	return s, nil
}

var errCharOverflow = mcerr.Local(mcerr.CategoryData, "a VARCHAR or CHAR value is longer in bytes than its declared length and the SDK dropped it; cast the column to STRING")

// jsonValue converts a value to a JSON-marshalable form for complex columns.
//
//nolint:nilnil // a SQL NULL is a nil value, not a missing one
func jsonValue(v data.Data) (any, error) {
	switch x := v.(type) {
	case *data.Array:
		if x == nil {
			return nil, nil
		}
		out := make([]any, 0, x.Len())
		for _, e := range x.ToSlice() {
			j, err := jsonValue(e)
			if err != nil {
				return nil, err
			}
			out = append(out, j)
		}
		return out, nil
	case *data.Map:
		if x == nil {
			return nil, nil
		}
		out := orderedObject{}
		for k, e := range x.ToGoMap() {
			key, err := mapKey(k)
			if err != nil {
				return nil, err
			}
			j, err := jsonValue(e)
			if err != nil {
				return nil, err
			}
			out = append(out, field{key, j})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
		return out, nil
	case *data.Struct:
		if x == nil {
			return nil, nil
		}
		out := orderedObject{}
		for _, f := range x.Fields() {
			j, err := jsonValue(f.Value)
			if err != nil {
				return nil, err
			}
			out = append(out, field{f.Name, j})
		}
		return out, nil
	case *data.Decimal:
		if x == nil || !x.Valid {
			return nil, nil
		}
		return json.Number(x.Value()), nil
	case *data.Json:
		if x == nil || !x.Valid {
			return nil, nil
		}
		if !json.Valid([]byte(x.Data)) {
			return nil, mcerr.Local(mcerr.CategoryData, "a JSON value is not valid JSON")
		}
		return json.RawMessage(x.Data), nil
	}
	plain, err := convertValue(v)
	if err != nil {
		return nil, err
	}
	switch p := plain.(type) {
	case time.Time:
		return p.Format(time.RFC3339Nano), nil
	case float64:
		return finiteOrString(p), nil
	}
	return plain, nil
}

// finiteOrString keeps NaN and the infinities, which JSON cannot hold, as text.
func finiteOrString(f float64) any {
	if f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308 {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return f
}

// mapKey renders a map key as a JSON object key.
func mapKey(k data.Data) (string, error) {
	v, err := convertValue(k)
	if err != nil {
		return "", err
	}
	switch x := v.(type) {
	case nil:
		return "null", nil
	case string:
		return x, nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case bool:
		return strconv.FormatBool(x), nil
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case []byte:
		return base64.StdEncoding.EncodeToString(x), nil
	}
	return "", mcerr.Local(mcerr.CategoryData, "a map key cannot be encoded as JSON")
}

type field struct {
	name  string
	value any
}

// orderedObject marshals as a JSON object that keeps its field order.
type orderedObject []field

func (o orderedObject) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, f := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := json.Marshal(f.name)
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		v, err := json.Marshal(f.value)
		if err != nil {
			return nil, err
		}
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// scanType is the Go type a column scans into.
func scanType(t datatype.DataType) reflect.Type {
	switch t.ID() {
	case datatype.TINYINT, datatype.SMALLINT, datatype.INT, datatype.BIGINT:
		return reflect.TypeFor[int64]()
	case datatype.FLOAT, datatype.DOUBLE:
		return reflect.TypeFor[float64]()
	case datatype.BOOLEAN:
		return reflect.TypeFor[bool]()
	case datatype.DATE, datatype.DATETIME, datatype.TIMESTAMP, datatype.TIMESTAMP_NTZ:
		return reflect.TypeFor[time.Time]()
	case datatype.BINARY, datatype.GEOGRAPHY, datatype.JSON, datatype.ARRAY, datatype.MAP, datatype.STRUCT:
		return reflect.TypeFor[[]byte]()
	default:
		return reflect.TypeFor[string]()
	}
}
