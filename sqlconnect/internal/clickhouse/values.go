package clickhouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func mapValue(raw string, value any) (any, error) {
	kind := canonicalType(raw)
	if kind == "unsupported" {
		return nil, adapterError("CH_TYPE_UNSUPPORTED", "unsupported native type", nil)
	}
	typ, _ := parseType(raw)
	if value == nil {
		return nil, nil
	}
	// Nullable native columns can return typed pointers inside an interface.
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	value = rv.Interface()
	switch typ.name {
	case "Nullable", "LowCardinality":
		return mapValue(typ.args[0], value)
	case "SimpleAggregateFunction":
		return mapValue(typ.args[1], value)
	case "Array", "Nested":
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return nil, encodingError()
		}
		child := ""
		if typ.name == "Array" {
			child = typ.args[0]
		} else {
			child = "Tuple(" + strings.Join(typ.args, ",") + ")"
		}
		result := make([]any, rv.Len())
		for i := range result {
			mapped, err := mapValue(child, rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			result[i] = mapped
		}
		return result, nil
	case "Tuple":
		names, types, _ := tupleFields(typ.args)
		values := make([]any, len(types))
		if names[0] != "" {
			if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
				return nil, encodingError()
			}
			for i, name := range names {
				item := rv.MapIndex(reflect.ValueOf(name).Convert(rv.Type().Key()))
				if !item.IsValid() {
					return nil, encodingError()
				}
				values[i] = item.Interface()
			}
		} else {
			if (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) || rv.Len() != len(types) {
				return nil, encodingError()
			}
			for i := range values {
				values[i] = rv.Index(i).Interface()
			}
		}
		for i := range values {
			mapped, err := mapValue(types[i], values[i])
			if err != nil {
				return nil, err
			}
			values[i] = mapped
		}
		if names[0] == "" {
			return values, nil
		}
		result := make(map[string]any, len(names))
		for i, name := range names {
			result[name] = values[i]
		}
		return result, nil
	}
	if strings.HasPrefix(typ.name, "Decimal") {
		scaleIndex := 0
		if typ.name == "Decimal" {
			scaleIndex = 1
		}
		scale, _ := strconv.ParseInt(typ.args[scaleIndex], 10, 32)
		if decimal, ok := value.(interface{ StringFixed(int32) string }); ok {
			return decimal.StringFixed(int32(scale)), nil
		}
		return nil, encodingError()
	}
	switch kind {
	case "string":
		var text string
		switch v := value.(type) {
		case string:
			text = v
		case []byte:
			text = string(v)
		case big.Int:
			text = v.String()
		case time.Duration:
			return formatDuration(v, typ), nil
		case fmt.Stringer:
			text = v.String()
		default:
			switch rv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				text = strconv.FormatUint(rv.Uint(), 10)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				text = strconv.FormatInt(rv.Int(), 10)
			default:
				return nil, encodingError()
			}
		}
		if !utf8.ValidString(text) {
			return nil, encodingError()
		}
		return text, nil
	case "datetime":
		timestamp, ok := value.(time.Time)
		if !ok {
			return nil, encodingError()
		}
		if typ.name == "Date" || typ.name == "Date32" {
			return timestamp.Format(time.RFC3339), nil
		}
		return timestamp.UTC().Format(time.RFC3339Nano), nil
	case "float":
		if rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64 {
			return nil, encodingError()
		}
		number := rv.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, nil
		}
		return value, nil
	case "int", "boolean":
		return value, nil
	case "json":
		var encoded []byte
		switch v := value.(type) {
		case string:
			encoded = []byte(v)
		case []byte:
			encoded = v
		default:
			var err error
			encoded, err = json.Marshal(value)
			if err != nil {
				return nil, encodingError()
			}
		}
		if !utf8.Valid(encoded) {
			return nil, encodingError()
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		var result any
		if err := decoder.Decode(&result); err != nil {
			return nil, encodingError()
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, encodingError()
		}
		return result, nil
	}
	return nil, encodingError()
}
func encodingError() error {
	return adapterError("CH_VALUE_ENCODING", "native value cannot be exported losslessly", nil)
}

func formatDuration(duration time.Duration, typ nativeType) string {
	negative := duration < 0
	// Avoid negating MinInt64 in the signed domain.
	nanos := uint64(duration)
	if negative {
		nanos = -nanos
	}
	seconds := nanos / uint64(time.Second)
	text := fmt.Sprintf("%02d:%02d:%02d", seconds/3600, (seconds/60)%60, seconds%60)
	if typ.name == "Time64" {
		precision, _ := strconv.Atoi(typ.args[0])
		if precision > 9 {
			precision = 9
		}
		if precision > 0 {
			text += "." + fmt.Sprintf("%09d", nanos%uint64(time.Second))[:precision]
		}
	}
	if negative {
		text = "-" + text
	}
	return text
}
