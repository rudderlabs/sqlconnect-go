package maxcompute_test

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func generator(n int) string {
	return "(SELECT pos FROM (SELECT posexplode(split(repeat('a,', " + itoa(n) + "), ',')) AS (pos, v)) g)"
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestIntegrationRead(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	t.Run("D-C1 a 25,000-row result is read in full", func(t *testing.T) {
		var reported int64
		rows, err := db.QueryContext(maxcomputequery.WithRowCountHandler(ctx, func(n int64) { reported = n }), "SELECT pos FROM "+generator(25000))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		n := 0
		for rows.Next() {
			n++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 25000, n, "no 10,000-row cap")
		require.EqualValues(t, 25000, reported)
	})

	t.Run("D-C2 a 250,000-row ordered result read by ranges arrives in order", func(t *testing.T) {
		start := time.Now()
		rows, err := db.QueryContext(ctx, "SELECT pos, CONCAT('row-', CAST(pos AS STRING)) AS label FROM "+generator(250000)+" ORDER BY pos")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		want := int64(0)
		for rows.Next() {
			var pos int64
			var label string
			require.NoError(t, rows.Scan(&pos, &label))
			require.Equal(t, want, pos, "rows arrive in order")
			want++
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 250000, want)
		t.Logf("250,000 rows in %s", time.Since(start).Round(time.Millisecond))
	})

	t.Run("a cancelled read stops", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		rows, err := db.QueryContext(cctx, "SELECT pos FROM "+generator(250000))
		require.NoError(t, err)
		require.True(t, rows.Next())
		cancel()
		for rows.Next() {
		}
		require.ErrorIs(t, rows.Err(), context.Canceled)
		require.NoError(t, rows.Close())
	})

	t.Run("an empty result", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT pos FROM "+generator(10)+" WHERE pos < 0")
		require.NoError(t, err)
		cols, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, "BIGINT", cols[0].DatabaseTypeName())
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	})

	t.Run("D-C7 time values scan as UTC whatever the process zone", func(t *testing.T) {
		shanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)
		saved := time.Local
		time.Local = shanghai
		defer func() { time.Local = saved }()
		var ts, ntz, dt, d time.Time
		err = db.QueryRowContext(ctx, `SELECT FROM_UTC_TIMESTAMP(1763460000123L, 'UTC') AS ts,
			CAST('2026-11-18 10:00:00.123456789' AS TIMESTAMP_NTZ) AS ntz,
			CAST(FROM_UTC_TIMESTAMP(1763460000000L, 'UTC') AS DATETIME) AS dt,
			DATE '2026-11-18' AS d`).Scan(&ts, &ntz, &dt, &d)
		require.NoError(t, err)
		for _, v := range []time.Time{ts, ntz, dt, d} {
			require.Equal(t, time.UTC, v.Location())
		}
		require.Equal(t, time.UnixMilli(1763460000123).UTC(), ts, "TIMESTAMP is the instant")
		require.Equal(t, time.Date(2026, 11, 18, 10, 0, 0, 123456789, time.UTC), ntz, "TIMESTAMP_NTZ is the wall time, nanoseconds kept")
		require.Equal(t, time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC), d)
		t.Logf("datetime from instant 1763460000000 ms: %s", dt.Format(time.RFC3339Nano))
	})

	t.Run("types", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SELECT CAST(1 AS TINYINT) AS ti, CAST(2 AS SMALLINT) AS si, 3 AS i, 4000000000L AS bi,
			CAST(1.5 AS FLOAT) AS f, CAST(1.1 AS DOUBLE) AS dbl,
			CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)) AS dec,
			'it''s' AS s, CAST('v' AS VARCHAR(10)) AS vc, CAST('c' AS CHAR(3)) AS ch, TRUE AS b,
			UNHEX('00AB') AS bin, JSON '{"a":[1,2]}' AS j,
			ARRAY(1, 2, NULL) AS arr, MAP('k1', 1, 'k2', NULL) AS m,
			NAMED_STRUCT('city', 'Pune', 'zip', 411001, 'dec', CAST('1.10' AS DECIMAL(10,2))) AS st,
			CAST(NULL AS STRING) AS n`)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		types, err := rows.ColumnTypes()
		require.NoError(t, err)
		var names []string
		for _, ct := range types {
			names = append(names, ct.DatabaseTypeName())
			_, ok := ct.Nullable()
			require.False(t, ok, "nullability is unknown")
		}
		t.Logf("types: %v", names)
		require.Equal(t, "DECIMAL(38,18)", names[6])
		require.True(t, strings.HasPrefix(names[16-2], "MAP<"), names[14])
		require.True(t, rows.Next())
		vals := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		require.NoError(t, rows.Err())
		require.Equal(t, int64(1), vals[0])
		require.Equal(t, int64(4000000000), vals[3])
		require.Equal(t, 1.5, vals[4])
		require.Equal(t, 1.1, vals[5])
		require.Equal(t, "12345678901234567890.123456789012345678", vals[6], "DECIMAL keeps its digits")
		require.Equal(t, "its", vals[7], "'' is two adjacent literals on MaxCompute")
		require.Equal(t, "v", vals[8])
		require.Equal(t, true, vals[10])
		require.Equal(t, []byte{0x00, 0xab}, vals[11])
		require.JSONEq(t, `{"a":[1,2]}`, string(vals[12].([]byte)))
		require.JSONEq(t, `[1,2,null]`, string(vals[13].([]byte)))
		require.JSONEq(t, `{"k1":1,"k2":null}`, string(vals[14].([]byte)))
		require.Equal(t, `{"city":"Pune","zip":411001,"dec":1.1}`, string(vals[15].([]byte)), "struct field order kept; the decimal is the server's text")
		require.Nil(t, vals[16])
	})

	t.Run("VARCHAR values longer in bytes than the declared length fail loudly", func(t *testing.T) {
		// The SDK's reader checks VARCHAR(n) by bytes and silently empties a
		// longer multi-byte value; the driver turns that into an error.
		var s sql.NullString
		err := db.QueryRowContext(ctx, `SELECT CAST('中文中文' AS VARCHAR(5)) AS v`).Scan(&s)
		require.ErrorContains(t, err, "longer in bytes than its declared length")
		require.ErrorContains(t, err, "cast the column to STRING")
		require.ErrorContains(t, err, "(column v)")
		// Arabic letters are 2 bytes each in UTF-8.
		err = db.QueryRowContext(ctx, `SELECT CAST('مرحبا' AS VARCHAR(5)) AS greeting`).Scan(&s)
		require.ErrorContains(t, err, "(column greeting)")
		err = db.QueryRowContext(ctx, `SELECT CAST('مرحبا' AS CHAR(5)) AS c`).Scan(&s)
		require.ErrorContains(t, err, "longer in bytes than its declared length")
		require.NoError(t, db.QueryRowContext(ctx, `SELECT CAST('中文' AS VARCHAR(6)) AS v`).Scan(&s))
		require.Equal(t, "中文", s.String)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT CAST('مرحبا' AS STRING) AS v`).Scan(&s))
		require.Equal(t, "مرحبا", s.String)
	})

	t.Run("bind arguments round trip", func(t *testing.T) {
		in := []any{
			`it's a "quoted" \ backslash; newline` + "\n" + "tab\t中文 ? -- /* x */",
			"",
			"\x00\x01\x7f",
			int64(4000000000),
			int64(-2147483649),
			-7,
			1e21,
			-2.5,
			1.0,
			true,
			[]byte{0, 1, 0xff},
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(in)), ", ")
		rows, err := db.QueryContext(ctx, "SELECT "+placeholders, in...)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		require.True(t, rows.Next())
		out := make([]any, len(in))
		ptrs := make([]any, len(in))
		for i := range out {
			ptrs[i] = &out[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		require.NoError(t, rows.Err())
		require.Equal(t, in[0], out[0])
		require.Equal(t, in[1], out[1])
		require.Equal(t, in[2], out[2])
		require.Equal(t, int64(4000000000), out[3])
		require.Equal(t, int64(-2147483649), out[4])
		require.Equal(t, int64(-7), out[5])
		require.Equal(t, 1e21, out[6])
		require.Equal(t, -2.5, out[7])
		require.Equal(t, 1.0, out[8])
		require.Equal(t, true, out[9])
		require.Equal(t, []byte{0, 1, 0xff}, out[10])
	})

	t.Run("time wrappers are exact without a time-zone hint", func(t *testing.T) {
		ns := time.Date(2026, 11, 18, 10, 0, 0, 123456789, time.UTC)
		ms := time.UnixMilli(1763460000123).UTC()
		var ntz, inst time.Time
		var same bool
		require.NoError(t, db.QueryRowContext(ctx, "SELECT ?, ?, ? = CAST('2026-11-18 10:00:00.123456789' AS TIMESTAMP_NTZ)",
			maxcomputequery.TimestampNTZ(ns), maxcomputequery.TimestampInstant(ms), maxcomputequery.TimestampNTZ(ns)).Scan(&ntz, &inst, &same))
		require.Equal(t, ns, ntz)
		require.Equal(t, ms, inst)
		require.True(t, same)
		rows, err := db.QueryContext(ctx, "SELECT ?", ns)
		if err == nil {
			_ = rows.Close()
			require.NoError(t, rows.Err())
		}
		require.ErrorIs(t, err, maxcomputequery.ErrTimeValue)
	})

	t.Run("a script that ends in a SELECT returns its rows", func(t *testing.T) {
		rows, err := db.QueryContext(maxcomputequery.WithScript(ctx), "@a := SELECT pos FROM "+generator(5)+"; SELECT COUNT(*) AS c FROM @a;")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		require.True(t, rows.Next())
		var c int64
		require.NoError(t, rows.Scan(&c))
		require.NoError(t, rows.Err())
		require.EqualValues(t, 5, c)
	})

	t.Run("a statement without rows through Query is not a query", func(t *testing.T) {
		// A SET of an unprotected key runs as a job without rows; a protected key
		// is refused before submit (ErrProtectedSet, unit-tested in the driver).
		rows, err := db.QueryContext(ctx, "SET odps.sql.mapper.split.size=256")
		if err == nil {
			_ = rows.Close()
			require.NoError(t, rows.Err())
		}
		require.ErrorContains(t, err, "not_a_query")
	})
}
