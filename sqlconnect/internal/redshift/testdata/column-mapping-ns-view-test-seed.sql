CREATE TABLE "{{.schema}}"."temp_table" (
    _order INT,
    _int INT,
    _int2 INT2,
    _int4 INT4,
    _int8 INT8,
    _integer INTEGER,
    _smallint SMALLINT,
    _bigint BIGINT,
    _real REAL,
    _float FLOAT,
    _float4 FLOAT4,
    _float8 FLOAT8,
    _numeric NUMERIC(10,2),
    _double DOUBLE PRECISION,
    _text TEXT,
    _varchar VARCHAR(10),
    _charvar CHARACTER VARYING,
    _nchar NCHAR(10),
    _bpchar BPCHAR,
    _character CHARACTER(10),
    _date DATE,
    _time TIME,
    _timetz TIME WITH TIME ZONE,
    _timestamptz TIMESTAMPTZ,
    _timestampntz TIMESTAMP WITHOUT TIME ZONE,
    _timestampwtz TIMESTAMP WITH TIME ZONE, 
    _timestamp TIMESTAMP,
    _boolean BOOLEAN,
    _bool BOOL
);

INSERT INTO "{{.schema}}"."temp_table"
    (_order, _int, _int2, _int4, _int8, _integer, _smallint, _bigint, _real, _float, _float4, _float8, _numeric, _double, _text, _varchar, _charvar, _nchar, _bpchar, _character, _date, _time, _timetz, _timestamptz, _timestampntz, _timestampwtz, _timestamp, _boolean, _bool)
VALUES
    (1, 1,    1,    1,    1,    1,    1,    1,    1,  1.1,  1,  1.1,  1.1,  1.1,  'abc', 'abc', 'abc', 'abc', 'abc',     'abc', '2004-10-19', '10:23:54.123', '10:23:54.1234+02', '2004-10-19 10:23:54.12345+02', '2004-10-19 10:23:54.123456', '2004-10-19 10:23:54.1234567+02', '2004-10-19 10:23:54.12345678+02', true,  true ),
    (2, 0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    '',    '',    '',    '',    '',    '',    '2004-10-19', '10:23:54', '10:23:54+02', '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54+02', '2004-10-19 10:23:54+02', false, false), 
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,  NULL,  NULL,  NULL,  NULL,  NULL,  NULL,         NULL,       NULL,          NULL,                     NULL,                  NULL,                     NULL,                     NULL,  NULL );


CREATE VIEW "{{.schema}}"."column_mappings_test_vw_ns" as (
    SELECT * FROM "{{.schema}}"."temp_table"
) with no schema binding;