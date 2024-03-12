CREATE TABLE "{{.schema}}"."COLUMN_MAPPINGS_TEST" (
    _order INT,
    _int INT,
    _number NUMBER(10,2),
    _decimal DECIMAL(10,2),
    _numeric NUMERIC(10,2),
    _integer INTEGER,
    _bigint BIGINT,
    _smallint SMALLINT,
    _tinyint TINYINT,
    _float FLOAT,
    _float4 FLOAT4,
    _float8 FLOAT8,
    _double DOUBLE,
    _real REAL,
    _double_precision DOUBLE PRECISION,
    _boolean BOOLEAN,
    _text TEXT,
    _varchar VARCHAR,
    _char CHAR,
    _character CHARACTER,
    _string STRING,
    _binary BINARY,
    _varbinary VARBINARY,
    _date DATE,
    _datetime DATETIME,
    _time TIME,
    _timestamp TIMESTAMP,
    _timestampntz TIMESTAMP_NTZ,
    _timestampltz TIMESTAMP_LTZ,
    _timestamptz TIMESTAMP_TZ,
    _variant VARIANT,
    _object OBJECT,
    _array ARRAY
);

INSERT INTO "{{.schema}}"."COLUMN_MAPPINGS_TEST"
    (_order, _int, _number, _decimal, _numeric, _integer, _bigint, _smallint, _tinyint, _float, _float4, _float8, _double, _real, _double_precision, _boolean, _text, _varchar, _char, _character, _string, _binary, _varbinary, _date, _datetime, _time, _timestamp, _timestampntz, _timestampltz, _timestamptz, _variant, _object, _array) 
SELECT
    1, 1,    1.1,  1.1,  1.1,  1,    1,    1,    1,    1.1,  1.1,  1.1,  1.1,  1.1,  1.1,  true,  't',  'vc', 'c', 'c', 's',  TO_BINARY('bin', 'UTF-8'), TO_BINARY('vbin', 'UTF-8'), '2021-7-1', '2017-01-01 12:00:00', '12:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', TO_VARIANT(PARSE_JSON('{"key": "value", "key1": null}')), object_construct('key', 'value', 'key1', null), array_construct(1,2,3,null);

INSERT INTO "{{.schema}}"."COLUMN_MAPPINGS_TEST"
    (_order, _int, _number, _decimal, _numeric, _integer, _bigint, _smallint, _tinyint, _float, _float4, _float8, _double, _real, _double_precision, _boolean, _text, _varchar, _char, _character, _string, _binary, _varbinary, _date, _datetime, _time, _timestamp, _timestampntz, _timestampltz, _timestamptz, _variant, _object, _array) 
SELECT
    2, 0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    false, '',   '',   '',   '',   '',    '', '',      '2021-7-1', '2017-01-01 12:00:00', '12:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', '2014-01-01 16:00:00', 'string'::VARIANT, object_construct(), array_construct(); 


INSERT INTO "{{.schema}}"."COLUMN_MAPPINGS_TEST"
    (_order, _int, _number, _decimal, _numeric, _integer, _bigint, _smallint, _tinyint, _float, _float4, _float8, _double, _real, _double_precision, _boolean, _text, _varchar, _char, _character, _string, _binary, _varbinary, _date, _datetime, _time, _timestamp, _timestampntz, _timestampltz, _timestamptz, _variant, _object, _array) 
SELECT
    3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,  NULL, NULL, NULL, NULL, NULL,  NULL,   NULL,       NULL,                  NULL,       NULL,                  NULL,                  NULL,                  NULL,                  NULL,                  NULL, NULL, NULL;