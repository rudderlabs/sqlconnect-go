CREATE TABLE `{{.schema}}`.`column_mappings_test` (
    _order INT64,
    _array ARRAY<STRING>,
    _bignumeric BIGNUMERIC(2,1),
    _bignumericnoscale BIGNUMERIC(1,0),
    _bigdecimal BIGDECIMAL,
    _bool BOOL,
    _boolean BOOLEAN,
    _bytes BYTES,
    _date DATE,
    _datetime DATETIME,
    _float64 FLOAT64,
    _geo GEOGRAPHY,
    _int64 INT64,
    _int INT,
    _smallint SMALLINT,
    _integer INTEGER,
    _bigint BIGINT,
    _tinyint TINYINT,
    _byteint BYTEINT,
    _interval INTERVAL,
    _json JSON,
    _numeric NUMERIC,
    _decimal NUMERIC,
    _string STRING(10), 
    _struct STRUCT<a STRING, b INT64>,
    _time TIME,
    _timestamp TIMESTAMP,
);

INSERT INTO `{{.schema}}`.`column_mappings_test`
    (_order, _array, _bignumeric, _bignumericnoscale, _bigdecimal, _bool, _boolean, _bytes, _date, _datetime, _float64, _geo, _int64, _int, _smallint, _integer, _bigint, _tinyint, _byteint, _interval, _json, _numeric, _decimal, _string, _struct, _time, _timestamp) 
VALUES
    (1, ['ONE'], 1.1,  1,    1,    TRUE,  TRUE,  B"abc", '2014-09-27', '2014-09-27 12:30:00.45', 1.1,  ST_GEOGFROMTEXT('POINT(32 90)'),  1,    1,    1,    1,    1,    1,    1,    INTERVAL 1 YEAR, JSON '{"key": "value"}', 1,    1,    'string',  ('string', 1), '12:30:00.45', '2014-09-27 12:30:00.45-08'),
    (2, [],      0.0,  0,    0,    FALSE, FALSE, B"",    '2014-09-27', '2014-09-27 12:30:00.45', 0.0,  ST_GEOGFROMTEXT('POINT EMPTY'),   0,    0,    0,    0,    0,    0,    0,    INTERVAL 1 YEAR, JSON '{}',               0,    0,    '',        ('', 0),       '12:30:00.45', '2014-09-27 12:30:00.45-08'),
    (3, NULL,    NULL, NULL, NULL, NULL,  NULL,  NULL,   NULL,         NULL,                     NULL, NULL,                             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,            NULL,                    NULL, NULL, NULL,      NULL,           NULL,         NULL);