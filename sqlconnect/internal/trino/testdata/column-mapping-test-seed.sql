CREATE TABLE "{{.schema}}"."column_mappings_test" (
    _order INT,
    _int INT,
    _tinyint TINYINT,
    _smallint SMALLINT,
    _integer INTEGER,
    _bigint BIGINT,
    _real REAL,
    _double DOUBLE,
    _decimal DECIMAL(2,1),
    _varchar VARCHAR(3),
    _char CHAR(3),
    _varbinary VARBINARY,
    _boolean BOOLEAN,
    _date DATE,
    _timestamp TIMESTAMP,
    _array ARRAY<INT>,
    _map MAP<VARCHAR(3), INT>
);

INSERT INTO "{{.schema}}"."column_mappings_test"
    (_order, _int, _tinyint, _smallint, _integer, _bigint, _real, _double, _decimal, _varchar, _char, _varbinary, _boolean, _date, _timestamp, _array, _map)
VALUES
    (1, 1,    TINYINT '1', SMALLINT '1', 1,    BIGINT '1',    REAL '1.1',  DOUBLE '1.1',  DECIMAL '1.1',  'abc', CHAR 'abc', VARBINARY 'abc', true,  DATE '2004-10-19', TIMESTAMP '2004-10-19 10:23:54 UTC', ARRAY[1, 2, 3], MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]) ),
    (2, 0,    TINYINT '0', SMALLINT '0', 0,    BIGINT '0',    REAL '0',    DOUBLE '0',    DECIMAL '0',    '',    CHAR '',    VARBINARY  '',   false, DATE '2004-10-19', TIMESTAMP '2004-10-19 10:23:54 UTC', ARRAY[],        MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]) ), 
    (3, NULL, NULL,        NULL,         NULL, NULL,          NULL,        NULL,          NULL,           NULL,  NULL,       NULL,            NULL,  NULL,              NULL,                            NULL,           NULL                                  );