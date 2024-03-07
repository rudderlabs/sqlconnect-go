CREATE TABLE `{{.schema}}`.`column_mappings_test` (
    _order INT,
    _decimal DECIMAL(2,1),
    _numeric NUMERIC(2,1),
    _dec DEC(2,1),
    _int INT,
    _integer INTEGER,
    _bigint BIGINT,
    _long LONG,
    _smallint SMALLINT,
    _short SHORT,
    _tinyint TINYINT,
    _byte BYTE,
    _float FLOAT,
    _real REAL,
    _double DOUBLE,
    _boolean BOOLEAN,
    _string STRING,
    _char CHAR(1),
    _varchar VARCHAR(1),
    _binary BINARY,
    _date DATE,
    _timestamp TIMESTAMP,
    _timestampntz TIMESTAMP_NTZ,
    _array ARRAY<INT>,
    _map MAP<STRING,STRING>,
    _struct STRUCT<col1:string,col2:int>
);

INSERT INTO `{{.schema}}`.`column_mappings_test`
    (_order, _decimal, _numeric, _dec, _int, _integer, _bigint, _long, _smallint, _short, _tinyint, _byte, _float, _real, _double, _boolean, _string, _char, _varchar, _binary, _date, _timestamp, _timestampntz, _array, _map, _struct) 
VALUES
    (1, 1.1,  1.1,  1.1,  1,    1,    1,    1,    1,    1,    1,    1,    1.1,  1.1,  1.1,  true,  's',  's',  's',  X'1', CAST('2020-12-31' AS DATE), '2021-7-1T8:43:28UTC+3', '2021-7-1T8:43:28.123456', ARRAY(1,2,3,NULL), map('key', 'value', 'key1', NULL), struct('val1', 1)    ),
    (2, 0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    false, '',   '',   '',   X'',  CAST('2020-12-31' AS DATE), '2021-7-1T8:43:28UTC+3', '2021-7-1T8:43:28.123456', ARRAY(),           map('',''),                        struct('val1', NULL) ), 
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,  NULL, NULL, NULL, NULL, NULL,                        NULL,                    NULL,                     NULL,              NULL,                              NULL                 );