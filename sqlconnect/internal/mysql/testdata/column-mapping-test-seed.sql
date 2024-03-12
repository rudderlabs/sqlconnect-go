CREATE TABLE `{{.schema}}`.`column_mappings_test` (
    _order INT,
    _integer INTEGER,
    _int INT,
    _tinyint TINYINT,
    _smallint SMALLINT,
    _mediumint MEDIUMINT,
    _bigint BIGINT,
    _decimal DECIMAL(5,2),
    _numeric NUMERIC(5,2),
    _float FLOAT,
    _double DOUBLE,
    _bit BIT(64),
    _char CHAR(1),
    _varchar VARCHAR(10),
    _bin BINARY(8),
    _varbinary VARBINARY(100),
    _blob BLOB,
    _tinyblob TINYBLOB,
    _mediumblob MEDIUMBLOB,
    _longblob LONGBLOB,
    _text TEXT(10),
    _tinytext TINYTEXT,
    _mediumtext MEDIUMTEXT,
    _longtext LONGTEXT,
    _enum ENUM('1', '2', '3'),
    _set SET('one', 'two', 'three'),
    _date DATE,
    _datetime DATETIME,
    _timestamp TIMESTAMP,
    _time TIME,
    _year YEAR,
    _json JSON
);

INSERT INTO `{{.schema}}`.`column_mappings_test`
    (_order, _integer, _int, _tinyint, _smallint, _mediumint, _bigint, _decimal, _numeric, _float, _double, _bit, _char, _varchar, _bin, _varbinary, _blob, _tinyblob, _mediumblob, _longblob, _text, _tinytext, _mediumtext, _longtext, _enum, _set, _date, _datetime, _timestamp, _time, _year, _json)
VALUES
    (1, 1,    1,    1,    1,    1,    1,    1.1,  1.1,  1.1,  1.1,  b'101111100111', 'a',  'abc', 'a',  'a',  'b',  'b',  'b',  'b',  't',  't',  't',  't',  '1', 'one,two',  '2020-01-01', '2020-01-01 15:10:10', '2020-01-01 15:10:10', '10:45:15', '2020', '{"key": "value"}'),
    (2, 0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,               '',   '',    '',   '',   '',   '',   '',   '',   '',   '',   '',   '',   '1', '',         '2020-01-01', '2020-01-01 15:10:10', '2020-01-01 15:10:10', '10:45:15', '2020', '{}'              ), 
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,            NULL, NULL,  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,      NULL,         NULL,                  NULL,                   NULL,       NULL,   NULL              );