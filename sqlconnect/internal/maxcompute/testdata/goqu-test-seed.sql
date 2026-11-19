CREATE TABLE `{{.schema}}`.`goqu_test` (
    _string STRING,
    _int INT,
    _float DOUBLE,
    _boolean BOOLEAN,
    _timestamp TIMESTAMP
) LIFECYCLE 1;

INSERT INTO `{{.schema}}`.`goqu_test` (_string, _int, _float, _boolean, _timestamp)
SELECT 'string', 1, 1.1, TRUE, FROM_UTC_TIMESTAMP(1609459200000L, 'UTC');
