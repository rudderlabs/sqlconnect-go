CREATE TABLE `{{.schema}}`.`goqu_test` (
    _string STRING(10), 
    _int INT,
    _float BIGNUMERIC(2,1),
    _boolean BOOLEAN,
    _timestamp TIMESTAMP
);

INSERT INTO `{{.schema}}`.`goqu_test`
    (_string, _int, _float, _boolean, _timestamp)
VALUES
    ('string', 1, 1.1, TRUE, '2021-01-01T00:00:00Z');