CREATE TABLE `{{.schema}}`.`goqu_test` (
    _string Nullable(String),
    _int Nullable(Int64),
    _float Nullable(Float64),
    _boolean Nullable(Bool),
    _timestamp Nullable(DateTime64(9, 'UTC'))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO `{{.schema}}`.`goqu_test`
    (_string, _int, _float, _boolean, _timestamp)
VALUES
    ('string', 1, 1.1, true, '2021-01-01 00:00:00');
