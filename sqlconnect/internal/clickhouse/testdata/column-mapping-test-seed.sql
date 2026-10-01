CREATE TABLE `{{.schema}}`.`column_mappings_test` (
    _order Int32,
    _int Nullable(Int32),
    _bigint Nullable(Int64),
    _double Nullable(Float64),
    _varchar Nullable(String),
    _boolean Nullable(Bool),
    _date Nullable(Date),
    _timestamp Nullable(DateTime64(9, 'UTC')),
    _array Array(Int32)
) ENGINE = MergeTree ORDER BY _order;

INSERT INTO `{{.schema}}`.`column_mappings_test`
    (_order, _int, _bigint, _double, _varchar, _boolean, _date, _timestamp, _array)
VALUES
    (1, 1,    1,    1.1,  'abc', true,  '2004-10-19', '2004-10-19 10:23:54', [1, 2, 3]),
    (2, 0,    0,    0,    '',    false, '2004-10-19', '2004-10-19 10:23:54', []),
    (3, NULL, NULL, NULL, NULL,  NULL,  NULL,         NULL,                  []);
