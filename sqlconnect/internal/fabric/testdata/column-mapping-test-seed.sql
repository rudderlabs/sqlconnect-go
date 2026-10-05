CREATE TABLE [{{.schema}}].[column_mappings_test] (
    [_order] INT,
    [_tinyint] TINYINT,
    [_smallint] SMALLINT,
    [_int] INT,
    [_bigint] BIGINT,
    [_decimal] DECIMAL(10,2),
    [_numeric] NUMERIC(10,2),
    [_float] FLOAT,
    [_real] REAL,
    [_bit] BIT,
    [_char] CHAR(3),
    [_varchar] VARCHAR(10),
    [_varbinary] VARBINARY(10),
    [_date] DATE,
    [_datetime2] DATETIME2(6)
);

INSERT INTO [{{.schema}}].[column_mappings_test]
    ([_order], [_tinyint], [_smallint], [_int], [_bigint], [_decimal], [_numeric], [_float], [_real], [_bit], [_char], [_varchar], [_varbinary], [_date], [_datetime2])
VALUES
    (1, 1, 1, 1, 1, 1.10, 1.10, 1.1, 1.1, 1, 'abc', 'abc', 0x616263, '2004-10-19', '2004-10-19T10:23:54.123456'),
    (2, 0, 0, 0, 0, 0.00, 0.00, 0.0, 0.0, 0, '   ', '', 0x, '2004-10-19', '2004-10-19T10:23:54.000000'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
