CREATE OR ALTER FUNCTION django_date_extract(@lookup_type NVARCHAR(16), @d DATETIME2)
    RETURNS INT
AS
    BEGIN RETURN
    CASE @lookup_type
               WHEN 'year' THEN DATEPART(YEAR, @d)
               WHEN 'quarter' THEN DATEPART(QUARTER, @d)
               WHEN 'month' THEN DATEPART(MONTH, @d)
               WHEN 'dayofyear' THEN DATEPART(DAYOFYEAR, @d)
               WHEN 'day' THEN DATEPART(DAY, @d)
               WHEN 'weekday' THEN DATEPART(WEEKDAY, @d)
               WHEN 'hour' THEN DATEPART(HOUR, @d)
               WHEN 'minute' THEN DATEPART(MINUTE, @d)
               WHEN 'second' THEN DATEPART(SECOND, @d)
               WHEN 'millisecond' THEN DATEPART(MILLISECOND, @d)
               WHEN 'microsecond' THEN DATEPART(MICROSECOND, @d)
               WHEN 'nanosecond' THEN DATEPART(NANOSECOND, @d)
               WHEN 'tzoffset' THEN DATEPART(TZOFFSET, @d)
               WHEN 'iso_week' THEN DATEPART(ISO_WEEK, @d)
                -- differences with django
               WHEN 'iso_week_day' THEN 1 + (DATEPART(WEEKDAY, @d) + @@DATEFIRST - 2) % 7
               WHEN 'iso_year' THEN YEAR(DATEADD(day, 26 - DATEPART(ISO_WEEK, @d), @d))
               WHEN 'week' THEN DATEPART(ISO_WEEK, @d)
               WHEN 'week_day' THEN DATEPART(WEEKDAY, @d)
        END
END

GO


CREATE OR
ALTER FUNCTION django_datetime_trunc(@lookup_type NVARCHAR(16), @d DATETIME2)
    RETURNS DATETIME2
AS
BEGIN
    DECLARE @zero DATETIME2 = '19000101'
    RETURN
        CASE @lookup_type
            WHEN 'year' THEN DATEADD(YEAR, DATEDIFF_BIG(YEAR, @zero, @d), @zero)
            WHEN 'quarter' THEN DATEADD(QUARTER, DATEDIFF_BIG(QUARTER, @zero, @d), @zero)
            WHEN 'month' THEN DATEADD(MONTH, DATEDIFF_BIG(MONTH, @zero, @d), @zero)
            WHEN 'dayofyear' THEN DATEADD(DAYOFYEAR, DATEDIFF_BIG(DAYOFYEAR, @zero, @d), @zero)
            WHEN 'day' THEN DATEADD(DAY, DATEDIFF_BIG(DAY, @zero, @d), @zero)
            WHEN 'weekday' THEN DATEADD(WEEKDAY, DATEDIFF_BIG(WEEKDAY, @zero, @d), @zero)
            WHEN 'hour' THEN DATEADD(HOUR, DATEDIFF_BIG(HOUR, @zero, @d), @zero)
            WHEN 'minute' THEN DATEADD(MINUTE, DATEDIFF_BIG(MINUTE, @zero, @d), @zero)
            -- diff for django
            WHEN 'week' THEN DATEADD(WEEK, DATEDIFF_BIG(WEEK, DATEADD(DAY, -1, @zero), DATEADD(DAY, -1, @d)), @zero)
            WHEN 'second' THEN CAST(@d AS DATETIME2(0))
        END
END

GO




CREATE OR
ALTER FUNCTION django_lpad(@s NVARCHAR(MAX), @len INT, @fill NVARCHAR(MAX))
    RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @diff INT = @len - LEN(@s)
    IF @diff <= 0 RETURN LEFT(@s, @len)
    -- could get complicated if len(@fill) > 1 so keep it simple
    RETURN LEFT(REPLICATE(@fill, @diff), @diff) + @s
END

GO

CREATE OR
ALTER FUNCTION django_rpad(@s NVARCHAR(MAX), @len INT, @fill NVARCHAR(MAX))
    RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @diff INT = @len - LEN(@s)
    IF @diff <= 0 RETURN LEFT(@s, @len)
    -- could get complicated if len(@fill) > 1 so keep it simple
    RETURN @s + LEFT(REPLICATE(@fill, @diff), @diff)
END

GO

CREATE OR
ALTER FUNCTION django_bitshift(
    @Num INT
, @Shift SMALLINT /* Positive - Right Shift, Negative - Left Shift */
, @Circular BIT /* 0 - Not Circular Shift, 1 - Circular Shift */
) RETURNS INT AS
-- all credits to http://slavasql.blogspot.com/2019/12/t-sql-bitwise-shifting.html
BEGIN
    DECLARE @BigNum BIGINT = @Num;

    WHILE @Shift != 0
        SELECT @BigNum = CASE
                             WHEN SIGN(@Shift) > 0
                                 THEN (@BigNum - (@BigNum & 1)) / 2
                                 + 0x80000000 * (@BigNum & 1) * @Circular
                             ELSE (@BigNum - (@BigNum & 0x80000000)) * 2
                                 + SIGN(@BigNum & 0x80000000) * @Circular
            END,
               @Shift -= SIGN(@Shift);

    RETURN CAST(SUBSTRING(CAST(@BigNum AS BINARY(8)), 5, 4) AS INT);
END

GO

CREATE OR
ALTER FUNCTION django_dtdelta(@date DATETIME2, @duration BIGINT)
    RETURNS DATETIME2
AS
BEGIN
    DECLARE @max_int INT = 2147483647 -- POWER(CAST(2 AS BIGINT), CAST(31 AS BIGINT)) - 1
    DECLARE @sign SMALLINT = IIF(@duration < 0, -1, 1)
    DECLARE @temp BIGINT
    DECLARE @us_in_a_day BIGINT = 86400000000 --1000000 * 60 * 60 * 24
    SET @duration = @sign * @duration
    WHILE @duration > @max_int
        BEGIN
            IF @duration > @us_in_a_day
                BEGIN
                    SET @temp = @duration / @us_in_a_day
                    IF @temp > @max_int SET @temp = @max_int
                    SET @date = DATEADD(DAY, @temp * @sign, @date)
                    SET @duration = @duration - (@temp * @us_in_a_day)
                END
            ELSE
                BEGIN
                    SET @date = DATEADD(MICROSECOND, @max_int * @sign, @date)
                    SET @duration = @duration - @max_int
                END
        END
    RETURN DATEADD(MICROSECOND, @duration * @sign, @date)
END
