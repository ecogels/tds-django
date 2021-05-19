
class Introspection:
    table_list = 'SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = SCHEMA_NAME()'

    table_description = """
SELECT c.name, OBJECT_DEFINITION(c.default_object_id),
     IIF(c.collation_name = CAST(DATABASEPROPERTYEX(DB_NAME(),'collation') AS VARCHAR(256)), NULL, c.collation_name),
     c.is_identity, i.seed_value, i.increment_value
FROM sys.objects o
     INNER JOIN sys.columns c ON o.object_id = c.object_id
     LEFT JOIN sys.identity_columns i ON c.object_id = i.object_id
WHERE o.schema_id = SCHEMA_ID() AND o.name = %s"""

    sequences = """
SELECT c.name FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
WHERE t.schema_id = SCHEMA_ID() AND t.name = %s AND c.is_identity = 1"""

    relations = """
SELECT e.COLUMN_NAME AS column_name , c.TABLE_NAME AS referenced_table_name, d.COLUMN_NAME AS referenced_column_name
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS a
INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS b
    ON a.CONSTRAINT_NAME = b.CONSTRAINT_NAME AND a.TABLE_SCHEMA = b.CONSTRAINT_SCHEMA
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE AS c
    ON b.UNIQUE_CONSTRAINT_NAME = c.CONSTRAINT_NAME AND b.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS d
    ON c.CONSTRAINT_NAME = d.CONSTRAINT_NAME AND c.CONSTRAINT_SCHEMA = d.CONSTRAINT_SCHEMA
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS e
    ON a.CONSTRAINT_NAME = e.CONSTRAINT_NAME AND a.TABLE_SCHEMA = e.TABLE_SCHEMA
WHERE a.TABLE_SCHEMA = SCHEMA_NAME() AND a.TABLE_NAME = %s AND a.CONSTRAINT_TYPE = 'FOREIGN KEY'"""

    key_columns = """
SELECT c.name AS column_name, rt.name AS referenced_table_name, rc.name AS referenced_column_name
FROM sys.foreign_key_columns fk
INNER JOIN sys.tables t ON t.object_id = fk.parent_object_id
INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = fk.parent_column_id
INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
INNER JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fk.referenced_column_id
WHERE t.schema_id = SCHEMA_ID() AND t.name = %s"""

    get_constraints = """
SELECT
    kc.constraint_name,
    kc.column_name,
    tc.constraint_type,
    fk.referenced_table_name,
    fk.referenced_column_name
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kc
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc ON
    kc.table_schema = tc.table_schema AND
    kc.table_name = tc.table_name AND
    kc.constraint_name = tc.constraint_name
LEFT OUTER JOIN (
    SELECT
        ps.name AS table_schema,
        pt.name AS table_name,
        pc.name AS column_name,
        rt.name AS referenced_table_name,
        rc.name AS referenced_column_name
    FROM
        sys.foreign_key_columns fkc
    INNER JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id
    INNER JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
    INNER JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND
        fkc.parent_column_id = pc.column_id
    INNER JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id
    INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
    INNER JOIN sys.columns rc ON
        fkc.referenced_object_id = rc.object_id AND
        fkc.referenced_column_id = rc.column_id
) fk ON
    kc.table_schema = fk.table_schema AND
    kc.table_name = fk.table_name AND
    kc.column_name = fk.column_name
WHERE
    kc.table_schema = SCHEMA_NAME() AND
    kc.table_name = %s
ORDER BY
    kc.constraint_name ASC,
    kc.ordinal_position ASC """

    get_checks = """
SELECT kc.constraint_name, kc.column_name, cc.CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS kc
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c ON
        kc.table_schema = c.table_schema AND
        kc.table_name = c.table_name AND
        kc.constraint_name = c.constraint_name
    JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc ON
        cc.CONSTRAINT_CATALOG = c.CONSTRAINT_CATALOG
        AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
WHERE kc.table_schema = SCHEMA_NAME()
    AND kc.table_name =  %s """

    get_indices = """
SELECT
    i.name AS index_name,
    i.is_unique,
    i.is_primary_key,
    i.type,
    i.type_desc,
    ic.is_descending_key,
    i.has_filter,
    i.filter_definition,
    c.name AS column_name
FROM
    sys.tables AS t
INNER JOIN sys.schemas AS s ON
    t.schema_id = s.schema_id
INNER JOIN sys.indexes AS i ON
    t.object_id = i.object_id
INNER JOIN sys.index_columns AS ic ON
    i.object_id = ic.object_id AND
    i.index_id = ic.index_id
INNER JOIN sys.columns AS c ON
    ic.object_id = c.object_id AND
    ic.column_id = c.column_id
WHERE
    t.schema_id = SCHEMA_ID() AND
    t.name = %s
ORDER BY
    i.index_id ASC,
    ic.index_column_id ASC """

    get_default = """
SELECT c.name, d.name, d.definition
FROM sys.tables t
JOIN sys.default_constraints d ON d.parent_object_id = t.object_id
JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = d.parent_column_id
WHERE t.name = %s
AND t.schema_id = SCHEMA_ID()"""

    get_fks = """
SELECT OBJECT_NAME(f.parent_object_id) source_table,
    COL_NAME(fc.parent_object_id, fc.parent_column_id) source_col,
    OBJECT_NAME(f.referenced_object_id) target_table,
    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) target_col
FROM sys.foreign_keys AS f
    INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
WHERE f.schema_id = SCHEMA_ID()"""

    get_identities = """
SELECT o.name, i.seed_value FROM sys.objects o
INNER JOIN sys.columns c ON o.object_id = c.object_id
LEFT JOIN sys.identity_columns i ON c.object_id = i.object_id
WHERE o.schema_id = SCHEMA_ID() AND c.is_identity = 1 AND o.name IN (%s)"""


class Misc:
    delete_table = """
DECLARE @query NVARCHAR(MAX) = N'';
SELECT @query += N' ALTER TABLE ' + QUOTENAME(cs.name) + '.' + QUOTENAME(ct.name)  + ' DROP CONSTRAINT ' +
QUOTENAME(fk.name) + ';'  FROM sys.foreign_keys AS fk 
INNER JOIN sys.tables AS ct ON fk.parent_object_id = ct.[object_id]
INNER JOIN sys.schemas AS cs ON ct.[schema_id] = cs.[schema_id]
WHERE fk.referenced_object_id = OBJECT_ID('%(table)s');
EXEC(@query);
DROP TABLE %(table)s """
