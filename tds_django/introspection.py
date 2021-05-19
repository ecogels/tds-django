from collections import namedtuple
from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo, FieldInfo as BaseFieldInfo

from django.db.models.indexes import Index

from tds_django.sql.queries import Introspection

FieldInfo = namedtuple('FieldInfo', BaseFieldInfo._fields + ('identity', 'seed', 'increment',))


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Map type codes to Django Field types.
    data_types_reverse = {48: 'SmallIntegerField', 52: 'SmallIntegerField', 56: 'IntegerField', 127: 'BigIntegerField',
                          50: 'BooleanField', 59: 'FloatField', 62: 'FloatField', 122: 'DecimalField',
                          60: 'DecimalField', 175: 'CharField', 167: 'CharField', 239: 'CharField', 231: 'CharField',
                          35: 'TextField', 99: 'TextField', 173: 'BinaryField', 165: 'BinaryField', 34: 'ImageField',
                          108: 'DecimalField', 106: 'DecimalField', 40: 'DateField', 41: 'TimeField',
                          42: 'DateTimeField', 43: 'DurationField', 58: 'DateTimeField', 61: 'DateTimeField',
                          36: 'UUIDField'}

    ignored_tables = []

    def get_field_type(self, data_type, description):
        field_type = super().get_field_type(data_type, description)
        if description.identity:
            if field_type == 'BigIntegerField':
                return 'BigAutoField'
            if field_type == 'SmallIntegerField':
                return 'SmallAutoField'
            return 'AutoField'
        return field_type

    def get_table_list(self, cursor):
        """
        Returns a list of table and view names in the current database.
        """
        cursor.execute(Introspection.table_list)
        types = {'BASE TABLE': 't', 'VIEW': 'v'}
        return [TableInfo(row[0], types.get(row[1]))
                for row in cursor.fetchall()
                if row[0] not in self.ignored_tables]

    def get_table_description(self, cursor, table_name):
        """ As of django 3.2 django still does not take defaults into account
            Ignores collation if it is the server default
        """
        cursor.execute(Introspection.table_description, (table_name, ))
        extras = {line[0]: line[1:] for line in cursor.fetchall()}

        cursor.execute('SELECT TOP 1 * FROM %s' % self.connection.ops.quote_name(table_name, ))
        return [FieldInfo(*column, *extras[column[0]]) for column in cursor.description]

    def get_sequences(self, cursor, table_name, table_fields=()):
        cursor.execute(Introspection.sequences, (table_name, ))
        # SQL Server allows only one identity column per table
        # https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
        row = cursor.fetchone()
        return [{'table': table_name, 'column': row[0]}] if row else []

    def get_relations(self, cursor, table_name):
        cursor.execute(Introspection.relations, (table_name, ))
        return {item[0]: (item[2], item[1]) for item in cursor.fetchall()}

    def get_key_columns(self, cursor, table_name):
        """
        Returns a list of (column_name, referenced_table_name, referenced_column_name) for all
        key columns in given table.
        """
        cursor.execute(Introspection.key_columns, (table_name, ))
        return [tuple(row) for row in cursor.fetchall()]

    def get_constraints(self, cursor, table_name):
        """
        Retrieves any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Returns a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.
         * orders: The order (ASC/DESC) defined for the columns of indexes
         * type: The type of the index (btree, hash, etc.)
        """
        constraints = {}
        # Loop over the key table, collecting things as constraints
        # This will get PKs, FKs, and uniques, but not CHECK
        cursor.execute(Introspection.get_constraints, (table_name, ))
        for constraint, column, kind, ref_table, ref_column in cursor.fetchall():
            # If we're the first column, make the record
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': [],
                    'primary_key': kind.lower() == 'primary key',
                    'unique': kind.lower() in ['primary key', 'unique'],
                    'foreign_key': (ref_table, ref_column) if kind.lower() == 'foreign key' else None,
                    'check': False,
                    'index': False,
                }
            # Record the details
            constraints[constraint]['columns'].append(column)
        # Now get CHECK constraint columns
        cursor.execute(Introspection.get_checks, (table_name, ))
        for constraint, column, clause in cursor.fetchall():
            # If we're the first column, make the record
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False,
                    'clause': clause,
                }
            # Record the details
            constraints[constraint]['columns'].append(column)
        # indices
        cursor.execute(Introspection.get_indices, (table_name, ))
        indices = {}
        for name, unique, primary, type_, desc, order, has_filter, filter_definition, column in cursor.fetchall():
            if name not in indices:
                indices[name] = {
                    'columns': [],
                    'primary_key': primary,
                    'unique': unique,
                    'foreign_key': None,
                    'check': False,
                    'index': True,
                    'has_filter': has_filter,
                    'filter_definition': filter_definition,
                    'orders': [],
                    'type': Index.suffix if type_ in (1, 2) else desc.lower(),
                }
            indices[name]['columns'].append(column)
            indices[name]['orders'].append('DESC' if order == 1 else 'ASC')
        for name, constraint in indices.items():
            if name not in constraints:
                constraints[name] = constraint
        return constraints

    def get_table_defaults(self, cursor, table_name):
        cursor.execute(Introspection.get_default, (table_name, ))
        return {line[0]: line[1:] for line in cursor.fetchall()}
