import re
from django.conf import settings
from django.db import DatabaseError, OperationalError
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.models import Exists, ExpressionWrapper, Lookup
from django.db.models.expressions import RawSQL
from django.db.models.sql.where import WhereNode
from django.utils import timezone

from tds_django.sql.queries import Introspection
from .tz import iana_win_map


class DatabaseOperations(BaseDatabaseOperations):
    cast_char_field_without_max_length = 'NVARCHAR(MAX)'
    compiler_module = 'tds_django.compiler'
    # explain_prefix = 'SET SHOWPLAN_XML ON;'
    _re_utc_offset = re.compile(r'^utc[+-]', re.IGNORECASE)

    def savepoint_create_sql(self, sid):
        return 'SAVE TRANSACTION %s' % self.quote_name(sid)

    def savepoint_rollback_sql(self, sid):
        return 'ROLLBACK TRANSACTION %s' % self.quote_name(sid)

    def _prepare_tzname_delta(self, tzname):
        if '/' in tzname:
            t = iana_win_map.get(tzname, False)
            if not t:
                raise ValueError(f"Invalid TimeZone {tzname}")
            return t
        elif self._re_utc_offset.match(tzname):
            return tzname[3:]
        return tzname

    def _sql_tz(self, sql, params, tzname):
        tzname = self._prepare_tzname_delta(tzname)
        if tzname[0] in '+-':
            return f"SWITCHOFFSET({sql}, '{tzname}')", params
        else:
            return f"{sql} AT TIME ZONE '{tzname}'", params

    def _convert_sql_to_tz(self, sql, params, tzname):
        if tzname and settings.USE_TZ and self.connection.timezone_name != tzname:
            sql, params = self._sql_tz(sql, params, self.connection.timezone_name or 'UTC')
            return self._sql_tz(sql, params, tzname)
        return sql, params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return self.date_extract_sql(lookup_type, sql, params)


    def date_extract_sql(self, lookup_type, sql, params):
        # TODO GET SCHEMA. IN CONNECTION?
        if not lookup_type.lower() in 'year quarter month dayofyear day weekday hour minute week second ' \
                'millisecond microsecond nanosecond tzoffset iso_week iso_week_day iso_year week_day'.split():
            raise OperationalError(f'Lookup {lookup_type} not supported.')
        return f"dbo.django_date_extract('{lookup_type.lower()}', {sql})", params

    def datetime_cast_date_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f'CAST({sql} AS DATE)', params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self.datetime_trunc_sql(lookup_type, sql, params, tzname)
        return f'CAST({sql} AS DATE)', params

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        if not lookup_type.lower() in 'year quarter month dayofyear day weekday hour minute week second'.split():
            raise OperationalError(f'Lookup {lookup_type} not supported.')
        return f"dbo.django_datetime_trunc('{lookup_type}', {sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f'CAST({sql} as TIME)', params

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        """ similar mysql """
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        fields = {
            'hour': r'hh\:\0\0\:\0\0',
            'minute': r'hh\:mm\:\0\0',
            'second': r'hh\:mm\:ss',
        }
        if lookup_type in fields:
            return f"CAST(FORMAT(CAST({sql} AS TIME), '{fields[lookup_type]}') AS TIME)", params
        else:
            return f'CAST({sql} as TIME)', params

    def quote_name(self, name):
        """
        Returns a quoted version of the given table, index or column name. Does
        not quote the given name if it's already been quoted.
        """
        if name.startswith('[') and name.endswith(']'):
            return name
        return '[%s]' % name

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []
        sql = ['EXEC sp_MSforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT ALL"']

        if allow_cascade:
            with self.connection.cursor() as cursor:
                cursor.execute(Introspection.get_fks)
                m = {(a, b): (c, d) for a, b, c, d in cursor.fetchall()}
                cols_to_del = set()
                while True:
                    newm = {}
                    for (s, k), (t, c) in m.items():
                        if t in tables or (t, c) in cols_to_del:
                            sql.append('DELETE FROM %s WHERE %s IS NOT NULL' % (self.quote_name(s),
                                                                                  self.quote_name(k)))
                            cols_to_del.add((s, k))
                        else:
                            newm[(s, k)] = (t, c)
                    if len(newm) == len(m):
                        break
                    m = newm

        sql.extend(
            '%s %s %s;' % (
                style.SQL_KEYWORD('DELETE'),
                style.SQL_KEYWORD('FROM'),
                style.SQL_FIELD(self.quote_name(table_name)),
            ) for table_name in tables
        )
        if reset_sequences:
            qids = Introspection.get_identities % \
                   ', '.join('%s' for _ in tables)
            with self.connection.cursor() as cursor:
                cursor.execute(qids, tuple(tables))
                for t, i in cursor.fetchall():
                    sql.append('DBCC CHECKIDENT (%s, RESEED, %d)' % (self.quote_name(t), i))

        sql += ['EXEC sp_MSforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL";']
        return sql

    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'DateTimeField':
            converters.append(self.convert_datetimefield_value)
        elif internal_type == 'FloatField':
            converters.append(self.convert_floatfield_value)
        elif internal_type in ('BooleanField', 'NullBooleanField'):
            converters.append(self.convert_booleanfield_value)
        return converters

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if settings.USE_TZ and not timezone.is_aware(value):
                value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_floatfield_value(self, value, expression, connection):
        if value is not None:
            value = float(value)
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        return bool(value) if value in (0, 1) else value

    def conditional_expression_supported_in_where_clause(self, expression):
        """ same as oracle """
        if isinstance(expression, (Exists, Lookup, WhereNode,)):
            return True
        if isinstance(expression, ExpressionWrapper) and expression.conditional:
            return self.conditional_expression_supported_in_where_clause(expression.expression)
        if isinstance(expression, RawSQL) and expression.conditional:
            return True
        return False

    def max_name_length(self):
        return 128

    def return_insert_columns(self, fields):
        if not fields:
            return '', ()
        field_names = []
        params = []
        for field in fields:
            field_names.append('INSERTED.%s' % self.quote_name(field.column))
            params.append(field)
        return 'OUTPUT %s' % ', '.join(field_names), ()

    def fetch_returned_insert_columns(self, cursor, returning_params):
        return cursor.fetchone()

    def pk_default_value(self):
        """
        Return the value to use during an INSERT statement to specify that
        the field should use its default value.
        """
        return 'NULL'

    def check_expression_support(self, expression):
        """It feels useless to send less info to the user than what would be returned from sql server. """
        pass

    def _get_limit_offset_params(self, low_mark, high_mark):
        offset = low_mark or 0
        if high_mark is not None:
            return (high_mark - offset), offset
        return None, offset

    def limit_offset_sql(self, low_mark, high_mark):
        limit, offset = self._get_limit_offset_params(low_mark, high_mark)
        if limit and not offset:
            return ''  # need TOP x, done in compiler
        return ' '.join(sql for sql in (
            ('OFFSET %d ROWS' % offset) if offset else None,
            ('FETCH NEXT %d ROWS ONLY' % limit) if limit else None,
        ) if sql)

    def combine_expression(self, connector, sub_expressions):
        """
        SQL Server requires special cases for some operators in query expressions
        """
        if connector == '^':
            return 'POWER(CAST(%s AS FLOAT), %s)' % tuple(sub_expressions)
        elif connector == '<<':
            return 'dbo.django_bitshift(%s, -1 * %s, 0)' % tuple(sub_expressions)
        elif connector == '>>':
            return 'dbo.django_bitshift(%s, %s, 0)' % tuple(sub_expressions)
        elif connector == '#':
            return super().combine_expression('^', sub_expressions)
        return super().combine_expression(connector, sub_expressions)

    def subtract_temporals(self, internal_type, lhs, rhs):
        lhs_sql, lhs_params = lhs
        rhs_sql, rhs_params = rhs
        return f'DATEDIFF_BIG(microsecond, {rhs_sql}, {lhs_sql})', (*rhs_params, *lhs_params)

    def combine_duration_expression(self, connector, sub_expressions):
        if connector not in ['+', '-',]:
            raise DatabaseError('Invalid connector for timedelta: %s.' % connector)
        lhs, rhs = sub_expressions
        sign = '-' if connector == '-' else ''
        if lhs.startswith('dbo'):
            col, sql = rhs, lhs
        else:
            col, sql = lhs, rhs
        return sql.format(col, sign)

    def format_for_duration_arithmetic(self, sql):
        return 'dbo.django_dtdelta({}, {}%s)' % sql

    def adapt_timefield_value(self, value):
        if value is None:
            return None
        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value
        if timezone.is_aware(value):
            raise ValueError("SQL Server backend does not support timezone-aware times.")
        return str(value)

    def adapt_datetimefield_value(self, value):
        """
        Transforms a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None

        if hasattr(value, 'resolve_expression'):
            return value

        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(self.connection.timezone).replace(tzinfo=None)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware datetimes when USE_TZ is False.")

        return value

    def bulk_batch_size(self, fields, objs):
        if fields:
            return self.connection.features.max_query_params // len(fields)
        return len(objs)

    def cache_key_culling_sql(self):
        return 'SELECT [cache_key] FROM %s ORDER BY cache_key OFFSET %%s ROWS FETCH FIRST 1 ROWS ONLY'

    def last_executed_query(self, cursor, sql, params):
        if params:
            m = tuple(f'{self.connection.SchemaEditorClass.quote_value(p) if p is not None else "NULL"}' for p in params)
            return sql % m
        return sql

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return 'UPPER(%s)'
        if internal_type == 'UUIDField':
            return 'LOWER(%s)'
        return '%s'

    def prep_for_iexact_query(self, x):
        return x

    def regex_lookup(self, lookup_type):
        if lookup_type == 'regex':
            return 'dbo.django_regex(%s, %s) = 1'
        return 'dbo.django_iregex(%s, %s) = 1'
