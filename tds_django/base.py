import pytds
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings
from django.db import IntegrityError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.asyncio import async_unsafe
from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor
from .validation import DatabaseValidation


class DatabaseWrapper(BaseDatabaseWrapper):
    display_name = 'tds-django'
    vendor = 'sqlserver'
    Database = pytds

    data_types = {
        'AutoField': 'INT',
        'BigAutoField': 'BIGINT',
        'BigIntegerField': 'BIGINT',
        'BinaryField': 'VARBINARY(MAX)',
        'BooleanField': 'BIT',
        'CharField': 'NVARCHAR(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'DATETIME2',
        'DecimalField': 'DECIMAL(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'BIGINT',
        'FileField': 'NVARCHAR(%(max_length)s)',
        'FilePathField': 'NVARCHAR(%(max_length)s)',
        'FloatField': 'DOUBLE PRECISION',
        'IntegerField': 'INT',
        'IPAddressField': 'NVARCHAR(15)',
        'GenericIPAddressField': 'NVARCHAR(39)',
        'JSONField': 'NVARCHAR(MAX)',
        'NullBooleanField': 'BIT',
        'OneToOneField': 'INT',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveIntegerField': 'INT',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'NVARCHAR(%(max_length)s)',
        'SmallAutoField': 'SMALLINT',
        'SmallIntegerField': 'SMALLINT',
        'TextField': 'NVARCHAR(MAX)',
        'TimeField': 'TIME',
        'UUIDField': 'UNIQUEIDENTIFIER',
    }

    _limited_data_types = ('NVARCHAR(MAX)',)

    data_types_suffix = {
        'AutoField': 'IDENTITY (1, 1)',
        'BigAutoField': 'IDENTITY (1, 1)',
        'SmallAutoField': 'IDENTITY (1, 1)',
    }

    data_type_check_constraints = {
        'JSONField': '(ISJSON("%(column)s") = 1)',
        'PositiveIntegerField': '[%(column)s] >= 0',
        'PositiveSmallIntegerField': '[%(column)s] >= 0',
    }
    operators = {
        # Since '=' is used not only for string comparison there is no way
        # to make it case (in)sensitive.
        'exact': '= %s',
        'iexact': "= UPPER(%s)",
        'contains': "LIKE %s ESCAPE '\\'",
        'icontains': "LIKE UPPER(%s) ESCAPE '\\'",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE '\\'",
        'endswith': "LIKE %s ESCAPE '\\'",
        'istartswith': "LIKE UPPER(%s) ESCAPE '\\'",
        'iendswith': "LIKE UPPER(%s) ESCAPE '\\'",
    }

    # copied from pg
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '[\]'), '%%', '[%%]'), '_', '[_]')"
    pattern_ops = {
        'contains': "LIKE '%%' + {} + '%%'",
        'icontains': "LIKE '%%' + UPPER({}) + '%%'",
        'startswith': "LIKE {} + '%%'",
        'istartswith': "LIKE UPPER({}) + '%%'",
        'endswith': "LIKE '%%' + {}",
        'iendswith': "LIKE '%%' + UPPER({})",
    }

    SchemaEditorClass = DatabaseSchemaEditor

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation

    def get_connection_params(self):
        settings_dict = self.settings_dict
        # TODO warnings for user
        # skipped: as_dict use_tz bytes_to_unicode row_strategy server(?)
        allowed_params = 'timeout login_timeout appname tds_version use_mars auth readonly load_balancer ' \
                         'failover_partner cafile sock validate_host enc_login_only disable_connect_retry ' \
                         'pooling use_sso'.split()
        conn_params = {
            'dsn': settings_dict['HOST'] or 'localhost',
            'port': settings_dict['PORT'] or 1433,
            'database': settings_dict['NAME'],
            'user': settings_dict['USER'],
            'password': settings_dict['PASSWORD'],
            'autocommit': getattr(settings, 'AUTOCOMMIT', False),
            **{k: v for k, v in settings_dict.items() if k in allowed_params},
         }
        return conn_params

    def get_new_connection(self, conn_params):
        conn = self.Database.connect(**conn_params)
        return conn

    def init_connection_state(self):
        pass

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    @async_unsafe
    def create_cursor(self, name=None):
        return self.connection.cursor()

    def _savepoint_commit(self, sid):
        pass

    def disable_constraint_checking(self):
        with self.cursor() as cursor:
            cursor.execute('EXEC sp_MSforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT ALL";')
        return True

    def enable_constraint_checking(self):
        self.needs_rollback, needs_rollback = False, self.needs_rollback
        try:
            with self.cursor() as cursor:
                # !! we do not check when re-enabling constraint !!
                cursor.execute('EXEC sp_MSforeachtable "ALTER TABLE ? WITH NOCHECK CHECK CONSTRAINT ALL";')
        finally:
            self.needs_rollback = needs_rollback

    def check_constraints(self, table_names=None):
        with self.cursor() as cursor:
            self.clear_connection_messages()
            if table_names is None:
                sql = ['DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS, NO_INFOMSGS']
            else:
                sql = ["DBCC CHECKCONSTRAINTS ('%s') WITH ALL_CONSTRAINTS, NO_INFOMSGS" % t for t in table_names]
            for s in sql:
                cursor.execute(s)
                if cursor.description:
                    r = cursor.fetchone()
                    raise IntegrityError(r)

    def is_usable(self):
        return not self.connection._closed

    def get_connection_messages(self, cursor):
        return cursor.messages

    def clear_connection_messages(self):
        pass
