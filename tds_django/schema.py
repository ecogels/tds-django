import binascii
import datetime
import copy

from django.db.backends.utils import strip_quotes
from django.apps.registry import Apps
from django.db.backends.base.schema import BaseDatabaseSchemaEditor, _related_non_m2m_objects
from django.utils.encoding import force_str
from django.db.backends.ddl_references import Statement

from tds_django.sql.queries import Misc


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_column = 'ALTER TABLE %(table)s ADD %(column)s %(definition)s'
    sql_alter_column_default = 'ADD DEFAULT %(default)s FOR %(column)s'
    sql_delete_column = 'ALTER TABLE %(table)s DROP COLUMN %(column)s'
    sql_alter_column_no_default = 'DROP CONSTRAINT IF EXISTS %(column)s'
    sql_alter_column_type = 'ALTER COLUMN %(column)s %(type)s %(collation)s'
    sql_delete_index = 'DROP INDEX %(name)s ON %(table)s'
    sql_delete_table = Misc.delete_table
    sql_rename_column = "EXEC sp_rename '%(table)s.%(old_column)s', %(new_column)s, 'COLUMN'"
    sql_rename_table = 'EXEC sp_rename %(old_table)s, %(new_table)s'

    sql_alter_column_not_null = 'ALTER COLUMN %(column)s %(type)s NOT NULL'
    sql_alter_column_null = 'ALTER COLUMN %(column)s %(type)s NULL'

    sql_drop_constraint = 'ALTER TABLE %(table)s DROP CONSTRAINT IF EXISTS %(name)s'

    _auto_field_types = {'AutoField', 'BigAutoField', 'SmallAutoField'}

    def prepare_default(self, value):
        return self.quote_value(value)

    @staticmethod
    def quote_value(value):
        """
        Returns a quoted version of the value so it's safe to use in an SQL
        string. This is not safe against injection from user code; it is
        intended only for use in making SQL scripts or preparing default values
        for particularly tricky backends (defaults are not user-defined, though,
        so this is safe).
        """
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return "'%s'" % value
        elif isinstance(value, str):
            return "N'%s'" % value.replace("'", "''")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            return "0x%s" % force_str(binascii.hexlify(value))
        elif isinstance(value, bool):
            return '1' if value else '0'
        else:
            return str(value)

    def _alter_column_default_sql(self, model, old_field, new_field, drop=False):
        if drop:
            with self.connection.cursor() as cursor:
                defaults = self.connection.introspection.get_table_defaults(cursor, model._meta.db_table)
                constraint_name = defaults.get(new_field.column, ['DEF_DOES_NOT_EXISTS'])[0]
            return (self.sql_alter_column_no_default % {
                'column': constraint_name,
            }, [])
        return super()._alter_column_default_sql(model, old_field, new_field)

    def _is_identity_column(self, table_name, column_name):
        with self.connection.cursor() as cursor:
            sequences = self.connection.introspection.get_sequences(cursor, table_name)
        return column_name in [s['column'] for s in sequences]

    def _drop_pk(self, table_name, column_name):
        with self.connection.cursor() as cursor:
            constraints = self.connection.introspection.get_constraints(cursor, table_name)
            # drop primary key
            for name, props in constraints.items():
                if props['primary_key'] and column_name in props['columns']:
                    self.execute(self.sql_drop_constraint % {
                        'table': self.quote_name(table_name),
                        'name': self.quote_name(name)
                    })
                    # django 3.2 only supports one pk
                    break

    def _set_field_new_type_null_status(self, field, new_type):
        """ same mysql """
        if field.null:
            new_type += ' NULL'
        else:
            new_type += ' NOT NULL'
        return new_type

    def _alter_field(self, model, old_field, new_field, old_type, new_type,
                     old_db_params, new_db_params, strict=False):
        old_internal = old_field.get_internal_type()
        new_internal = new_field.get_internal_type()
        if (old_internal in self._auto_field_types) != (new_internal in self._auto_field_types):
            # need to recreate either column or table
            # choose remaking table like sqlite yolo
            # delete FKs and their index
            for _old_rel, new_rel in _related_non_m2m_objects(old_field, new_field):
                self._remove_constraints(new_rel.related_model, new_rel.field.column)

            self._remake_table(model, alter_field=(old_field, new_field))

            # Rebuild tables with FKs pointing to this field.
            if new_field.unique and old_type != new_type:
                related_models = set()
                opts = new_field.model._meta
                for remote_field in opts.related_objects:
                    # Ignore self-relationship since the table was already rebuilt.
                    if remote_field.related_model == model:
                        continue
                    if not remote_field.many_to_many:
                        if remote_field.field_name == new_field.name:
                            related_models.add(remote_field.related_model)
                    elif new_field.primary_key and remote_field.through._meta.auto_created:
                        related_models.add(remote_field.through)
                if new_field.primary_key:
                    for many_to_many in opts.many_to_many:
                        # Ignore self-relationship since the table was already rebuilt.
                        if many_to_many.related_model == model:
                            continue
                        if many_to_many.remote_field.through._meta.auto_created:
                            related_models.add(many_to_many.remote_field.through)
                for related_model in related_models:
                    self._remake_table(related_model)
        else:
            # null does not allow "post actions"
            post_actions = []
            if old_field.null and not new_field.null:
                # drop and recreate indices
                post_actions = self._remove_constraints(model, old_field.column, new_field, index=True, pk=False,
                                                        fk=False, check=False, unique=False)
            super()._alter_field(model, old_field, new_field, old_type, new_type, old_db_params, new_db_params, strict)
            if self._field_becomes_null_unique(old_field, new_field):
                column = new_field.column
                condition = '%s IS NOT NULL' % self.quote_name(column)
                sql = self._create_unique_sql(model, [new_field], condition=condition)
                post_actions.append(sql)

            for sql in post_actions:
                self.execute(sql)

    def remove_field(self, model, field):
        """
        Remove a field from a model. Usually involves deleting a column,
        but for M2Ms may involve deleting a table.
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.remote_field.through._meta.auto_created:
            return self.delete_model(field.remote_field.through)
        # It might not actually have a column behind it
        if field.db_parameters(connection=self.connection)['type'] is None:
            return
        # PATCH START
        self._remove_constraints(model, field.column)
        # PATCH END
        # Delete the column
        sql = self.sql_delete_column % {
            'table': self.quote_name(model._meta.db_table),
            'column': self.quote_name(field.column),
        }
        self.execute(sql)
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()
        # Remove all deferred statements referencing the deleted column.
        for sql in list(self.deferred_sql):
            if isinstance(sql, Statement) and sql.references_column(model._meta.db_table, field.column):
                self.deferred_sql.remove(sql)

    def _remake_table(self, model, create_field=None, delete_field=None, alter_field=None):
        """
        Similar to sqlite
        """
        # Self-referential fields must be recreated rather than copied from
        # the old model to ensure their remote_field.field_name doesn't refer
        # to an altered field.
        def is_self_referential(f):
            return f.is_relation and f.remote_field.model is model

        # Work out the new fields dict / mapping
        body = {
            f.name: f.clone() if is_self_referential(f) else f
            for f in model._meta.local_concrete_fields
        }
        # Since mapping might mix column names and default values,
        # its values must be already quoted.
        mapping = {f.column: self.quote_name(f.column) for f in model._meta.local_concrete_fields}
        # This maps field names (not columns) for things like unique_together
        rename_mapping = {}
        # If any of the new or altered fields is introducing a new PK,
        # remove the old one
        restore_pk_field = None
        if getattr(create_field, 'primary_key', False) or (
                alter_field and getattr(alter_field[1], 'primary_key', False)):
            for name, field in list(body.items()):
                if field.primary_key:
                    field.primary_key = False
                    restore_pk_field = field
                    if field.auto_created:
                        del body[name]
                        del mapping[field.column]
        # Add in any created fields
        if create_field:
            body[create_field.name] = create_field
            # Choose a default and insert it into the copy map
            if not create_field.many_to_many and create_field.concrete:
                mapping[create_field.column] = self.quote_value(
                    self.effective_default(create_field)
                )
        # Add in any altered fields
        if alter_field:
            old_field, new_field = alter_field
            body.pop(old_field.name, None)
            mapping.pop(old_field.column, None)
            body[new_field.name] = new_field
            if old_field.null and not new_field.null:
                case_sql = 'coalesce(%(col)s, %(default)s)' % {
                    'col': self.quote_name(old_field.column),
                    'default': self.quote_value(self.effective_default(new_field))
                }
                mapping[new_field.column] = case_sql
            else:
                mapping[new_field.column] = self.quote_name(old_field.column)
            rename_mapping[old_field.name] = new_field.name
        # Remove any deleted fields
        if delete_field:
            del body[delete_field.name]
            del mapping[delete_field.column]
            # Remove any implicit M2M tables
            if delete_field.many_to_many and delete_field.remote_field.through._meta.auto_created:
                return self.delete_model(delete_field.remote_field.through)
        # Work inside a new app registry
        apps = Apps()

        # Work out the new value of unique_together, taking renames into
        # account
        unique_together = [
            [rename_mapping.get(n, n) for n in unique]
            for unique in model._meta.unique_together
        ]

        # Work out the new value for index_together, taking renames into
        # account
        index_together = [
            [rename_mapping.get(n, n) for n in index]
            for index in model._meta.index_together
        ]

        indexes = model._meta.indexes
        if delete_field:
            indexes = [
                index for index in indexes
                if delete_field.name not in index.fields
            ]

        constraints = list(model._meta.constraints)

        # Provide isolated instances of the fields to the new model body so
        # that the existing model's internals aren't interfered with when
        # the dummy model is constructed.
        body_copy = copy.deepcopy(body)

        # Construct a new model with the new fields to allow self referential
        # primary key to resolve to. This model won't ever be materialized as a
        # table and solely exists for foreign key reference resolution purposes.
        # This wouldn't be required if the schema editor was operating on model
        # states instead of rendered models.
        meta_contents = {
            'app_label': model._meta.app_label,
            'db_table': model._meta.db_table,
            'unique_together': unique_together,
            'index_together': index_together,
            'indexes': indexes,
            'constraints': constraints,
            'apps': apps,
        }
        meta = type('Meta', (), meta_contents)
        body_copy['Meta'] = meta
        body_copy['__module__'] = model.__module__
        type(model._meta.object_name, model.__bases__, body_copy)

        # Construct a model with a renamed table name.
        body_copy = copy.deepcopy(body)
        meta_contents = {
            'app_label': model._meta.app_label,
            'db_table': 'new__%s' % strip_quotes(model._meta.db_table),
            'unique_together': unique_together,
            'index_together': index_together,
            'indexes': indexes,
            'constraints': constraints,
            'apps': apps,
        }
        meta = type('Meta', (), meta_contents)
        body_copy['Meta'] = meta
        body_copy['__module__'] = model.__module__
        new_model = type('New%s' % model._meta.object_name, model.__bases__, body_copy)

        # Create a new table with the updated schema.
        self.create_model(new_model)

        # Copy data from the old table into the new table
        sql = 'INSERT INTO %(table)s (%(source)s) SELECT %(dest)s FROM %(old_table)s' % {
            'table': self.quote_name(new_model._meta.db_table),
            'source': ', '.join(self.quote_name(x) for x in mapping),
            'dest': ', '.join(mapping.values()),
            'old_table': self.quote_name(model._meta.db_table),
        }
        if new_model._meta.auto_field:
            sql = 'SET IDENTITY_INSERT %(table)s ON;%(sql)s; SET IDENTITY_INSERT %(table)s OFF' % {
                'sql': sql,
                'table': self.quote_name(new_model._meta.db_table),
            }
        self.execute(sql)

        # Delete the old table to make way for the new
        self.delete_model(model, handle_autom2m=False)

        # Rename the new table to take way for the old
        self.alter_db_table(new_model, new_model._meta.db_table, model._meta.db_table)  # , disable_constraints=False,

        # Run deferred SQL on correct table
        for sql in self.deferred_sql:
            self.execute(sql)
        self.deferred_sql = []
        # Fix any PK-removed field
        if restore_pk_field:
            restore_pk_field.primary_key = True

    def delete_model(self, model, handle_autom2m=True):
        if handle_autom2m:
            super().delete_model(model)
        else:
            # Delete the table (and only that)
            self.execute(self.sql_delete_table % {
                'table': self.quote_name(model._meta.db_table),
            })
            # Remove all deferred statements referencing the deleted table.
            for sql in list(self.deferred_sql):
                if isinstance(sql, Statement) and sql.references_table(model._meta.db_table):
                    self.deferred_sql.remove(sql)

    def _remove_constraints(self, model, column_name, new_field=None, index=True, pk=True, unique=True, fk=True,
                            check=True):
        """ remove constraints and return constraint creation sql """
        table_name = model._meta.db_table
        reverse = []
        new_db_params = new_field.db_parameters(connection=self.connection) if new_field else None
        with self.connection.cursor() as cursor:
            constraints = self.connection.introspection.get_constraints(cursor, table_name)
        for name, info in constraints.items():
            if column_name in info['columns']:
                sql = None
                if info['foreign_key'] and fk:
                    sql = self.sql_drop_constraint
                elif info['primary_key'] and pk:
                    sql = self.sql_delete_pk
                    if new_field and new_field.primary_key:
                        reverse.append(self.sql_create_pk % {
                            'table': self.quote_name(table_name), 'name': self.quote_name(name),
                            'columns': self.quote_name(new_field.column)
                        })
                elif info['check'] and check:
                    sql = self.sql_delete_check
                    if new_field and new_db_params['check']:
                        clause = info['clause']
                        if column_name != new_field.column:
                            clause = clause.replace(self.quote_name(column_name), self.quote_name(new_field.column))
                        reverse.append(self.sql_create_check % {
                            'table': self.quote_name(table_name), 'name': self.quote_name(name),
                            'check': clause,
                        })
                elif info['unique'] and unique:
                    sql = self.sql_delete_unique
                    if new_field:
                        columns = [model._meta.get_field(c) if c != column_name else new_field for c in info['columns']]
                        reverse.append(self._create_unique_sql(model, columns, name))
                elif info['index'] and index:
                    sql = self.sql_delete_index
                    if new_field and not (len(info['columns']) == 1 and info['unique'] and not new_field.unique):
                        columns = [c if c != column_name else new_field.column for c in info['columns']]
                        # from null unique to unique
                        if not new_field.null and len(columns) == 1 and info['unique'] and info['has_filter'] and \
                                ('(%s IS NOT NULL)' % self.quote_name(column_name)) == info['filter_definition']:
                            condition = ''
                        else:
                            condition = f" WHERE {info['filter_definition']}" if info['has_filter'] else ''
                        reverse.append('CREATE %s INDEX %s ON %s (%s)%s' % (
                            'UNIQUE' if info['unique'] else '',
                            self.quote_name(name),
                            self.quote_name(table_name),
                            ', '.join(self.quote_name(c) for c in columns),
                            condition,
                        ))
                if sql:
                    self.execute(sql % {
                        'table': self.quote_name(table_name),
                        'name': self.quote_name(name),
                    })
        return reverse if new_field else None

    def _alter_column_type_sql(self, model, old_field, new_field, new_type, old_collation, new_collation):
        fragment, more = super()._alter_column_type_sql(model, old_field, new_field, new_type, old_collation,
                                                        new_collation)
        if not new_field.null:
            (sql, params) = fragment
            fragment = (sql + ' NOT NULL', params)
        # at this point the field would have already be renamed
        todo = self._remove_constraints(model, new_field.column, new_field)
        more += [(sql, []) for sql in todo]
        return fragment, more

    def _collate_sql(self, collation, old_collation=None, table_name=None):
        return ' COLLATE ' + collation if collation else ''

    def _field_should_be_indexed(self, model, field):
        """ we prevent the nullable unique constraint at table creation so we need to do it now """
        create_index = super()._field_should_be_indexed(model, field) or (field.unique and field.null)
        if create_index:
            db_type = field.db_type(self.connection)
            if db_type is not None and db_type.lower() in 'nvarchar(max)':
                return False
        return create_index

    def column_sql(self, model, field, include_default=False):
        """ we need to delay nullable unique """
        if field.unique and field.null:
            field._unique = False
            sql, params = super().column_sql(model, field, include_default)
            field._unique = True
            return sql, params
        return super().column_sql(model, field, include_default)

    def _create_index_sql(self, model, *, fields=None, sql=None, suffix='', **kwargs):
        """ for nullable unique constraint """
        if not sql and len(fields) == 1 and fields[0].unique and fields[0].null:
            column = fields[0].column
            condition = '%s IS NOT NULL' % self.quote_name(column)
            return self._create_unique_sql(model, fields, condition=condition)
        return super()._create_index_sql(model, fields=fields, sql=sql, suffix=suffix, **kwargs)

    def _unique_should_be_added(self, old_field, new_field):
        if self._field_becomes_null_unique(old_field, new_field):
            return False
        return super()._unique_should_be_added(old_field, new_field)

    def _field_becomes_null_unique(self, old_field, new_field):
        return not (old_field.null and old_field.unique) and new_field.null and new_field.unique
