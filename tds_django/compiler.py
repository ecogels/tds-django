import re

from itertools import chain
from django.db.models.expressions import Subquery, RawSQL
from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    _re_constant = re.compile(r'^\s*\(?\s*\d+\s*\)?\s*')

    def as_sql(self, with_limits=True, with_col_aliases=False):
        if self.query.subquery and not with_limits:
            self.query.clear_ordering(force=True)

        sql, params = super().as_sql(with_limits=with_limits, with_col_aliases=with_col_aliases)
        if with_limits and not self.query.low_mark and self.query.high_mark is not None:
            if self.query.combinator == 'union':
                try:
                    idx = sql.index('ORDER BY')
                    sql = f'SELECT TOP {self.query.high_mark} * FROM ({sql[:idx]}) t {sql[idx:]}'
                except ValueError:
                    sql = f'SELECT TOP {self.query.high_mark} * FROM ({sql}) t'
            else:
                needle = 'SELECT DISTINCT' if self.query.distinct else 'SELECT'
                sql = sql.replace(needle, needle + ' TOP %d' % self.query.high_mark, 1)
        if not params and hasattr(self, 'escape_if_noparams'):
            sql = sql % ()
        return sql, params

    def get_order_by(self):
        order_by = super().get_order_by()
        if order_by:
            # TODO? sql server cannot order by constants
            pass
        elif self.query.low_mark:  # if offset, need order
            order_by = [(None, ('1 ASC', [], None)), ]
        return order_by

    def collapse_group_by(self, expressions, having):
        expressions = super().collapse_group_by(expressions, having)

        def allowed(e):
            if isinstance(e, RawSQL):
                return not self._re_constant.match(e.sql)
            if not (hasattr(e, 'contains_column_references') and e.contains_column_references):
                return False
            if isinstance(e, Subquery):
                return False
            return True

        return [e for e in expressions if allowed(e)]


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def fix_auto(self, sql, opts, fields, qn):
        if opts.auto_field is not None:
            # db_column is None if not explicitly specified by model field
            auto_field_column = opts.auto_field.db_column or opts.auto_field.column
            columns = [f.column for f in fields]
            if auto_field_column in columns:
                id_insert_sql = []
                table = qn(opts.db_table)
                sql_format = 'SET IDENTITY_INSERT %s ON; %s; SET IDENTITY_INSERT %s OFF'
                for q, p in sql:
                    id_insert_sql.append((sql_format % (table, q, table), p))
                sql = id_insert_sql

        return sql

    def as_sql(self):
        result = self._as_sql()
        if self.query.fields:
            # remove id if explicit insert default
            opts = self.query.get_meta()
            result = self.fix_auto(result, opts, self.query.fields or [opts.pk], self.connection.ops.quote_name)
        return result

    def _as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement(
            on_conflict=self.query.on_conflict,
        )
        result = ["%s %s" % (insert_statement, qn(opts.db_table))]

        # PATCH START for sql server we ignore identity fields differently
        if self.query.fields:
            fields = self.query.fields
            result.append('(%s)' % ', '.join(qn(f.column) for f in fields))
            value_rows = [
                [self.prepare_value(field, self.pre_save_val(field, obj)) for field in fields]
                for obj in self.query.objs
            ]
        else:
            # An empty object.
            value_rows = []
            fields = []
        # PATCH END


        # Currently the backends just accept values when generating bulk
        # queries and generate their own placeholders. Doing that isn't
        # necessary and it should be possible to use placeholders and
        # expressions in bulk inserts too.
        can_bulk = (
            not self.returning_fields and self.connection.features.has_bulk_insert
        )

        placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)

        on_conflict_suffix_sql = self.connection.ops.on_conflict_suffix_sql(
            fields,
            self.query.on_conflict,
            (f.column for f in self.query.update_fields),
            (f.column for f in self.query.unique_fields),
        )
        if (
            self.returning_fields
            and self.connection.features.can_return_columns_from_insert
        ):
            if self.connection.features.can_return_rows_from_bulk_insert:
                result.append(
                    self.connection.ops.bulk_insert_sql(fields, placeholder_rows)
                )
                params = param_rows
            # PATCH START
            elif self.query.fields:
                result.append('VALUES (%s)' % ', '.join(placeholder_rows[0]))
                params = [param_rows[0]]
            else:
                result.append('DEFAULT VALUES')
                params = []
            # PATCH END

            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            # Skip empty r_sql to allow subclasses to customize behavior for
            # 3rd party backends. Refs #19096.
            r_sql, self.returning_params = self.connection.ops.return_insert_columns(
                self.returning_fields
            )
            if r_sql:
                # PATCH START sql needs output before values
                result.insert(-1, r_sql)
                # PATCH END
                params += [self.returning_params]
            return [(" ".join(result), tuple(chain.from_iterable(params)))]

        if can_bulk:
            # PATCH START
            if not self.query.fields:
                result.append('DEFAULT VALUES')
                sql = ';'.join(' '.join(result) for _ in self.query.objs)
                return [(sql, ())]
            # PATCH END

            result.append(self.connection.ops.bulk_insert_sql(fields, placeholder_rows))
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            return [(" ".join(result), tuple(p for ps in param_rows for p in ps))]
        else:
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            return [
                (" ".join(result + ["VALUES (%s)" % ", ".join(p)]), vals)
                for p, vals in zip(placeholder_rows, param_rows)
            ]


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    def as_sql(self):
        sql, params = super().as_sql()
        if sql:
            sql = '; '.join(['SET NOCOUNT OFF', sql])
        return sql, params


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_sql(self):
        sql, params = super().as_sql()
        if sql:
            sql = '; '.join(['SET NOCOUNT OFF', sql])
        return sql, params


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
