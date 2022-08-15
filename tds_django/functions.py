from django.db.models import BooleanField, IntegerField, Lookup
from django.db.models.aggregates import Avg, Count, StdDev, Variance
from django.db.models.expressions import Value, OrderBy, OrderByList, Exists, RawSQL, Window, ExpressionList, Case, When, \
    DurationExpression, CombinedExpression
from django.db.models.fields.json import HasKeyLookup
from django.db.models.functions import Now, ATan2, Chr, Collate, Greatest, Least, Length, LPad, Random, \
    Repeat, RPad, StrIndex, Substr, Log, Ln, Mod, Round, Degrees, Power, Radians, RowNumber
from django.db.models.lookups import BuiltinLookup


def as_sqlserver(expression):
    def decorator(func):
        setattr(expression, 'as_sqlserver', func)
        return func
    return decorator


@as_sqlserver(Value)
def value_(self, compiler, connection, **extra):
    if compiler.query.group_by and self.value is not None and isinstance(self.output_field, IntegerField):
        # sql server would fail with group by col * %s so we replace literal
        return str(int(self.value)), ()
    return self.as_sql(compiler, connection, **extra)


@as_sqlserver(Now)
def now(self, compiler, connection, **extra):
    return self.as_sql(compiler, connection, template='SYSDATETIME()', **extra)


@as_sqlserver(Avg)
def avg(self, compiler, connection):
    return self.as_sql(compiler, connection, template='%(function)s(%(distinct)sCAST(%(field)s AS FLOAT))')


@as_sqlserver(Chr)
def chr(self, compiler, connection):
    return self.as_sql(compiler, connection, function='NCHAR')


@as_sqlserver(Collate)
def collate(self, compiler, connection, **extra_context):
    extra_context.setdefault('collation', self.collation)
    return self.as_sql(compiler, connection, **extra_context)


@as_sqlserver(Count)
def count(self, compiler, connection):
    return self.as_sql(compiler, connection, function='COUNT_BIG')


@as_sqlserver(Degrees)
@as_sqlserver(Radians)
def degrad(self, compiler, connection):
    float_tpl = '%(function)s(CAST(%(field)s AS FLOAT))'
    return self.as_sql(compiler, connection, template=float_tpl)


@as_sqlserver(Greatest)
def greatest(self, compiler, connection):
    template = '(SELECT MAX(value) FROM (VALUES (%(expressions)s)) AS _%(function)s(value))'
    return self.as_sql(compiler, connection, arg_joiner='), (', template=template)


@as_sqlserver(Least)
def least(self, compiler, connection):
    template = '(SELECT MIN(value) FROM (VALUES (%(expressions)s)) AS _%(function)s(value))'
    return self.as_sql(compiler, connection, arg_joiner='), (', template=template)


@as_sqlserver(Length)
def length_fn(self, compiler, connection):
    return self.as_sql(compiler, connection, function='LEN')


@as_sqlserver(LPad)
def lpad(self, compiler, connection):
    function = 'dbo.django_lpad'
    return self.as_sql(compiler, connection, function=function)


# @as_sqlserver(MD5)
# def md5(self, compiler, connection):
#     template = "LOWER(CONVERT(VARCHAR(32), HashBytes('%(function)s', CAST(%(expressions)s AS VARCHAR)), 2))"
#     return self.as_sql(compiler, connection, template=template)

#
# @as_sqlserver(SHA1)
# def sha1(self, compiler, connection):
#     template = "CONVERT(VARCHAR(32), HashBytes('%(function)s', %(expressions)s), 2)"
#     return self.as_sql(compiler, connection, template=template)


@as_sqlserver(Random)
def random(self, compiler, connection):
    return self.as_sql(compiler, connection, function='RAND')


@as_sqlserver(Repeat)
def repeat(self, compiler, connection):
    return self.as_sql(compiler, connection, function='REPLICATE')


@as_sqlserver(RPad)
def rpad(self, compiler, connection):
    function = 'dbo.django_rpad'
    return self.as_sql(compiler, connection, function=function)


@as_sqlserver(StdDev)
def stddev(self, compiler, connection):
    function = 'STDEV'
    if self.function == 'STDDEV_POP':
        function = 'STDEVP'
    return self.as_sql(compiler, connection, function=function)


@as_sqlserver(StrIndex)
def strindex(self, compiler, connection):
    clone = self.copy()
    clone.source_expressions.reverse()
    sql = clone.as_sql(compiler, connection, function='CHARINDEX')
    return sql


@as_sqlserver(Substr)
def substr(self, compiler, connection, **extra):
    if len(self.get_source_expressions()) < 3:
        clone = self.copy()
        clone.get_source_expressions().append(Value(2**31 - 1))
        return clone.as_sql(compiler, connection, **extra)
    return self.as_sql(compiler, connection, **extra)


@as_sqlserver(Variance)
def variance(self, compiler, connection):
    function = 'VAR'
    if self.function == 'VAR_POP':
        function = '%sP' % function
    return self.as_sql(compiler, connection, function=function)


@as_sqlserver(ATan2)
def atan2(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, function='ATN2', **extra_context)


@as_sqlserver(Log)
def log(self, compiler, connection, **extra_context):
    clone = self.copy()
    clone.set_source_expressions(self.get_source_expressions()[::-1])
    return clone.as_sql(compiler, connection, **extra_context)


@as_sqlserver(Ln)
def ln(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, function='LOG', **extra_context)


@as_sqlserver(Mod)
def sqlserver_mod(self, compiler, connection, **extra_context):
    compiler.escape_if_noparams = True
    return self.as_sql(compiler, connection, template='%(expressions)s', arg_joiner=' %% ', **extra_context)


@as_sqlserver(Power)
def sqlserver_pow(self, compiler, connection, **extra_context):
    template = '%(function)s(CAST(%(expressions)s)'
    return self.as_sql(compiler, connection, **extra_context, template=template, arg_joiner=' AS FLOAT),')


@as_sqlserver(Round)
def sqlserver_round(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, template='%(function)s(%(expressions)s, 0)', **extra_context)


@as_sqlserver(OrderBy)
def orderby(self, compiler, connection, **extra):
    if self.nulls_last or self.nulls_first:
        # sql server throws error "A constant expression was encountered in the ORDER BY" otherwise
        if isinstance(self.expression, RawSQL) and compiler._re_constant.match(self.expression.sql):
            return '%s %s' % (self.expression.sql, 'DESC' if self.descending else 'ASC'), ()
        isnull = 0 if self.nulls_first else 1
        isnotnull = 1 if self.nulls_first else 0
        template = 'IIF(%%(expression)s IS NULL, %d, %d) ASC, %%(expression)s %%(ordering)s' % (isnull, isnotnull)
        copy = self.copy()
        copy.nulls_last = copy.nulls_first = False  # otherwise as_sql overwrites template
        return copy.as_sql(compiler, connection, template=template, **extra)

    if isinstance(self.expression, (CombinedExpression, BuiltinLookup, Exists)) and \
            not isinstance(self.expression, (DurationExpression,)) and \
            isinstance(self.expression.output_field, BooleanField):
        # problem with DurationExpression is that output_field is calculated too early if we check it here
        copy = self.copy()
        copy.expression = Case(When(self.expression, then=True), default=False)
        return copy.as_sql(compiler, connection, **extra)
    return self.as_sql(compiler, connection, **extra)


@as_sqlserver(HasKeyLookup)
def has_key_lookup(self, compiler, connection):
    return self.as_sql(compiler, connection, template="JSON_VALUE(%s,  %%s) IS NOT NULL")


@as_sqlserver(Window)
def window(self, compiler, connection, **extra):
    if self.order_by is None and isinstance(self.source_expression, RowNumber):
        copy = self.copy()
        copy.order_by = OrderByList(*[expr for expr, _ in compiler.get_order_by()])
        return copy.as_sql(compiler, connection, **extra)
    return self.as_sql(compiler, connection, **extra)


@as_sqlserver(Lookup)
def lookup_fn(self, compiler, connection):
    # mostly copied from oracle
    compiler.escape_if_noparams = True
    wrapped = False
    exprs = []
    for expr in (self.lhs, self.rhs):
        if connection.ops.conditional_expression_supported_in_where_clause(expr) or isinstance(expr, (BuiltinLookup,)):
            expr = Case(When(expr, then=True), default=False)
            wrapped = True
        exprs.append(expr)
    lookup = type(self)(*exprs) if wrapped else self
    return lookup.as_sql(compiler, connection)
