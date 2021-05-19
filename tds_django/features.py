from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):

    supports_timezones = False  # has datetimeoffset but would break things if mixed with datetimefield2?

    # json
    supports_json_field = False
    can_introspect_json_field = False  # ISJSON only validates json objects, not literals
    has_json_object_function = False
    supports_json_field_contains = False
    
    allow_sliced_subqueries_with_in = False  # TODO CHECK
    can_create_inline_fk = False

    can_return_columns_from_insert = True
    can_return_id_from_insert = True
    can_rollback_ddl = True
    # can_use_chunked_reads = False
    greatest_least_ignores_nulls = True
    has_case_insensitive_like = False
    has_bulk_insert = True

    has_native_uuid_field = True
    has_real_datatype = True
    # has_zoneinfo_database = False
    # ignores_table_name_case = False  # depends on db collation
    implied_column_null = True
    max_query_params = 1000
    requires_literal_defaults = True

    # supported_explain_formats = {'XML', }
    supports_boolean_expr_in_select_clause = False
    supports_covering_indexes = True
    supports_expression_indexes = False  # would need to manage computed column
    supports_ignore_conflicts = False
    supports_index_on_text_field = False
    supports_over_clause = True
    supports_paramstyle_pyformat = False  # TODO
    supports_partially_nullable_unique_constraints = False
    supports_sequence_reset = False  # TODO
    supports_subqueries_in_group_by = False
    supports_temporal_subtraction = True
    supports_transactions = True

    test_db_allows_multiple_connections = False

    django_test_skips = {
        "no need to test this": {
          'cache.tests.FileBasedCacheTests',
          'cache.tests.FileBasedCachePathLibTests',
          'cache.tests.LocMemCacheTests',
        },
        "test assumption not correct for SQL Server": {
            # sql server allows double quoted table names
            'schema.tests.SchemaTests.test_add_foreign_key_quoted_db_table',
            # hardcoded SUBSTR
            'custom_lookups.tests.BilateralTransformTests.test_transform_order_by',
            # hardcoded RETURNING
            'queries.test_db_returning.ReturningValuesTests.test_insert_returning',
            'queries.test_db_returning.ReturningValuesTests.test_insert_returning_multiple',
            # expects LIMIT but sql server has TOP
            'queries.test_qs_combinators.QuerySetSetOperationTests.test_exists_union',
        },
        "tests for test db creation. Fail because we override create_test_db": {
            'backends.base.test_creation.TestDbCreationTests.test_mark_expected_failures_and_skips_call',
            'backends.base.test_creation.TestDbCreationTests.test_migrate_test_setting_false',
            'backends.base.test_creation.TestDbCreationTests.test_migrate_test_setting_true',
        },
        "SQL Server need more queries than what is expected (deleting and recreating indexes)": {
            'schema.tests.SchemaTests.test_unique_and_reverse_m2m',
            'schema.tests.SchemaTests.test_unique_no_unnecessary_fk_drops',
            'admin_views.tests.GroupAdminTest.test_group_permission_performance',
            'admin_views.tests.UserAdminTest.test_user_permission_performance',
        },
        "test uses cursor directly and we don't fix the query, the query should set (SET IDENTITY_INSERT off)": {
            'migrations.test_operations.OperationTests.test_create_model_with_constraint',
        },
        "unsupported by SQL Server": {
            'aggregation.tests.AggregateTestCase.test_aggregation_subquery_annotation_values_collision',
            'db_functions.math.test_mod.ModTests.test_float',
            # subquery in avg
            'expressions_case.tests.CaseExpressionTests.test_annotate_with_in_clause',
            # ORDER BY LOB in window function
            'expressions_window.tests.WindowFunctionTests.test_key_transform',
            # NTH_RESULT: could be done
            'expressions_window.tests.WindowFunctionTests.test_nth_returns_null',
            'expressions_window.tests.WindowFunctionTests.test_nthvalue',
            # order by constant
            'ordering.tests.OrderingTests.test_order_by_constant_value',
            # distinct + subquery, could work if rewrite distinct with group by
            'ordering.tests.OrderingTests.test_orders_nulls_first_on_filtered_subquery',
            # sql server does not allow like predicates index
            'indexes.tests.PartialIndexTests.test_multiple_conditions',
        },
        "Test is using hardcoded values that are different for sql server": {
            'aggregation.tests.AggregateTestCase.test_count_star',
            'cache.tests.CreateCacheTableForDBCacheTests.test_createcachetable_observes_database_router',
        },
        "Avg are cast as float, with can cause issues with decimals": {
            'aggregation_regress.tests.AggregationTests.test_values_list_annotation_args_ordering',
        },
        "client unsupported": {
            "dbshell.tests.DbshellCommandTestCase.test_command_missing",
        },
        "I don't understand": {

            'model_fields.test_decimalfield.DecimalFieldTests.test_roundtrip_with_trailing_zeros',
            # also: extra "is an old API that we aim to deprecate"
            'queries.tests.Queries5Tests.test_extra_select_literal_percent_s',

            # problems with unicode? hash functions in sql server return different result from python
            'db_functions.text.test_md5.MD5Tests.test_basic',
            'db_functions.text.test_md5.MD5Tests.test_transform',
            'db_functions.text.test_sha1.SHA1Tests.test_basic',
            'db_functions.text.test_sha1.SHA1Tests.test_transform',
            'db_functions.text.test_sha224.SHA224Tests.test_basic',
            'db_functions.text.test_sha224.SHA224Tests.test_transform',
            'db_functions.text.test_sha256.SHA256Tests.test_basic',
            'db_functions.text.test_sha256.SHA256Tests.test_transform',
            'db_functions.text.test_sha384.SHA384Tests.test_basic',
            'db_functions.text.test_sha384.SHA384Tests.test_transform',
            'db_functions.text.test_sha512.SHA512Tests.test_basic',
            'db_functions.text.test_sha512.SHA512Tests.test_transform',
        },
        "To check?": {
            # pass with wrong error without mars, not pass on mars, pytds bug?
            'backends.tests.BackendTestCase.test_cursor_contextmanager',
            # TODO json, currenlty marked as not available in features
            # 'model_fields.test_jsonfield.TestQuerying.test_has_any_keys',
            # 'model_fields.test_jsonfield.TestQuerying.test_array_key_contains',
            # 'model_fields.test_jsonfield.TestQuerying.test_contained_by',
            # 'model_fields.test_jsonfield.TestQuerying.test_contains_contained_by_with_key_transform',
            # 'model_fields.test_jsonfield.TestQuerying.test_contains_primitives',
            # 'model_fields.test_jsonfield.TestQuerying.test_deep_lookup_array',
            # 'model_fields.test_jsonfield.TestQuerying.test_deep_lookup_mixed',
            # 'model_fields.test_jsonfield.TestQuerying.test_deep_lookup_objs',
            # 'model_fields.test_jsonfield.TestQuerying.test_deep_lookup_transform',
            # 'model_fields.test_jsonfield.TestQuerying.test_exact',
            # 'model_fields.test_jsonfield.TestQuerying.test_exact_complex',
            # 'model_fields.test_jsonfield.TestQuerying.test_expression_wrapper_key_transform',
            # 'model_fields.test_jsonfield.TestQuerying.test_join_key_transform_annotation_expression',
            # 'model_fields.test_jsonfield.TestQuerying.test_isnull_key_or_none',
            # 'model_fields.test_jsonfield.TestQuerying.test_isnull',
            # 'model_fields.test_jsonfield.TestQuerying.test_deep_values',
            # 'model_fields.test_jsonfield.TestQuerying.test_contains',
            # 'model_fields.test_jsonfield.TestQuerying.test_has_key',
            # 'model_fields.test_jsonfield.TestQuerying.test_has_key_deep',
            # 'model_fields.test_jsonfield.TestQuerying.test_has_key_list',
            # 'model_fields.test_jsonfield.TestQuerying.test_has_key_null_value',
            # 'model_fields.test_jsonfield.TestQuerying.test_has_keys',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_iregex',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_quoted_string',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_regex',
            # 'model_fields.test_jsonfield.TestQuerying.test_lookups_with_key_transform',
            # 'model_fields.test_jsonfield.TestQuerying.test_order_grouping_custom_decoder',
            # 'model_fields.test_jsonfield.TestQuerying.test_ordering_grouping_by_count',
            # 'model_fields.test_jsonfield.TestQuerying.test_ordering_grouping_by_key_transform',
            # 'model_fields.test_jsonfield.JSONFieldTests.test_db_check_constraints',
            # 'model_fields.test_jsonfield.TestQuerying.test_isnull_key',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_in',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_transform_expression',
            # 'model_fields.test_jsonfield.TestQuerying.test_key_values',
            # 'model_fields.test_jsonfield.TestQuerying.test_nested_key_transform_expression',
            # 'model_fields.test_jsonfield.TestQuerying.test_none_key',
            # 'model_fields.test_jsonfield.TestQuerying.test_none_key_and_exact_lookup',
            # 'model_fields.test_jsonfield.TestQuerying.test_none_key_exclude',
            # 'model_fields.test_jsonfield.TestQuerying.test_ordering_by_transform',
            # 'model_fields.test_jsonfield.TestQuerying.test_shallow_lookup_obj_target',
        },
        "SQL Server does not natively support unique (with multiple) nullable fields so a FK to such field will fail": {
            "many_to_one_null.tests.ManyToOneNullTests.test_add_efficiency",
            "many_to_one_null.tests.ManyToOneNullTests.test_assign_clear_related_set",
            "many_to_one_null.tests.ManyToOneNullTests.test_assign_with_queryset",
            "many_to_one_null.tests.ManyToOneNullTests.test_clear_efficiency",
            "many_to_one_null.tests.ManyToOneNullTests.test_created_via_related_set",
            "many_to_one_null.tests.ManyToOneNullTests.test_created_without_related",
            "many_to_one_null.tests.ManyToOneNullTests.test_get_related",
            "many_to_one_null.tests.ManyToOneNullTests.test_related_null_to_field",
            "many_to_one_null.tests.ManyToOneNullTests.test_related_set",
            "many_to_one_null.tests.ManyToOneNullTests.test_remove_from_wrong_set",
            "many_to_one_null.tests.ManyToOneNullTests.test_set",
            "many_to_one_null.tests.ManyToOneNullTests.test_set_clear_non_bulk",
        }
    }

    # Collation names for use by the Django test suite.
    test_collations = {
        'ci': 'Latin1_General_CI_AS',  # Case-insensitive.
        'cs': 'Latin1_General_BIN2',  # Case-sensitive.
        'non_default': 'Korean_90_BIN',  # Non-default.
        'swedish_ci': 'Finnish_Swedish_CI_AI'  # Swedish case-insensitive.
    }

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            'GenericIPAddressField': 'CharField',
            'PositiveBigIntegerField': 'BigIntegerField',
            'PositiveIntegerField': 'IntegerField',
            'PositiveSmallIntegerField': 'SmallIntegerField',
            'DurationField': 'BigIntegerField',
        }
