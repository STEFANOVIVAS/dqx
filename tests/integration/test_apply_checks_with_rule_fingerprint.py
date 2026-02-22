from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import sql_query
from chispa import assert_df_equality
from tests.integration.conftest import (
    REPORTING_COLUMNS,
    RUN_TIME,
    EXTRA_PARAMS,
    RUN_ID,  
)
SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS


def test_apply_checks_with_fingerprints(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
            user_metadata={"tag1": "value11", "tag2": "value21"},
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="b",
            user_metadata={"tag1": "value12", "tag2": "value22"},
        ),
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            check_func_kwargs={"column": "c"},  # alternative way of defining column
            user_metadata={"tag1": "value13", "tag2": "value23"},
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                [
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value11", "tag2": "value21"},
                    }
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    },
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"tag1": "value11", "tag2": "value21"},
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata_with_fingerprints(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "name": "a_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}},
        },
        {
            "name": "b_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "c_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "c"}},
        },
        {
            "name": "a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": [1, 3, 4]}},
        },
        {
            "name": "c_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "c", "allowed": [1, 3, 4]}},
        },
    ]

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_in_list",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)

def test_apply_checks_with_sql_query_and_ref_df_with_fingerprints(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    # sensor data
    sensor_schema = "measurement_id: int, sensor_id: int, reading_value: int"
    sensor_df = spark.createDataFrame([[1, 1, 4], [1, 2, 1], [2, 2, 110]], sensor_schema)

    # reference specs
    sensor_specs_df = spark.createDataFrame(
        [
            [1, 5],
            [2, 100],
        ],
        "sensor_id: int, min_threshold: int",
    )

    query = """
            WITH joined AS (
                SELECT
                    sensor.*,
                    COALESCE(specs.min_threshold, 100) AS effective_threshold
                FROM {{ sensor }} sensor
                LEFT JOIN {{ sensor_specs }} specs
                    ON sensor.sensor_id = specs.sensor_id
            )
            SELECT
                sensor_id,
                MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS condition
            FROM joined
            GROUP BY sensor_id
        """

    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["sensor_id"],
                "condition_column": "condition",
                "msg": "one of the sensor reading is greater than limit",
                "name": "sensor_reading_check",
                "input_placeholder": "sensor",
            },
        ),
    ]

    ref_dfs = {"sensor_specs": sensor_specs_df}
    checked = dq_engine.apply_checks(sensor_df, checks, ref_dfs=ref_dfs)

    expected = spark.createDataFrame(
        [
            [1, 1, 4, None, None],
            [
                1,
                2,
                1,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                2,
                2,
                110,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        sensor_schema + REPORTING_COLUMNS,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)