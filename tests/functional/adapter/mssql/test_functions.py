import pytest
from dbt.tests.util import get_connection, run_dbt


# -- Scalar function fixtures --

scalar_function_sql = """SELECT @x + @y"""

scalar_function_schema_yml = """
functions:
  - name: add_numbers
    config:
      type: scalar
    returns:
      data_type: INT
    arguments:
      - name: x
        data_type: INT
      - name: y
        data_type: INT
"""

scalar_function_with_default_sql = """SELECT @x + @y"""

scalar_function_with_default_schema_yml = """
functions:
  - name: add_numbers_with_default
    config:
      type: scalar
    returns:
      data_type: INT
    arguments:
      - name: x
        data_type: INT
      - name: y
        data_type: INT
        default_value: 10
"""

scalar_function_deterministic_sql = """SELECT @x * @y"""

scalar_function_deterministic_schema_yml = """
functions:
  - name: multiply_numbers
    config:
      type: scalar
      volatility: deterministic
    returns:
      data_type: INT
    arguments:
      - name: x
        data_type: INT
      - name: y
        data_type: INT
"""

scalar_function_varchar_sql = """SELECT UPPER(@input_text)"""

scalar_function_varchar_schema_yml = """
functions:
  - name: upper_text
    config:
      type: scalar
    returns:
      data_type: NVARCHAR(200)
    arguments:
      - name: input_text
        data_type: NVARCHAR(200)
"""


# -- Table function fixtures --

table_function_sql = """SELECT @x AS a, @y AS b"""

table_function_schema_yml = """
functions:
  - name: get_pair
    config:
      type: table
    returns:
      data_type: TABLE
    arguments:
      - name: x
        data_type: INT
      - name: y
        data_type: INT
"""

table_function_no_args_sql = """SELECT 1 AS id, 'hello' AS val"""

table_function_no_args_schema_yml = """
functions:
  - name: get_constant
    config:
      type: table
    returns:
      data_type: TABLE
"""


# -- Aggregate function fixtures --

aggregate_function_sql = """SELECT @x"""

aggregate_function_schema_yml = """
functions:
  - name: my_agg
    config:
      type: aggregate
    returns:
      data_type: INT
    arguments:
      - name: x
        data_type: INT
"""


class TestScalarFunction:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "add_numbers.sql": scalar_function_sql,
            "schema.yml": scalar_function_schema_yml,
        }

    def test_scalar_function_creates(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify the function actually works by calling it
        with get_connection(project.adapter):
            result = project.run_sql(
                "SELECT dbo.add_numbers(3, 4) AS result",
                fetch="one",
            )
            assert result[0] == 7

    def test_scalar_function_is_idempotent(self, project):
        """CREATE OR ALTER should allow re-running without errors."""
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestScalarFunctionWithDefault:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "add_numbers_with_default.sql": scalar_function_with_default_sql,
            "schema.yml": scalar_function_with_default_schema_yml,
        }

    def test_scalar_function_default_value(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        with get_connection(project.adapter):
            # Call with both args
            result = project.run_sql(
                "SELECT dbo.add_numbers_with_default(3, 4) AS result",
                fetch="one",
            )
            assert result[0] == 7

            # Call using DEFAULT for the second arg
            result = project.run_sql(
                "SELECT dbo.add_numbers_with_default(5, DEFAULT) AS result",
                fetch="one",
            )
            assert result[0] == 15


class TestScalarFunctionDeterministic:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "multiply_numbers.sql": scalar_function_deterministic_sql,
            "schema.yml": scalar_function_deterministic_schema_yml,
        }

    def test_deterministic_function_creates(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        with get_connection(project.adapter):
            result = project.run_sql(
                "SELECT dbo.multiply_numbers(3, 4) AS result",
                fetch="one",
            )
            assert result[0] == 12

    def test_deterministic_function_has_schemabinding(self, project):
        """Verify WITH SCHEMABINDING was applied (function is marked deterministic)."""
        run_dbt(["run"])
        with get_connection(project.adapter):
            result = project.run_sql(
                """
                SELECT OBJECTPROPERTY(
                    OBJECT_ID('dbo.multiply_numbers'),
                    'IsSchemaBound'
                ) AS is_schema_bound
                """,
                fetch="one",
            )
            assert result[0] == 1


class TestScalarFunctionVarchar:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "upper_text.sql": scalar_function_varchar_sql,
            "schema.yml": scalar_function_varchar_schema_yml,
        }

    def test_varchar_function(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        with get_connection(project.adapter):
            result = project.run_sql(
                "SELECT dbo.upper_text('hello world') AS result",
                fetch="one",
            )
            assert result[0] == "HELLO WORLD"


class TestTableFunction:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "get_pair.sql": table_function_sql,
            "schema.yml": table_function_schema_yml,
        }

    def test_table_function_creates(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        with get_connection(project.adapter):
            result = project.run_sql(
                "SELECT * FROM dbo.get_pair(10, 20)",
                fetch="one",
            )
            assert result[0] == 10
            assert result[1] == 20

    def test_table_function_is_idempotent(self, project):
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTableFunctionNoArgs:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "get_constant.sql": table_function_no_args_sql,
            "schema.yml": table_function_no_args_schema_yml,
        }

    def test_table_function_no_args(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        with get_connection(project.adapter):
            result = project.run_sql(
                "SELECT * FROM dbo.get_constant()",
                fetch="one",
            )
            assert result[0] == 1
            assert result[1] == "hello"


class TestAggregateFunctionRaisesError:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "my_agg.sql": aggregate_function_sql,
            "schema.yml": aggregate_function_schema_yml,
        }

    def test_aggregate_function_raises_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "CLR" in results[0].message or "aggregate" in results[0].message.lower()
