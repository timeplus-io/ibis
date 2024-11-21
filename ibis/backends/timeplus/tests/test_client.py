from __future__ import annotations

import os

import pandas as pd
import pytest

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.util import gen_name

cc = pytest.importorskip("proton_driver")


def test_connection(con):
    assert con is not None
    assert isinstance(con, ibis.backends.timeplus.Backend)


def test_run_sql(con):
    query = "SELECT * FROM ibis_testing.functional_alltypes"
    table = con.sql(query)

    fa = con.table("functional_alltypes")
    assert isinstance(table, ir.Table)
    assert table.schema() == fa.schema()

    expr = table.limit(10)
    result = expr.execute(settings={"query_mode": "table"})
    assert len(result) == 10


def test_get_schema(con):
    t = con.table("functional_alltypes")
    schema = con.get_schema("functional_alltypes")
    assert t.schema() == schema


def test_list_tables(con):
    result = set(con.list_tables())

    assert {"astronauts", "batting", "diamonds", "functional_alltypes"} <= result
    assert len(con.list_tables(like="diamonds")) == 1


def test_list_databases(con):
    result = set(con.list_databases())
    assert {"information_schema", "ibis_testing", "system", "default"} <= result


def test_create_and_drop_table(con, temp_table):
    sch = ibis.schema([("test", "string")])

    con.create_table(temp_table, schema=sch)
    assert con.table(temp_table) is not None

    con.drop_table(temp_table)

    assert temp_table not in con.list_tables()


@pytest.mark.parametrize(
    ("query", "expected_schema"),
    [
        (
            "SELECT 1 as a, 2 + dummy as b",
            ibis.schema(dict(a=dt.UInt8(nullable=True), b=dt.UInt16(nullable=True))),
        ),
        (
            "SELECT string_col, sum(double_col) as b FROM functional_alltypes GROUP BY string_col",
            ibis.schema(
                dict(
                    string_col=dt.String(nullable=True),
                    b=dt.Float64(nullable=True),
                )
            ),
        ),
    ],
)
def test_list_tables_database(con):
    tables = con.list_tables()
    tables2 = con.list_tables(database=con.current_database)
    # some overlap, but not necessarily identical because
    # a database may have temporary tables added/removed between list_tables
    # calls
    assert set(tables) & set(tables2)


@pytest.fixture
def tmpcon(worker_id):
    dbname = f"timeplus_database_{worker_id}"
    con = ibis.timeplus.connect(
        host=os.environ.get("IBIS_TEST_TIMEPLUS_HOST", "localhost"),
        user=os.environ.get("IBIS_TEST_TIMEPLUS_USER", "proton"),
        port=int(os.environ.get("IBIS_TEST_TIMEPLUS_PORT", 8463)),
        password=os.environ.get("IBIS_TEST_TIMEPLUS_PASSWORD", "proton@t+"),
    )
    con.create_database(dbname)
    yield con
    con.drop_database(dbname)


def test_list_tables_empty_database(tmpcon):
    assert tmpcon.list_tables()


@pytest.mark.parametrize("temp", [True, False], ids=["temp", "no_temp"])
def test_create_table_no_data(con, temp, temp_table):
    schema = ibis.schema(dict(a="!int", b="string"))
    t = con.create_table(temp_table, schema=schema, temp=temp, engine="Memory")
    assert t.execute(settings={"query_mode": "table"}).empty


def _create_temp_table_with_schema(con, temp_table_name, schema, data=None):
    if con.name == "druid":
        pytest.xfail("druid doesn't implement create_table")
    elif con.name == "flink":
        pytest.xfail(
            "flink doesn't implement create_table from schema without additional arguments"
        )
    temporary = con.create_table(temp_table_name, schema=schema)

    if data is not None and isinstance(data, pd.DataFrame):
        assert not data.empty
        tmp = con.create_table(temp_table_name, data, overwrite=True)
        return tmp

    return temporary


@pytest.fixture
def test_employee_schema() -> ibis.schema:
    return ibis.schema(
        {
            "first_name": "string",
            "last_name": "string",
            "department_name": "string",
            "salary": "float64",
        }
    )


@pytest.fixture
def employee_empty_temp_table(con, test_employee_schema):
    temp_table_name = gen_name("temp_employee_empty_table")
    _create_temp_table_with_schema(con, temp_table_name, test_employee_schema)
    yield temp_table_name
    con.drop_table(temp_table_name, force=True)


@pytest.fixture
def test_employee_data_2():
    import pandas as pd

    df2 = pd.DataFrame(
        {
            "first_name": ["X", "Y", "Z"],
            "last_name": ["A", "B", "C"],
            "department_name": ["XX", "YY", "ZZ"],
            "salary": [400.0, 500.0, 600.0],
        }
    )

    return df2


def test_insert_no_overwrite_from_dataframe(
    con, test_employee_data_2, employee_empty_temp_table
):
    temporary = con.table(employee_empty_temp_table)
    con.insert(employee_empty_temp_table, obj=test_employee_data_2)
    result = temporary.execute(settings={"query_mode": "table"})
    assert len(result) == 3
    assert (
        result.sort_values("first_name")
        .reset_index(drop=True)
        .equals(test_employee_data_2.sort_values("first_name").reset_index(drop=True))
    ), "The DataFrames are not equal"
