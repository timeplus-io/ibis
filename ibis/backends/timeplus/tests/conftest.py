from __future__ import annotations

import concurrent.futures
import os
import subprocess
from typing import TYPE_CHECKING, Any

import pytest
import sqlglot as sg

import ibis
import ibis.expr.types as ir
from ibis import util
from ibis.backends.tests.base import ServiceBackendTest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from pathlib import Path

TIMEPLUS_HOST = os.environ.get("IBIS_TEST_TIMEPLUS_HOST", "localhost")
TIMEPLUS_PORT = int(os.environ.get("IBIS_TEST_TIMEPLUS_PORT", 8463))
TIMEPLUS_USER = os.environ.get("IBIS_TEST_TIMEPLUS_USER", "proton")
TIMEPLUS_PASS = os.environ.get("IBIS_TEST_TIMEPLUS_PASSWORD", "proton@t+")
IBIS_TEST_TIMEPLUS_DB = os.environ.get("IBIS_TEST_DATA_DB", "ibis_testing")


class TestConf(ServiceBackendTest):
    check_dtype = False
    returned_timestamp_unit = "s"
    supports_json = False
    force_sort = True
    rounding_method = "half_to_even"
    data_volume = "/var/lib/proton/user_files/ibis"
    service_name = "timeplus"
    deps = ("proton_driver",)
    supports_tpch = True
    supports_tpcds = True
    # # Query 14 seems to require a bit more room here
    tpc_absolute_tolerance = 0.0001

    @property
    def native_bool(self) -> bool:
        [(value,)] = self.connection.con.execute("SELECT true")
        return isinstance(value, bool)

    @property
    def test_files(self) -> Iterable[Path]:
        return self.data_dir.joinpath("parquet").glob("*.parquet")

    def postload(self, **kw: Any):
        # reconnect to set the database to the test database
        self.connection = self.connect(database=IBIS_TEST_TIMEPLUS_DB, **kw)

    @staticmethod
    def connect(
        *, tmpdir, worker_id, settings: Mapping[str, Any] | None = None, **kw: Any
    ):
        if settings is None:
            settings = {}

        # without this setting TPC-DS 19 and 24 will fail
        settings.setdefault("allow_experimental_join_condition", 1)

        return ibis.timeplus.connect(
            host=TIMEPLUS_HOST,
            port=TIMEPLUS_PORT,
            password=TIMEPLUS_PASS,
            user=TIMEPLUS_USER,
            settings=settings,
            **kw,
        )

    @staticmethod
    def greatest(f: Callable[..., ir.Value], *args: ir.Value) -> ir.Value:
        if len(args) > 2:
            raise NotImplementedError(
                "Timeplus does not support more than 2 arguments to greatest"
            )
        return f(*args)

    @staticmethod
    def least(f: Callable[..., ir.Value], *args: ir.Value) -> ir.Value:
        if len(args) > 2:
            raise NotImplementedError(
                "Timeplus does not support more than 2 arguments to least"
            )
        return f(*args)

    def _load_data(
        self,
        *,
        database: str = IBIS_TEST_TIMEPLUS_DB,
        **_,
    ) -> None:
        """Load test data into a ClickHouse backend instance.

        Parameters
        ----------
        data_dir
            Location of test data
        script_dir
            Location of scripts defining schemas
        """

        con = self.connection
        client = con.con

        client.execute(f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Atomic")

        util.consume(map(client.execute, self.ddl_script))

    def _load_tpc(self, *, suite, scale_factor):
        con = self.connection
        schema = f"tpc{suite}"
        con.con.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
        parquet_dir = self.data_dir.joinpath(schema, f"sf={scale_factor}", "parquet")
        assert parquet_dir.exists(), parquet_dir
        for path in parquet_dir.glob("*.parquet"):
            table_name = path.with_suffix("").name
            con.con.execute(
                f"CREATE VIEW IF NOT EXISTS {schema}.{table_name} AS "
                f"SELECT * FROM file('ibis/{schema}/{path.name}', 'Parquet')"
            )

    def preload(self):
        super().preload()

        suites = ("tpch", "tpcds")

        service_name = self.service_name
        data_volume = self.data_volume

        for suite in suites:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "exec",
                    service_name,
                    "mkdir",
                    "-p",
                    f"{data_volume}/{suite}",
                ],
                check=True,
            )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for fut in concurrent.futures.as_completed(
                executor.submit(
                    subprocess.run,
                    [
                        "docker",
                        "compose",
                        "cp",
                        str(path),
                        f"{service_name}:{data_volume}/{suite}/{path.name}",
                    ],
                    check=True,
                )
                for suite in suites
                for path in self.data_dir.joinpath(suite).rglob("*.parquet")
            ):
                fut.result()

    def _transform_tpc_sql(self, parsed, *, suite, leaves):
        def add_catalog_and_schema(node):
            if isinstance(node, sg.exp.Table) and node.name in leaves:
                return node.__class__(
                    catalog=f"tpc{suite}",
                    **{k: v for k, v in node.args.items() if k != "catalog"},
                )
            return node

        return parsed.transform(add_catalog_and_schema)


@pytest.fixture
def simple_table(simple_schema):
    return ibis.table(simple_schema, name="table")


@pytest.fixture(scope="session")
def con(tmp_path_factory, data_dir, worker_id):
    return TestConf.load_data(data_dir, tmp_path_factory, worker_id).connection


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()
