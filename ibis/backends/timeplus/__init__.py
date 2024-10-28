from __future__ import annotations

import contextlib
from functools import partial
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_plus

import sqlglot as sg
import sqlglot.expressions as sge
import toolz
from proton_driver.client import Client

import ibis
import ibis.backends.sql.compilers as sc
import ibis.common.exceptions as com
import ibis.config
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
from ibis import util
from ibis.backends import BaseBackend, CanCreateDatabase
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers.base import C
from ibis.backends.timeplus.ddl import (
    CreateDatabase,
    DropDatabase,
    DropTable,
    DropView,
    InsertSelect,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from urllib.parse import ParseResult

    import pandas as pd
    import polars as pl
    import pyarrow as pa


def _to_memtable(v):
    return ibis.memtable(v).op() if not isinstance(v, ops.InMemoryTable) else v


class Backend(SQLBackend, CanCreateDatabase):
    name = "timeplus"
    compiler = sc.timeplus.compiler
    supports_temporary_tables = False
    supports_python_udfs = False
    supports_in_memory_tables = True

    @property
    def current_database(self) -> str:
        with contextlib.suppress(AttributeError):
            result = self.con.execute(
                sg.select(self.compiler.f.current_database()).sql(
                    dialect=self.dialect, pretty=True
                )
            )
        db = result[0] if result else None
        return db

    def raw_sql(self, query: str | sge.Expression, **kwargs) -> Any:
        """Can only be used to execute streaming select query, or an error will occur.

        Parameters
        ----------
        query
            SQL query that to be executed, must be a streaming query.

        **kwargs: Additional options for executing the query, which might include
            parameters like settings, etc.
        """
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        # get settings if existed
        settings = kwargs.pop("settings", {})
        return self.con.execute_iter(query, settings=settings)

    def _safe_raw_sql(self, query: str | sge.Expression, **kwargs) -> Any:
        try:
            result = self.raw_sql(query, **kwargs)
            return result
        except Exception as e:
            raise com.IbisError(f"An error occurred while executing sql, {e!s}") from e

    def do_connect(
        self,
        host: str = "localhost",
        port: int | None = None,
        database: str = "default",
        user: str = "default",
        password: str = "",
        client_name: str = "ibis",
        settings: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a timeplus client for use with Ibis.

        Parameters
        ----------
        host
            Host name of the timeplus server
        port
            Timeplus HTTP server's port. If not passed, the value depends on
            whether `secure` is `True` or `False`.
        database
            Default database when executing queries
        user
            User to authenticate with
        password
            Password to authenticate with
        client_name
            Name of client that will appear in timeplus logs
        settings
            Timeplus settings
        kwargs
            Client specific keyword arguments

        Examples
        --------
        >>> import ibis
        >>> client = ibis.timeplus.connect()
        >>> client
        <ibis.timeplus.client.TimepluisClient object at 0x...>

        """
        if settings is None:
            settings = {}
        self.con = Client(
            host=host,
            port=port if port is not None else 8463,
            database=database,
            user=user,
            password=password,
            client_name=client_name,
            **kwargs,
        )

    def disconnect(self) -> None:
        """Close the connection to the backend."""
        self.con.close()

    @util.experimental
    @classmethod
    def from_connection(self, con: Client) -> Backend:
        """Create an Ibis client from an existing Timeplus Connect Client instance.

        Parameters
        ----------
        con
            An existing Timeplus Connect Client instance.
        """
        new_backend = self()
        new_backend._can_reconnect = False
        new_backend.con = con
        return new_backend

    def list_databases(self, like: str | None = None) -> list[str]:
        """List databases existed.

        Parameters
        ----------
        like
            A pattern to use for listing tables.
        """
        query = "SHOW DATABASES"
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)

        results = self.con.execute(query)

        if results:
            databases = [item[0] for item in results]
        else:
            databases = []
        return self._filter_with_like(databases, like)

    def list_tables(
        self, like: str | None = None, database: str | None = None
    ) -> list[str]:
        """List the tables in the database.

        Parameters
        ----------
        like
            A pattern to use for listing tables.
        database
            Database to list tables from. Default behavior is to show tables in
            the current database.
        """

        query = sg.select(C.name).from_(sg.table("tables", db="system"))

        if database is None:
            database = self.compiler.f.current_database()
        else:
            database = sge.convert(database)

        query = query.where(C.database.eq(database))
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)

        results = self.con.execute(query)

        if results:
            tables = [item[0] for item in results]
        else:
            tables = []
        return self._filter_with_like(tables, like)

    @property
    def version(self) -> str:
        return "2.4.5"

    def create_database(self, name: str) -> None:
        """Create a new database.

        Parameters
        ----------
        name : str
            Name of the new database.
        """

        statement = CreateDatabase(name=name)
        query = statement.compile()
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self.con.execute(query)

    def drop_database(self, name: str) -> None:
        """Drop a database with name `name`.

        Parameters
        ----------
        name : str
            Name of the database to be dropped.

        """

        statement = DropDatabase(name=name)
        query = statement.compile()
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self.con.execute(query)

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        """Return a Schema object for the indicated table and database.

        Parameters
        ----------
        table_name
            May **not** be fully qualified. Use `database` if you want to
            qualify the identifier.
        catalog
            Catalog name, not supported by Timeplus
        database
            Database name

        Returns
        -------
        sch.Schema
            Ibis schema

        """
        if catalog is not None:
            raise com.UnsupportedBackendFeatureError(
                "`catalog` namespaces are not supported by timeplus"
            )
        query = sge.Describe(this=sg.table(table_name, db=database))
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        results = self.con.execute(query)
        names, types = zip(*[(name, type_) for name, type_, *_ in results])
        return sch.Schema(
            dict(zip(names, map(self.compiler.type_mapper.from_string, types)))
        )

    def _collect_in_memory_tables(
        self, expr: ir.Table | None, external_tables: Mapping | None = None
    ):
        memtables = {op.name: op for op in expr.op().find(ops.InMemoryTable)}
        externals = toolz.valmap(_to_memtable, external_tables or {})
        return toolz.merge(memtables, externals)

    def _normalize_external_tables(self, external_tables=None):
        """Get external tables."""
        external_data = []
        n = 0
        type_mapper = self.compiler.type_mapper

        for name, obj in (external_tables or {}).items():
            n += 1
            if not (schema := obj.schema):
                raise TypeError(f"Schema is empty for external table {name}")

            structure = [
                (name, type_mapper.to_string(typ.copy(nullable=not typ.is_nested())))
                for name, typ in schema.items()
            ]

            data_with_time = [
                {**record} for record in obj.data.to_frame().to_dict(orient="records")
            ]
            external_data.append(
                {
                    "name": name,
                    "data": data_with_time,
                    "structure": structure,
                    "fmt": "Arrow",
                }
            )
        if not n:
            return None
        return external_data

    def table(
        self,
        name: str,
        database: str | None = None,
        catalog: str | None = None,
    ) -> ir.Table:
        """Return a table expression from a table or view in the database.

        Parameters
        ----------
        name
            Table name.
        database
            Database in which the table resides.
        catalog
            Catalog is not supported by timeplus.

        Returns
        -------
        Table
            Table named `name` from `database`

        """

        if database is not None and not isinstance(database, str):
            raise com.IbisTypeError(
                f"`database` must be a string; got {type(database)}"
            )
        if catalog is not None:
            raise com.UnsupportedBackendFeatureError(
                "`catalog` namespaces are not supported by timeplus"
            )
        schema = self.get_schema(name, database=database)

        node = ops.DatabaseTable(
            name,
            schema=schema,
            source=self,
            namespace=ops.Namespace(database=database),
        )
        return node.to_expr()

    def create_table(
        self,
        name: str,
        obj: ir.Table
        | pd.DataFrame
        | pa.Table
        | pl.DataFrame
        | pl.LazyFrame
        | None = None,
        *,
        schema: sch.SchemaLike | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool | None = None,
        # backend specific arguments
        engine: str | None = None,
        order_by: Iterable[str] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ir.Table:
        """Create a stream in a Timeplus database.

        Parameters
        ----------
        name
            Name of the stream to create
        obj
            Optional data to create the stream with
        schema
            Optional names and types of the stream
        database
            Database to create the stream in
        temp
            Create a temporary table. This is not yet supported, and exists for
            API compatibility.
        overwrite
            Whether to overwrite the table
        engine
            The engine to use.
        order_by
            String column names to order by. Required for some table engines like `MergeTree`.
        settings
            Key-value pairs of settings for table creation

        Returns
        -------
        Table
            The new table

        """
        if overwrite is not None:
            raise com.IbisError("`overwrite` namespaces are not supported by timeplus")
        if obj is None and schema is None:
            raise com.IbisError("The `schema` or `obj` parameter is required")
        if temp is None:
            raise com.IbisError("`temp` namespaces are not supported by timeplus")
        if schema is not None:
            schema = ibis.schema(schema)
        if obj is not None and not isinstance(obj, ir.Expr):
            obj = ibis.memtable(obj, schema=schema)

        this = sge.Schema(
            this=sg.table(name, db=database),
            expressions=[
                sge.ColumnDef(
                    this=sg.to_identifier(name, quoted=self.compiler.quoted),
                    kind=self.compiler.type_mapper.from_ibis(typ),
                )
                for name, typ in (schema or obj.schema()).items()
            ],
        )

        if engine:
            properties = [
                sge.EngineProperty(this=sg.to_identifier(engine, quoted=False))
            ]
        else:
            properties = []

        if settings:
            properties.append(
                sge.SettingsProperty(
                    expressions=[
                        sge.SetItem(
                            this=sge.EQ(
                                this=sg.to_identifier(name),
                                expression=sge.convert(value),
                            )
                        )
                        for name, value in settings.items()
                    ]
                )
            )

        if order_by is not None:
            # engine == "MergeTree" requires an order by clause, which is the
            # empty tuple if order_by is False-y
            properties.append(
                sge.Order(
                    expressions=[
                        sge.Ordered(
                            this=sge.Tuple(
                                expressions=list(map(sg.column, order_by or ()))
                            )
                        )
                    ]
                )
            )

        external_tables = {}
        expression = None

        if obj is not None:
            expression = self.compiler.to_sqlglot(obj)
            external_tables.update(self._collect_in_memory_tables(obj))
            code = sge.Create(
                this=this,
                kind="STREAM",
                expression=expression,
                properties=sge.Properties(expressions=properties),
            )
        else:
            code = sge.Create(
                this=this,
                kind="STREAM",
                expression=expression,
                properties=sge.Properties(expressions=properties),
            )

        external_data = self._normalize_external_tables(external_tables)
        sql = code.sql(dialect=self.dialect, pretty=True)
        self.con.execute(sql, external_tables=external_data)

        return self.table(name, database=database)

    def drop_table(
        self,
        name: str,
        *,
        database: tuple[str | str] | str | None = None,
        temp: bool = False,
        force: bool = False,
    ) -> None:
        """Drop a table.

        Parameters
        ----------
        name
            Name of the table to drop.
        database
            Name of the database where the table exists, if not the default.
        catalog
            Name of the catalog where the table exists, if not the default.
        temp
            Whether the table is temporary or not.
        force
            If `False`, an exception is raised if the table does not exist.

        """
        statement = DropTable(
            table_name=name,
            database=database,
            must_exist=not force,
            temporary=temp,
        )
        with contextlib.suppress(AttributeError):
            query = statement.compile()
            query = query.sql(dialect=self.dialect, pretty=True)
        self.con.execute(query)

    def create_view(
        self,
        name: str,
        obj: ir.Table,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> ir.Table:
        expression = self.compiler.to_sqlglot(obj)

        query = sge.Create(
            this=sg.table(name, db=database),
            kind="VIEW",
            replace=overwrite,
            expression=expression,
        )

        external_tables = self._collect_in_memory_tables(obj)
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self.con.execute(query, external_tables=external_tables)
        return self.table(name, database=database)

    def drop_view(
        self,
        name: str,
        *,
        database: str | None = None,
        temp: bool = False,
        force: bool = False,
    ) -> None:
        """Drop a view.

        Parameters
        ----------
        name
            Name of the view to drop.
        database
            Name of the database where the view exists, if not the default.
        temp
            Whether the view is temporary or not.
        force
            If `False`, an exception is raised if the view does not exist.

        """
        # TODO(deepyaman): Support (and differentiate) permanent views.

        statement = DropView(
            name=name,
            database=database,
            must_exist=(not force),
            temporary=temp,
        )
        query = statement.compile()
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self.con.execute(query)

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        name = util.gen_name("timeplus_metadata")
        sql = f"CREATE VIEW {name} AS {query}"
        with contextlib.suppress(AttributeError):
            sql = sql.sql(dialect=self.dialect)
        self.con.execute(sql)
        try:
            return self.get_schema(name)
        finally:
            sql = f"DROP VIEW {name}"
            with contextlib.suppress(AttributeError):
                sql = sql.sql(dialect=self.dialect, pretty=True)
            self.con.execute(sql)

    def _from_url(self, url: ParseResult, **kwargs) -> BaseBackend:
        """Connect to a backend using a URL `url`.

        Parameters
        ----------
        url
            URL with which to connect to a backend.
        kwargs
            Additional keyword arguments

        Returns
        -------
        BaseBackend
            A backend instance

        """
        database = url.path[1:]

        connect_args = {
            "user": url.username,
            "password": unquote_plus(url.password or ""),
            "host": url.hostname,
            "database": database or "",
            "port": url.port,
            **kwargs,
        }

        kwargs.update(connect_args)
        self._convert_kwargs(kwargs)

        return self.connect(**kwargs)

    def compile(
        self,
        expr: ir.Expr,
        limit: str | None = None,
        params: Mapping[ir.Expr, Any] | None = None,
        pretty: bool = False,
    ):
        return super().compile(
            expr, params=params, pretty=pretty
        )  # Discard `limit` and other kwargs.

    def execute(
        self,
        expr: ir.Expr,
        limit: str | None = "default",
        params: Mapping[ir.Scalar, Any] | None = None,
        external_tables: Mapping[str, pd.DataFrame] | None = None,
        showtime: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Execute an expression."""
        import pandas as pd

        table_expr = expr.as_table()
        sql = self.compile(table_expr, params=params, limit=limit)

        # add _tp_time into schema
        if "_tp_time" not in table_expr.schema().names:
            table_expr = table_expr.mutate(_tp_time=ibis.now())

        schema = table_expr.schema()

        external_tables = self._collect_in_memory_tables(expr, external_tables)
        external_data = self._normalize_external_tables(external_tables)
        # get settings if existed
        settings = kwargs.pop("settings", {})
        if "query_mode" in settings and settings["query_mode"] == "table":
            # if the query mode is historical, we'll need to wait for 2s after the data is inserted
            self.con.execute("SELECT SLEEP(2)")
            df = self.con.query_dataframe(
                sql, external_tables=external_data, settings=settings
            )
            columns = schema.names
            # drop _tp_time according to parameter
            if showtime is not True:
                if "_tp_time" in expr:
                    expr = expr.drop(["_tp_time"])
                if "_tp_time" in df:
                    df = df.drop(columns=["_tp_time"])
                names = [col for col in columns if col != "_tp_time"]

            if df.empty:
                df = pd.DataFrame(columns=names)
            else:
                df.columns = list(names)

            return expr.__pandas_result__(df)
        else:
            # streaming query
            itr = self.con.execute_iter(
                sql, external_tables=external_data, settings=settings
            )
            return itr

    def _register_udfs(self, expr: ir.Expr) -> None:
        for udf_node in expr.op().find(ops.ScalarUDF):
            register_func = getattr(
                self, f"_register_{udf_node.__input_type__.name.lower()}_udf"
            )
            register_func(udf_node)

    def insert(
        self,
        name: str,
        obj: pd.DataFrame | ir.Table,
        database: str | None = None,
        overwrite: bool = False,
        **kwargs: Any,
    ):
        # ir.Table, pa.Table, dict, pd.DataFrame
        if isinstance(obj, ir.Table):
            statement = InsertSelect(
                name,
                self.compile(obj),
                database=database,
            )
            return self._safe_raw_sql(statement.compile(), **kwargs)
        elif not isinstance(obj, ir.Table):
            obj = ibis.memtable(obj)

        query = self._build_insert_from_table(target=name, source=obj)
        external_tables = self._collect_in_memory_tables(obj, {})
        external_data = self._normalize_external_tables(external_tables)
        # get settings if existed
        settings = kwargs.pop("settings", {})
        return self.con.execute(
            query.sql(self.name), external_tables=external_data, settings=settings
        )

    def to_pyarrow(
        self,
        expr: ir.Expr,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        external_tables: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        with self.to_pyarrow_batches(
            expr=expr,
            params=params,
            limit=limit,
            external_tables=external_tables,
            **kwargs,
        ) as reader:
            table = reader.read_all()

        return expr.__pyarrow_result__(table)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        limit: int | str | None = None,
        params: Mapping[ir.Scalar, Any] | None = None,
        external_tables: Mapping[str, Any] | None = None,
        chunk_size: int = 1_000_000,
        **kwargs: Any,
    ) -> pa.ipc.RecordBatchReader:
        """Execute expression and return an iterator of pyarrow record batches.

        This method is eager and will execute the associated expression
        immediately.

        Parameters
        ----------
        expr
            Ibis expression to export to pyarrow
        limit
            An integer to effect a specific row limit. A value of `None` means
            "no limit". The default is in `ibis/config.py`.
        params
            Mapping of scalar parameter expressions to value.
        external_tables
            External data
        chunk_size
            Maximum number of row to return in a single chunk
        kwargs
            Extra arguments passed directly to clickhouse-connect

        Returns
        -------
        results
            RecordBatchReader

        Notes
        -----
        There are a variety of ways to implement clickhouse -> record batches.

        1. FORMAT ArrowStream -> record batches via raw_query
           This has the same type conversion problem(s) as `to_pyarrow`.
           It's harder to address due to lack of `cast` on `RecordBatch`.
           However, this is a ClickHouse problem: we should be able to get
           string data out without a bunch of settings/permissions rigmarole.
        2. Native -> Python objects -> pyarrow batches
           This is what is implemented, using `query_column_block_stream`.
        3. Native -> Python objects -> DataFrame chunks -> pyarrow batches
           This is not implemented because it adds an unnecessary pandas step in
           between Python object -> arrow. We can go directly to record batches
           without pandas in the middle.

        """
        import pyarrow as pa

        table = expr.as_table()
        sql = self.compile(table, limit=limit, params=params)

        external_tables = self._collect_in_memory_tables(expr, external_tables)
        external_data = self._normalize_external_tables(external_tables)
        # get settings if existed
        settings = kwargs.pop("settings", {})
        settings["insert_block_size"] = chunk_size

        def batcher(
            sql: str, *, schema: pa.Schema, settings, **kwargs
        ) -> Iterator[pa.RecordBatch]:
            blocks = self.con.execute_iter(
                sql, external_tables=external_data, settings=settings, **kwargs
            )
            data = []
            for block in blocks:
                data.append(tuple(block))

            yield from map(
                partial(pa.RecordBatch.from_arrays, schema=schema), [list(zip(*data))]
            )

        schema = table.schema().to_pyarrow()

        return pa.ipc.RecordBatchReader.from_batches(
            schema, batcher(sql, schema=schema, settings=settings, **kwargs)
        )
