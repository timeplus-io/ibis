from __future__ import annotations

import sqlglot as sg

from ibis.backends.sql.datatypes import TimeplusType
from ibis.backends.sql.ddl import DML, CreateDDL, DropObject


class TimeplusBase:
    dialect = "timeplus"

    def format_dtype(self, dtype):
        sql_string = TimeplusType.from_ibis(dtype)
        if dtype.is_timestamp():
            return (
                f"TIMESTAMP({dtype.scale})" if dtype.scale is not None else "TIMESTAMP"
            )
        else:
            return sql_string.sql("timeplus") + " NOT NULL" * (not dtype.nullable)

    def format_properties(self, props):
        tokens = []
        for k, v in sorted(props.items()):
            tokens.append(f"  '{k}'='{v}'")
        return "(\n{}\n)".format(",\n".join(tokens))


class DropTable(TimeplusBase, DropObject):
    _object_type = "STREAM"

    def __init__(
        self,
        table_name: str,
        database: str | None = None,
        must_exist: bool = True,
        temporary: bool = False,
    ):
        super().__init__(must_exist=must_exist)
        self.table_name = table_name
        self.database = database
        self.temporary = temporary

    def _object_name(self):
        return self.scoped_name(self.table_name, self.database)

    def compile(self):
        temporary = "TEMPORARY " if self.temporary else ""
        if_exists = "" if self.must_exist else "IF EXISTS "
        object_name = self._object_name()
        return f"DROP {temporary}{self._object_type} {if_exists}{object_name}"


class DropView(DropTable):
    _object_type = "VIEW"

    def __init__(
        self,
        name: str,
        database: str | None = None,
        must_exist: bool = True,
        temporary: bool = False,
    ):
        super().__init__(
            table_name=name,
            database=database,
            must_exist=must_exist,
            temporary=temporary,
        )


class _DatabaseObject:
    def _object_name(self):
        name = sg.to_identifier(self.name, quoted=True).sql(dialect=self.dialect)
        return name


class CreateDatabase(TimeplusBase, _DatabaseObject, CreateDDL):
    def __init__(
        self,
        name: str,
    ):
        self.name = name

    def compile(self):
        create_decl = "CREATE DATABASE"
        create_line = f"{create_decl}{self._object_name()}"

        return f"{create_line}"


class DropDatabase(TimeplusBase, _DatabaseObject, DropObject):
    _object_type = "DATABASE"

    def __init__(self, name: str, must_exist: bool = True):
        super().__init__(must_exist=must_exist)
        self.name = name


class InsertSelect(TimeplusBase, DML):
    def __init__(
        self,
        table_name,
        select_expr,
        database: str | None = None,
        partition=None,
        partition_schema=None,
        overwrite=False,
    ):
        self.table_name = table_name
        self.database = database
        self.select = select_expr
        self.partition = partition
        self.partition_schema = partition_schema
        self.overwrite = overwrite

    def compile(self):
        if self.overwrite:
            cmd = "INSERT INTO"
        else:
            cmd = "INSERT INTO"

        if self.partition is not None:
            part = self.format_partition(self.partition, self.partition_schema)
            partition = f" {part} "
        else:
            partition = ""

        select_query = self.select
        scoped_name = self.scoped_name(self.table_name, self.database)
        return f"{cmd} {scoped_name}{partition}\n{select_query}"
