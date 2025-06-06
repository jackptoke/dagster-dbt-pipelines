from dagster import IOManager
from dagster_university.io.duckpond import DuckDB, SQL


class MotherduckIOManager(IOManager):
    def __init__(self, duckdb: DuckDB, prefix=""):
        self.duckdb = duckdb
        self.prefix = prefix

    def _get_table_name(self, context):
        if context.has_asset_key:
            id = context.get_asset_key()
        else:
            id = context.get_identifier()
        return f"{self.prefix}{'_'.join(id)}"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(
                f"Expected asset to return a SQL; got {select_statement!r}"
            )

        self.duckdb.query(
            SQL(
                "create or replace table $table_name as $select_statement",
                table_name=self._get_table_name(context),
                select_statement=select_statement,
            )
        )

    def load_input(self, context) -> SQL:
        return SQL("select * from $table_name", table_name=self._get_table_name(context))


