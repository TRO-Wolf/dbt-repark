"""Repark SQLAdapter implementation."""

from __future__ import annotations

from typing import Any

from dbt.adapters.base import available
from dbt.adapters.base.column import Column
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.repark.connections import DEFAULT_MATERIALIZATION, ReparkConnectionManager


class ReparkAdapter(SQLAdapter):
    """SQLAdapter over an embedded repark engine (Spark door public surface)."""

    ConnectionManager = ReparkConnectionManager

    # Documented adapter default — dbt core NodeConfig still defaults to "view";
    # operator projects must set +materialized: table (sample_project does). View mat refuses.
    default_materialization: str = DEFAULT_MATERIALIZATION

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @classmethod
    def quote(cls, identifier: str) -> str:
        # Spark-door identifier quoting (backticks).
        return f"`{identifier}`"

    def debug_query(self) -> None:
        self.execute("select 1 as dbt_repark_ok")

    def _session(self) -> Any:
        return self.connections.get_thread_connection().handle.session

    def list_schemas(self, database: str) -> list[str]:
        return self.list_schemas_via_catalog(database)

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> list[BaseRelation]:
        catalog_name = schema_relation.database or "spark_catalog"
        schema = schema_relation.schema or "default"
        tables = self.list_tables_via_catalog(catalog_name, schema)
        relations: list[BaseRelation] = []
        for name in tables:
            relations.append(
                self.Relation.create(
                    database=catalog_name,
                    schema=schema,
                    identifier=name,
                    type="table",
                )
            )
        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> list[Column]:
        pairs = self.get_columns_via_describe(relation)
        return [Column.create(name, dtype) for name, dtype in pairs]

    # -------------------------------------------------------------------------
    # Public Catalog API helpers (no private repark imports).
    # -------------------------------------------------------------------------

    @available
    def list_schemas_via_catalog(self, catalog_name: str) -> list[str]:
        """Return namespace names under *catalog_name* via public Catalog API when possible."""
        session = self._session()
        catalog = getattr(session, "catalog", None)
        if catalog is not None and hasattr(catalog, "listDatabases"):
            try:
                dbs = catalog.listDatabases()
                names: list[str] = []
                for item in dbs:
                    if isinstance(item, str):
                        names.append(item)
                    else:
                        name = getattr(item, "name", None) or getattr(item, "namespace", None)
                        if name is None and hasattr(item, "__getitem__"):
                            name = item[0]
                        if name is not None:
                            names.append(str(name))
                return names
            except Exception:
                pass
        table = session.sql(f"SHOW NAMESPACES IN {catalog_name}").to_arrow()
        if table.num_columns == 0:
            return []
        col = table.column(0)
        return [str(col[i].as_py()) for i in range(table.num_rows)]

    @available
    def list_tables_via_catalog(self, catalog_name: str, schema: str) -> list[str]:
        session = self._session()
        catalog = getattr(session, "catalog", None)
        if catalog is not None and hasattr(catalog, "listTables"):
            try:
                # Spark Catalog.listTables(dbName) — schema is the namespace.
                tables = catalog.listTables(schema)
                names: list[str] = []
                for item in tables:
                    if isinstance(item, str):
                        names.append(item)
                    else:
                        name = getattr(item, "name", None)
                        if name is None and hasattr(item, "__getitem__"):
                            name = item[0]
                        if name is not None:
                            names.append(str(name))
                return names
            except Exception:
                pass
        return []

    def get_columns_via_describe(self, relation: Any) -> list[tuple[str, str]]:
        """Return (name, type) pairs via DESCRIBE (public SQL)."""
        session = self._session()
        rendered = relation.render() if hasattr(relation, "render") else str(relation)
        table = session.sql(f"DESCRIBE {rendered}").to_arrow()
        if table.num_rows == 0 or table.num_columns < 2:
            return []
        names = table.column(0)
        types = table.column(1)
        out: list[tuple[str, str]] = []
        for i in range(table.num_rows):
            n = names[i].as_py()
            if n is None or str(n).startswith("#") or str(n).strip() == "":
                continue
            out.append((str(n), str(types[i].as_py())))
        return out
