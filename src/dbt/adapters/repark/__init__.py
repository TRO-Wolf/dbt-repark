"""dbt-repark adapter plugin (type: ``repark``)."""

from dbt.adapters.base import AdapterPlugin

from dbt.adapters.repark.connections import ReparkConnectionManager  # noqa: F401
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.impl import ReparkAdapter
from dbt.include import repark

Plugin = AdapterPlugin(
    adapter=ReparkAdapter,  # type: ignore[arg-type]
    credentials=ReparkCredentials,
    include_path=repark.PACKAGE_PATH,
)
