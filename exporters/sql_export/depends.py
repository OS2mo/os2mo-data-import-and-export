from typing import Annotated

from fastapi import Depends
from fastramqpi.depends import from_user_context

from .sql_export import SqlExport as SqlExport_  # type: ignore

SqlExport = Annotated[SqlExport_, Depends(from_user_context("sql_exporter"))]
