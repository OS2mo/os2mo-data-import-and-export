import sqlite3
from datetime import date
from datetime import datetime
from typing import Any
from typing import cast
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import click
from ra_utils.load_settings import load_setting


class DBOverview:
    def __init__(self, rundb_path: str):
        self.run_db = rundb_path

    def read_db_content(self, *fields):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        if len(fields) == 0:
            fields = ["*"]

        query = "SELECT " + ", ".join([*fields]) + " FROM runs ORDER BY id"
        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            status = "id: {}, from: {}, to: {}, status: {}"
            print(status.format(*row))

    def _read_last_line(self, *fields) -> Union[Any, List[Any]]:
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = "SELECT " + ", ".join([*fields]) + " FROM runs ORDER BY id DESC LIMIT 1"
        c.execute(query)
        result = c.fetchone()
        if len(result) == 1:
            return result[0]
        return result

    def read_current_status(self) -> Tuple[bool, str]:
        today = date.today()
        midnight = datetime.min.time()
        midnight_today = datetime.combine(today, midnight)

        to_date, status = self._read_last_line("to_date", "status")
        if "Running" in status:
            return (False, "Not ready to run")
        if to_date < midnight_today:
            return (False, "Not up to date")
        return (True, "Status ok")

    def _delete_line(self, id: int) -> Tuple[bool, str]:
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = "DELETE FROM runs WHERE id=?"
        c.execute(query, (id,))
        conn.commit()
        return (True, "Status ok")

    def delete_last_row(self, force: bool = False) -> Tuple[bool, str]:
        current_status = self.read_current_status()
        if current_status[0] and not force:
            return (True, "Status ok, no delete")

        last_id: int = cast(int, self._read_last_line("id"))
        status, msg = self._delete_line(last_id)
        if status:
            return (True, "Deleted last row")
        return status, msg

    def create_db(self) -> Tuple[bool, str]:
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                from_date timestamp,
                to_date timestamp,
                status text
            )
        """
        )
        conn.commit()
        conn.close()
        return (True, "Status ok")


@click.group()
@click.option("--rundb-path", help="Path to the rundb to operate on.")
@click.option("--rundb-variable", help="Settings.json variable to use for rundb path.")
@click.pass_context
def cli(ctx, rundb_path: Optional[str] = None, rundb_variable: Optional[str] = None):
    rundb: str = ""
    if rundb_path:
        rundb = cast(str, rundb_path)
    elif rundb_variable:
        rundb = load_setting(rundb_variable)()
    else:
        raise click.ClickException("Must provide either rundb-path or rundb-variable")

    ctx.ensure_object(dict)
    ctx.obj["dboverview"] = DBOverview(rundb)


@cli.command()
@click.pass_context
def create(ctx):
    """Create a new run_db."""
    db_overview = ctx.obj["dboverview"]
    db_overview.create_db()


@cli.command()
@click.pass_context
def read_rundb(ctx):
    """Load the run_db and print all contents."""
    db_overview = ctx.obj["dboverview"]
    db_overview.read_db_content()


@cli.command()
@click.pass_context
def read_current_status(ctx):
    """Report back the current status of the run_db."""
    db_overview = ctx.obj["dboverview"]
    status, msg = db_overview.read_current_status()
    print(status, msg)

    if not status:
        raise click.ClickException("Job is already running or dates do not match!")


@cli.command()
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Remove last line regardless of if it is problematic or not",
)
@click.pass_context
def remove_already_running(ctx, force: bool):
    """Remove problematic entry from the run_db."""
    db_overview = ctx.obj["dboverview"]
    print(db_overview.delete_last_row(force))


if __name__ == "__main__":
    cli()
