import sqlite3
from datetime import date
from datetime import datetime

import click
from ra_utils.load_settings import load_settings


class SQLite:
    def __init__(self, file="sqlite.db"):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class DBOverview:
    def __init__(self, settings=None):
        settings = settings or load_settings()
        self.run_db = SQLite(settings["integrations.SD_Lon.import.run_db"])

    def table_exists(self) -> bool:
        with self.run_db as c:
            query = """
                SELECT
                    count(name)
                FROM
                    sqlite_master
                WHERE
                    type='table' AND
                    name='runs'
            """
            c.execute(query)
            if c.fetchone()[0] == 1:
                return True
            return False

    def create_table(self):
        with self.run_db as c:
            query = """
                CREATE TABLE runs (
                    id INTEGER PRIMARY KEY,
                    from_date timestamp,
                    to_date timestamp,
                    status text
                )
            """
            c.execute(query)

    def read_db_content(self):
        with self.run_db as c:
            query = """
                SELECT
                    *
                FROM
                    runs
                ORDER BY
                    id
            """
            c.execute(query)
            rows = c.fetchall()
            for row in rows:
                status = "id: {}, from: {}, to: {}, status: {}"
                print(status.format(*row))

    def read_last_line(self, *fields):
        with self.run_db as c:
            query = f"""
                SELECT
                    { ", ".join([*fields]) }
                FROM
                    runs
                ORDER BY
                    id
                DESC LIMIT 1
            """
            c.execute(query)
            result = c.fetchone()
            if len(result) == 1:
                return result[0]
            return result

    def delete_line(self, id):
        with self.run_db as c:
            query = """
                DELETE FROM
                    runs
                WHERE
                    id=?
            """
            c.execute(query, (id,))

    def insert_row(self, from_date, to_date, message):
        with self.run_db as c:
            query = """
                INSERT INTO
                    runs (from_date, to_date, status)
                VALUES
                    (?, ?, ?)
            """
            c.execute(query, (from_date, to_date, message.format(datetime.now())))

    def read_current_status(self):
        today = date.today()
        midnight = datetime.min.time()
        midnight_today = datetime.combine(today, midnight)

        to_date, status = self.read_last_line("to_date", "status")
        if "Running" in status:
            return (False, "Not ready to run")
        if to_date < midnight_today:
            return (False, "Not up to date")
        return (True, "Status ok")

    def delete_last_row(self, force=False):
        current_status = self.read_current_status()
        if current_status[0] and not force:
            return "Status ok, no delete"

        last_id = self.read_last_line("id")
        print(last_id)
        self.delete_line(last_id)
        return "Deleted last row"


@click.command()
def read_rundb():
    """Load the run_db and print current status."""
    db_overview = DBOverview()
    if not db_overview.table_exists():
        raise Exception("No run-db exists")
    db_overview.read_db_content()
    status, msg = db_overview.read_current_status()
    print(status, msg)

    if not status:
        raise Exception("Job is already running or dates do not match!")
    # TODO: If this is a common action, make a command for it?
    # print(db_overview.delete_last_row(force=True))


if __name__ == "__main__":
    read_rundb()
