import click
import json
import pathlib
import sqlite3
from datetime import date
from datetime import datetime


from integrations.SD_Lon.sd_common import load_settings


class DBOverview(object):
    def __init__(self):
        settings = load_settings()
        self.run_db = settings['integrations.SD_Lon.import.run_db']

    def read_db_content(self):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'SELECT * FROM runs ORDER BY id'
        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            status = 'id: {}, from: {}, to: {}, status: {}'
            print(status.format(*row))

    def read_last_line(self, *fields):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'SELECT ' + ", ".join([*fields]) + ' FROM runs ORDER BY id DESC LIMIT 1'
        c.execute(query)
        result = c.fetchone()
        if len(result) == 1:
            return result[0]
        return result

    def delete_line(self, id):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'DELETE FROM runs WHERE id=?'
        c.execute(query, (id,))
        conn.commit()

    def read_current_status(self):
        today = date.today()
        midnight = datetime.min.time()
        midnight_today = datetime.combine(today, midnight)

        to_date, status = self.read_last_line("to_date", "status")
        if 'Running' in status:
            return (False, 'Not ready to run')
        if to_date < midnight_today:
            return (False, 'Not up to date')
        return (True, 'Status ok')

    def delete_last_row(self, force=False):
        current_status = self.read_current_status()
        if current_status[0] and not force:
            return 'Status ok, no delete'

        last_id = self.read_last_line("id")
        print(last_id)
        self.delete_line(last_id)
        return 'Deleted last row'


@click.command()
def read_rundb():
    """Load the run_db and print current status."""
    db_overview = DBOverview()
    db_overview.read_db_content()
    status, msg = db_overview.read_current_status()
    print(status, msg)

    if not status:
        raise Exception("Job is already running or dates don't match!")
    # TODO: If this is a common action, make a command for it?
    # print(db_overview.delete_last_row(force=True))


if __name__ == '__main__':
    read_rundb()
