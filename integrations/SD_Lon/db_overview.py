import click
import json
import pathlib
import sqlite3
from datetime import date
from datetime import datetime


class DBOverview(object):
    def __init__(self):
        # TODO: Soon we have done this 48 times. Should we make a small settings
        # importer, that will also handle datatype for specicic keys?
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        settings = json.loads(cfg_file.read_text())

        self.run_db = settings['integrations.SD_Lon.import.run_db']

    def read_db_content(self):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'select * from runs order by id'
        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            status = 'id: {}, from: {}, to: {}, status: {}'
            print(status.format(row[0], row[1], row[2], row[3]))

    def read_current_status(self):
        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'SELECT to_date, status FROM runs ORDER BY id DESC LIMIT 1'
        c.execute(query)
        today = date.today()
        midnight = datetime.min.time()
        midnight_today = datetime.combine(today, midnight)

        to_date, status = c.fetchone()
        if 'Running' in status:
            return (False, 'Not ready to run')
        if to_date < midnight_today:
            return (False, 'Not up to date')
        return (True, 'Status ok')

    def delete_last_row(self, force=False):
        current_status = self.read_current_status()
        if current_status[0] and not force:
            return 'Status ok, no delete'

        conn = sqlite3.connect(self.run_db, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        query = 'select id from runs order by id desc limit 1'
        c.execute(query)
        row = c.fetchone()

        query = 'delete from runs where id=?'
        c.execute(query, (row[0],))
        conn.commit()
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
