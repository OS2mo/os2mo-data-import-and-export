import json
import pathlib
import sqlite3
from ra_utils.load_settings import load_settings

# TODO:
# This file exists in two quite similar versions.
# Consider to merge into a common tool.


class DBOverview(object):
    def __init__(self):
        self.settings = load_settings()

    def read_db_content(self):
        conn = sqlite3.connect(self.settings['integrations.opus.import.run_db'],
                               detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'select * from runs order by id'
        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            status = 'id: {}, dump date: {}, status: {}'
            print(status.format(row[0], row[1], row[2]))

    def read_current_status(self):
        conn = sqlite3.connect(self.settings['integrations.opus.import.run_db'],
                               detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()

        query = 'select * from runs order by id desc limit 1'
        c.execute(query)
        row = c.fetchone()
        if 'Running' in row[2]:
            status = (False, 'Not ready to run')
        else:
            status = (True, 'Status ok')
        return status

    def delete_last_row(self, force=False):
        current_status = self.read_current_status()
        if current_status[0] and not force:
            return 'Status ok, no delete'

        conn = sqlite3.connect(self.settings['integrations.opus.import.run_db'],
                               detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        query = 'select id from runs order by id desc limit 1'
        c.execute(query)
        row = c.fetchone()

        query = 'delete from runs where id=?'
        c.execute(query, (row[0],))
        conn.commit()
        return 'Deleted last row'


if __name__ == '__main__':
    db_overview = DBOverview()

    # print(db_overview.delete_last_row())
    # print(db_overview.delete_last_row(force=True))
    # print(db_overview.delete_last_row(force=True))
    db_overview.read_db_content()
    print(db_overview.read_current_status())
