import pathlib
import random
import tempfile
import unittest

import sqlalchemy

from exporters.sql_export.lc_for_jobs_db import get_engine


class Test(unittest.TestCase):
    def test_get_engine(self):
        testnumber = random.randint(1, 1000)
        with tempfile.TemporaryDirectory() as d:
            pd = pathlib.Path(d)
            dbpath = pd / "testdb"
            myengine = sqlalchemy.create_engine("sqlite:///{}.db".format(dbpath))
            myengine.execute("create table x(x integer);")
            myengine.execute("insert into x values(%d)" % testnumber)
            myengine.dispose()
            yourengine = get_engine(dbpath)
            result = yourengine.execute("select x from x")
            self.assertEqual(result.first(), (testnumber,))
