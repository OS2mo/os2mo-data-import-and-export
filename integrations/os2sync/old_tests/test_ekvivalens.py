import json
import os
import pathlib
import time
import unittest

from integrations.os2sync import __main__, config

config.logformat = "%(message)s"


class Tests(unittest.TestCase):
    maxDiff = None

    def run_once(self, directory, cache, **overrides):
        starttime = time.time()
        pd = pathlib.Path(directory)
        log = pd / "log"
        hk = pd / "hk"
        cache = pd / cache
        config.settings["OS2SYNC_HASH_CACHE"] = str(hk)
        config.settings["MOX_LOG_FILE"] = str(log)
        # set this in settings file instead
        # config.settings["OS2SYNC_API_URL"] = "stub"
        config.settings.update(overrides)
        if not cache.exists():
            __main__.main(config.settings)
            cache.write_text(hk.read_text())
            hk.unlink()

        endtime = time.time()
        print("\ntime consumed for", directory, overrides, "is", endtime - starttime)
        return json.loads(cache.read_text())

    def test_ekvivalens(self):
        self.maxDiff = None
        testdir = "/tmp/os2sync-test"
        os.makedirs(testdir, exist_ok=True)
        old = self.run_once(testdir, "old", OS2SYNC_USE_LC_DB=False)
        new = self.run_once(testdir, "new", OS2SYNC_USE_LC_DB=True)
        self.assertEqual(old, new)
