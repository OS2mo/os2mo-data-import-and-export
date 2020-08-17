import unittest
from integrations.os2sync import config, __main__
import pathlib
import os
import json

config.logformat = "%(message)s"


class Tests(unittest.TestCase):
    maxDiff = None

    def run_once(self, directory, **overrides):
        pd = pathlib.Path(directory)
        log = pd / "log"
        hk = pd / "hk"
        config.settings["OS2SYNC_HASH_CACHE"] = str(hk)
        config.settings["MOX_LOG_FILE"] = str(log)
        config.settings.update(overrides)
        print(log)
        if not log.exists():
            __main__.main()

        lines = log.read_text().split("\n")

        results = {"post": {}, "delete": {}}
        for line in lines:
            if line.startswith("POST"):
                js = line.split(" ", maxsplit=2)[-1].replace("'", '"')
                try:
                    js = json.loads(js)["json"]
                except:
                    print(js)
                    continue
                if "Positions" in js:
                    js["Positions"] = sorted(js["Positions"],
                                             key=lambda x: x["OrgUnitUuid"])
                if "Tasks" in js:
                    js["Tasks"] = sorted(js["Tasks"])
                if "ContactForTasks" in js:
                results["post"].setdefault(js["Uuid"], js)

        return results

    def test_ekvivalens(self):
        self.maxDiff = None
        os.makedirs("/tmp/os2sync-test/old", exist_ok=True)
        old = self.run_once("/tmp/os2sync-test/old", OS2SYNC_USE_LC_DB=False)
        new = self.run_once("/tmp/os2sync-test/new", OS2SYNC_USE_LC_DB=True)
        self.assertEqual(old, new)
