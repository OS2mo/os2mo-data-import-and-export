import json
import re
from datetime import datetime

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration.ad_reader import ADParameterReader
from os2mo_helpers.mora_helpers import MoraHelper


class CompareEndDate(ADParameterReader):
    def __init__(self):
        super().__init__()
        self.helper = MoraHelper(
            hostname=self.all_settings["global"]["mora.base"], use_cache=False
        )

    def scan_ad(self):
        # Find all AD users whose end date (= "extensionAttribute9") is in the
        # year 9999.
        # This does not in itself indicate an error, as users with current or
        # future engagements without an end date will have an AD end date of
        # 9999-12-31.
        bp = self._ps_boiler_plate()
        cmd = (
            self._build_user_credential()
            + "Get-ADUser -Filter 'extensionattribute9 -like \"9999-*\"'"
            + bp["complete"]
            + " -Properties cn,hkstsuuid,extensionattribute9"
            + " | ConvertTo-Json"
        )
        return map(
            lambda ad_user: (
                ad_user["CN"],
                ad_user["extensionattribute9"],
                ad_user.get("hkstsuuid"),
            ),
            self._run_ps_script(cmd)
        )

    def get_mo_engagements(self, mo_user_uuid):
        return [
            {
                "from": eng["validity"]["from"],
                "to": eng["validity"]["to"],
                "is_primary": eng["is_primary"],
            }
            for eng in self.helper.read_user_engagement(
                mo_user_uuid, calculate_primary=True, read_all=True, skip_past=False,
            )
        ]

    def compare_mo(self):
        # Compare AD users to MO users
        ad_hits = self.scan_ad()
        users = {}
        for cn, end_date, mo_user_uuid in ad_hits:
            if mo_user_uuid:
                mo_engagements = self.get_mo_engagements(mo_user_uuid)
                if mo_engagements:
                    if not any([eng["to"] is None for eng in mo_engagements]):
                        users[mo_user_uuid] = end_date
        return users


class UpdateEndDate(AD):

    def get_correct_end_date(self, doc):
        candidates = sorted(
            [val["to"] for val in doc if val["is_primary"]],
            key=lambda to_date: datetime.strptime(to_date, "%Y-%m-%d").date(),
            reverse=True,
        )
        return candidates[0]

    def get_changes(self, users):
        for uuid, doc in users:
            end_date = self.get_correct_end_date(doc)
            yield uuid, end_date

    def get_update_cmd(self, uuid, end_date):
        cmd_f = """
        Get-ADUser %(complete)s -Filter 'hkstsuuid -eq "%(uuid)s"' |
        Set-ADUser %(credentials)s -Replace @{extensionattribute9="%(end_date)s"} |
        ConvertTo-Json
        """
        cmd = cmd_f % dict(
            uuid=uuid,
            end_date=end_date,
            complete=self._ps_boiler_plate()["complete"],
            credentials=self._ps_boiler_plate()["credentials"],
        )
        return cmd

    def run(self, cmd):
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))


if __name__ == "__main__":

    c = CompareEndDate()
    users = c.compare_mo()
    u = UpdateEndDate()

    for uuid, end_date in u.get_changes(users):
        cmd = u.get_update_cmd(uuid, end_date)
        print("Command to run: ")
        print(cmd)
        choice = input("Type 'Y' to run command, or any other key to skip this user: ")
        if choice.lower() == "y":
            # result = u.run(cmd)
            print("Result: %r" % result)
        print()

    print("All done")
