from datetime import datetime

import click
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_setting
from tqdm import tqdm

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration.ad_reader import ADParameterReader


class CompareEndDate(ADParameterReader):
    def __init__(self, enddate_field, uuid_field, settings=None):
        super().__init__(all_settings=settings)
        self.helper = MoraHelper(
            hostname=self.all_settings["global"]["mora.base"], use_cache=False
        )
        self.enddate_field = enddate_field
        self.uuid_field = uuid_field
        self.cpr_field = self.all_settings["primary"]["cpr_field"]

    def scan_ad(self):
        # Find all AD users whose end date is in the year 9999.
        # This does not in itself indicate an error, as users with current or
        # future engagements without an end date will have an AD end date of
        # 9999-12-31.
        bp = self._ps_boiler_plate()
        cmd = (
            self._build_user_credential()
            + f"Get-ADUser -Filter '{self.enddate_field} -like \"9999-*\"'"
            + bp["complete"]
            + f" -Properties cn,{self.uuid_field},{self.enddate_field}"
            + " | ConvertTo-Json"
        )
        return map(
            lambda ad_user: (
                ad_user["CN"],
                ad_user[self.enddate_field],
                ad_user.get(self.uuid_field),
            ),
            self._run_ps_script(cmd),
        )

    def get_mo_engagements(self, mo_user_uuid):
        return [
            {
                "from": eng["validity"]["from"],
                "to": eng["validity"]["to"],
                "is_primary": eng["is_primary"],
            }
            for eng in self.helper.read_user_engagement(
                mo_user_uuid,
                calculate_primary=True,
                read_all=True,
                skip_past=False,
            )
        ]

    def compare_mo(self):
        # Compare AD users to MO users
        print("Find users from AD")
        ad_hits = list(self.scan_ad())

        print("Check MO engagements")
        users = {}
        for cn, end_date, mo_user_uuid in tqdm(ad_hits, unit="user"):
            if mo_user_uuid:
                mo_engagements = self.get_mo_engagements(mo_user_uuid)
                if mo_engagements:
                    if not any([eng["to"] is None for eng in mo_engagements]):
                        users[mo_user_uuid] = {
                            "cn": cn,
                            "mo_engagements": mo_engagements,
                        }
        return users


class UpdateEndDate(AD):
    def __init__(self, enddate_field, uuid_field, cpr_field, settings=None):
        super().__init__(all_settings=settings)
        self.enddate_field = enddate_field
        self.uuid_field = uuid_field
        self.cpr_field = cpr_field

    def get_correct_end_date(self, doc):
        candidates = sorted(
            [val["to"] for val in doc if val["is_primary"]],
            key=lambda to_date: datetime.strptime(to_date, "%Y-%m-%d").date(),
            reverse=True,
        )
        return candidates[0]

    def get_changes(self, users):
        for uuid, doc in users.items():
            end_date = self.get_correct_end_date(doc["mo_engagements"])
            yield uuid, end_date

    def get_update_cmd(self, uuid, end_date):
        cmd_f = """
        Get-ADUser %(complete)s -Filter '%(uuid_field)s -eq "%(uuid)s"' |
        Set-ADUser %(credentials)s -Replace @{%(enddate_field)s="%(end_date)s"} |
        ConvertTo-Json
        """
        cmd = cmd_f % dict(
            uuid=uuid,
            end_date=end_date,
            enddate_field=self.enddate_field,
            uuid_field=self.uuid_field,
            complete=self._ps_boiler_plate()["complete"],
            credentials=self._ps_boiler_plate()["credentials"],
        )
        return cmd

    def run(self, cmd):
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))


@click.command()
@click.option(
    "--enddate-field",
    default=load_setting("integrations.ad_writer.fixup_enddate_field"),
)
@click.option("--uuid-field", default=load_setting("integrations.ad.write.uuid_field"))
@click.option("--dry-run", is_flag=True)
def cli(enddate_field, uuid_field, dry_run):
    """Fix enddates of terminated users.
    AD-writer does not support writing enddate of a terminated employee,
    this script finds and corrects the enddate in AD of terminated engagements.
    """

    c = CompareEndDate(enddate_field, uuid_field)
    users = c.compare_mo()
    u = UpdateEndDate(enddate_field, uuid_field, c.cpr_field)
    users = u.get_changes(users)

    for uuid, end_date in tqdm(users, unit="user", desc="Changing enddate in AD"):
        cmd = u.get_update_cmd(uuid, end_date)
        if dry_run:
            print("Command to run: ")
            print(cmd)
        else:
            result = u.run(cmd)
            if result:
                print("Result: %r" % result)

    print("All done")


if __name__ == "__main__":
    cli()
