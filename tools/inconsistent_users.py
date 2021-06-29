import csv
import json
import pathlib
import pprint

import click
import pandas as pd

from os2mo_helpers.mora_helpers import MoraHelper


class UserComparison():
    def __init__(
        self, settings, mora_helper: MoraHelper, mapping_file_path, user_file_path
    ):
        self.settings = settings
        self.mora_helper = mora_helper
        self.mapping_file_path = mapping_file_path
        self.user_file_path = user_file_path

    def get_mo_users(self):
        users = self.mora_helper.read_all_users()

        return {user['name']: user['uuid'] for user in users}

    def get_csv_mapping(self):
        with open(self.mapping_file_path, 'r') as f:
            csv_reader = csv.DictReader(f, delimiter=";")
            mapping = {line["mo_uuid"]: line["ad_guid"] for line in csv_reader}
        return mapping

    def get_file_users(self) -> set:
        with pd.ExcelFile(self.user_file_path) as xlsx_file:
            sheet = xlsx_file.parse('Rettighedstildelinger')
            xlsx_users = {row.Bruger for index, row in sheet.iterrows()}
        return xlsx_users

    def run(self):
        mo_users = self.get_mo_users()
        mapping = self.get_csv_mapping()
        file_users = self.get_file_users()

        mapped = [
            (name, mo_users[name], mapping[mo_users[name]]) for name in file_users
        ]

        print("{} users found in file".format(len(mapped)))

        def filter_fn(row):
            name, mo_uuid, ad_guid = row
            return mo_uuid != ad_guid

        inconsistent_users = list(filter(filter_fn, mapped))

        print("{} inconsistent users found".format(len(inconsistent_users)))
        pprint.pprint(inconsistent_users)


def load_settings():
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No setting file")
    return json.loads(cfg_file.read_text())


@click.command()
@click.argument('user_file_path', type=click.File('rb'))
def main(**args):
    user_file_path = args['user_file_path']
    mapping_file_path = "cpr_mo_ad_map.csv"

    settings = load_settings()
    mora_helper = MoraHelper(settings['mora.base'])

    comparison = UserComparison(settings, mora_helper, mapping_file_path, user_file_path)
    comparison.run()


if __name__ == '__main__':
    main()
