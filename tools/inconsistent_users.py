import argparse
import csv
import json
import pathlib
import pprint

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
            mapping = {line["sam_account_name"]: (line["ad_guid"], line['mo_uuid']) for line in csv_reader}
        return mapping

    def get_file_users(self) -> set:
        with open(self.user_file_path, 'r') as f:
            csv_reader = csv.DictReader(f, delimiter=";")
            mapping = {line["Bruger ID"] for line in csv_reader}
        return mapping

    def run(self):
        # mo_users = self.get_mo_users()
        mapping = self.get_csv_mapping()
        file_users = self.get_file_users()

        mapped = []
        for name in file_users:
            mapped_user = mapping.get(name)
            if not mapped_user:
                print("{} not found in mapping".format(name))
                continue
            mapped.append((name, mapped_user))

        # mapped = [
        #     (name, mapping[name]) for name in file_users
        # ]

        print("{} users found in file".format(len(mapped)))

        def filter_fn(row):
            name, tup = row
            mo_uuid, ad_guid = tup
            return mo_uuid != ad_guid

        inconsistent_users = list(filter(filter_fn, mapped))

        print("{} inconsistent users found".format(len(inconsistent_users)))
        pprint.pprint(inconsistent_users)


def load_settings():
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No setting file")
    return json.loads(cfg_file.read_text())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('user_file_path')
    args = parser.parse_args()

    user_file_path = args.user_file_path
    mapping_file_path = "cpr_mo_ad_map.csv"

    settings = load_settings()
    mora_helper = MoraHelper(settings['mora.base'])

    comparison = UserComparison(settings, mora_helper, mapping_file_path, user_file_path)
    comparison.run()


if __name__ == '__main__':
    main()
