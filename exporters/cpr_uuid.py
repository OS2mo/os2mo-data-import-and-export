import os
import argparse

from os2mo_helpers.mora_helpers import MoraHelper

MORA_BASE = os.environ.get('MORA_BASE')


def main(root_uuid):
    mh = MoraHelper(hostname=MORA_BASE, export_ansi=True)
    org = mh.read_organisation()

    mapping = {}

    i = 0
    employees = mh.read_all_users()
    for i in range(0, len(employees)):
        if i % 100 == 0:
            print('{}/{}'.format(i, len(employees)))

        uuid = employees[i]['uuid']
        user = mh.read_user(uuid, org_uuid=org)
        cpr = user['cpr_no']
        mapping[cpr] = uuid


def cli():
    """
    Command line interface for the AD writer class.
    """
    parser = argparse.ArgumentParser(description='AD Writer')
    parser.add_argument('--root-uuid', nargs=1, metavar='root_uuid')

    args = vars(parser.parse_args())

    if args.get('root_uuid'):
        main(args.get('root_uuid')[0])


if __name__ == '__main__':
    cli()
