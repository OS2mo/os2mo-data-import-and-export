#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Make a number of pre-defined queries into MO.
These are for the general case, specific version for each municipality
is typically a better solution.
"""

import time
from mora_helpers import MoraHelper
import common_queries as cq


if __name__ == '__main__':
    threaded_speedup = True
    t = time.time()

    mh = MoraHelper()

    org = mh.read_organisation()
    roots = mh.read_top_units(org)

    trees = {}
    max_height = 0
    for root in roots:
        name = root['name']
        uuid = root['uuid']
        trees[name] = mh.read_ou_tree(uuid)
        if trees[name]['root'].height > max_height:
            main_root = name
    print('Find main tree: {}'.format(time.time() - t))
    nodes = trees[main_root]

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    filename = 'alle_lederfunktioner_os2mo.csv'
    cq.export_managers(mh, nodes, filename)
    print('Alle ledere: {}s'.format(time.time() - t))

    filename = 'alle-medarbejdere-stilling-email_os2mo.csv'
    cq.export_all_employees(mh, nodes, filename)
    print('alle-medarbejdere-stilling-email_os2mo.csv: {}s'.format(time.time() - t))

    filename = 'org_incl-medarbejdere.csv'
    cq.export_orgs(mh, nodes, filename)
    print('org_incl-medarbejdere.csv: {}s'.format(time.time() - t))

    filename = 'adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv'
    cq.export_adm_org(mh, nodes, filename)
    print('adm-org-incl-start-stop: {}s'.format(time.time() - t))

    # filename = 'Teams-tilknyttede.csv'
    # cq.export_all_teams(mh, nodes, filename)
    # print('Teams: {}s'.format(time.time() - t))
