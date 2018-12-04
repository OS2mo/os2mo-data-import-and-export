#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO.
These are specfic for Viborg
"""

import time
from mora_helpers import MoraHelper
import common_queries as cq


if __name__ == '__main__':
    threaded_speedup = False
    t = time.time()

    mh = MoraHelper()

    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == 'Viborg Kommune':
            viborg = root['uuid']

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    nodes = mh.read_ou_tree(viborg)
    print('Read nodes: {}s'.format(time.time() - t))

    filename = 'Alle_lederfunktioner_os2mo.csv'
    cq.export_managers(mh, nodes, filename)
    print('Alle ledere: {}s'.format(time.time() - t))

    filename = 'AlleViborg-stilling-email_os2mo.csv'
    cq.export_all_employees(mh, nodes, filename)
    print('AlleViborg-stilling-email: {}s'.format(time.time() - t))

    filename = 'Viborg_org_incl-medarbejdere.csv'
    cq.export_orgs(mh, nodes, filename)
    print('Viborg org incl medarbejdere: {}s'.format(time.time() - t))

    filename = 'Adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv'
    cq.export_adm_org(mh, nodes, filename)
    print('Adm-org-incl-start-stop: {}s'.format(time.time() - t))

    # filename = 'Teams-tilknyttede.csv'
    # cq.export_all_teams(mh, nodes, filename)
    # print('Teams: {}s'.format(time.time() - t))
