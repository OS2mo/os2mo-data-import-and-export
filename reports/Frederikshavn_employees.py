#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import os
import time
from anytree import PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper
import common_queries as cq

MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:5000')

if __name__ == '__main__':
    threaded_speedup = False
    t = time.time()

    mh = MoraHelper(hostname=MORA_BASE, export_ansi=False)

    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    print(roots)
    for root in roots:
        if root['name'] == 'Frederikshavn Kommune':
            frederikshavn = root['uuid']


    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    nodes = mh.read_ou_tree(frederikshavn)
    print('Read nodes: {}s'.format(time.time() - t))

    filename = 'AlleBK-stilling-email_os2mo.csv'
    cq.export_all_employees(mh, nodes, filename)
    print('AlleBK-stilling-email: {}s'.format(time.time() - t))

    print('Export completed')
