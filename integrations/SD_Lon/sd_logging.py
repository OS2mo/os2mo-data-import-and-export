#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import logging

LOG_LEVEL = logging.DEBUG


def start_logging(log_file, detail_logging=None):
    if detail_logging is None:
        detail_logging = (
            "sdChangedAt",
            "sdCommon",
            "sdFixDepartments",
            "sdImport",
            "sdMox",
            "mora-helper",
        )

    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=log_file,
    )
