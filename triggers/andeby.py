#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Trigger demonstration module.
This could be used as a template.
The imports are the bare minimum for a trigger module

"""

import logging
from mora import mapping
from mora.triggers import Trigger
from mora.service.handlers import RequestType

logger = logging.getLogger("andeby")


def andeby_trigger_function(data):
    """ This is the function that is called with data from the handler module
    """
    logger.warning("andeby example called")


def register(app):
    """ Here the function above is registered to get triggered
        every time an org unit has been terminated
    """
    combi = (mapping.ORG_UNIT, RequestType.TERMINATE, Trigger.Event.ON_AFTER)
    Trigger.on(*combi)(andeby_trigger_function)
