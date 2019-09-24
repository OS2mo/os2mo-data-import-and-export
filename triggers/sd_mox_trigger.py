# 
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
SD Mox trigger.module
"""

import logging
import pathlib
import datetime
import sys
import json

# OS2MO imports
from mora import mapping
from mora.triggers import Trigger
from mora.service.handlers import RequestType

# os2mo-data-import-and-export imports
import customer
custpath = pathlib.Path(customer.__file__).parent
sys.path.append(str(custpath))

from customer.integrations.SD_Lon import (
    sd_mox,
    sd_logging,
    sd_common
)

sdmox_config = {}
sd_logging.start_logging("")

def read_config(app):
    cfg_file = custpath / "settings" / app.config["CUSTOMER_CONFIG_FILE"]
    cfg = json.loads(cfg_file.read_text())
    sdmox_config.update(
        sd_common.get_prefixed_configuration(
            cfg,
            sd_mox.CFG_PREFIX
        )
    )
    sdmox_config["sd_common"] = sd_common.get_prefixed_configuration(
        cfg,
        sd_common.CFG_PREFIX
    )

    import pdb; pdb.set_trace()
    sd_mox_work({})

def sd_mox_work(data):
    from_date = datetime.datetime(2019, 7, 1, 0, 0)
    mox = sd_mox.sdMox(from_date, **sdmox_config)

    unit_code = '06GÅ'
    unit_level = 'Afdelings-niveau'
    parent = {
        'unit_code': '32D9',
        'uuid': '32d9b4ed-eff2-4fa9-a372-c697eed2a597',
        'level': 'NY2-niveau'
    }

    department = mox.read_department(unit_code=unit_code, unit_level=unit_level)

    unit_uuid = '31b43f5d-d8e8-4bd2-8420-a41148ca229f'
    unit_name = 'Daw dav'
    if department:
        errors = mox._check_department(department, unit_name, unit_code, unit_uuid)
        print(errors)
    else:
        print('Department does not exist')



def sd_mox_pretrigger(data):
    """ This is the function that is called with data from the handler module
    """
    ErrorCodes.E_INTEGRATION_ERROR()


def register(app):
    """ Here the function above is registered to get triggered
        every time an org unit has been terminated
    """
    combi = (mapping.ORG_UNIT, RequestType.CREATE, Trigger.Event.ON_BEFORE)
    Trigger.on(*combi)(sd_mox_pretrigger)
    read_config(app)

