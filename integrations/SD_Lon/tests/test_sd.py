# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import unittest
import freezegun
from integrations.SD_Lon.sd import SD, CFG_PREFIX

class Tests(unittest.TestCase):
    maxDiff = None

    def test_create(self):
        config={
            CFG_PREFIX + "INSTITUTION_IDENTIFIER": "x",
            CFG_PREFIX + "SD_USER": "y",
            CFG_PREFIX + "SD_PASSWORD": "1",
            CFG_PREFIX + "BASE_URL": "2",
        }
        # see - config values are prefixed now
        self.assertIn("integrations.SD_Lon.sd_common.INSTITUTION_IDENTIFIER", config)

        sd = SD.create(config)
        # but sd takes away the prefix

        self.assertEqual({
            "INSTITUTION_IDENTIFIER": "x",
            "SD_USER": "y",
            "SD_PASSWORD": "1",
            "BASE_URL": "2",
        }, sd.config)
