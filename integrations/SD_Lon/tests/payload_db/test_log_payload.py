# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from payload_db import log_payload


def test_log_payload():
    log_payload("", "", "")
