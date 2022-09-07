#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

from unittest.mock import patch, MagicMock

import pytest
from parameterized import parameterized
from requests.exceptions import HTTPError
from requests import Response

from os2mo_data_import.utilities import ImportUtility


@parameterized.expand([(0,), (9,)])
def test_retrying_mo_data_insertion_should_pass_in_less_max_10_tries(retries):
    # Arrange
    import_utility = ImportUtility("http://mox.base", "http://mora.base", False)
    import_utility.mo_request_retry_delay = 0.05

    r = Response()
    r.status_code = 200
    mock_json = MagicMock()
    mock_json.return_value = "uuid"
    r.json = mock_json

    mora_helper = MagicMock()
    mora_helper._mo_post.side_effect = retries*[HTTPError()] + [r]
    import_utility.mh = mora_helper

    # Act
    resp = import_utility.insert_mora_data("/some/resource", {})

    # Assert
    assert resp == "uuid"


def test_retrying_mo_data_insertion_should_fail_after_10_retries():
    # Arrange
    import_utility = ImportUtility("http://mox.base", "http://mora.base", False)
    import_utility.mo_request_retry_delay = 0.05

    mora_helper = MagicMock()
    mora_helper._mo_post.side_effect = 10*[HTTPError()]
    import_utility.mh = mora_helper

    # Act + Assert
    with pytest.raises(HTTPError):
        import_utility.insert_mora_data("/some/resource", {})
