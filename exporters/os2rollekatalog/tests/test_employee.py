from unittest.mock import patch
from uuid import uuid4

import pytest
from requests import Response
from tenacity import stop_after_attempt
from tenacity import wait_none

from exporters.os2rollekatalog.os2rollekatalog_integration import LDAPError
from exporters.os2rollekatalog.os2rollekatalog_integration import get_ldap_user_info


def test_get_ldap_user_info_user_not_found():
    response = Response()
    response.status_code = 404
    with patch("requests.get", return_value=response):
        res = get_ldap_user_info("example.com", str(uuid4()))
    assert res == ("", "")


def test_get_ldap_user_info_flaky_ldap():
    response = Response()
    response.status_code = 500

    # Overwrite retrying parameters:
    get_ldap_user_info.retry.wait = wait_none()
    get_ldap_user_info.retry.stop = stop_after_attempt(6)

    with patch("requests.get", return_value=response) as requests_mock:
        with pytest.raises(LDAPError):
            get_ldap_user_info("example.com", str(uuid4()))
    assert requests_mock.call_count == 6
