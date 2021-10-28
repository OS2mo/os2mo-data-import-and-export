##
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import unittest
from unittest.mock import patch

from os2mo_helpers.mora_helpers import MoraHelper


class TestMoLookup(unittest.TestCase):
    def test_auth_enabled_set_correctly(self):
        mora_helper = MoraHelper()
        self.assertTrue(mora_helper.auth_enabled)

        mora_helper = MoraHelper(auth_enabled=False)
        self.assertFalse(mora_helper.auth_enabled)

    @patch('requests.get')
    def test_authorization_header_not_set_for_lookup_when_auth_disabled(self, mock_get):
        mora_helper = MoraHelper(auth_enabled=False)

        mora_helper._mo_lookup("mock uuid", "some-endpoint")

        mock_get.assert_called_once_with(
            "http://localhost:5000/service/some-endpoint", headers=dict(), params={}
        )

    @patch('ra_utils.headers.TokenSettings.get_headers')
    @patch('requests.get')
    def test_authorization_header_set_for_lookup_when_auth_enabled(self, mock_requests_get, mock_get_headers):
        mora_helper = MoraHelper()
        headers = {"Authorization": "Bearer token"}
        mock_get_headers.return_value = headers

        mora_helper._mo_lookup("mock uuid", "some-endpoint")

        mock_requests_get.assert_called_once_with(
            "http://localhost:5000/service/some-endpoint", headers=headers, params={}
        )

    @patch('requests.post')
    def test_authorization_header_not_set_for_post_when_auth_disabled(self, mock_post):
        mora_helper = MoraHelper()

        mora_helper._mo_post("some-endpoint", dict(), force=False)

        mock_post.assert_called_once_with(
            "http://localhost:5000/service/some-endpoint", headers=dict(), params=dict(), json=dict()
        )
        assert False

    # @patch('ra_utils.headers.TokenSettings.get_headers')
    # @patch('requests.get')
    # def test_authorization_header_set_for_post_when_auth_enabled(self, mock_requests_get, mock_get_headers):
    #     mora_helper = MoraHelper()
    #     headers = {"Authorization": "Bearer token"}
    #     mock_get_headers.return_value = headers
    #
    #     mora_helper._mo_lookup("mock uuid", "some-endpoint")
    #
    #     mock_requests_get.assert_called_once_with(
    #         "http://localhost:5000/service/some-endpoint", headers=headers, params={}
    #     )
