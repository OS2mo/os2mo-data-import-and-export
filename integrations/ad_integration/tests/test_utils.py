import requests
from unittest import TestCase


class MOTestMixin(object):
    """Mixin to connect to MO and check service configuration.

    Used by the MOTestCase class.
    """

    def get_mo_host(self):
        """Get the base URI for the MO test instance.

        Examples:
            http://localhost:5000
            https://moratest.magenta.dk

        Returns:
            str: URI including schema for the MO test instance.
        """
        return "http://localhost:5000"

    def _fetch_mo_service_configuration(self):
        """Fetch the /service/configuration endpoint on MO.

        Example:
            {
                "read_only": false,
                ...,
                "show_user_key": true
            }

        Returns:
            dict: The JSON response from MO as a dict or None
        """
        host = self.get_mo_host()
        url = host + 'service/configuration'
        response = requests.get(url)
        if response.status_code != 200:
            return None
        return response.json()

    def _check_mo_ready_for_testing(self):
        """Check if a MO instance can be reached and is in readonly mode.

        Example:
            "Unable to reach MO instance"

        Returns:
            str: The reason why MO is not reading for testing or None if ready.
        """
        configuration = self._fetch_mo_service_configuration()
        read_only_key = 'read_only'
        if configuration == None:
            return "Unable to reach MO instance"
        elif read_only_key not in configuration:
            return "MO instance did not return readonly status"
        if configuration[read_only_key] == False:
            # Consider putting MO into read-only mode using:
            # curl -X PUT -H 'Content-Type: application/json' \
            #      -d '{"status": true}' http://localhost:5000/read_only/
            return "MO instance is NOT readonly"
        return None


class MOTestCase(TestCase, MOTestMixin):
    """TestCase, which verifies MO connection in setUP()."""

    def setUp(self):
        # Fetch MO status, and skipTest if any issues are found.
        status = self._check_mo_ready_for_testing()
        if status:
            self.skipTest(status)
