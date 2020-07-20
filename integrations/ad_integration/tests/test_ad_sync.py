# TODO: Fix imports in module
import sys
from os.path import dirname

sys.path.append(dirname(__file__))
sys.path.append(dirname(__file__) + "/..")

from unittest import TestCase

from parameterized import parameterized


from test_utils import TestADMoSyncMixin, dict_modifier, mo_modifier


class TestADMoSync(TestCase, TestADMoSyncMixin):
    def setUp(self):
        self._setup_admosync()

    def test_user_edit(self):
        """Test user edit ps_script code.

        The common code is not tested.
        """
        self.ad_sync.update_all_users()
