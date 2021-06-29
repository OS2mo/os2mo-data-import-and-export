from unittest import TestCase
from unittest.mock import patch

from integrations.SD_Lon.fix_departments import FixDepartments
from parameterized import parameterized

default_root = "abc"
alternate_root = "def"


class TestFixDepartments(TestCase):
    @parameterized.expand(
        [
            ("default", {"mora.base": "123"}, default_root),
            (
                "alternate",
                {
                    "mora.base": "123",
                    "integrations.SD_Lon.fix_departments_root": alternate_root,
                },
                alternate_root,
            ),
        ]
    )
    @patch(
        "integrations.SD_Lon.fix_departments.FixDepartments.get_institution",
        lambda x: "anything",
    )
    @patch(
        "integrations.SD_Lon.fix_departments.MoraHelper.read_classes_in_facet",
        lambda x, y: [[{"user_key": "Enhed"}]],
    )
    def test_root(self, name, settings, target_root):
        """
        Test that read_person does the expected transformation.
        """

        with patch("integrations.SD_Lon.fix_departments.load_settings") as m1, patch(
            "integrations.SD_Lon.fix_departments.MoraHelper.read_organisation"
        ) as m2:
            m1.return_value = settings
            m2.return_value = default_root
            fixer = FixDepartments()
            self.assertEqual(target_root, fixer.org_uuid)
