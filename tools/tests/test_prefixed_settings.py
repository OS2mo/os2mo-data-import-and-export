import unittest

from tools import prefixed_settings
class Tests(unittest.TestCase):
    
    def test_prefixed_settings_with_dot(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            {
                "manuel.børnehave.CYKEL": "Børnehavens",
                "manuel.BIL": "Din",
                "manuel.CYKEL": "Min",
                "manuella": "Theirs"
            }, "manuel.")), [
                'export BIL="Din"', 
                'export CYKEL="Min"'
            ])

    def test_prefixed_settings_no_dot(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            {
                "manuel.børnehave.CYKEL":"Børnehavens",
                "manuel.CYKEL": "Min",
                "manuel.BIL": "Din",
                "manuella": "Theirs"
            }, "manuel")), [
                'export BIL="Din"', 
                'export CYKEL="Min"',
            ])

    def test_prefixed_settings_int(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            { "manuel.torsk": 0, }, "manuel")),
            ['export torsk=0']
        )

    def test_prefixed_settings_float(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            { "manuel.torsk": 0.1, }, "manuel")),
            ['export torsk=0.1']
        )

    def test_prefixed_settings_boolean(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            { "manuel.torsk": False, }, "manuel")),
            ['export torsk=false']
        )
 
    def test_prefixed_settings_more_dots(self):
        self.assertEqual(sorted(prefixed_settings.extract_prefixed_envvars(
            {
                "manuel.børnehave.CYKEL":"Børnehavens",
                "manuel.BIL":"Din",
            }, "manuel.børnehave")), ['export CYKEL="Børnehavens"'])
