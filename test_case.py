import os
import unittest

from hypothesis import Verbosity, settings as h_settings

h_settings.register_profile("ci", max_examples=100, deadline=None)
h_settings.register_profile("dev", max_examples=10)
h_settings.register_profile("debug", max_examples=10, verbosity=Verbosity.verbose)
h_settings.load_profile(os.getenv(u"HYPOTHESIS_PROFILE", "default"))


class DipexTestCase(unittest.TestCase):
    pass
