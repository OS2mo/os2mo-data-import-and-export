import los_org


class TestConsolidatePayloads:
    def test_consolidate_identical(self):
        """Should correctly consolidate identical consecutive payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
            {"val": 123, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 123, "validity": {"from": "2013-01-01", "to": "2013-12-31"}},
            {"val": 123, "validity": {"from": "2014-01-01", "to": "2014-12-31"}},
        ]

        expected = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2014-12-31"}}
        ]

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_non_identical(self):
        """Should handle two consecutive non-identical payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 456, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_non_consecutive(self):
        """Should handle non-consecutive payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_mixed(self):
        """Should handle a mix between identical and non-identical payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
            {"val": 456, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 789, "validity": {"from": "2013-01-01", "to": "2013-12-31"}},
            {"val": 789, "validity": {"from": "2014-01-01", "to": "2014-12-31"}},
        ]

        expected = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2011-12-31"}},
            {"val": 456, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 789, "validity": {"from": "2013-01-01", "to": "2014-12-31"}},
        ]

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_single_element(self):
        """Should return same single payload"""
        payloads = [
            {"val": 456, "validity": {"from": "2011-01-01", "to": "2011-12-31"}}
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_empty(self):
        """Should trivially handle empty input"""
        payloads = []

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual
