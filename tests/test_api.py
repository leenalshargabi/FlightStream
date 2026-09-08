#Basic tests for the FlightStream OpenSky API normalization layer

import unittest

from api.api import state_to_record

class TestStateToRecord(unittest.TestCase):
    def test_state_to_record_normalizes_values(self):
        state = [
            "C07D0B",
            "CGVJE   ",
            "Canada",
            1788814438,
            1788814438,
            -74.4099,
            45.6391,
            464.82,
            False,
            26.76,
            271.1,
            3.58,
            None,
            487.68,
            None,
            False,
            0,
        ]

        record = state_to_record(state, 1788814439)

        self.assertIsNotNone(record)
        self.assertEqual(record["icao24"], "c07d0b")
        self.assertEqual(record["callsign"], "CGVJE")
        self.assertEqual(record["origin_country"], "Canada")
        self.assertEqual(record["longitude"], -74.4099)
        self.assertEqual(record["latitude"], 45.6391)
        self.assertIsNone(record["category"])
        self.assertEqual(record["snapshot_time"], 1788814439)

    def test_invalid_short_state_is_skipped(self):
        self.assertIsNone(state_to_record(["abc123"], 123))


if __name__ == "__main__":
    unittest.main()
