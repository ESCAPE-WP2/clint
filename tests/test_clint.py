import argparse
import unittest
from clint import add_run_arguments, read_config_csv, read_storage_csv, sustainable_rate_mbps

class TestClint(unittest.TestCase):

    def test_read_config_csv(self):
        data = read_config_csv("tests/sample_config.csv")
        self.assertGreater(len(data), 0)
        self.assertIn('src', data[0])
        self.assertIn('dst', data[0])

    def test_read_storage_csv(self):
        data = read_storage_csv("tests/sample_storage.lst", True)
        self.assertGreater(len(data), 0)
        self.assertIn('did', data[0])
        self.assertIn('bytes', data[0])


class TestSustainableRateMbps(unittest.TestCase):

    def test_repo_pool_is_far_below_config_target(self):
        # EOS-OPENDATA-ATLAS.lst in this repo: 3 x 80GB, with the defaults
        # rule-lifetime=7200 and expiration-delay=300. This is the exact
        # shortfall the gap analysis found in clint_injection.log.
        pool_bytes = 3 * 80_000_000_000
        rate = sustainable_rate_mbps(pool_bytes, rule_lifetime=7200, expiration_delay=300)
        self.assertAlmostEqual(rate, 256, delta=1)

    def test_bigger_pool_sustains_a_higher_rate(self):
        small = sustainable_rate_mbps(pool_bytes=100_000_000_000, rule_lifetime=7200, expiration_delay=300)
        big = sustainable_rate_mbps(pool_bytes=1_000_000_000_000, rule_lifetime=7200, expiration_delay=300)
        self.assertGreater(big, small)

    def test_longer_cycle_lowers_sustainable_rate(self):
        short_cycle = sustainable_rate_mbps(pool_bytes=100_000_000_000, rule_lifetime=3600, expiration_delay=0)
        long_cycle = sustainable_rate_mbps(pool_bytes=100_000_000_000, rule_lifetime=36000, expiration_delay=0)
        self.assertGreater(short_cycle, long_cycle)


class TestAddRunArguments(unittest.TestCase):

    def test_defaults_match_current_clint_py_behaviour(self):
        parser = argparse.ArgumentParser()
        add_run_arguments(parser)
        args = parser.parse_args([])
        self.assertEqual(args.config, 'config.csv')
        self.assertEqual(args.storage_dir, '.')
        self.assertEqual(args.injection_interval, 900)
        self.assertEqual(args.rule_lifetime, 7200)
        self.assertEqual(args.max_injection_factor, 0.20)
        self.assertEqual(args.fudge_factor, 0)
        self.assertEqual(args.expiration_delay, 300)
        self.assertFalse(args.big_first)
        self.assertFalse(args.dryrun)
        self.assertFalse(args.skip_validation)
        self.assertFalse(args.single_thread)


if __name__ == "__main__":
    unittest.main()
