import datetime
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import clint


def _dataset(did, size):
    return {'did': did, 'bytes': size, 'injected_at': datetime.datetime.utcfromtimestamp(0)}


def _run_in_tmpdir(fn):
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            return fn(tmpdir)
        finally:
            os.chdir(old_cwd)


class TestDurationStopsImmediately(unittest.TestCase):

    def test_duration_zero_stops_before_any_cycle(self):
        # event_object.wait is NOT patched here: if the deadline check didn't
        # fire, the real 900s-default interval would hang the test.
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        rucio_client = MagicMock()

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=900, rule_lifetime=60,
                    fudge_factor=0, dryrun=True, expiration_delay=0,
                    max_injection_factor=1.0, user='tester', skip_validation=True,
                    duration=0)
            return True

        self.assertTrue(_run_in_tmpdir(run))
        rucio_client.add_replication_rule.assert_not_called()


class TestTotalVolumeStopsLink(unittest.TestCase):

    def test_link_stops_once_total_volume_reached(self):
        # 3 datasets of 100 bytes; total-volume of 0.0000002GB (200 bytes)
        # should stop the link after the 2nd injected dataset.
        storage_data = {'SRC': [_dataset(f'scope:d{i}', 100) for i in range(3)]}
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.return_value = ['rule-x']

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client), \
                 patch('clint.event_object.wait', return_value=False):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=60,
                    fudge_factor=0, dryrun=False, expiration_delay=100000,
                    max_injection_factor=0, user='tester', skip_validation=True,
                    total_volume=200 / 1e9, max_backlog_factor=0)
            with open('clint_heartbeat.json') as f:
                return json.load(f)

        heartbeat = _run_in_tmpdir(run)
        self.assertGreaterEqual(heartbeat['SRC->DST']['cumulative_bytes'], 200)
        # A huge expiration_delay means, without the stop condition, the loop
        # would spin forever re-selecting from an exhausted fresh pool.
        # Reaching this assertion at all proves the link actually stopped.


def _ledger_events(src, dst):
    with open(clint.ledger_filename(src, dst)) as f:
        return [json.loads(line) for line in f]


class TestBacklogThrottlingSingleCycle(unittest.TestCase):
    """Runs run_transfer_thread for exactly one cycle via a patched event_object."""

    def test_second_cycle_throttles_on_unresolved_backlog(self):
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.return_value = ['rule-1']
        rucio_client.get_replication_rule.return_value = {'state': 'REPLICATING'}

        # wait() returns False once (continue to cycle 2), then True (stop).
        waits = iter([False, True])

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client), \
                 patch('clint.event_object.wait', side_effect=lambda _t: next(waits)):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=60,
                    fudge_factor=0, dryrun=False, expiration_delay=100000,
                    max_injection_factor=0, user='tester', skip_validation=True,
                    max_backlog_factor=0.0000001)
            return _ledger_events('SRC', 'DST')

        events = _run_in_tmpdir(run)
        throttled = [e for e in events if e['event'] == 'throttled']
        injected = [e for e in events if e['event'] == 'injected']
        # Cycle 1 injects (empty backlog); cycle 2 sees the still-REPLICATING
        # rule as backlog exceeding the tiny factor, and throttles.
        self.assertEqual(len(injected), 1)
        self.assertEqual(len(throttled), 1)


if __name__ == "__main__":
    unittest.main()
