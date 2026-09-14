import argparse
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


def _ledger_events(src, dst):
    with open(clint.ledger_filename(src, dst)) as f:
        return [json.loads(line) for line in f]


def _heartbeat():
    with open('clint_heartbeat.json') as f:
        return json.load(f)


def _base_run_args(**overrides):
    defaults = dict(
        config='config.csv', storage_dir='.', injection_interval=900,
        max_injection_factor=0.2, rule_lifetime=7200, big_first=False,
        fudge_factor=0, expiration_delay=300, dryrun=True,
        skip_validation=False, single_thread=False,
        duration=None, total_volume=None, max_backlog_factor=3.0,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestIsRseExpression(unittest.TestCase):

    def test_plain_rse_name_is_not_an_expression(self):
        self.assertFalse(clint.is_rse_expression('CERN-EOS-DISK'))

    def test_boolean_expression_is_detected(self):
        self.assertTrue(clint.is_rse_expression('tier=2&country=it'))


class TestStartupValidation(unittest.TestCase):

    def test_fails_fast_on_typo_src(self):
        def run(tmpdir):
            with open('config.csv', 'w') as f:
                f.write('MISSING,DST,10\n')
            with open('REAL.lst', 'w') as f:
                f.write('scope:d1,100\n')
            rucio_client = MagicMock()
            rucio_client.whoami.return_value = {'account': 'tester'}
            with patch('clint.rucio.client.Client', return_value=rucio_client):
                with self.assertRaises(SystemExit) as cm:
                    clint.run(_base_run_args())
            return cm.exception.code

        self.assertEqual(_run_in_tmpdir(run), 1)

    def test_fails_fast_on_rse_expression_destination(self):
        def run(tmpdir):
            with open('config.csv', 'w') as f:
                f.write('SRC,tier=2&country=it,10\n')
            with open('SRC.lst', 'w') as f:
                f.write('scope:d1,100\n')
            rucio_client = MagicMock()
            rucio_client.whoami.return_value = {'account': 'tester'}
            with patch('clint.rucio.client.Client', return_value=rucio_client):
                with self.assertRaises(SystemExit) as cm:
                    clint.run(_base_run_args())
            return cm.exception.code

        self.assertEqual(_run_in_tmpdir(run), 1)


class TestHasRoomForCycle(unittest.TestCase):

    def test_blocks_when_free_space_below_needed(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = [{'source': 'storage', 'free': 100, 'total': 1000}]
        rucio_client.get_local_account_usage.return_value = []
        self.assertFalse(clint.has_room_for_cycle(rucio_client, 'acct', 'DST', needed_bytes=500))

    def test_allows_when_space_and_quota_sufficient(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = [{'source': 'storage', 'free': 10000, 'total': 20000}]
        rucio_client.get_local_account_usage.return_value = [
            {'rse': 'DST', 'bytes_limit': 5000, 'bytes_remaining': 4000}]
        self.assertTrue(clint.has_room_for_cycle(rucio_client, 'acct', 'DST', needed_bytes=100))

    def test_blocks_when_quota_insufficient(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = [{'source': 'storage', 'free': 10000, 'total': 20000}]
        rucio_client.get_local_account_usage.return_value = [
            {'rse': 'DST', 'bytes_limit': 5000, 'bytes_remaining': 50}]
        self.assertFalse(clint.has_room_for_cycle(rucio_client, 'acct', 'DST', needed_bytes=100))

    def test_query_failure_allows_by_default(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.side_effect = Exception('boom')
        rucio_client.get_local_account_usage.side_effect = Exception('boom')
        self.assertTrue(clint.has_room_for_cycle(rucio_client, 'acct', 'DST', needed_bytes=100))


class TestSpaceGateInCycle(unittest.TestCase):

    def test_cycle_is_blocked_and_logged_when_destination_full(self):
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = [{'source': 'storage', 'free': 0, 'total': 1000}]
        rucio_client.get_local_account_usage.return_value = []

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client), \
                 patch('clint.event_object.wait', return_value=True):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=60,
                    fudge_factor=0, dryrun=False, expiration_delay=0,
                    max_injection_factor=0, user='tester', skip_validation=True,
                    max_backlog_factor=0)
            return _ledger_events('SRC', 'DST'), _heartbeat()

        events, heartbeat = _run_in_tmpdir(run)
        blocked = [e for e in events if e['event'] == 'blocked_no_space']
        self.assertEqual(len(blocked), 1)
        rucio_client.add_replication_rule.assert_not_called()
        self.assertEqual(heartbeat['SRC->DST']['state'], 'blocked')


class TestCircuitBreaker(unittest.TestCase):

    def test_quarantines_after_consecutive_failed_cycles(self):
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.side_effect = Exception('rucio down')
        rucio_client.get_rse_usage.return_value = []
        rucio_client.get_local_account_usage.return_value = []

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client), \
                 patch('clint.event_object.wait', return_value=False):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=60,
                    fudge_factor=0, dryrun=False, expiration_delay=100000,
                    max_injection_factor=1.0, user='tester', skip_validation=True,
                    max_backlog_factor=0)
            return _ledger_events('SRC', 'DST')

        events = _run_in_tmpdir(run)
        disabled = [e for e in events if e['event'] == 'link_disabled']
        self.assertEqual(len(disabled), 1)
        self.assertEqual(disabled[0]['consecutive_failed_cycles'], clint.CIRCUIT_BREAKER_THRESHOLD)
        cycles = [e for e in events if e['event'] == 'cycle']
        self.assertEqual(len(cycles), clint.CIRCUIT_BREAKER_THRESHOLD)


class TestGuardedThread(unittest.TestCase):

    def test_malformed_did_is_caught_and_recorded_as_died(self):
        storage_data = {'SRC': [{'did': 'baddid_no_colon', 'bytes': 100,
                                  'injected_at': datetime.datetime.utcfromtimestamp(0)}]}
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = []
        rucio_client.get_local_account_usage.return_value = []

        def run(tmpdir):
            outcomes = []
            with patch('clint.rucio.client.Client', return_value=rucio_client):
                clint._run_transfer_thread_guarded(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100}, outcomes,
                    storage_data, 1, 60, 0, False, 0, 1.0, 'tester', True,
                    max_backlog_factor=0)
            return outcomes, _ledger_events('SRC', 'DST')

        outcomes, events = _run_in_tmpdir(run)
        self.assertEqual(outcomes, ['died'])
        died = [e for e in events if e['event'] == 'link_died']
        self.assertEqual(len(died), 1)

    def test_run_exits_nonzero_when_every_link_dies(self):
        def run(tmpdir):
            with open('config.csv', 'w') as f:
                f.write('SRC,DST,10\n')
            with open('SRC.lst', 'w') as f:
                f.write('baddid_no_colon,100\n')
            rucio_client = MagicMock()
            rucio_client.whoami.return_value = {'account': 'tester'}
            rucio_client.get_rse_usage.return_value = []
            rucio_client.get_local_account_usage.return_value = []
            with patch('clint.rucio.client.Client', return_value=rucio_client):
                with self.assertRaises(SystemExit) as cm:
                    clint.run(_base_run_args(single_thread=False, skip_validation=True, dryrun=False))
            return cm.exception.code

        self.assertEqual(_run_in_tmpdir(run), 1)


class TestDeadlineSchedulingAndTelemetry(unittest.TestCase):

    def test_cycle_event_records_duration_and_heartbeat_has_pool_telemetry(self):
        storage_data = {'SRC': [_dataset('scope:d1', 1000), _dataset('scope:d2', 1000)]}
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.return_value = ['rule-x']
        rucio_client.get_rse_usage.return_value = []
        rucio_client.get_local_account_usage.return_value = []

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=rucio_client), \
                 patch('clint.event_object.wait', return_value=True):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=7200,
                    fudge_factor=0, dryrun=False, expiration_delay=300,
                    max_injection_factor=1.0, user='tester', skip_validation=True,
                    max_backlog_factor=0)
            return _ledger_events('SRC', 'DST'), _heartbeat()

        events, heartbeat = _run_in_tmpdir(run)
        cycle_events = [e for e in events if e['event'] == 'cycle']
        self.assertEqual(len(cycle_events), 1)
        self.assertIn('cycle_duration_s', cycle_events[0])
        self.assertGreaterEqual(cycle_events[0]['cycle_duration_s'], 0)

        entry = heartbeat['SRC->DST']
        self.assertEqual(entry['state'], 'running')
        self.assertEqual(entry['eligible_pool_bytes'], 2000)
        expected_sustainable = clint.sustainable_rate_mbps(2000, 7200, 300)
        self.assertAlmostEqual(entry['sustainable_mbps'], expected_sustainable)
        self.assertIn('cycle_duration_s', entry)


if __name__ == "__main__":
    unittest.main()
