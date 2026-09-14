import datetime
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

import clint
import clint_monitor as monitor


def _run_in_tmpdir(fn):
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            return fn(tmpdir)
        finally:
            os.chdir(old_cwd)


def _write_ledger(src, dst, records):
    with open(clint.ledger_filename(src, dst), 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')


def _injected(rule_id, did, size, ts):
    return {'ts': ts, 'event': 'injected', 'rule_id': rule_id, 'did': did, 'bytes': size,
            'lifetime': 60, 'account': 'tester'}


def _rse_usage(free, total):
    return [{'source': 'storage', 'free': free, 'total': total, 'used': total - free, 'files': 1}]


class TestBuildScorecard(unittest.TestCase):

    def test_scorecard_arithmetic_matches_hand_computed_fixture(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        ts = now.isoformat()

        def run(tmpdir):
            _write_ledger('SRC', 'DST', [
                _injected('rule-ok', 'scope:d1', 1000, ts),
                _injected('rule-repl', 'scope:d2', 2000, ts),
                _injected('dry-run', 'scope:d3', 3000, ts),
            ])
            rucio_client = MagicMock()
            rucio_client.get_replication_rule.side_effect = lambda rid: {
                'rule-ok': {'state': 'OK'},
                'rule-repl': {'state': 'REPLICATING'},
            }[rid]
            rucio_client.get_rse_usage.return_value = _rse_usage(free=500, total=1000)
            return monitor.build_scorecard(rucio_client, 'SRC', 'DST', mbps=10, window_seconds=1200, now=now)

        scorecard = _run_in_tmpdir(run)

        self.assertEqual(scorecard['injected_bytes'], 6000)
        # OK rule (1000) + dry-run (3000, always treated as complete) = 4000
        self.assertEqual(scorecard['completed_bytes'], 4000)
        # Only the still-REPLICATING rule counts as backlog.
        self.assertEqual(scorecard['backlog_bytes'], 2000)
        self.assertEqual(scorecard['stuck_rule_ids'], [])
        self.assertAlmostEqual(scorecard['achieved_mbps'], 4000 / 1200 / 125000)
        self.assertEqual(scorecard['dest_usage_fraction'], 0.5)
        self.assertEqual(scorecard['target_bytes'], int(125000 * 10) * 1200)

    def test_missing_ledger_file_is_not_an_error(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.return_value = _rse_usage(free=100, total=100)

        def run(tmpdir):
            return monitor.build_scorecard(rucio_client, 'SRC', 'DST', mbps=10, window_seconds=1200,
                                            now=datetime.datetime.now(datetime.timezone.utc))

        scorecard = _run_in_tmpdir(run)
        self.assertEqual(scorecard['injected_bytes'], 0)
        self.assertEqual(scorecard['completed_bytes'], 0)

    def test_unqueryable_rule_is_treated_as_stuck_backlog(self):
        now = datetime.datetime.now(datetime.timezone.utc)

        def run(tmpdir):
            _write_ledger('SRC', 'DST', [_injected('rule-x', 'scope:d1', 500, now.isoformat())])
            rucio_client = MagicMock()
            rucio_client.get_replication_rule.side_effect = Exception('rule not found')
            rucio_client.get_rse_usage.return_value = []
            return monitor.build_scorecard(rucio_client, 'SRC', 'DST', mbps=10, window_seconds=1200, now=now)

        scorecard = _run_in_tmpdir(run)
        self.assertEqual(scorecard['backlog_bytes'], 500)
        self.assertEqual(scorecard['stuck_rule_ids'], ['rule-x'])
        self.assertIsNone(scorecard['dest_usage_fraction'])


class TestAnomalyFlag(unittest.TestCase):

    LINKS = [{'src': 'SRC', 'dst': 'DST', 'mbps': 10}]

    def _scorecard(self, backlog_bytes=0, target_bytes=1000, stuck=None, achieved_mbps=10, usage=0.5):
        return {'src': 'SRC', 'dst': 'DST', 'link_mbps': 10, 'target_bytes': target_bytes,
                'backlog_bytes': backlog_bytes, 'stuck_rule_ids': stuck or [],
                'achieved_mbps': achieved_mbps, 'dest_usage_fraction': usage}

    def test_no_anomaly_when_everything_healthy(self):
        scorecards = [self._scorecard() for _ in range(4)]
        self.assertEqual(monitor.check_anomalies(scorecards, self.LINKS), [])

    def test_flags_sustained_backlog(self):
        scorecards = [self._scorecard(backlog_bytes=3000, target_bytes=1000) for _ in range(3)]
        reasons = monitor.check_anomalies(scorecards, self.LINKS)
        self.assertTrue(any('backlog' in r for r in reasons))

    def test_single_backlogged_cycle_does_not_flag(self):
        scorecards = [self._scorecard()] * 2 + [self._scorecard(backlog_bytes=3000, target_bytes=1000)]
        self.assertEqual(monitor.check_anomalies(scorecards, self.LINKS), [])

    def test_flags_stuck_rules(self):
        scorecards = [self._scorecard(stuck=['rule-1'])]
        reasons = monitor.check_anomalies(scorecards, self.LINKS)
        self.assertTrue(any('stuck' in r for r in reasons))

    def test_flags_low_achieved_ratio_over_window(self):
        scorecards = [self._scorecard(achieved_mbps=1) for _ in range(4)]  # 1/10 = 10% << 60%
        reasons = monitor.check_anomalies(scorecards, self.LINKS)
        self.assertTrue(any('achieved' in r for r in reasons))

    def test_flags_high_destination_usage(self):
        scorecards = [self._scorecard(usage=0.95)]
        reasons = monitor.check_anomalies(scorecards, self.LINKS)
        self.assertTrue(any('used' in r for r in reasons))

    def test_flag_file_written_then_cleared(self):
        def run(tmpdir):
            monitor.update_anomaly_flag('ANOMALY.flag', ['SRC->DST: something wrong'])
            written = os.path.exists('ANOMALY.flag')
            monitor.update_anomaly_flag('ANOMALY.flag', [])
            cleared = not os.path.exists('ANOMALY.flag')
            return written, cleared

        written, cleared = _run_in_tmpdir(run)
        self.assertTrue(written)
        self.assertTrue(cleared)


class TestMainEndToEnd(unittest.TestCase):

    def test_main_never_crashes_when_rucio_client_setup_fails(self):
        # rucio.client.Client() raises ConfigNotFound-style errors when no
        # rucio.cfg is present, exactly as observed in this sandbox -- main()
        # must still exit 0.
        def run(tmpdir):
            with open('config.csv', 'w') as f:
                f.write('SRC,DST,10\n')
            return monitor.main(['--config', 'config.csv', '--out', 'scorecards.jsonl'])

        exit_code = _run_in_tmpdir(run)
        self.assertEqual(exit_code, 0)

    def test_main_skips_one_broken_link_and_still_exits_zero(self):
        rucio_client = MagicMock()
        rucio_client.get_rse_usage.side_effect = Exception('boom')

        def run(tmpdir):
            with open('config.csv', 'w') as f:
                f.write('SRC,DST,10\n')
            from unittest.mock import patch
            with patch('clint_monitor.rucio.client.Client', return_value=rucio_client):
                code = monitor.main(['--config', 'config.csv', '--out', 'scorecards.jsonl'])
            return code, os.path.exists('scorecards.jsonl')

        code, out_exists = _run_in_tmpdir(run)
        self.assertEqual(code, 0)
        # The link's own get_rse_usage raising is caught inside
        # _rse_usage_fraction, so the scorecard is still written.
        self.assertTrue(out_exists)


if __name__ == "__main__":
    unittest.main()
