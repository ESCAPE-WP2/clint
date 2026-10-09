"""Rules that expire and are removed by Rucio must not count as backlog (clint.py and clint_monitor.py)."""
import datetime
import unittest
from unittest.mock import MagicMock, patch

from rucio.common.exception import RuleNotFound

import clint
import clint_monitor as monitor
from tests.test_clint_monitor import _injected, _run_in_tmpdir, _write_ledger
from tests.test_p1 import _dataset, _ledger_events


class TestRefreshBacklog(unittest.TestCase):

    def test_removed_rule_leaves_the_backlog_and_is_recorded(self):
        pending = [{'rule_id': 'r-gone', 'bytes': 100}]

        def run(tmpdir):
            client = MagicMock()
            client.get_replication_rule.side_effect = RuleNotFound('expired')
            result = clint.refresh_backlog(client, pending, 'SRC', 'DST')
            return result, _ledger_events('SRC', 'DST')

        (backlog, still_pending), events = _run_in_tmpdir(run)
        self.assertEqual(backlog, 0)
        self.assertEqual(still_pending, [])
        self.assertEqual([(e['event'], e['rule_id']) for e in events], [('rule_gone', 'r-gone')])

    def test_ok_rule_is_recorded(self):
        pending = [{'rule_id': 'r-ok', 'bytes': 100}]

        def run(tmpdir):
            client = MagicMock()
            client.get_replication_rule.return_value = {'state': 'OK'}
            clint.refresh_backlog(client, pending, 'SRC', 'DST')
            return _ledger_events('SRC', 'DST')

        events = _run_in_tmpdir(run)
        self.assertEqual([(e['event'], e['rule_id'], e['bytes']) for e in events], [('rule_ok', 'r-ok', 100)])

    def test_unknown_error_stays_backlog(self):
        pending = [{'rule_id': 'r-x', 'bytes': 100}]
        client = MagicMock()
        client.get_replication_rule.side_effect = Exception('network down')
        backlog, still_pending = clint.refresh_backlog(client, pending)
        self.assertEqual(backlog, 100)
        self.assertEqual(still_pending, pending)

    def test_thread_records_rule_ok_with_the_throttle_off(self):
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        client = MagicMock()
        client.add_replication_rule.return_value = ['rule-1']
        client.get_replication_rule.return_value = {'state': 'OK'}
        waits = iter([False, True])

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=client), \
                 patch('clint.event_object.wait', side_effect=lambda _t: next(waits)):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                    storage_data, injection_interval=1, rule_lifetime=60,
                    fudge_factor=0, dryrun=False, expiration_delay=100000,
                    max_injection_factor=0, user='tester', skip_validation=True,
                    max_backlog_factor=0)
            return _ledger_events('SRC', 'DST')

        events = _run_in_tmpdir(run)
        self.assertEqual([e['rule_id'] for e in events if e['event'] == 'rule_ok'], ['rule-1'])


class TestScorecardWithRemovedRules(unittest.TestCase):

    def _scorecard(self, ledger_extra, query_error):
        now = datetime.datetime.now(datetime.timezone.utc)
        ts = now.isoformat()

        def run(tmpdir):
            _write_ledger('SRC', 'DST', [_injected('rule-a', 'scope:d1', 1000, ts)] + ledger_extra(ts))
            client = MagicMock()
            client.get_replication_rule.side_effect = query_error
            client.get_rse_usage.return_value = []
            card = monitor.build_scorecard(client, 'SRC', 'DST', mbps=10, window_seconds=1200, now=now)
            return card, client

        return _run_in_tmpdir(run)

    def test_removed_rule_seen_ok_counts_as_completed_without_a_query(self):
        card, client = self._scorecard(lambda ts: [{'ts': ts, 'event': 'rule_ok', 'rule_id': 'rule-a', 'bytes': 1000}],
                                       RuleNotFound('expired'))
        self.assertEqual(card['backlog_bytes'], 0)
        self.assertEqual(card['stuck_rule_ids'], [])
        self.assertEqual(card['expired_unconfirmed_bytes'], 0)
        self.assertGreater(card['achieved_mbps'], 0)
        client.get_replication_rule.assert_not_called()

    def test_removed_rule_never_seen_ok_is_neither_backlog_nor_complete(self):
        card, _ = self._scorecard(lambda ts: [], RuleNotFound('expired'))
        self.assertEqual(card['backlog_bytes'], 0)
        self.assertEqual(card['stuck_rule_ids'], [])
        self.assertEqual(card['expired_unconfirmed_bytes'], 1000)
        self.assertEqual(card['achieved_mbps'], 0)


if __name__ == "__main__":
    unittest.main()
