import datetime
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import clint


def _dataset(did, size):
    return {'did': did, 'bytes': size, 'injected_at': datetime.datetime.utcfromtimestamp(0)}


def _ledger_events(src, dst):
    with open(clint.ledger_filename(src, dst)) as f:
        return [json.loads(line) for line in f]


class TestCrossLinkFreshnessIsolation(unittest.TestCase):
    """
    Before the fix, two links sharing one source mutated the same dataset
    dicts: the first link marking a dataset 'injected_at' made it look
    stale to the second link too, starving it. run_transfer_thread now
    deep-copies its source's dataset list at thread start, so two
    sequential (or concurrent) calls against the same storage_data must
    each see the full pool as fresh.
    """

    def test_two_links_sharing_a_source_both_get_full_budget(self):
        storage_data = {
            'SRC': [_dataset('scope:d1', 100), _dataset('scope:d2', 100)],
        }
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.return_value = ['rule-x']

        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with patch('clint.rucio.client.Client', return_value=rucio_client), \
                     patch('clint.event_object.wait', return_value=True):
                    for dst in ('DST1', 'DST2'):
                        clint.run_transfer_thread(
                            {'src': 'SRC', 'dst': dst, 'mbps': 100},
                            storage_data, injection_interval=1, rule_lifetime=60,
                            fudge_factor=0, dryrun=False, expiration_delay=0,
                            max_injection_factor=1.0, user='tester', skip_validation=True)

                dst1_injected = [e for e in _ledger_events('SRC', 'DST1') if e['event'] == 'injected']
                dst2_injected = [e for e in _ledger_events('SRC', 'DST2') if e['event'] == 'injected']
            finally:
                os.chdir(old_cwd)

        self.assertEqual(len(dst1_injected), 2)
        self.assertEqual(len(dst2_injected), 2)

    def test_ledger_records_rule_id_for_every_injection(self):
        storage_data = {'SRC': [_dataset('scope:d1', 100)]}
        rucio_client = MagicMock()
        rucio_client.add_replication_rule.return_value = ['rule-abc']

        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with patch('clint.rucio.client.Client', return_value=rucio_client), \
                     patch('clint.event_object.wait', return_value=True):
                    clint.run_transfer_thread(
                        {'src': 'SRC', 'dst': 'DST', 'mbps': 100},
                        storage_data, injection_interval=1, rule_lifetime=60,
                        fudge_factor=0, dryrun=False, expiration_delay=0,
                        max_injection_factor=1.0, user='tester', skip_validation=True)
                events = _ledger_events('SRC', 'DST')
            finally:
                os.chdir(old_cwd)

        injected = [e for e in events if e['event'] == 'injected']
        cycle = [e for e in events if e['event'] == 'cycle']
        self.assertEqual(injected[0]['rule_id'], 'rule-abc')
        self.assertEqual(injected[0]['did'], 'scope:d1')
        self.assertEqual(len(cycle), 1)


class TestCheckpointIsAtomic(unittest.TestCase):

    def test_dump_used_datasets_leaves_no_tmp_file_behind(self):
        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                clint.dump_used_datasets({'scope:d1': datetime.datetime.utcnow()}, 'SRC', 'DST')
                self.assertTrue(os.path.exists('used_datasets_dump_SRC_to_DST.csv'))
                self.assertFalse(os.path.exists('used_datasets_dump_SRC_to_DST.csv.tmp'))
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
