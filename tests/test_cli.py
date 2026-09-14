import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import cli


def _write_fixture(tmpdir, mbps):
    config_path = os.path.join(tmpdir, 'config.csv')
    with open(config_path, 'w') as f:
        f.write(f'RSE1,RSE2,{mbps}\n')

    with open(os.path.join(tmpdir, 'RSE1.lst'), 'w') as f:
        # 1 dataset, 100GB. With the default rule-lifetime=7200 and
        # expiration-delay=300, this pool sustains ~13.3 Mbps.
        f.write('test_scope:test_dataset,100000000000\n')

    return config_path


def _mock_rucio_client():
    client = MagicMock()
    client.whoami.return_value = {'account': 'testuser'}
    client.get_rse.return_value = {'rse': 'RSE2'}
    client.get_rse_usage.return_value = [
        {'source': 'storage', 'free': 500_000_000_000, 'total': 1_000_000_000_000},
    ]
    client.get_local_account_usage.return_value = [
        {'rse': 'RSE2', 'bytes': 0, 'bytes_limit': 0, 'bytes_remaining': 0},
    ]
    return client


class TestCmdPreflight(unittest.TestCase):

    def test_feasible_config_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = _write_fixture(tmpdir, mbps=10)
            with patch('cli.rucio.client.Client', return_value=_mock_rucio_client()):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = cli.main(['preflight', '--config', config_path, '--storage-dir', tmpdir, '--json'])
        self.assertEqual(code, 0)
        self.assertIn('"feasible": true', out.getvalue())

    def test_infeasible_config_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = _write_fixture(tmpdir, mbps=10000)
            with patch('cli.rucio.client.Client', return_value=_mock_rucio_client()):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = cli.main(['preflight', '--config', config_path, '--storage-dir', tmpdir, '--json'])
        self.assertEqual(code, 1)
        self.assertIn('"feasible": false', out.getvalue())

    def test_infeasible_config_with_force_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = _write_fixture(tmpdir, mbps=10000)
            with patch('cli.rucio.client.Client', return_value=_mock_rucio_client()):
                code = cli.main(['preflight', '--config', config_path, '--storage-dir', tmpdir, '--force'])
        self.assertEqual(code, 0)

    def test_missing_destination_rse_is_infeasible(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = _write_fixture(tmpdir, mbps=10)
            client = _mock_rucio_client()
            client.get_rse.side_effect = cli.RSENotFound('RSE2 not found')
            with patch('cli.rucio.client.Client', return_value=client):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = cli.main(['preflight', '--config', config_path, '--storage-dir', tmpdir, '--json'])
        self.assertEqual(code, 1)
        self.assertIn('"feasible": false', out.getvalue())


if __name__ == "__main__":
    unittest.main()
