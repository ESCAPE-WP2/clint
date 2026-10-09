import datetime
import json
import os
import tempfile
import unittest

import clint_plot as plot

T0 = datetime.datetime(2026, 10, 9, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _at(seconds):
    return (T0 + datetime.timedelta(seconds=seconds)).isoformat()


def _write(folder, src, dst, events):
    with open(os.path.join(folder, f'clint_ledger_{src}_to_{dst}.jsonl'), 'w') as f:
        for e in events:
            f.write(json.dumps(e) + '\n')


def _events():
    return [
        {'ts': _at(0), 'event': 'injected', 'rule_id': 'r1', 'bytes': 100, 'lifetime': 900},
        {'ts': _at(0), 'event': 'cycle', 'wanted_bytes': 90},
        {'ts': _at(120), 'event': 'injected', 'rule_id': 'r2', 'bytes': 100, 'lifetime': 900},
        {'ts': _at(120), 'event': 'cycle', 'wanted_bytes': 90},
        {'ts': _at(120), 'event': 'skipped_bad', 'did': 'x'},
        {'ts': _at(240), 'event': 'rule_ok', 'rule_id': 'r1', 'bytes': 100},
        {'ts': _at(240), 'event': 'injected', 'rule_id': 'dry-run', 'bytes': 999},
        {'ts': _at(240), 'event': 'cycle', 'wanted_bytes': 90},
        {'ts': _at(360), 'event': 'rule_gone', 'rule_id': 'r2', 'bytes': 100},
    ]


class TestSeries(unittest.TestCase):

    def setUp(self):
        self.events = [dict(e, _t=plot._ts(e['ts'])) for e in _events()]

    def test_curves_ignore_dry_run_and_count_gone_as_not_in_flight(self):
        s = plot.link_series(self.events, T0)
        self.assertEqual(s['injected'][1][-1], 200)
        self.assertEqual(s['completed'][1][-1], 100)
        self.assertEqual(s['wanted'][1][-1], 270)
        self.assertEqual(s['in_flight'][1][-1], 0)
        self.assertEqual(max(s['in_flight'][1]), 200)
        self.assertEqual(s['injected'][0][:3], [0.0, 0.0, 2.0])

    def test_latency_is_time_from_rule_to_first_ok(self):
        self.assertEqual(plot.latencies(self.events), [240.0])

    def test_counts_and_cycle_length(self):
        counts = plot.event_counts(self.events)
        self.assertEqual((counts['rule_ok'], counts['rule_gone'], counts['skipped_bad']), (1, 1, 1))
        self.assertEqual(plot.cycle_seconds(self.events), 120.0)

    def test_summary_rates(self):
        summary = plot.run_summary({('A', 'B'): self.events})
        # 3 cycles: from 0 to 240 s plus one cycle of 120 s = 360 s
        self.assertAlmostEqual(summary['achieved_mbps'], 100 / 360 / 125000)
        self.assertAlmostEqual(summary['wanted_mbps'], 270 / 360 / 125000)


class TestFiles(unittest.TestCase):

    def test_load_run_reads_every_ledger_and_skips_bad_lines(self):
        with tempfile.TemporaryDirectory() as d:
            _write(d, 'SRC_A', 'DST_B', _events())
            with open(os.path.join(d, 'clint_ledger_SRC_A_to_DST_C.jsonl'), 'w') as f:
                f.write('not json\n' + json.dumps(_events()[0]) + '\n')
            links = plot.load_run(d)
        self.assertEqual(sorted(links), [('SRC_A', 'DST_B'), ('SRC_A', 'DST_C')])
        self.assertEqual(len(links[('SRC_A', 'DST_C')]), 1)

    def test_empty_folder_gives_no_links(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(plot.load_run(d), {})

    def test_main_draws_one_figure_per_run_and_a_comparison(self):
        try:
            import matplotlib  # noqa: F401
        except ImportError:
            self.skipTest('matplotlib is not installed')
        with tempfile.TemporaryDirectory() as d:
            for name in ('one', 'two'):
                os.makedirs(os.path.join(d, name))
                _write(os.path.join(d, name), 'SRC', 'DST', _events())
            out = os.path.join(d, 'out')
            code = plot.main([os.path.join(d, 'one'), os.path.join(d, 'two'), '--out', out])
            files = sorted(os.listdir(out))
            sizes = [os.path.getsize(os.path.join(out, f)) for f in files]
        self.assertEqual(code, 0)
        self.assertEqual(files, ['clint_compare.png', 'clint_run_one.png', 'clint_run_two.png'])
        self.assertTrue(all(s > 5000 for s in sizes))

    def test_main_reports_a_folder_without_ledgers(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(plot.main([d, '--out', d]), 1)


if __name__ == "__main__":
    unittest.main()
