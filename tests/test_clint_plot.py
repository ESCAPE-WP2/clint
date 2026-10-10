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

    def test_empty_panels_and_many_runs_do_not_crash(self):
        try:
            import matplotlib  # noqa: F401
        except ImportError:
            self.skipTest('matplotlib is not installed')
        dry = [{'ts': _at(0), 'event': 'injected', 'rule_id': 'dry-run', 'bytes': 5, 'lifetime': 900},
               {'ts': _at(0), 'event': 'cycle', 'wanted_bytes': 5}]
        with tempfile.TemporaryDirectory() as d:
            folders = []
            for i in range(14):
                folder = os.path.join(d, f's{i:02d}')
                os.makedirs(folder)
                _write(folder, 'SRC', 'DST', dry if i == 0 else _events())
                folders.append(folder)
            out = os.path.join(d, 'out')
            self.assertEqual(plot.main(folders + ['--out', out]), 0)
            self.assertIn('clint_compare.png', os.listdir(out))
            self.assertIn('clint_run_s00.png', os.listdir(out))

    def test_names_drop_the_common_source(self):
        names, origin = plot._link_names({('S', 'A'): [], ('S', 'B'): []})
        self.assertEqual((names[('S', 'A')], origin), ('A', 'from S'))
        names, origin = plot._link_names({('S', 'A'): [], ('T', 'B'): []})
        self.assertEqual((names[('T', 'B')], origin), ('T→B', ''))

    def test_main_reports_a_folder_without_ledgers(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(plot.main([d, '--out', d]), 1)


if __name__ == "__main__":
    unittest.main()


class TestClassPlots(unittest.TestCase):

    def _rows(self, folder):
        import clint_export as ex
        links = {('S', 'D'): [dict(e, _t=plot._ts(e['ts'])) for e in _events()]}
        rules = ex.rule_rows(links, 'ann')
        cycles = ex.cycle_rows(links, 'ann')
        rules[0].update({'closed_at': _at(300), 'request_created_at': _at(0), 'submitted_at': _at(20),
                         'started_at': _at(22), 'transferred_at': _at(23), 'conveyor_s': 20.0})
        ex.write_csv(os.path.join(folder, 'clint_rules_ann.csv'), ex.RULE_FIELDS, rules)
        ex.write_csv(os.path.join(folder, 'clint_cycles_ann.csv'), ex.CYCLE_FIELDS, cycles)

    def test_export_rows_and_status(self):
        import clint_export as ex
        links = {('S', 'D'): [dict(e, _t=plot._ts(e['ts'])) for e in _events()]}
        rules = ex.rule_rows(links, 'ann')
        self.assertEqual([(r['rule_id'], r['status']) for r in rules], [('r1', 'ok_seen'), ('r2', 'expired_unseen')])
        self.assertEqual(rules[0]['seen_latency_s'], 240.0)
        cycles = ex.cycle_rows(links, 'ann')
        self.assertEqual([c['skipped_bad'] for c in cycles], [0, 0, 1])  # a skip belongs to the next cycle event (CLINT logs it at the end)

    def test_csv_round_trip_and_achievement(self):
        with tempfile.TemporaryDirectory() as d:
            self._rows(d)
            rules, cycles = plot.load_csv_class(d)
        ratios = plot.achievement(rules, cycles)
        self.assertAlmostEqual(ratios[('ann', 'D')], 100 / 270)  # r1 done (100 B) of 270 B wanted
        self.assertEqual(plot.ratio_color(0.95), 'green')
        self.assertEqual(plot.ratio_color(0.7), 'gold')
        self.assertEqual(plot.ratio_color(0.5), 'orange')
        self.assertEqual(plot.ratio_color(0.49), 'red')

    def test_wanted_is_spread_over_the_cycle_length(self):
        with tempfile.TemporaryDirectory() as d:
            self._rows(d)
            rules, cycles = plot.load_csv_class(d)
        t0, edges, done, wanted = plot.class_series(rules, cycles, bin_s=60)
        self.assertAlmostEqual(sum(wanted), 270)           # nothing lost
        self.assertAlmostEqual(wanted[0], wanted[1])       # 90 B over 120 s: 45 B in each 60 s bin
        self.assertEqual(plot.stage_medians(rules)['ann']['conveyor_s'], 20.0)

    def test_class_figure_is_drawn(self):
        try:
            import matplotlib  # noqa: F401
        except ImportError:
            self.skipTest('matplotlib is not installed')
        with tempfile.TemporaryDirectory() as d:
            self._rows(d)
            out = os.path.join(d, 'out')
            self.assertEqual(plot.main(['--csv', d, '--out', out]), 0)
            self.assertGreater(os.path.getsize(os.path.join(out, 'clint_class.png')), 5000)


class TestExactTarget(unittest.TestCase):

    def _cycles(self, rows):
        out = []
        for t, mbps, interval in rows:
            out.append({'participant': 'ann', 'src': 'S', 'dst': 'D', 'wanted_bytes': mbps * 125000 * interval,
                        '_t': plot._ts(_at(t)), '_mbps': float(mbps), '_interval': float(interval)})
        return out

    def test_a_run_that_stops_early_does_not_count_the_full_interval(self):
        # phase 1: 6 Mbps, cycle 120 s, cycles at 0 and 120, then phase 2 starts at 180 (phase 1 stopped after 60 s)
        cycles = self._cycles([(0, 6, 120), (120, 6, 120), (180, 8, 100)])
        wanted = plot._spread_wanted(cycles, plot._ts(_at(0)), 60, 5)
        self.assertAlmostEqual(sum(wanted), 6 * 125000 * (120 + 60) + 8 * 125000 * 100)
        self.assertAlmostEqual(wanted[1] / 60 / 125000, 6)   # steady 6 Mbps in the second minute
        self.assertAlmostEqual(wanted[4] / 60 / 125000, 8 * 40 / 60)  # phase 2: 100 s from t=180 to 280

    def test_cycle_event_carries_the_link_rate_and_interval(self):
        import clint
        from unittest.mock import MagicMock, patch
        client = MagicMock()
        client.add_replication_rule.return_value = ['rule-1']
        client.get_replication_rule.return_value = {'state': 'REPLICATING'}
        waits = iter([True])

        def run(tmpdir):
            with patch('clint.rucio.client.Client', return_value=client), \
                 patch('clint.event_object.wait', side_effect=lambda _t: next(waits)):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 6}, {'SRC': [{'did': 's:d', 'bytes': 100, 'injected_at': datetime.datetime.utcfromtimestamp(0)}]},
                    injection_interval=120, rule_lifetime=60, fudge_factor=0, dryrun=False, expiration_delay=1,
                    max_injection_factor=1000, user='t', skip_validation=True, max_backlog_factor=0)
            return plot.read_ledger(clint.ledger_filename('SRC', 'DST'))

        events = _run_in_tmpdir(run)
        cycle = [e for e in events if e['event'] == 'cycle'][0]
        self.assertEqual((cycle['link_mbps'], cycle['interval_s']), (6, 120))


def _run_in_tmpdir(fn):
    old = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        try:
            return fn(d)
        finally:
            os.chdir(old)


class TestAchievementLag(unittest.TestCase):

    def test_a_rule_that_finishes_after_the_run_does_not_count(self):
        with tempfile.TemporaryDirectory() as d:
            import clint_export as ex
            links = {('S', 'D'): [dict(e, _t=plot._ts(e['ts'])) for e in _events()]}
            rules = ex.rule_rows(links, 'ann')
            rules[0]['closed_at'] = _at(1000)   # the run ends at 240 + 120 = 360 s
            ex.write_csv(os.path.join(d, 'clint_rules_ann.csv'), ex.RULE_FIELDS, rules)
            ex.write_csv(os.path.join(d, 'clint_cycles_ann.csv'), ex.CYCLE_FIELDS, ex.cycle_rows(links, 'ann'))
            loaded_rules, cycles = plot.load_csv_class(d)
        # r1: ok_seen at 240 but closed_at at 1000: the closed time wins, so it is late
        self.assertEqual(plot.achievement(loaded_rules, cycles)[('ann', 'D')], 0.0)
