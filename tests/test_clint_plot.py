import datetime
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import clint_export as ex
import clint_plot as plot

T0 = datetime.datetime(2026, 10, 9, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _at(seconds):
    return (T0 + datetime.timedelta(seconds=seconds)).isoformat()


def _write(folder, src, dst, events):
    with open(os.path.join(folder, f'clint_ledger_{src}_to_{dst}.jsonl'), 'w') as f:
        for e in events:
            f.write(json.dumps(e) + '\n')


def _events(with_rate=False):
    cycle = {'link_mbps': 6, 'interval_s': 120} if with_rate else {}
    return [
        {'ts': _at(0), 'event': 'injected', 'rule_id': 'r1', 'bytes': 100, 'lifetime': 900},
        dict({'ts': _at(0), 'event': 'cycle', 'wanted_bytes': 90}, **cycle),
        {'ts': _at(120), 'event': 'injected', 'rule_id': 'r2', 'bytes': 100, 'lifetime': 900},
        dict({'ts': _at(120), 'event': 'cycle', 'wanted_bytes': 90}, **cycle),
        {'ts': _at(120), 'event': 'skipped_bad', 'did': 'x'},
        {'ts': _at(240), 'event': 'rule_ok', 'rule_id': 'r1', 'bytes': 100},
        {'ts': _at(240), 'event': 'injected', 'rule_id': 'dry-run', 'bytes': 999},
        dict({'ts': _at(240), 'event': 'cycle', 'wanted_bytes': 90}, **cycle),
        {'ts': _at(360), 'event': 'rule_gone', 'rule_id': 'r2', 'bytes': 100},
    ]


def _links(events=None):
    return {('S', 'D'): [dict(e, _t=plot._ts(e['ts'])) for e in (events or _events())]}


def _run_folder(base, name, events=None):
    folder = os.path.join(base, name)
    os.makedirs(folder)
    _write(folder, 'SRC', 'DST', events or _events())
    return folder


def _has_matplotlib():
    try:
        import matplotlib  # noqa: F401
        return True
    except ImportError:
        return False


class TestReading(unittest.TestCase):

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


class TestExport(unittest.TestCase):

    def test_rule_rows_status_and_dry_run_left_out(self):
        rules = ex.rule_rows(_links(), 'ann')
        self.assertEqual([(r['rule_id'], r['status']) for r in rules], [('r1', 'ok_seen'), ('r2', 'expired_unseen')])
        self.assertEqual(rules[0]['seen_latency_s'], 240.0)

    def test_cycle_rows_count_the_events_before_the_cycle_event(self):
        cycles = ex.cycle_rows(_links(), 'ann')
        self.assertEqual([c['skipped_bad'] for c in cycles], [0, 0, 1])  # CLINT logs a skip before its cycle event

    def test_cycle_rows_carry_rate_and_interval(self):
        cycles = ex.cycle_rows(_links(_events(with_rate=True)), 'ann')
        self.assertEqual((cycles[0]['link_mbps'], cycles[0]['interval_s']), (6, 120))

    def test_cycle_event_is_written_with_rate_and_interval(self):
        import clint
        client = MagicMock()
        client.add_replication_rule.return_value = ['rule-1']
        client.get_replication_rule.return_value = {'state': 'REPLICATING'}
        waits = iter([True])

        def run():
            with patch('clint.rucio.client.Client', return_value=client), \
                 patch('clint.event_object.wait', side_effect=lambda _t: next(waits)):
                clint.run_transfer_thread(
                    {'src': 'SRC', 'dst': 'DST', 'mbps': 6},
                    {'SRC': [{'did': 's:d', 'bytes': 100, 'injected_at': datetime.datetime.utcfromtimestamp(0)}]},
                    injection_interval=120, rule_lifetime=60, fudge_factor=0, dryrun=False, expiration_delay=1,
                    max_injection_factor=1000, user='t', skip_validation=True, max_backlog_factor=0)
            return plot.read_ledger(clint.ledger_filename('SRC', 'DST'))

        old = os.getcwd()
        with tempfile.TemporaryDirectory() as d:
            os.chdir(d)
            try:
                events = run()
            finally:
                os.chdir(old)
        cycle = [e for e in events if e['event'] == 'cycle'][0]
        self.assertEqual((cycle['link_mbps'], cycle['interval_s']), (6, 120))

    def test_stage_times_are_added_from_the_request_records(self):
        rows = ex.rule_rows(_links(), 'ann')
        client = MagicMock()
        client.list_files.return_value = [{'scope': 'f', 'name': 'file.dat'}]
        record = {'rule_id': 'r1', 'state': 'DONE', 'external_host': 'https://fts', 'external_id': 'job', 'retry_count': 0,
                  'created_at': T0, 'submitted_at': T0 + datetime.timedelta(seconds=20),
                  'started_at': T0 + datetime.timedelta(seconds=22), 'transferred_at': T0 + datetime.timedelta(seconds=23),
                  'updated_at': T0 + datetime.timedelta(seconds=200)}
        for r in rows:
            r['did'] = 's:ds'
        with patch.object(ex, 'fetch_requests', return_value=[record]):
            found = ex.add_stage_times(rows, client)
        self.assertEqual(found, 1)  # r2 has another rule id: no stage times
        self.assertEqual((rows[0]['conveyor_s'], rows[0]['fts_queue_s'], rows[0]['transfer_s'], rows[0]['after_transfer_s']),
                         (20.0, 2.0, 1.0, 177.0))
        self.assertEqual(rows[0]['fts_job_id'], 'job')
        self.assertEqual(rows[1].get('conveyor_s', ''), '')


class TestNumbers(unittest.TestCase):

    def _class_rows(self, folder, events=None):
        rules = ex.rule_rows(_links(events), 'ann')
        cycles = ex.cycle_rows(_links(events), 'ann')
        rules[0].update({'closed_at': _at(300), 'request_created_at': _at(0), 'submitted_at': _at(20),
                         'started_at': _at(22), 'transferred_at': _at(23), 'conveyor_s': 20.0})
        ex.write_csv(os.path.join(folder, 'clint_rules_ann.csv'), ex.RULE_FIELDS, rules)
        ex.write_csv(os.path.join(folder, 'clint_cycles_ann.csv'), ex.CYCLE_FIELDS, cycles)

    def test_achievement_and_colour_bands(self):
        with tempfile.TemporaryDirectory() as d:
            self._class_rows(d)
            rules, cycles = plot.load_csv_class(d)
        self.assertAlmostEqual(plot.achievement(rules, cycles)[('ann', 'D')], 100 / 270)  # r1 done of 270 B wanted
        self.assertEqual([plot.ratio_color(x) for x in (0.95, 0.7, 0.5, 0.49)], ['green', 'gold', 'orange', 'red'])

    def test_a_rule_that_finishes_after_the_run_does_not_count(self):
        with tempfile.TemporaryDirectory() as d:
            self._class_rows(d)
            rules, cycles = plot.load_csv_class(d)
        rules[0]['_done'] = plot._ts(_at(1000))  # the run ends at 240 + 120 = 360 s
        self.assertEqual(plot.achievement(rules, cycles)[('ann', 'D')], 0.0)

    def test_wanted_is_spread_over_the_cycle_length(self):
        with tempfile.TemporaryDirectory() as d:
            self._class_rows(d)
            rules, cycles = plot.load_csv_class(d)
        t0, edges, done, wanted = plot.class_series(rules, cycles, bin_s=60)
        self.assertAlmostEqual(sum(wanted), 270)       # nothing lost
        self.assertAlmostEqual(wanted[0], wanted[1])   # 90 B over 120 s: 45 B in each 60 s bin
        self.assertEqual(plot.stage_medians(rules)['ann']['conveyor_s'], 20.0)

    def test_a_run_that_stops_early_does_not_count_the_full_interval(self):
        def cycle(t, mbps, interval):
            return {'participant': 'ann', 'src': 'S', 'dst': 'D', 'wanted_bytes': mbps * 125000 * interval,
                    '_t': plot._ts(_at(t)), '_mbps': float(mbps), '_interval': float(interval)}
        # phase 1: 6 Mbps, cycle 120 s, cycles at 0 and 120; phase 2 starts at 180: phase 1 stopped after 60 s
        wanted = plot._spread_wanted([cycle(0, 6, 120), cycle(120, 6, 120), cycle(180, 8, 100)], plot._ts(_at(0)), 60, 5)
        self.assertAlmostEqual(sum(wanted), 6 * 125000 * (120 + 60) + 8 * 125000 * 100)
        self.assertAlmostEqual(wanted[1] / 60 / 125000, 6)
        self.assertAlmostEqual(wanted[4] / 60 / 125000, 8 * 40 / 60)

    def test_without_stage_times_the_time_until_ok_was_seen_is_used(self):
        rules, _ = plot._typed(ex.rule_rows(_links(), 'ann'), [])
        self.assertEqual(plot.stage_medians(rules), {'ann': {'seen_latency_s': 240.0}})


class TestAlign(unittest.TestCase):

    def test_runs_made_at_different_times_start_together(self):
        late = [dict(e, ts=_at(5000 + (plot._ts(e['ts']) - T0).total_seconds())) for e in _events()]
        rules, cycles = plot.rows_from_ledgers({'early': plot.load_run_from_events(_events()),
                                                'late': plot.load_run_from_events(late)})
        plot.align_starts(rules, cycles)
        starts = {who: min(r['_inj'] for r in rules if r['participant'] == who) for who in ('early', 'late')}
        self.assertEqual(starts['early'], starts['late'])
        late_done = [r['_done'] for r in rules if r['participant'] == 'late' and r['_done']]
        self.assertEqual(late_done, [plot._ts(_at(240))])  # shifted back by 5000 s, like the rule_ok of the early run


@unittest.skipUnless(_has_matplotlib(), 'matplotlib is not installed')
class TestFigures(unittest.TestCase):

    def _size(self, path):
        return os.path.getsize(path) if os.path.exists(path) else 0

    def test_one_run_gives_one_figure(self):
        with tempfile.TemporaryDirectory() as d:
            folder = _run_folder(d, 'one')
            out = os.path.join(d, 'out')
            self.assertEqual(plot.main([folder, '--out', out]), 0)
            self.assertEqual(os.listdir(out), ['clint_run_one.png'])
            self.assertGreater(self._size(os.path.join(out, 'clint_run_one.png')), 5000)

    def test_many_runs_give_one_class_figure_also_with_empty_runs(self):
        dry = [{'ts': _at(0), 'event': 'injected', 'rule_id': 'dry-run', 'bytes': 5, 'lifetime': 900},
               {'ts': _at(0), 'event': 'cycle', 'wanted_bytes': 5}]
        with tempfile.TemporaryDirectory() as d:
            folders = [_run_folder(d, f's{i:02d}', dry if i == 0 else None) for i in range(14)]
            out = os.path.join(d, 'out')
            self.assertEqual(plot.main(folders + ['--out', out]), 0)
            self.assertEqual(os.listdir(out), ['clint_class.png'])

    def test_class_figure_from_csv_files_and_without_mplhep(self):
        with tempfile.TemporaryDirectory() as d:
            TestNumbers()._class_rows(d)
            out = os.path.join(d, 'out')
            # An import of mplhep fails now: the plain style is used. Only this one entry is changed and put back:
            # patch.dict on the whole table would drop the matplotlib modules loaded in between.
            missing = object()
            saved = sys.modules.get('mplhep', missing)
            sys.modules['mplhep'] = None
            try:
                self.assertEqual(plot.main(['--csv', d, '--out', out]), 0)
            finally:
                if saved is missing:
                    sys.modules.pop('mplhep', None)
                else:
                    sys.modules['mplhep'] = saved
            self.assertGreater(self._size(os.path.join(out, 'clint_class.png')), 5000)

    def test_folder_without_ledgers_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(plot.main([d, '--out', d]), 1)


if __name__ == "__main__":
    unittest.main()
