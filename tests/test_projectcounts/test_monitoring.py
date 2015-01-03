# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
  projectcounts unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for aggregator.projectcounts.

"""

import aggregator
import testcases
import os
import datetime
import nose
import re


@nose.tools.nottest
class MonitoringTestCase(testcases.ProjectcountsDataTestCase):
    """TestCase for monitoring functions"""
    def setUp(self):
        super(MonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.data_dir_abs

    def get_relevant_daily_date_strs(self):
        today = datetime.date.today()

        for day_offset in range(-10, 0):
            date = (today + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            yield date_str

    def get_relevant_weekly_date_strs(self):
        today = datetime.date.today()

        for day_offset in range(-30, 0):
            date = (today + datetime.timedelta(days=day_offset))
            if date.weekday() == 6:
                date_str = date.strftime('%GW%V')
                yield date_str

    def get_relevant_monthly_date_strs(self):
        today = datetime.date.today()

        for day_offset in range(-90, 0):
            date = (today + datetime.timedelta(days=day_offset))
            if date.day == 1:
                yield (date - datetime.timedelta(days=1)).strftime('%Y-%m')

    def get_relevant_yearly_date_strs(self):
        today = datetime.date.today()

        for year_offset in range(-3, 0):
            yield str(today.year + year_offset)

    def get_relevant_date_strs(self):
        for date_str in self.get_relevant_daily_date_strs():
            yield date_str

    def create_valid_aggregated_projects(self):
        for dbname in [
            'enwiki',
            'jawiki',
            'dewiki',
            'eswiki',
            'frwiki',
            'ruwiki',
            'itwiki',
            'foo',
        ]:
            for dir_rel in ['daily_raw', 'daily']:
                csv_file_abs = os.path.join(
                    self.data_dir_abs, dir_rel, dbname + '.csv')
                with open(csv_file_abs, 'w') as file:
                    for date_str in self.get_relevant_daily_date_strs():
                        file.write(
                            '%s,137037034,123456789,12345678,1234567%s' % (
                                date_str, aggregator.CSV_LINE_ENDING))

            csv_file_abs = os.path.join(
                self.data_dir_abs, 'weekly_rescaled', dbname + '.csv')
            with open(csv_file_abs, 'w') as file:
                for date_str in self.get_relevant_weekly_date_strs():
                    file.write(
                        '%s,137037034,123456789,12345678,1234567%s' % (
                            date_str, aggregator.CSV_LINE_ENDING))

            csv_file_abs = os.path.join(
                self.data_dir_abs, 'monthly_rescaled', dbname + '.csv')
            with open(csv_file_abs, 'w') as file:
                for date_str in self.get_relevant_monthly_date_strs():
                    file.write(
                        '%s,137037034,123456789,12345678,1234567%s' % (
                            date_str, aggregator.CSV_LINE_ENDING))

            csv_file_abs = os.path.join(
                self.data_dir_abs, 'yearly_rescaled', dbname + '.csv')
            with open(csv_file_abs, 'w') as file:
                for date_str in self.get_relevant_yearly_date_strs():
                    file.write(
                        '%s,803037034,723456789,72345678,7234567%s' % (
                            date_str, aggregator.CSV_LINE_ENDING))

    def assert_has_item_by_re(self, haystack, pattern):
        for straw in haystack:
            if re.search(pattern, straw):
                return
        self.fail("Could not find '%s' in %s" % (pattern, haystack))

    def test_validity_no_csvs(self):
        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as no csvs could get found
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, 'not find any CSVs')

    def test_validity_no_enwiki(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        os.unlink(enwiki_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as enwiki.csv is missing
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[mM]issing.*enwiki')

    def test_validity_only_big_wikis(self):
        self.create_valid_aggregated_projects()

        foo_file_abs = os.path.join(self.csv_dir_abs, 'foo.csv')
        os.unlink(foo_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as no csvs for other wikis than the big wikis are
        # present.
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, 'big wikis')

    def test_validity_enwiki_empty(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as enwiki has no reading
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[nN]o lines')

    def test_validity_valid(self):
        self.create_valid_aggregated_projects()

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        self.assertEquals(issues, [])

    def test_validity_enwiki_no_current(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        lines = []
        for date_str in self.get_relevant_date_strs():
            lines.append('%s,135925923,123456789,12345678,123456' % (date_str))
        del lines[-1]
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as enwiki has no reading for today
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, 'too old')

    def test_validity_enwiki_too_low_desktop(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        lines = []
        for date_str in self.get_relevant_date_strs():
            lines.append('%s,13580245,0,12345678,1234567' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as the desktop count is too low
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[dD]esktop.*too.*low')

    def test_validity_enwiki_too_low_mobile(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        lines = []
        for date_str in self.get_relevant_date_strs():
            lines.append('%s,124691356,123456789,0,1234567' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as the mobile count is too low
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[mM]obile.*too.*low')

    def test_validity_enwiki_too_low_zero(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        lines = []
        for date_str in self.get_relevant_date_strs():
            lines.append('%s,135802467,123456789,12345678,0' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as the zero count is too low
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[zZ]ero.*too low')

    def test_validity_enwiki_total_does_not_add_up(self):
        self.create_valid_aggregated_projects()

        enwiki_file_abs = os.path.join(self.csv_dir_abs, 'enwiki.csv')
        lines = []
        for date_str in self.get_relevant_date_strs():
            lines.append('%s,200000000,123456789,12345678,123456' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            self.data_dir_abs)

        # At least one issue, as the total is not the sum of tho other colums.
        nose.tools.assert_greater_equal(len(issues), 1)
        self.assert_has_item_by_re(issues, '[tT]otal.*not.*sum')


@nose.tools.istest
class DailyRawMonitoringTestCase(MonitoringTestCase):
    def setUp(self):
        super(DailyRawMonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.daily_raw_dir_abs


@nose.tools.istest
class DailyMonitoringTestCase(DailyRawMonitoringTestCase):
    def setUp(self):
        super(DailyMonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.daily_dir_abs


@nose.tools.istest
class WeeklyMonitoringTestCase(DailyRawMonitoringTestCase):
    def setUp(self):
        super(WeeklyMonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.weekly_dir_abs

    def get_relevant_date_strs(self):
        for date_str in self.get_relevant_weekly_date_strs():
            yield date_str


@nose.tools.istest
class MonthlyMonitoringTestCase(DailyRawMonitoringTestCase):
    def setUp(self):
        super(MonthlyMonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.monthly_dir_abs

    def get_relevant_date_strs(self):
        for date_str in self.get_relevant_monthly_date_strs():
            yield date_str


@nose.tools.istest
class YearlyMonitoringTestCase(DailyRawMonitoringTestCase):
    def setUp(self):
        super(YearlyMonitoringTestCase, self).setUp()
        self.csv_dir_abs = self.yearly_dir_abs

    def get_relevant_date_strs(self):
        for date_str in self.get_relevant_yearly_date_strs():
            yield date_str
