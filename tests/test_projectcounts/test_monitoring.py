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


class MonitoringTestCase(testcases.ProjectcountsTestCase):
    """TestCase for monitoring functions"""
    def create_valid_aggregated_projects(self, tmp_dir_abs):
        today = datetime.date.today()
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
            csv_file_abs = os.path.join(tmp_dir_abs, dbname + '.csv')
            with open(csv_file_abs, 'w') as file:
                for day_offset in range(-10, 0):
                    date = (today + datetime.timedelta(days=day_offset))
                    date_str = date.isoformat()
                    file.write('%s,137037034,123456789,12345678,1234567%s' % (
                        date_str, aggregator.CSV_LINE_ENDING))

    def test_validity_no_csvs(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as no csvs could get found
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_no_enwiki(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        os.unlink(enwiki_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki.csv is missing
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_only_big_wikis(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        foo_file_abs = os.path.join(tmp_dir_abs, 'foo.csv')
        os.unlink(foo_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as no csvs for other wikis than the big wikis are
        # present.
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_empty(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki has no reading
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_no_today(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        yesterday = aggregator.parse_string_to_date('yesterday')
        lines = []
        for day_offset in range(-10, 0):
            date = (yesterday + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            lines.append('%s,135925923,123456789,12345678,123456' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki has no reading for today
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_too_low_desktop(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        today = datetime.date.today()
        lines = []
        for day_offset in range(-10, 0):
            date = (today + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            lines.append('%s,13580245,0,12345678,1234567' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki has no reading for today
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_too_low_mobile(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        today = datetime.date.today()
        lines = []
        for day_offset in range(-10, 0):
            date = (today + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            lines.append('%s,124691356,123456789,0,1234567' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki has no reading for today
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_too_low_zero(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        today = datetime.date.today()
        lines = []
        for day_offset in range(-10, 0):
            date = (today + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            lines.append('%s,135802467,123456789,12345678,0' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as enwiki has no reading for today
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_enwiki_total_does_not_add_up(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        today = datetime.date.today()
        lines = []
        for day_offset in range(-10, 0):
            date = (today + datetime.timedelta(days=day_offset))
            date_str = date.isoformat()
            lines.append('%s,200000000,123456789,12345678,123456' % (date_str))
        self.create_file(enwiki_file_abs, lines)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        # At least one issue, as the total is not the sum of tho other colums.
        nose.tools.assert_greater_equal(len(issues), 1)

    def test_validity_valid(self):
        tmp_dir_abs = self.create_tmp_dir_abs()

        self.create_valid_aggregated_projects(tmp_dir_abs)

        issues = aggregator.get_validity_issues_for_aggregated_projectcounts(
            tmp_dir_abs)

        self.assertEquals(issues, [])
