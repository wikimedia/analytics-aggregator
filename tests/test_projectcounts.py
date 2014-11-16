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
import unittest
import os
import datetime
import nose
import tempfile
import logging
import shutil

FIXTURES_DIR_ABS = os.path.join(os.path.dirname(__file__), "fixtures")


class ProjectcountsTestCase(unittest.TestCase):
    def create_tmp_dir_abs(self):
        # Since we have to have the file visible in the file system, we cannot
        # use *TemporaryFile, and have to resort to mkdtemp
        tmp_dir_abs = tempfile.mkdtemp(prefix='test_projectcounts')

        logging.error("Creating tmp directory '%s'" % (
            tmp_dir_abs))

        try:
            self.tmp_dirs_abs.append(tmp_dir_abs)
        except AttributeError:
            self.tmp_dirs_abs = [tmp_dir_abs]

        return tmp_dir_abs

    def create_empty_file(self, file_abs):
        open(os.path.join(file_abs), 'w').close()

    def create_file(self, file_abs, lines):
        with open(file_abs, 'w') as file:
            for line in lines:
                file.write(line + aggregator.CSV_LINE_ENDING)

    def assert_file_content_equals(self, actual_file_abs, expected_lines):
        header = 'Date,Total,Desktop site,Mobile site,Zero site'
        expected_lines.insert(0, header)
        with open(actual_file_abs, 'r') as file:
            for expected_line in expected_lines:
                try:
                    self.assertEquals(file.next(), expected_line +
                                      aggregator.CSV_LINE_ENDING)
                except StopIteration:
                    self.fail("File '%s' is missing the line:\n%s" % (
                        actual_file_abs, expected_line))
            try:
                extra_line = file.next()
                self.fail("More lines than expected in file '%s'. First "
                          "extra line:\n%s" % (actual_file_abs, extra_line))
            except StopIteration:
                pass

    def setUp(self):
        aggregator.clear_cache()

    def tearDown(self):
        try:
            for tmp_dir_abs in self.tmp_dirs_abs:
                logging.error("Cleaning up tmp directory '%s'" % (
                    tmp_dir_abs))
                shutil.rmtree(tmp_dir_abs)
        except AttributeError:
            pass


class BasicTestCase(ProjectcountsTestCase):
    """TestCase for helper functions"""
    def test_aggregate_for_date_missing_hours_2014_11_01(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-missing-hours')
        date = datetime.date(2014, 11, 1)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_missing_hours_2014_11_02(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-missing-hours')
        date = datetime.date(2014, 11, 2)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_missing_hours_2014_11_03(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-missing-hours')
        date = datetime.date(2014, 11, 3)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_enwiki_different_per_day_1(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*1000 + 23*12 = 24276
        expected = {'en': 24276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_enwiki_different_per_day_2(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 2)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*2000 + 23*12 = 48276
        expected = {'en': 48276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_enwiki_different_per_day_3(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 3)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*3000 + 23*12 = 72276
        expected = {'en': 72276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_different_wiki(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-different-wikis')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.aggregate_for_date(fixture, date)

        expected = {'en': 1, 'de': 26, 'fr': 8}

        self.assertEquals(actual, expected)

    def test_get_daily_count_en(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-different-wikis')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'en', date)

        self.assertEquals(actual, 1)

    def test_get_daily_count_de(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-different-wikis')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'de', date)

        self.assertEquals(actual, 26)

    def test_get_daily_count_fr(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-different-wikis')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'fr', date)

        self.assertEquals(actual, 8)

    def test_get_daily_count_empty_abbreviation(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-different-wikis')
        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'foo', date)

        self.assertEquals(actual, 0)


class ProjectAggregationTestCase(ProjectcountsTestCase):
    """TestCase for project aggregation functions"""
    def test_update_per_project_no_csvs(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-missing-hours')
        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        aggregator.update_per_project_csvs_for_dates(fixture, tmp_dir_abs,
                                                     date, date)

    def test_update_per_project_single_csvs_missing_hours(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-missing-hours')
        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        nose.tools.assert_raises(
            RuntimeError,
            aggregator.update_per_project_csvs_for_dates,
            fixture,
            tmp_dir_abs,
            date,
            date)

    def test_update_per_project_single_csvs_missing_hours_existing(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-11-01,1,2,3,4'
            ])

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-01,1,2,3,4',
            ])

    def test_update_per_project_single_csvs_3days_2014_11_01(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-01,24276,24276,0,0',
            ])

    def test_update_per_project_single_csvs_3days_2014_11_02(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 2)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-02,48276,48276,0,0',
            ])

    def test_update_per_project_single_csvs_3days_2014_11_03(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 3)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-03,72276,72276,0,0',
            ])

    def test_update_per_project_single_csvs_3days_prefilled(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 2)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-11-03,1,2,3,4',
            '2014-11-01,5,6,7,8',
            ])

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-01,5,6,7,8',
            '2014-11-02,48276,48276,0,0',
            '2014-11-03,1,2,3,4',
            ])

    def test_update_per_project_single_csvs_3days_doubled(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 2)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-11-01,1,2,3,4',
            '2014-11-01,2,3,4,5',
            ])

        nose.tools.assert_raises(
            RuntimeError,
            aggregator.update_per_project_csvs_for_dates,
            fixture,
            tmp_dir_abs,
            date,
            date)

    def test_update_per_project_single_csvs_3days_prefilled_range(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        first_date = datetime.date(2014, 11, 1)
        last_date = datetime.date(2014, 11, 3)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_empty_file(enwiki_file_abs)

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            first_date,
            last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-01,24276,24276,0,0',
            '2014-11-02,48276,48276,0,0',
            '2014-11-03,72276,72276,0,0',
            ])

    def test_update_daily_forced_recomputation(self):
        fixture = os.path.join(FIXTURES_DIR_ABS,
                               '2014-11-3days-enwiki-day-times-100-plus-hour')
        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-11-01,1,2,3,4'
            ])

        aggregator.update_per_project_csvs_for_dates(
            fixture,
            tmp_dir_abs,
            date,
            date,
            True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-11-01,24276,24276,0,0',
            ])

    def test_update_daily_forced_recomputation_missing_hours(self):
        fixture = os.path.join(FIXTURES_DIR_ABS, '2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        enwiki_file_abs = os.path.join(tmp_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-11-01,1,2,3,4'
            ])

        nose.tools.assert_raises(
            RuntimeError,
            aggregator.update_per_project_csvs_for_dates,
            fixture,
            tmp_dir_abs,
            date,
            date,
            True)


class MonitoringTestCase(ProjectcountsTestCase):
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
