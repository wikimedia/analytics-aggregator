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
  Unit tests for per project aggregation
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for per project aggregation of
  aggregator.projectcounts.

"""

import aggregator
import testcases
import os
import datetime
import nose


class ProjectAggregationTestCase(testcases.ProjectcountsTestCase):
    """TestCase for project aggregation functions"""
    def test_update_per_project_no_csvs(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        tmp_dir_abs = self.create_tmp_dir_abs()

        aggregator.update_per_project_csvs_for_dates(fixture, tmp_dir_abs,
                                                     date, date)

    def test_update_per_project_single_csvs_missing_hours(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs(
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
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

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
