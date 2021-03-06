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
  Unit tests for projectcounts helper functions
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for helpers in aggregator.projectcounts.

"""

import aggregator
import testcases
import datetime
import nose


class BasicTestCase(testcases.ProjectcountsTestCase):
    """TestCase for helper functions"""
    def test_aggregate_for_date_missing_hours_2014_11_01(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_missing_hours_2014_11_02(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_missing_hours_2014_11_03(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date, fixture, date)

    def test_aggregate_for_date_missing_hours_2014_11_01_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date,
                                 fixture, date, allow_bad_data=False)

    def test_aggregate_for_date_missing_hours_2014_11_02_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date,
                                 fixture, date, allow_bad_data=False)

    def test_aggregate_for_date_missing_hours_2014_11_03_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date,
                                 fixture, date, allow_bad_data=False)

    def test_aggregate_for_date_missing_hours_2014_11_04_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 4)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.aggregate_for_date,
                                 fixture, date, allow_bad_data=False)

    def test_aggregate_for_date_missing_hours_2014_11_01_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.aggregate_for_date(fixture, date,
                                               allow_bad_data=True)
        # Each hour is 100 + the hour itself. The last hour is missing. So,
        # we're expecting 24*100 + 23*12 - 123 = 2553
        expected = {'en': 2553}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_missing_hours_2014_11_02_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        actual = aggregator.aggregate_for_date(fixture, date,
                                               allow_bad_data=True)
        # Each hour is 200 + the hour itself. The 12th hour is missing. So,
        # we're expecting 24*200 + 23*12 - 212 = 4864
        expected = {'en': 4864}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_missing_hours_2014_11_03_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        actual = aggregator.aggregate_for_date(fixture, date,
                                               allow_bad_data=True)
        # Each hour is 300 + the hour itself. The first and last hour are
        # missing. So, we're expecting 24*300 + 23*12 - 300 - 323 = 6853
        expected = {'en': 6853}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_missing_hours_2014_11_04_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 4)

        actual = aggregator.aggregate_for_date(fixture, date,
                                               allow_bad_data=True)

        # No hour files at all, so no data is expected
        self.assertEquals(actual, {})

    def test_aggregate_for_date_enwiki_different_per_day_1(self):
        fixture = self.get_fixture_dir_abs(
            '2014-11-3days-enwiki-day-times-100-plus-hour')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*1000 + 23*12 = 24276
        expected = {'en': 24276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_enwiki_different_per_day_2(self):
        fixture = self.get_fixture_dir_abs(
            '2014-11-3days-enwiki-day-times-100-plus-hour')

        date = datetime.date(2014, 11, 2)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*2000 + 23*12 = 48276
        expected = {'en': 48276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_enwiki_different_per_day_3(self):
        fixture = self.get_fixture_dir_abs(
            '2014-11-3days-enwiki-day-times-100-plus-hour')

        date = datetime.date(2014, 11, 3)

        actual = aggregator.aggregate_for_date(fixture, date)

        # Each hour is 1000 + the hour itself, so
        # we're expecting 24*3000 + 23*12 = 72276
        expected = {'en': 72276}

        self.assertEquals(actual, expected)

    def test_aggregate_for_date_different_wiki(self):
        fixture = self.get_fixture_dir_abs('2014-11-different-wikis')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.aggregate_for_date(fixture, date)

        expected = {'en': 1, 'de': 26, 'fr': 8}

        self.assertEquals(actual, expected)

    def test_get_daily_count_en(self):
        fixture = self.get_fixture_dir_abs('2014-11-different-wikis')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'en', date)

        self.assertEquals(actual, 1)

    def test_get_daily_count_de(self):
        fixture = self.get_fixture_dir_abs('2014-11-different-wikis')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'de', date)

        self.assertEquals(actual, 26)

    def test_get_daily_count_fr(self):
        fixture = self.get_fixture_dir_abs('2014-11-different-wikis')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'fr', date)

        self.assertEquals(actual, 8)

    def test_get_daily_count_empty_abbreviation(self):
        fixture = self.get_fixture_dir_abs('2014-11-different-wikis')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'foo', date)

        self.assertEquals(actual, 0)

    def test_get_daily_count_missing_hours_2014_11_01(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date)

    def test_get_daily_count_missing_hours_2014_11_02(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date)

    def test_get_daily_count_missing_hours_2014_11_03(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date)

    def test_get_daily_count_missing_hours_2014_11_04(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 4)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date)

    def test_get_daily_count_missing_hours_2014_11_01_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date, allow_bad_data=False)

    def test_get_daily_count_missing_hours_2014_11_02_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date, allow_bad_data=False)

    def test_get_daily_count_missing_hours_2014_11_03_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date, allow_bad_data=False)

    def test_get_daily_count_missing_hours_2014_11_04_no_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 4)

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.get_daily_count,
                                 fixture, 'en', date, allow_bad_data=False)

    def test_get_daily_count_missing_hours_2014_11_01_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'en', date,
                                            allow_bad_data=True)

        # Each hour is 100 + the hour itself. The last hour is missing. So,
        # we're expecting 24*100 + 23*12 - 123 = 2553
        self.assertEquals(actual, 2553)

    def test_get_daily_count_missing_hours_2014_11_02_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 2)

        actual = aggregator.get_daily_count(fixture, 'en', date,
                                            allow_bad_data=True)

        # Each hour is 200 + the hour itself. The 12th hour is missing. So,
        # we're expecting 24*200 + 23*12 - 212 = 4864
        self.assertEquals(actual, 4864)

    def test_get_daily_count_missing_hours_2014_11_03_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 3)

        actual = aggregator.get_daily_count(fixture, 'en', date,
                                            allow_bad_data=True)

        # Each hour is 300 + the hour itself. The first and last hour are
        # missing. So, we're expecting 24*300 + 23*12 - 300 - 323 = 6853
        self.assertEquals(actual, 6853)

    def test_get_daily_count_missing_hours_2014_11_04_allow_bad_data(self):
        fixture = self.get_fixture_dir_abs('2014-11-missing-hours')

        date = datetime.date(2014, 11, 4)

        actual = aggregator.get_daily_count(fixture, 'en', date,
                                            allow_bad_data=True)

        # No hour files at all, so no count is expected
        self.assertEquals(actual, 0)

    def test_rescale_counts_single_day(self):
        dates = [datetime.date(2014, 8, 3)]

        csv_data = {
            '2014-08-03': '2014-08-03,3,2,1'
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            1)

        self.assertEquals(actual, [3, 2, 1])

    def test_rescale_counts_more_days(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3330,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            3)

        self.assertEquals(actual, [6660, 6000, 600, 60])

    def test_rescale_counts_more_days_downscale_int(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3334,3002,301,31',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            1)

        self.assertEquals(actual, [2220, 2000, 200, 20])

    def test_rescale_counts_more_days_upscale(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3330,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            5)

        self.assertEquals(actual, [11100, 10000, 1000, 100])

    def test_rescale_counts_more_days_bad_dates_middle(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3330,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = [
            datetime.date(2014, 8, 5),
            ]

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            5)

        self.assertEquals(actual, [11100, 10000, 1000, 100])

    def test_rescale_counts_more_days_bad_dates_borders(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3330,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 6),
            ]

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            5)

        self.assertEquals(actual, [11100, 10000, 1000, 100])

    def test_rescale_counts_more_days_bad_dates_skew(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1110,1000,100,10',
            '2014-08-05': '2014-08-05,2220,2000,200,20',
            '2014-08-06': '2014-08-06,3330,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = [
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            5)

        self.assertEquals(actual, [5550, 5000, 500, 50])

    def test_rescale_counts_only_bad_dates(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-04': '2014-08-04,1000,100,10',
            '2014-08-05': '2014-08-05,2000,200,20',
            '2014-08-06': '2014-08-06,3000,300,30',
            '2014-08-07': '2014-08-07,1,2,3',
        }

        bad_dates = dates

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            5)

        self.assertIsNone(actual)

    def test_rescale_counts_no_data(self):
        dates = [datetime.date(2014, 8, 4)]

        csv_data = {}

        bad_dates = []

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.rescale_counts,
                                 csv_data,
                                 dates,
                                 bad_dates,
                                 5)

    def test_rescale_counts_no_data_for_date(self):
        dates = [datetime.date(2014, 8, 4)]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3',
            '2014-08-05': '2014-08-05,2000,200,20',
        }

        bad_dates = []

        nose.tools.assert_raises(RuntimeError,
                                 aggregator.rescale_counts,
                                 csv_data,
                                 dates,
                                 bad_dates,
                                 5)

    def test_rescale_counts_zero_and_empty_columns(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,100',
            '2014-08-04': '2014-08-04,18,0,3,5,0,7,10,',
            '2014-08-05': '2014-08-05,9,1,,  ,0,8,  ,',
            '2014-08-06': '2014-08-06,21,2,4,6,0,9,0 ,',
            '2014-08-07': '2014-08-07,98,11,12,13,14,15,16,17',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            3)

        self.assertEquals(actual, [68, 3, 10, 16, 0, 24, 15, None])

    def test_rescale_counts_zero_and_empty_columns_upscale(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,100',
            '2014-08-04': '2014-08-04,18,0,3,5,0, , ,10,',
            '2014-08-05': '2014-08-05,9,1,,  ,0,0,8,  ,',
            '2014-08-06': '2014-08-06,21,2,4,6,0, ,9,0 ,',
            '2014-08-07': '2014-08-07,98,11,12,13,14,15,16,17',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            4)

        self.assertEquals(actual, [94, 4, 14, 22, 0, 0, 34, 20, None])

    def test_rescale_counts_shorter_second_column(self):
        dates = [
            datetime.date(2014, 8, 4),
            datetime.date(2014, 8, 5),
            datetime.date(2014, 8, 6),
            ]

        csv_data = {
            '2014-08-03': '2014-08-03,100,200',
            '2014-08-04': '2014-08-04,3,1,2',
            '2014-08-05': '2014-08-05,3,3',
            '2014-08-06': '2014-08-06,4,4,,',
            '2014-08-07': '2014-08-07,300,400',
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            4)

        self.assertEquals(actual, [18, 10, 8, None])

    def test_rescale_override_total_column(self):
        dates = [datetime.date(2014, 8, 3)]

        csv_data = {
            '2014-08-03': '2014-08-03,1,2,3'
        }

        bad_dates = []

        actual = aggregator.rescale_counts(
            csv_data,
            dates,
            bad_dates,
            1)

        self.assertEquals(actual, [5, 2, 3])

    def test_get_daily_count_wrong_lines_2014_11_01(self):
        fixture = self.get_fixture_dir_abs('2014-11-wrong-lines')

        date = datetime.date(2014, 11, 1)

        actual = aggregator.get_daily_count(fixture, 'en', date,
                                            allow_bad_data=False)

        # Each hour is 100 + the hour itself. Wrong line is in hour 01.
        # we're expecting 24*100 + 23*12 = 2676
        self.assertEquals(actual, 2676)

        # Kept in case we want to revert to raising an exception.
        # nose.tools.assert_raises(RuntimeError,
        #                         aggregator.get_daily_count,
        #                         fixture, 'en', date, allow_bad_data=False)
