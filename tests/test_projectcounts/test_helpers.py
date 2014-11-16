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