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


class DailyProjectAggregationTestCase(testcases.ProjectcountsDataTestCase):
    """TestCase for 'daily' project aggregation functions"""
    def test_daily_csv_non_existing_csv_empty_data(self):
        date = datetime.date(2014, 7, 4)

        csv_data = {}

        nose.tools.assert_raises(
            RuntimeError,
            aggregator.update_daily_csv,
            self.data_dir_abs,
            'enwiki',
            csv_data,
            date,
            date)

    def test_daily_csv_non_existing_csv_existing_data_single(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')

        date = datetime.date(2014, 5, 16)

        csv_data = {'2014-05-16': '2014-05-16,1,2,3'}

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    date, date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-16,1,2,3'
            ])

    def test_daily_csv_non_existing_csv_existing_data_multiple(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-13': '2014-05-13,4,5,6',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-13,4,5,6',
            '2014-05-14,7,8,9',
            '2014-05-15,10,11,12',
            ])

    def test_daily_csv_non_existing_csv_existing_data_outside_date(self):
        date = datetime.date(2014, 5, 17)

        csv_data = {'2014-05-16': '2014-05-16,1,2,3'}

        nose.tools.assert_raises(
            RuntimeError,
            aggregator.update_daily_csv,
            self.data_dir_abs,
            'enwiki',
            csv_data,
            date,
            date)

    def test_daily_csv_existing_csv_existing_data_without_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-16,47,500'
            ])

        date = datetime.date(2014, 5, 16)

        csv_data = {'2014-05-16': '2014-05-16,47,167'}

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    date, date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-16,47,500'
            ])

    def test_daily_csv_existing_csv_existing_data_with_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-16,47,500'
            ])

        date = datetime.date(2014, 5, 16)

        csv_data = {'2014-05-16': '2014-05-16,47,167'}

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    date, date, force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-16,47,167'
            ])

    def test_daily_csv_existing_csv_existing_data_multiple_with_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-14,47,503',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-13': '2014-05-13,4,5,6',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date,
                                    force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,4,5,6',
            '2014-05-14,7,8,9',
            '2014-05-15,10,11,12',
            '2014-05-16,47,505',
            ])

    def test_daily_csv_bad_dates_outside_data_without_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-14,47,503',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)
        bad_dates = [
            datetime.date(2014, 5, 4)
            ]

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-13': '2014-05-13,4,5,6',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date, bad_dates=bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-14,47,503',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

    def test_daily_csv_bad_dates_outside_data_with_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-14,47,503',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)
        bad_dates = [
            datetime.date(2014, 5, 4)
            ]

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-13': '2014-05-13,4,5,6',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date, bad_dates=bad_dates,
                                    force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,4,5,6',
            '2014-05-14,7,8,9',
            '2014-05-15,10,11,12',
            '2014-05-16,47,505',
            ])

    def test_daily_csv_bad_dates_without_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)
        bad_dates = [
            datetime.date(2014, 5, 13),
            datetime.date(2014, 5, 14),
            datetime.date(2014, 5, 16)
            ]

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date, bad_dates=bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

    def test_daily_csv_bad_dates_with_force(self):
        enwiki_file_abs = os.path.join(self.daily_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-13,47,502',
            '2014-05-15,47,504',
            '2014-05-16,47,505',
            ])

        first_date = datetime.date(2014, 5, 13)
        last_date = datetime.date(2014, 5, 15)
        bad_dates = [
            datetime.date(2014, 5, 13),
            datetime.date(2014, 5, 14),
            datetime.date(2014, 5, 16)
            ]

        csv_data = {
            '2014-05-12': '2014-05-12,1,2,3',
            '2014-05-13': '2014-05-12,4,5,6',
            '2014-05-14': '2014-05-14,7,8,9',
            '2014-05-15': '2014-05-15,10,11,12',
            '2014-05-16': '2014-05-16,13,14,15',
            }

        aggregator.update_daily_csv(self.data_dir_abs, 'enwiki', csv_data,
                                    first_date, last_date, bad_dates=bad_dates,
                                    force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-05-12,47,501',
            '2014-05-15,10,11,12',
            '2014-05-16,47,505',
            ])
