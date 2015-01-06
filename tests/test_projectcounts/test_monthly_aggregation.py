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
  Unit tests for monthly per project aggregation
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for monthly per project aggregation of
  aggregator.projectcounts.

"""

import aggregator
import testcases
import os
import datetime


class MonthlyProjectAggregationTestCase(testcases.ProjectcountsDataTestCase):
    """TestCase for 'monthly' project aggregation functions"""
    def test_monthly_csv_non_existing_csv_31_day_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-07,48510,48000,480,30',
            ])

    def test_monthly_csv_non_existing_csv_30_day_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 6, 20)
        last_date = datetime.date(2014, 6, 30)

        csv_data = {
            '2014-05-31': '2014-05-31,1,2,3,4',
            '2014-07-01': '2014-07-01,5,6,7,8',
            }
        for day in range(1, 31):
            csv_data['2014-06-%02d' % day] = ('2014-06-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,46995,46500,465,30',
            ])

    def test_monthly_csv_non_existing_csv_29_day_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2012, 2, 20)
        last_date = datetime.date(2012, 2, 29)

        csv_data = {
            '2012-01-31': '2012-01-31,1,2,3,4',
            '2012-03-01': '2012-03-01,5,6,7,8',
            }
        for day in range(1, 30):
            csv_data['2012-02-%02d' % day] = ('2012-02-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2012-02,45480,45000,450,30',
            ])

    def test_monthly_csv_non_existing_csv_28_day_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 2, 20)
        last_date = datetime.date(2014, 2, 28)

        csv_data = {
            '2014-01-31': '2014-01-31,1,2,3,4',
            '2014-03-01': '2014-03-01,5,6,7,8',
            }
        for day in range(1, 30):
            csv_data['2014-02-%02d' % day] = ('2014-02-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-02,43965,43500,435,30',
            ])

    def test_monthly_csv_existing_csv_existing_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_existing_csv_existing_month_force(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date,
                                      force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,48510,48000,480,30',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_existing_csv_bad_dates_existing_month(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            ]

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,51121,50586,505,30',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_existing_csv_bad_last_day(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-07-31': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 31):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            datetime.date(2014, 7, 31),
            ]

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,49591,49071,490,30',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_existing_csv_only_bad_dates_no_existing_data(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        bad_dates = [datetime.date(2014, 7, day) for day in range(1, 32)]

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_existing_csv_only_bad_dates_existing_data(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-07,4,5,6,7',
            '2014-08,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        bad_dates = [datetime.date(2014, 7, day) for day in range(1, 32)]

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-06,1,2,3,4',
            '2014-08,8,9,10,11',
            ])

    def test_monthly_csv_zero_and_missing_data(self):
        enwiki_file_abs = os.path.join(self.monthly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 7, 20)
        last_date = datetime.date(2014, 7, 31)

        csv_data = {
            '2014-06-30': '2014-06-30,1,2,3,4',
            '2014-08-01': '2014-08-01,5,6,7,8',
            }
        for day in range(1, 32):
            csv_data['2014-07-%02d' % day] = ('2014-07-%02d,%d,%d00,%d,1'
                                              % (day, day * 101 + 1, day, day))

        csv_data['2014-07-10'] = '2014-07-10,11,0,10,1'
        csv_data['2014-07-20'] = '2014-07-20,2001,2000,,1'

        aggregator.update_monthly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                      first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014-07,47538,47032,476,30',
            ])
