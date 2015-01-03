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
  Unit tests for weekly per project aggregation
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for weekly per project aggregation of
  aggregator.projectcounts.

"""

import aggregator
import testcases
import os
import datetime


class WeeklyProjectAggregationTestCase(testcases.ProjectcountsDataTestCase):
    """TestCase for 'weekly' project aggregation functions"""
    def test_weekly_csv_non_existing_csv(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-03': '2014-07-03,4000000,4000,4,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W27,28000000,28000,28,7',
            ])

    def test_weekly_csv_existing_csv_existing_week(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-03': '2014-07-03,4000000,4000,4,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_existing_csv_existing_week_force(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-03': '2014-07-03,4000000,4000,4,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date,
                                     force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,28000000,28000,28,7',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_existing_csv_bad_dates_existing_week(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            ]

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,26600000,26600,26,7',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_existing_csv_bad_sunday(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            datetime.date(2014, 7, 6),
            ]

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,21000000,21000,21,7',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_existing_csv_only_bad_dates_no_existing_data(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        bad_dates = [
            datetime.date(2014, 6, 30),
            datetime.date(2014, 7, 1),
            datetime.date(2014, 7, 2),
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            datetime.date(2014, 7, 5),
            datetime.date(2014, 7, 6),
            ]

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_existing_csv_only_bad_dates_existing_data(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W27,4,5,6,7',
            '2014W28,8,9,10,11',
            ])

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,2000,2,1',
            '2014-07-02': '2014-07-02,3000000,3000,3,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        bad_dates = [
            datetime.date(2014, 6, 30),
            datetime.date(2014, 7, 1),
            datetime.date(2014, 7, 2),
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            datetime.date(2014, 7, 5),
            datetime.date(2014, 7, 6),
            ]

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W26,1,2,3,4',
            '2014W28,8,9,10,11',
            ])

    def test_weekly_csv_zero_and_missing_data(self):
        enwiki_file_abs = os.path.join(self.weekly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 7, 1)
        last_date = datetime.date(2014, 7, 7)

        csv_data = {
            '2014-06-29': '2014-06-29,1,2,3,4',
            '2014-06-30': '2014-06-30,1000000,1000,1,1',
            '2014-07-01': '2014-07-01,2000000,   0,2,1',
            '2014-07-02': '2014-07-02,3000000,3000, ,1',
            '2014-07-03': '2014-07-03,4000000,4000,4,1',
            '2014-07-04': '2014-07-04,5000000,5000,5,1',
            '2014-07-05': '2014-07-05,6000000,6000,6,1',
            '2014-07-06': '2014-07-06,7000000,7000,7,1',
            '2014-07-07': '2014-07-07,5,6,7,8',
            }

        aggregator.update_weekly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014W27,28000000,26000,29,7',
            ])
