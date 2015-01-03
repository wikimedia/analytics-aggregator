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
  Unit tests for yearly per project aggregation
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for yearly per project aggregation of
  aggregator.projectcounts.

"""

import aggregator
import testcases
import os
import datetime


class YearlyProjectAggregationTestCase(testcases.ProjectcountsDataTestCase):
    """TestCase for 'yearly' project aggregation functions"""
    def test_yearly_csv_non_existing_csv_365_day_year(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014,667950000,6679500,66795,365',
            ])

    def test_yearly_csv_non_existing_csv_366_day_year(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2012, 12, 20)
        last_date = datetime.date(2012, 12, 31)

        csv_data = {
            '2011-12-31': '2011-12-31,1,2,3,4',
            '2013-01-01': '2013-01-01,5,6,7,8',
            }
        for offset in range(1, 367):
            day = datetime.date(2012, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2012,669775000,6697750,66977,365',
            ])

    def test_yearly_csv_existing_csv_existing_year(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_existing_csv_existing_year_force(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date,
                                     force_recomputation=True)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,667950000,6679500,66795,365',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_existing_csv_bad_dates_existing_year(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            ]

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,667919834,6679198,66791,365',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_existing_csv_bad_last_day(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        bad_dates = [
            datetime.date(2014, 7, 3),
            datetime.date(2014, 7, 4),
            datetime.date(2014, 12, 31),
            ]

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,666084668,6660846,66608,365',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_existing_csv_only_bad_dates_no_existing_data(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        bad_dates = []

        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))
            bad_dates.append(day)

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_existing_csv_only_bad_dates_existing_data(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')
        self.create_file(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2014,4,5,6,7',
            '2015,8,9,10,11',
            ])

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        bad_dates = []

        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))
            bad_dates.append(day)

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date, bad_dates)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2013,1,2,3,4',
            '2015,8,9,10,11',
            ])

    def test_yearly_csv_zero_and_missing_data(self):
        enwiki_file_abs = os.path.join(self.yearly_dir_abs, 'enwiki.csv')

        first_date = datetime.date(2014, 12, 20)
        last_date = datetime.date(2014, 12, 31)

        csv_data = {
            '2013-12-31': '2013-12-31,1,2,3,4',
            '2015-01-01': '2015-01-01,5,6,7,8',
            }
        for offset in range(1, 366):
            day = datetime.date(2014, 1, 1)
            day += datetime.timedelta(days=offset - 1)
            day_str = day.isoformat()
            csv_data[day_str] = ('%s,%d0000,%d00,%d,1' %
                                 (day_str, offset, offset, offset))

        csv_data['2014-07-10'] = '2014-07-10,1910000,0,191,1'
        csv_data['2014-07-20'] = '2014-07-20,2010000,20100,,1'

        aggregator.update_yearly_csv(self.data_dir_abs, 'enwiki', csv_data,
                                     first_date, last_date)

        self.assert_file_content_equals(enwiki_file_abs, [
            '2014,667950000,6660400,66776,365',
            ])
