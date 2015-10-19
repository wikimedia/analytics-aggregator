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
    aggregator.projectcounts
    ~~~~~~~~~~~~~~~~~~~~~~~~

    This module contains functions to aggregate Wikimedia's
    projectcount files.
"""


import logging
import calendar
import datetime
import os
import glob
import util

PROJECTVIEWS_STRFTIME_PATTERN = ('%%Y%s%%Y-%%m%sprojectviews-%%Y%%m%%d-'
                                 '%%H0000' % (os.sep, os.sep))
PROJECTCOUNTS_STRFTIME_PATTERN = ('%%Y%s%%Y-%%m%sprojectcounts-%%Y%%m%%d-'
                                  '%%H0000' % (os.sep, os.sep))

CSV_HEADER = 'Date,Total,Desktop site,Mobile site,Zero site'

DATE_MOBILE_ADDED = datetime.date(2014, 9, 23)

cache = {}


def clear_cache():
    global cache
    logging.debug("Clearing projectcounts cache")
    cache = {}


def aggregate_for_date(
        source_dir_abs, date,
        allow_bad_data=False, output_projectviews=False):

    """Aggregates hourly projectcounts for a given day.

    This function does not attempt to cache the aggregated data. Either cache
    yourself, or is helper methods that cache, as
    get_hourly_count_for_webstatscollector_abbreviation.

    If one of the required 24 hourly files do not exist, cannot be read, or
    some other issue occurs, a RuntimeError is raised.

    The returned dictonary is keyed by the lowercase webstatscollector
    abbreviation, and values are the total counts for this day.

    :param source_dir_abs: Absolute directory to read the hourly projectcounts
        files from.
    :param date: The date to get the count for.
    :param allow_bad_data: If True, do not bail out, if some data is
        bad or missing. (Default: False)
    :param output_projectviews: If True, name the output files projectviews
        instead of projectcounts. (Default: False)
    """
    daily_data = {}
    if output_projectviews:
        output_format = PROJECTVIEWS_STRFTIME_PATTERN
    else:
        output_format = PROJECTCOUNTS_STRFTIME_PATTERN

    for hour in range(24):
        # Initialize with the relevant hour start ...
        hourly_file_datetime = datetime.datetime(date.year, date.month,
                                                 date.day, hour)
        # and add another hour since webstatscollector uses the interval end in
        # the file name :-(
        hourly_file_datetime += datetime.timedelta(hours=1)

        hourly_file_abs = os.path.join(
            source_dir_abs,
            hourly_file_datetime.strftime(output_format))

        if not os.path.isfile(hourly_file_abs):
            if allow_bad_data:
                # The file does not exist, but bad data is explicitly
                # allowed, so we continue aggregating
                continue
            else:
                raise RuntimeError("'%s' is not an existing file" % (
                    hourly_file_abs))

        logging.debug("Reading %s" % (hourly_file_abs))

        with open(hourly_file_abs, 'r') as hourly_file:
            for line in hourly_file:
                fields = line.split(' ')

                if len(fields) != 4:
                    logging.warn("File %s as an incorrect line: %s" % (
                        hourly_file_abs, line))
                    # Kept in case we want to get back to raising an error
                    # raise RuntimeError("Malformed line in '%s'" % (
                    #    hourly_file))
                else:
                    abbreviation = fields[0].lower()
                    count = int(fields[2])

                    daily_data[abbreviation] = daily_data.get(abbreviation, 0) \
                        + count

    return daily_data


def get_daily_count(source_dir_abs, webstatscollector_abbreviation, date,
                    allow_bad_data=False, output_projectviews=False):
    """Obtains the daily count for a webstatscollector abbreviation.

    Data gets cached upon read. For a day, the data is <50KB, so having many
    dates in cache is not resource intensive.

    :param source_dir_abs: Absolute directory to read the hourly projectcounts
        files from.
    :param webstatscollector_abbreviation: The webstatscollector abbreviation
        to get the count for
    :param date: The date to get the count for.
    :param allow_bad_data: If True, do not bail out, if some data is
        bad or missing. (Default: False)
    :param output_projectviews: If True, name the output files projectviews
        instead of projectcounts. (Default: False)
    """
    global cache
    try:
        source_dir_cache = cache[source_dir_abs]
    except KeyError:
        source_dir_cache = {}
        cache[source_dir_abs] = source_dir_cache

    try:
        date_data = source_dir_cache[date]
    except KeyError:
        date_data = aggregate_for_date(
            source_dir_abs, date, allow_bad_data, output_projectviews
        )
        source_dir_cache[date] = date_data

    return date_data.get(webstatscollector_abbreviation, 0)


def update_daily_csv(target_dir_abs, dbname, csv_data_input, first_date,
                     last_date, bad_dates=[], force_recomputation=False):
    """Updates daily per project CSVs from a csv data dictionary.

    The existing per project CSV files in target_dir_abs/daily are updated from
    first_date up to (and including) last_date.

    If the CSVs already has data for a given day, it is not recomputed, unless
    force_recomputation is True. But if a day is in the set of days that are
    considered, and it is also in bad_dates, the data for this day is removed
    regardless of force_recomputation.

    Upon any error, the function raises an exception.

    :param target_dir_abs: Absolute directory. CSVs are getting written to the
        'daily' subdirectory of target_dir_abs.
    :param dbname: The database name of the wiki to consider (E.g.: 'enwiki')
    :param csv_data_input: The data dict to aggregate from
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param bad_dates: List of dates considered having bad data. (Default: [])
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    """
    csv_dir_abs = os.path.join(target_dir_abs, 'daily')
    if not os.path.exists(csv_dir_abs):
        os.mkdir(csv_dir_abs)
    csv_file_abs = os.path.join(csv_dir_abs, dbname + '.csv')

    csv_data = util.parse_csv_to_first_column_dict(csv_file_abs)

    for date in util.generate_dates(first_date, last_date):
        date_str = date.isoformat()
        logging.debug("Updating csv '%s' for date '%s'" % (
            dbname, str(date)))
        if date in bad_dates:
            if date_str in csv_data:
                del csv_data[date_str]
        else:
            if date_str not in csv_data or force_recomputation:
                if date_str not in csv_data_input:
                    raise RuntimeError("No data for '%s' during daily "
                                       "aggregation" % (date_str))
                csv_data[date_str] = csv_data_input[date_str]

    util.write_dict_values_sorted_to_csv(
        csv_file_abs,
        csv_data,
        header=CSV_HEADER)


def rescale_counts(csv_data, dates, bad_dates, rescale_to):
    """Extracts relevant dates from CSV data, sums them up, and rescales them.

    If the dates only cover bad dates, None is returned.

    Each column is rescaled separatedly.
    Missing columns for good dates are not assumed to be 0.

    The first column is ignored, and assumed to hold the date for the reading.

    The second column is assumed to hold the sum of the remaining
    columns. This column is not rescaled, but the recomputed by
    summing the other rescaled columns. Thereby, we can guarantee that
    the "total sum" always is the sum of the other columns.

    Upon other errors, a RuntimeError is raised.

    The rescaled counts are returned as list of integers.

    :param csv_data_input: The data dict to get data from
    :param dates: The dates to sum up counts for
    :param bad_dates: List of dates considered having bad data.
    :param rescale_to: Rescale the good entries to this many entries.
    """
    ret = None
    aggregations = None
    columns = 0
    for date in dates:
        if date in bad_dates:
            continue
        date_str = date.isoformat()
        try:
            csv_line_items = csv_data[date_str].split(',')
        except KeyError:
            raise RuntimeError("No data for '%s'" % (date_str))

        # Getting rid if date column. No need to aggregate date columns.
        del csv_line_items[0]

        # Getting rid of the "total sum" column.
        # We always want the "total sum" column to be the sum of the
        # other columns in the row. Hence, we cannot simply rescale
        # the "total sum" column from the other rows, as that would on
        # the one hand give rounding artifacts, and on the other hand
        # would not work if some row is missing values for some
        # columns. Therefore, we don't rescale the "total sum" column,
        # but recompute it after the other columns' rescaled value is
        # known.
        del csv_line_items[0]

        if ret is None:
            ret = []
            aggregations = []

        # Make sure we can fit csv_line_items's columns into the aggregations
        while columns < len(csv_line_items):
            ret.append(0)
            aggregations.append(0)
            columns += 1

        for i in range(columns):
            try:
                ret[i] += int(csv_line_items[i].strip())
                aggregations[i] += 1
            except IndexError:
                # csv_line_times is shorter than ret.
                pass
            except ValueError:
                # No valid reading. (E.g. the empty string)
                pass

    if ret is not None:
        # Since we found readings, rescale.
        ret = [(ret[i] * rescale_to) / aggregations[i] if aggregations[i]
               else None
               for i in range(columns)]

        # Then recompute the "total sum" column and prepend it.
        ret.insert(0, sum([0 if i is None else i for i in ret]))
    return ret


def update_weekly_csv(target_dir_abs, dbname, csv_data_input, first_date,
                      last_date, bad_dates=[], force_recomputation=False):
    """Updates weekly per project CSVs from a csv data dictionary.

    The existing per project CSV files in target_dir_abs/weekly are updated for
    all weeks where Sunday in in the date interval from first_date up to (and
    including) last_date.

    For weekly aggregations, a week's total data is rescaled to 7 days.

    If a week under consideration contains no good date, it is removed.

    Upon any error, the function raises an exception.

    :param target_dir_abs: Absolute directory. CSVs are getting written to the
        'weekly_rescaled' subdirectory of target_dir_abs.
    :param dbname: The database name of the wiki to consider (E.g.: 'enwiki')
    :param csv_data_input: The data dict to aggregate from
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param bad_dates: List of dates considered having bad data. (Default: [])
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    """
    csv_dir_abs = os.path.join(target_dir_abs, 'weekly_rescaled')
    if not os.path.exists(csv_dir_abs):
        os.mkdir(csv_dir_abs)
    csv_file_abs = os.path.join(csv_dir_abs, dbname + '.csv')

    csv_data = util.parse_csv_to_first_column_dict(csv_file_abs)

    for date in util.generate_dates(first_date, last_date):
        if date.weekday() == 6:  # Sunday. End of ISO week
            date_str = date.strftime('%GW%V')
            logging.debug("Updating csv '%s' for date '%s'" % (
                dbname, str(date)))
            week_dates = set(date + datetime.timedelta(days=offset)
                             for offset in range(-6, 1))
            expected_good_dates = len(week_dates - set(bad_dates))
            need_recomputation = force_recomputation
            need_recomputation |= expected_good_dates != 7
            need_recomputation |= date_str not in csv_data
            if need_recomputation:
                if expected_good_dates == 0:
                    try:
                        del csv_data[date_str]
                    except KeyError:
                        # No reading was there to remove. That's ok :-)
                        pass
                else:
                    weekly_counts = rescale_counts(csv_data_input, week_dates,
                                                   bad_dates, 7)
                    util.update_csv_data_dict(
                        csv_data,
                        date_str,
                        *weekly_counts)

    util.write_dict_values_sorted_to_csv(
        csv_file_abs,
        csv_data,
        header=CSV_HEADER)


def update_monthly_csv(target_dir_abs, dbname, csv_data_input, first_date,
                       last_date, bad_dates=[], force_recomputation=False):
    """Updates monthly per project CSVs from a csv data dictionary.

    The existing per project CSV files in target_dir_abs/monthly_rescaled are
    updated for all months where the last day of the month is in the date
    interval from first_date up to (and including) last_date.

    For monthly aggregations, a month's total data is rescaled to 30 days.

    If a month under consideration contains no good date, it is removed.

    Upon any error, the function raises an exception.

    :param target_dir_abs: Absolute directory. CSVs are getting written to the
        'monthly_rescaled' subdirectory of target_dir_abs.
    :param dbname: The database name of the wiki to consider (E.g.: 'enwiki')
    :param csv_data_input: The data dict to aggregate from
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param bad_dates: List of dates considered having bad data. (Default: [])
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    """
    csv_dir_abs = os.path.join(target_dir_abs, 'monthly_rescaled')
    if not os.path.exists(csv_dir_abs):
        os.mkdir(csv_dir_abs)
    csv_file_abs = os.path.join(csv_dir_abs, dbname + '.csv')

    csv_data = util.parse_csv_to_first_column_dict(csv_file_abs)

    for date in util.generate_dates(first_date, last_date):
        if (date + datetime.timedelta(days=1)).day == 1:
            # date + 1 day is the first of a month, so date is the last of a
            # month. Let's compute for this month
            date_str = date.strftime('%Y-%m')
            logging.debug("Updating csv '%s' for date '%s'" % (
                dbname, date_str))
            days_in_month = date.day
            month_dates = set(datetime.date(date.year, date.month, day)
                              for day in range(1, days_in_month+1))
            expected_good_dates = len(month_dates - set(bad_dates))
            need_recomputation = force_recomputation
            need_recomputation |= expected_good_dates != days_in_month
            need_recomputation |= date_str not in csv_data
            if need_recomputation:
                if expected_good_dates == 0:
                    try:
                        del csv_data[date_str]
                    except KeyError:
                        # No reading was there to remove. That's ok :-)
                        pass
                else:
                    monthly_counts = rescale_counts(csv_data_input,
                                                    month_dates, bad_dates, 30)
                    util.update_csv_data_dict(
                        csv_data,
                        date_str,
                        *monthly_counts)

    util.write_dict_values_sorted_to_csv(
        csv_file_abs,
        csv_data,
        header=CSV_HEADER)


def update_yearly_csv(target_dir_abs, dbname, csv_data_input, first_date,
                      last_date, bad_dates=[], force_recomputation=False):
    """Updates yearly per project CSVs from a csv data dictionary.

    The existing per project CSV files in target_dir_abs/yearly_rescaled are
    updated for all years where the last day of the year is in the date
    interval from first_date up to (and including) last_date.

    For yearly aggregations, a year's total data is rescaled to 365 days.

    If a year under consideration contains no good date, it is removed.

    Upon any error, the function raises an exception.

    :param target_dir_abs: Absolute directory. CSVs are getting written to the
        'yearly_rescaled' subdirectory of target_dir_abs.
    :param dbname: The database name of the wiki to consider (E.g.: 'enwiki')
    :param csv_data_input: The data dict to aggregate from
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param bad_dates: List of dates considered having bad data. (Default: [])
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    """
    csv_dir_abs = os.path.join(target_dir_abs, 'yearly_rescaled')
    if not os.path.exists(csv_dir_abs):
        os.mkdir(csv_dir_abs)
    csv_file_abs = os.path.join(csv_dir_abs, dbname + '.csv')

    csv_data = util.parse_csv_to_first_column_dict(csv_file_abs)

    for date in util.generate_dates(first_date, last_date):
        if date.month == 12 and date.day == 31:
            # date is the last day of a year. Let's compute for this year
            date_str = date.strftime('%Y')
            logging.debug("Updating csv '%s' for date '%s'" % (
                dbname, date_str))
            days_in_year = 366 if calendar.isleap(date.year) else 365
            year_dates = set(date - datetime.timedelta(days=offset)
                             for offset in range(0, days_in_year))
            expected_good_dates = len(year_dates - set(bad_dates))
            need_recomputation = force_recomputation
            need_recomputation |= expected_good_dates != days_in_year
            need_recomputation |= date_str not in csv_data
            if need_recomputation:
                if expected_good_dates == 0:
                    try:
                        del csv_data[date_str]
                    except KeyError:
                        # No reading was there to remove. That's ok :-)
                        pass
                else:
                    yearly_counts = rescale_counts(csv_data_input, year_dates,
                                                   bad_dates, 365)
                    util.update_csv_data_dict(
                        csv_data,
                        date_str,
                        *yearly_counts)

    util.write_dict_values_sorted_to_csv(
        csv_file_abs,
        csv_data,
        header=CSV_HEADER)


def update_per_project_csvs_for_dates(
        source_dir_abs, target_dir_abs, first_date, last_date,
        bad_dates=[], additional_aggregators=[], force_recomputation=False,
        compute_all_projects=False, output_projectviews=False):
    """Updates per project CSVs from hourly projectcounts files.

    The existing per project CSV files in the daily_raw subdirectory of
    target_dir_abs are updated with daily data from first_date up to (and
    including) last_date.

    If the CSVs already has data for a given day, it is not recomputed, unless
    force_recomputation is True.

    Upon any error, the function raises an exception without cleaning or
    syncing up the CSVs. So if the first CSV could get updated, but there are
    issues with the second, the data written to the first CSV survives. Hence,
    the CSVs need not end with the same date upon error.

    :param source_dir_abs: Absolute directory to read the hourly projectcounts
        files from.
    :param target_dir_abs: Absolute directory of the per project CSVs.
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param bad_dates: List of dates considered having bad data. (Default: [])
    :param additionaly_aggregators: List of functions to additionally
        aggregate with. Those functions need to take target_dir_abs,
        dbname, csv_data_input, first_date, last_date, bad_dates, and
        force_recomputation as paramaters (in that order). dbname is
        the database name for the wiki to aggregate for, and
        csv_data_input is the CSV data dictionary for the daily_raw
        aggregation. The other parameters and just passed
        through. (Default: [])
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    :param compute_all_projects: If True, compute counts for all projects
        into a file named 'all.csv'.
    :param output_projectviews: If True, name the output files projectviews
        instead of projectcounts. (Default: False)
    """
    # Contains the aggregation of all data across projects indexed by date.
    all_projects_data = {}

    for csv_file_abs in sorted(glob.glob(os.path.join(
            target_dir_abs, 'daily_raw', '*.csv'))):
        dbname = os.path.basename(csv_file_abs)
        dbname = dbname.rsplit('.csv', 1)[0]

        if dbname == 'all':
            # 'all.csv' is an aggregation across all projects
            # and should not be processed.
            continue

        logging.info("Updating csv '%s'" % (csv_file_abs))

        csv_data = util.parse_csv_to_first_column_dict(csv_file_abs)

        for date in util.generate_dates(first_date, last_date):
            date_str = date.isoformat()
            logging.debug("Updating csv '%s' for date '%s'" % (
                dbname, str(date)))
            if date_str not in csv_data or force_recomputation:
                # Check if to allow bad data for this day
                allow_bad_data = date in bad_dates

                # desktop site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'desktop')
                count_desktop = get_daily_count(
                    source_dir_abs, abbreviation, date,
                    allow_bad_data, output_projectviews,
                )

                # mobile site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'mobile')
                count_mobile = get_daily_count(
                    source_dir_abs, abbreviation, date,
                    allow_bad_data, output_projectviews,
                )

                # zero site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'zero')
                count_zero = get_daily_count(
                    source_dir_abs, abbreviation, date,
                    allow_bad_data, output_projectviews,
                )

                count_total = count_desktop
                if date >= DATE_MOBILE_ADDED:
                    count_total += count_mobile + count_zero

                # injecting obtained data
                util.update_csv_data_dict(
                    csv_data,
                    date_str,
                    count_total,
                    count_desktop,
                    count_mobile if date >= DATE_MOBILE_ADDED else None,
                    count_zero if date >= DATE_MOBILE_ADDED else None)

        _write_raw_and_aggregated_csv_data(
            target_dir_abs,
            dbname,
            csv_data,
            first_date,
            last_date,
            additional_aggregators,
            bad_dates,
            force_recomputation)

        # Aggregates values across all projects
        if compute_all_projects:
            util.merge_sum_csv_data_dict(all_projects_data, csv_data)

    # Writes aggregations across all projects
    if compute_all_projects:
        oldest_date = util.parse_string_to_date(min(all_projects_data.keys()))
        newest_date = util.parse_string_to_date(max(all_projects_data.keys()))
        _write_raw_and_aggregated_csv_data(
            target_dir_abs,
            'all',
            all_projects_data,
            oldest_date,
            newest_date,
            additional_aggregators,
            bad_dates,
            force_recomputation)


def _write_raw_and_aggregated_csv_data(
        target_dir_abs, dbname, csv_data, first_date, last_date,
        additional_aggregators, bad_dates, force_recomputation):
    """
    Writes the data passed in the csv_data dict to various destinations:

    1. Writes the raw data (as it comes in the csv_data dict), to:
       <target_dir_abs>/daily_raw/<dbname>.csv

    2. Uses each aggregator in additional_aggregators to write the data
       to the aggregator's specific location. Note: Some of this method's
       parameters are just forwarded to these aggregators.

    :param target_dir_abs: Absolute directory of the per project CSVs.
    :param dbname: The database name of the wiki to consider (E.g.: 'enwiki')
    :param csv_data: The dict containing the data to be written.
    :param first_date: The first date to write non-existing data for.
    :param last_date: The last date to write non-existing data for.
    :param additional_aggregators: See update_per_project_csvs_for_dates.
    :param bad_dates: List of dates considered having bad data.
    :param force_recomputation: If True, recompute data for the given days.
    """
    csv_file_abs = os.path.join(target_dir_abs, 'daily_raw', dbname + '.csv')

    util.write_dict_values_sorted_to_csv(
        csv_file_abs,
        csv_data,
        header=CSV_HEADER)

    for additional_aggregator in additional_aggregators:
        additional_aggregator(
            target_dir_abs,
            dbname,
            csv_data,
            first_date,
            last_date,
            bad_dates=bad_dates,
            force_recomputation=force_recomputation)


def _get_validity_issues_for_aggregated_projectcounts_generic(
        csv_dir_abs, total_expected, desktop_site_expected,
        mobile_site_expected, zero_site_expected, current_date_strs):
    """Gets a list of obvious validity issues for a directory of CSVs

    :param csv_dir_abs: Absolute directory of the per project CSVs.
    :param total_expected: Expect at least that many views to overal on
        big wikis.
    :param desktop_site_expected: Expect at least that many views to desktop
        site on big wikis.
    :param mobile_site_expected: Expect at least that many views to mobile site
        on big wikis.
    :param zero_site_expected: Expect at least that many views to zero site on
        big wikis.
    :param current_date_strs: Expect one of those as date string of the final
        item in the CSVs.
    """
    issues = []
    dbnames = []

    big_wikis = [
        'enwiki',
        'jawiki',
        'dewiki',
        'eswiki',
        'frwiki',
        'ruwiki',
        'itwiki',
        ]

    for csv_file_abs in sorted(glob.glob(os.path.join(csv_dir_abs, '*.csv'))):
        logging.info("Checking csv '%s'" % (csv_file_abs))

        dbname = os.path.basename(csv_file_abs)
        dbname = dbname.rsplit('.csv', 1)[0]
        dbnames.append(dbname)

        with open(csv_file_abs, 'r') as file:
            lines = file.readlines()

            if len(lines):
                # Analyze last line
                last_line = (lines[-1]).rstrip('\n\r')
                last_line_split = last_line.split(',')
                if len(last_line_split) == 5:
                    # Check if last line is current.
                    try:
                        if last_line_split[0] not in current_date_strs:
                            issues.append("Last line of %s is too old "
                                          "'%s'" % (csv_file_abs, last_line))
                    except ValueError:
                        issues.append("Last line of %s is too old "
                                      "'%s'" % (csv_file_abs, last_line))

                    if dbname in big_wikis:
                        # Check total count
                        try:
                            if int(last_line_split[1]) < total_expected:
                                issues.append("Total count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Total count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check desktop count
                        try:
                            if int(last_line_split[2]) < desktop_site_expected:
                                issues.append("Desktop count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Desktop count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check mobile count
                        try:
                            if int(last_line_split[3]) < mobile_site_expected:
                                issues.append("Mobile count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Mobile count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check zero count
                        try:
                            if int(last_line_split[4]) < zero_site_expected:
                                issues.append("Zero count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Desktop count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check zero count
                        try:
                            if int(last_line_split[1]) != \
                                    int(last_line_split[2]) + \
                                    int(last_line_split[3]) + \
                                    int(last_line_split[4]):
                                issues.append(
                                    "Total column is not the sum of "
                                    "individual columns in '%s' for %s" % (
                                        last_line, csv_file_abs))
                        except ValueError:
                            # Some column is not a number. This has already
                            # been reported above, so we just pass.
                            pass

                else:
                    issues.append("Last line of %s does not have 5 columns: "
                                  "'%s'" % (csv_file_abs, last_line))
            else:
                issues.append("No lines for %s" % csv_file_abs)

    if not len(dbnames):
        issues.append("Could not find any CSVs")

    if set(big_wikis) - set(dbnames):
        issues.append("Not all big wikis covered (Missing: %s)" % (
            [x for x in (set(big_wikis) - set(dbnames))]))

    if not (set(dbnames) - set(big_wikis)):
        issues.append("No wikis beyond the big wikis")

    return sorted(issues)


def get_validity_issues_for_aggregated_projectcounts(data_dir_abs):
    """Gets a list of obvious validity issues of aggregated projectcount CSVs

    :param data_dir_abs: Absolute directory of the per project CSVs.
    """
    issues = []

    current_dates = [
        datetime.date.today(),
        util.parse_string_to_date('yesterday')
        ]

    # daily_raw files
    issues.extend(_get_validity_issues_for_aggregated_projectcounts_generic(
        os.path.join(data_dir_abs, 'daily_raw'),
        1000000, 1000000, 10000, 100,
        set(date.isoformat() for date in current_dates)
        ))

    # daily files
    issues.extend(_get_validity_issues_for_aggregated_projectcounts_generic(
        os.path.join(data_dir_abs, 'daily'),
        1000000, 1000000, 10000, 100,
        set(date.isoformat() for date in current_dates)
        ))

    # weekly files
    issues.extend(_get_validity_issues_for_aggregated_projectcounts_generic(
        os.path.join(data_dir_abs, 'weekly_rescaled'),
        10000000, 10000000, 100000, 1000,
        set((date - datetime.timedelta(days=6)).strftime('%GW%V')
            for date in current_dates)
        ))

    # monthly files
    issues.extend(_get_validity_issues_for_aggregated_projectcounts_generic(
        os.path.join(data_dir_abs, 'monthly_rescaled'),
        50000000, 50000000, 500000, 5000,
        set((
            datetime.date(date.year, date.month, 1)
            - datetime.timedelta(days=1)
            ).strftime('%Y-%m')
            for date in current_dates)
        ))

    # yearly files
    issues.extend(_get_validity_issues_for_aggregated_projectcounts_generic(
        os.path.join(data_dir_abs, 'yearly_rescaled'),
        700000000, 700000000, 7000000, 70000,
        set((
            datetime.date(date.year, 1, 1) - datetime.timedelta(days=1)
            ).strftime('%Y')
            for date in current_dates)
        ))
    return issues
