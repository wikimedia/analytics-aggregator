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
import datetime
import os
import glob
import util

PROJECTCOUNTS_STRFTIME_PATTERN = ('%%Y%s%%Y-%%m%sprojectcounts-%%Y%%m%%d-'
                                  '%%H0000' % (os.sep, os.sep))

CSV_HEADER = 'Date,Total,Desktop site,Mobile site,Zero site'

cache = {}


def clear_cache():
    global cache
    logging.debug("Clearing projectcounts cache")
    cache = {}


def aggregate_for_date(source_dir_abs, date, allow_bad_data=False):
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
    """
    daily_data = {}

    for hour in range(24):
        # Initialize with the relevant hour start ...
        hourly_file_datetime = datetime.datetime(date.year, date.month,
                                                 date.day, hour)
        # and add another hour since webstatscollector uses the interval end in
        # the file name :-(
        hourly_file_datetime += datetime.timedelta(hours=1)

        hourly_file_abs = os.path.join(
            source_dir_abs,
            hourly_file_datetime.strftime(PROJECTCOUNTS_STRFTIME_PATTERN))

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
                    raise RuntimeError("Malformed line in '%s'" % (
                        hourly_file))

                abbreviation = fields[0].lower()
                count = int(fields[2])

                daily_data[abbreviation] = daily_data.get(abbreviation, 0) \
                    + count

    return daily_data


def get_daily_count(source_dir_abs, webstatscollector_abbreviation, date,
                    allow_bad_data=False):
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
        date_data = aggregate_for_date(source_dir_abs, date, allow_bad_data)
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

    All dates are expected to have the same number of columns. In case they
    have not, the first good date is taken as reference. Missing columns for
    good dates are assumed to be 0.

    Upon other errors, a RuntimeError is raised.

    The rescaled counts are returned as list of integers.

    :param csv_data_input: The data dict to get data from
    :param dates: The dates to sum up counts for
    :param bad_dates: List of dates considered having bad data.
    :param rescale_to: Rescale the good entries to this many entries.
    """
    ret = None
    aggregations = 0
    for date in dates:
        if date in bad_dates:
            continue
        date_str = date.isoformat()
        try:
            csv_line_items = csv_data[date_str].split(',')
        except KeyError:
            raise RuntimeError("No data for '%s'" % (date_str))
        del csv_line_items[0]  # getting rid of date column
        if ret is None:
            ret = [0 for i in range(len(csv_line_items))]
        for i in range(len(ret)):
            try:
                ret[i] += int(csv_line_items[i])
            except IndexError:
                # csv_line_items has less items than the first good row.
                # We assume 0.
                pass
        aggregations += 1

    if ret is not None:
        # Since we found readings, rescale.
        ret = [(ret[i] * rescale_to) / aggregations for i in range(len(ret))]
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


def update_per_project_csvs_for_dates(
        source_dir_abs, target_dir_abs, first_date, last_date,
        bad_dates=[], additional_aggregators=[], force_recomputation=False):
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
    """
    for csv_file_abs in sorted(glob.glob(os.path.join(
            target_dir_abs, 'daily_raw', '*.csv'))):
        logging.info("Updating csv '%s'" % (csv_file_abs))

        dbname = os.path.basename(csv_file_abs)
        dbname = dbname.rsplit('.csv', 1)[0]

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
                    source_dir_abs, abbreviation, date, allow_bad_data)

                # mobile site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'mobile')
                count_mobile = get_daily_count(
                    source_dir_abs, abbreviation, date, allow_bad_data)

                # zero site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'zero')
                count_zero = get_daily_count(
                    source_dir_abs, abbreviation, date, allow_bad_data)

                # injecting obtained data
                util.update_csv_data_dict(
                    csv_data,
                    date_str,
                    count_desktop + count_mobile + count_zero,
                    count_desktop,
                    count_mobile,
                    count_zero)

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
                last_line = (lines[-1]).split('\n', 1)[0]  # Since the file is
                # opened in text mode by default, line ends are normalized to
                # LF, event though CRLF gets written.
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
    logging.error(set(date.isoformat() for date in current_dates))
    for x in set(date.isoformat() for date in current_dates):
        logging.error(x)
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
    return issues
