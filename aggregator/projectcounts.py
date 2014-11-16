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

CSV_LINE_ENDING = '\r\n'

cache = {}


def clear_cache():
    global cache
    logging.debug("Clearing projectcounts cache")
    cache = {}


def aggregate_for_date(source_dir_abs, date):
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


def get_daily_count(source_dir_abs, webstatscollector_abbreviation, date):
    """Obtains the daily count for a webstatscollector abbreviation.

    Data gets cached upon read. For a day, the data is <50KB, so having many
    dates in cache is not resource intensive.

    :param source_dir_abs: Absolute directory to read the hourly projectcounts
        files from.
    :param webstatscollector_abbreviation: The webstatscollector abbreviation
        to get the count for
    :param date: The date to get the count for.
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
        date_data = aggregate_for_date(source_dir_abs, date)
        source_dir_cache[date] = date_data

    return date_data.get(webstatscollector_abbreviation, 0)


def update_per_project_csvs_for_dates(
        source_dir_abs, target_dir_abs, first_date, last_date,
        force_recomputation=False):
    """Updates daily per project CSVs from hourly projectcounts files.

    The existing per project CSV files in target_dir_abs are updated with daily
    data from first_date up to (and including) last_date.

    If a CSVs already has data for a given day, it is not recomputed.

    Upon any error, the function raises an exception without cleaning or
    syncing up the CSVs. So if the first CSV could get updated, but there are
    issues with the second, the data written to the first CSV survives. Hence,
    the CSVs need not end with the same date upon error.

    :param source_dir_abs: Absolute directory to read the hourly projectcounts
        files from.
    :param target_dir_abs: Absolute directory of the per project CSVs.
    :param first_date: The first date to compute non-existing data for.
    :param last_date: The last date to compute non-existing data for.
    :param force_recomputation: If True, recompute data for the given days,
        even if it is already in the CSV. (Default: False)
    """
    for csv_file_abs in sorted(glob.glob(os.path.join(
            target_dir_abs, '*.csv'))):
        logging.info("Updating csv '%s'" % (csv_file_abs))

        csv_data = {}

        dbname = os.path.basename(csv_file_abs)
        dbname = dbname.rsplit('.csv', 1)[0]

        with open(csv_file_abs, 'r') as csv_file:
            for line in csv_file:
                date_str = line.split(',')[0]
                if date_str in csv_data:
                    raise RuntimeError(
                        "CSV contains the date '%s' twice" % (date_str))

                if date_str != 'Date':
                    # No header line
                    csv_data[date_str] = line.strip() + CSV_LINE_ENDING

        for date in util.generate_dates(first_date, last_date):
            date_str = date.isoformat()
            logging.debug("Updating csv '%s' for date '%s'" % (
                dbname, str(date)))
            if date_str not in csv_data or force_recomputation:
                # desktop site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'desktop')
                count_desktop = get_daily_count(
                    source_dir_abs, abbreviation, date)

                # mobile site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'mobile')
                count_mobile = get_daily_count(
                    source_dir_abs, abbreviation, date)

                # zero site
                abbreviation = util.dbname_to_webstatscollector_abbreviation(
                    dbname, 'zero')
                count_zero = get_daily_count(
                    source_dir_abs, abbreviation, date)

                # injecting obtained data
                csv_data[date_str] = '%s,%d,%d,%d,%d%s' % (
                    date_str,
                    count_desktop + count_mobile + count_zero,
                    count_desktop,
                    count_mobile,
                    count_zero,
                    CSV_LINE_ENDING)

        with open(csv_file_abs, 'w') as csv_file:
            csv_file.write('Date,Total,Desktop site,Mobile site,Zero site%s'
                           % (CSV_LINE_ENDING))
            csv_file.writelines(sorted(csv_data.itervalues()))


def get_validity_issues_for_aggregated_projectcounts(data_dir_abs):
    """Gets a list of obvious validity issues of aggregated projectcount CSVs

    :param data_dir_abs: Absolute directory of the per project CSVs.
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

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    for csv_file_abs in sorted(glob.glob(os.path.join(data_dir_abs, '*.csv'))):
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
                    # Check if last line is not older than yesterday
                    try:
                        last_line_date = util.parse_string_to_date(
                            last_line_split[0])
                        if last_line_date < yesterday:
                            issues.append("Last line of %s is too old "
                                          "'%s'" % (csv_file_abs, last_line))
                    except ValueError:
                        issues.append("Last line of %s is too old "
                                      "'%s'" % (csv_file_abs, last_line))

                    if dbname in big_wikis:
                        # Check total count
                        try:
                            if int(last_line_split[1]) < 1000000:
                                issues.append("Total count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Total count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check desktop count
                        try:
                            if int(last_line_split[2]) < 1000000:
                                issues.append("Desktop count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Desktop count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check mobile count
                        try:
                            if int(last_line_split[3]) < 10000:
                                issues.append("Desktop count of last line of "
                                              "%s is too low '%s'" % (
                                                  csv_file_abs, last_line))
                        except ValueError:
                            issues.append("Mobile count of last line of %s is"
                                          "not an integer '%s'" % (
                                              csv_file_abs, last_line))

                        # Check zero count
                        try:
                            if int(last_line_split[4]) < 100:
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
