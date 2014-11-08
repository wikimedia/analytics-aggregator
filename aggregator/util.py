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
    util
    ~~~~

    This module contains general utility functions.
"""

import datetime
import os

WEBSTATSCOLLECTOR_WHITELISTED_WIKIMEDIA_WIKIS = [
    'commons',
    'meta',
    'incubator',
    'species',
    'strategy',
    'outreach',
    'usability',
    'quality',
]

WEBSTATSCOLLECTOR_SUFFIX_ABBREVIATIONS = [
    # Using a list (not an object), as order is important, as we
    # consider the first match a win.
    ('foundationwiki', '.f'),
    ('mediawikiwiki', '.w'),
    ('wikidatawiki', '.wd'),
    ('wikibooks', '.b'),
    ('wiktionary', '.d'),
    ('wikimedia', '.m'),
    ('wikinews', '.n'),
    ('wikiquote', '.q'),
    ('wikisource', '.s'),
    ('wikiversity', '.v'),
    ('wikivoyage', '.voy'),
    # Have generic wiki last
    ('wiki', ''),
]


def parse_string_to_date(date_str):
    """Parse a string into a datetime.date.

    If the string cannot get parsed to a date, a ValueError is raised.

    :param date_str: String to parse to a datetime.date
    """
    if date_str == 'yesterday':
        return datetime.date.today() - datetime.timedelta(days=1)

    # Try to parse ISO date
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Could not parse '%s' as date" % (date_str))


def existing_dir_abs(dir):
    """Turns a directory name into an absolute directory name

    If the given directory does not exist, ValueError is raised

    :param dir: The directory to get the absolute name for
    """
    ret = os.path.abspath(dir)
    if not os.path.isdir(ret):
        raise ValueError("'dir' does not point to an existing directory")
    return ret


def generate_dates(first_date, last_date):
    """Generates all dates between two dates (including borders).

    If first_date > last_date, an infinite increasing sequence of
    dates is generated. So make sure that first_date <= last_date.

    :param first_date: The first date to generate
    :param last_date: The last date to generate
    """
    date = first_date
    while date <= last_date:
        yield date
        date += datetime.timedelta(days=1)


def dbname_to_webstatscollector_abbreviation(dbname, site='desktop'):
    """
    Gets the webstatscollector abbreviation for a site's database name

    If no webstatscollector abbreviation could be found, None is returned.

    :param dbname: The data base name for the wiki (e.g.: 'enwiki')
    :param site: The site to get the abbreviation for. Either 'desktop',
        'mobile', or 'zero'. (Default: 'desktop')
    """
    for (dbname_ending, new_ending) in WEBSTATSCOLLECTOR_SUFFIX_ABBREVIATIONS:
        if dbname.endswith(dbname_ending):
            # replacing last occurrence of dbname's ending with new_ending
            abbreviation = dbname.rsplit(dbname_ending, 1)[0] + new_ending

            # dbnames use “_” where webstatscollector uses “-”.
            abbreviation = abbreviation.replace('_', '-')

            # prepend www if it is just the root project to catch things like
            # wikidatawiki being served at www.wikidata.org
            if abbreviation.startswith('.'):
                abbreviation = "www" + abbreviation

            # Fix-up for wikimedia.org wikis
            if abbreviation in WEBSTATSCOLLECTOR_WHITELISTED_WIKIMEDIA_WIKIS:
                abbreviation += ".m"

            # Inject site modifier
            if site != 'desktop':  # desktop has no modifier -> short-circuit
                abbreviation_split = abbreviation.split('.')
                if site == 'mobile':
                    abbreviation_split.insert(1, 'm')
                elif site == 'zero':
                    abbreviation_split.insert(1, 'zero')
                abbreviation = '.'.join(abbreviation_split)

            return abbreviation
    return None
