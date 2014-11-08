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
  util unit tests
  ~~~~~~~~~~~~~~~

  This module contains tests for aggregator.util.

"""

import aggregator
import unittest
import nose
import datetime
import os


class UtilTestCase(unittest.TestCase):
    def test_parse_string_to_date_yesterday(self):
        actual = aggregator.parse_string_to_date('yesterday')

        expected = (datetime.date.today() - datetime.timedelta(days=1))
        self.assertEqual(actual, expected)

    def test_parse_string_to_date_2014_11_08(self):
        actual = aggregator.parse_string_to_date('2014-11-08')

        expected = datetime.date(2014, 11, 8)
        self.assertEqual(actual, expected)

    def test_parse_string_to_date_bogus_date(self):
        nose.tools.assert_raises(ValueError,
                                 aggregator.parse_string_to_date, 'foo')

    def test_existing_dir_abs_cwd(self):
        actual = aggregator.existing_dir_abs('.')

        expected = os.getcwd()
        self.assertEqual(actual, expected)

    def test_existing_dir_abs_non_existing_dir(self):
        nose.tools.assert_raises(ValueError,
                                 aggregator.existing_dir_abs,
                                 'non_exsting_dir')

    def test_generate_dates_same_date(self):
        date = datetime.date(2014, 11, 2)

        actual_generator = aggregator.generate_dates(date, date)
        actual_list = [i for i in actual_generator]

        expected = [date]
        self.assertEqual(actual_list, expected)

    def test_generate_dates_two_days(self):
        first_date = datetime.date(2014, 11, 1)
        last_date = datetime.date(2014, 11, 2)

        actual_generator = aggregator.generate_dates(first_date, last_date)
        actual_list = [i for i in actual_generator]

        expected = [first_date, last_date]
        self.assertEqual(actual_list, expected)

    def test_generate_dates_more_days(self):
        first_date = datetime.date(2014, 11, 10)
        last_date = datetime.date(2014, 11, 15)

        actual_generator = aggregator.generate_dates(first_date, last_date)
        actual_list = [i for i in actual_generator]

        expected = [
            datetime.date(2014, 11, 10),
            datetime.date(2014, 11, 11),
            datetime.date(2014, 11, 12),
            datetime.date(2014, 11, 13),
            datetime.date(2014, 11, 14),
            datetime.date(2014, 11, 15),
            ]
        self.assertEqual(actual_list, expected)

    def test_dbname_to_webstatscollector_abbreviation_enwiki(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation('enwiki')
        self.assertEqual(actual, 'en')

    def test_dbname_to_webstatscollector_abbreviation_enwiki_desktop(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'enwiki', 'desktop')
        self.assertEqual(actual, 'en')

    def test_dbname_to_webstatscollector_abbreviation_enwiki_mobile(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'enwiki', 'mobile')
        self.assertEqual(actual, 'en.m')

    def test_dbname_to_webstatscollector_abbreviation_enwiki_zero(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'enwiki', 'zero')
        self.assertEqual(actual, 'en.zero')

    def test_dbname_to_webstatscollector_abbreviation_wikibooks(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'dewikibooks')
        self.assertEqual(actual, 'de.b')

    def test_dbname_to_webstatscollector_abbreviation_wikibooks_desktop(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'dewikibooks', 'desktop')
        self.assertEqual(actual, 'de.b')

    def test_dbname_to_webstatscollector_abbreviation_wikibooks_mobile(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'dewikibooks', 'mobile')
        self.assertEqual(actual, 'de.m.b')

    def test_dbname_to_webstatscollector_abbreviation_wikibooks_zero(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'dewikibooks', 'zero')
        self.assertEqual(actual, 'de.zero.b')

    def test_dbname_to_webstatscollector_abbreviation_wiktionary(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'grwiktionary')
        self.assertEqual(actual, 'gr.d')

    def test_dbname_to_webstatscollector_abbreviation_wikinews(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'frwikinews')
        self.assertEqual(actual, 'fr.n')

    def test_dbname_to_webstatscollector_abbreviation_wikiquote(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'eswikiquote')
        self.assertEqual(actual, 'es.q')

    def test_dbname_to_webstatscollector_abbreviation_wikisource(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'zh_min_nanwikisource')
        self.assertEqual(actual, 'zh-min-nan.s')

    def test_dbname_to_webstatscollector_abbreviation_wikiversity(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'kowikiversity')
        self.assertEqual(actual, 'ko.v')

    def test_dbname_to_webstatscollector_abbreviation_wikivoyage(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'hewikivoyage')
        self.assertEqual(actual, 'he.voy')

    def test_dbname_to_webstatscollector_abbreviation_mediawikiwiki(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'mediawikiwiki')
        self.assertEqual(actual, 'www.w')

    def test_dbname_to_webstatscollector_abbreviation_wikidata(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'wikidatawiki')
        self.assertEqual(actual, 'www.wd')

    def test_dbname_to_webstatscollector_abbreviation_commons(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'commonswiki')
        self.assertEqual(actual, 'commons.m')

    def test_dbname_to_webstatscollector_abbreviation_commons_mobile(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'commonswiki', 'mobile')
        self.assertEqual(actual, 'commons.m.m')

    def test_dbname_to_webstatscollector_abbreviation_commons_zero(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'commonswiki', 'zero')
        self.assertEqual(actual, 'commons.zero.m')

    def test_dbname_to_webstatscollector_abbreviation_meta(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'metawiki')
        self.assertEqual(actual, 'meta.m')

    def test_dbname_to_webstatscollector_abbreviation_incubator(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'incubatorwiki')
        self.assertEqual(actual, 'incubator.m')

    def test_dbname_to_webstatscollector_abbreviation_species(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'specieswiki')
        self.assertEqual(actual, 'species.m')

    def test_dbname_to_webstatscollector_abbreviation_strategy(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'strategywiki')
        self.assertEqual(actual, 'strategy.m')

    def test_dbname_to_webstatscollector_abbreviation_outreach(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'outreachwiki')
        self.assertEqual(actual, 'outreach.m')

    def test_dbname_to_webstatscollector_abbreviation_usability(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'usabilitywiki')
        self.assertEqual(actual, 'usability.m')

    def test_dbname_to_webstatscollector_abbreviation_quality(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'qualitywiki')
        self.assertEqual(actual, 'quality.m')

    def test_dbname_to_webstatscollector_abbreviation_foo(self):
        actual = aggregator.dbname_to_webstatscollector_abbreviation(
            'foo')
        self.assertEqual(actual, None)
