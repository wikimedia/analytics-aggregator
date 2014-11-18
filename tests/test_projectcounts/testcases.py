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
  Test case base classes
  ~~~~~~~~~~~~~~~~~~~~~~

  This module contains bas classes for test cases.

"""

import aggregator
import unittest
import os
import tempfile
import logging
import shutil

FIXTURES_DIR_ABS = os.path.join(os.path.dirname(__file__), "fixtures")


class ProjectcountsTestCase(unittest.TestCase):
    def get_fixture_dir_abs(self, fixture_name):
        return os.path.join(FIXTURES_DIR_ABS, fixture_name)

    def create_tmp_dir_abs(self):
        # Since we have to have the file visible in the file system, we cannot
        # use *TemporaryFile, and have to resort to mkdtemp
        tmp_dir_abs = tempfile.mkdtemp(prefix='test_projectcounts')

        logging.error("Creating tmp directory '%s'" % (
            tmp_dir_abs))

        try:
            self.tmp_dirs_abs.append(tmp_dir_abs)
        except AttributeError:
            self.tmp_dirs_abs = [tmp_dir_abs]

        return tmp_dir_abs

    def create_empty_file(self, file_abs):
        open(os.path.join(file_abs), 'w').close()

    def create_file(self, file_abs, lines):
        with open(file_abs, 'w') as file:
            for line in lines:
                file.write(line + aggregator.CSV_LINE_ENDING)

    def assert_file_content_equals(self, actual_file_abs, expected_lines):
        header = 'Date,Total,Desktop site,Mobile site,Zero site'
        expected_lines.insert(0, header)
        with open(actual_file_abs, 'r') as file:
            for expected_line in expected_lines:
                try:
                    self.assertEquals(file.next(), expected_line +
                                      aggregator.CSV_LINE_ENDING)
                except StopIteration:
                    self.fail("File '%s' is missing the line:\n%s" % (
                        actual_file_abs, expected_line))
            try:
                extra_line = file.next()
                self.fail("More lines than expected in file '%s'. First "
                          "extra line:\n%s" % (actual_file_abs, extra_line))
            except StopIteration:
                pass

    def setUp(self):
        super(ProjectcountsTestCase, self).setUp()
        aggregator.clear_cache()

    def tearDown(self):
        try:
            for tmp_dir_abs in self.tmp_dirs_abs:
                logging.error("Cleaning up tmp directory '%s'" % (
                    tmp_dir_abs))
                shutil.rmtree(tmp_dir_abs)
        except AttributeError:
            pass
        finally:
            super(ProjectcountsTestCase, self).tearDown()


class ProjectcountsDataTestCase(ProjectcountsTestCase):
    def setUp(self):
        super(ProjectcountsDataTestCase, self).setUp()
        self.data_dir_abs = self.create_tmp_dir_abs()

        self.daily_raw_dir_abs = os.path.join(self.data_dir_abs, 'daily_raw')
        os.mkdir(self.daily_raw_dir_abs)

        self.daily_dir_abs = os.path.join(self.data_dir_abs, 'daily')
        os.mkdir(self.daily_dir_abs)

        self.weekly_dir_abs = os.path.join(self.data_dir_abs,
                                           'weekly_rescaled')
        os.mkdir(self.weekly_dir_abs)
