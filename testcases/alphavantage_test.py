# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Juan Salvador Magán Valero <jmaganvalero@gmail.com>
"""

import os
import datetime
import tempfile
import shutil
import subprocess

import six

from . import common

from pyalgotrade.tools import alphavantage
from pyalgotrade import bar
from pyalgotrade.barfeed import alphavantagefeed
from pyalgotrade.barfeed import csvfeed

try:
    # This will get environment variables set.
    from . import credentials
except:
    pass


ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
assert ALPHA_VANTAGE_API_KEY is not None, "ALPHA_VANTAGE_API_KEY not set"


def bytes_to_str(b):
    ret = b
    if six.PY3:
        # Convert the bytes to a string.
        ret = ret.decode()
    return ret


def check_output(*args, **kwargs):
    ret = subprocess.check_output(*args, **kwargs)
    if six.PY3:
        # Convert the bytes to a string.
        ret = bytes_to_str(ret)
    return ret


class ToolsTestCase(common.TestCase):

    def testDownloadAndParseDailyUsingApiKey(self):
        with common.TmpDir() as tmpPath:
            instrument = "ORCL"
            path = os.path.join(tmpPath, "alpha-vantage-daily-orcl.csv")
            alphavantage.download_daily_bars(instrument, path, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed()
            bf.addBarsFromCSV(instrument, path)
            bf.loadAll()

            for b in bf[instrument]:
                if b.getDateTime() == datetime.datetime(2019, 12, 17):
                    self.assertEqual(b.getOpen(), 53.89)
                    self.assertEqual(b.getHigh(), 54.06)
                    self.assertEqual(b.getLow(), 52.83)
                    self.assertEqual(b.getClose(), 52.84)
                    self.assertEqual(b.getVolume(), 19778245.0)
                    self.assertEqual(b.getPrice(), 52.84)
            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEquals(bf[instrument][-1].getAdjClose(), None)

    def testDownloadAndParseDaily_UseAdjClose(self):
        with common.TmpDir() as tmpPath:
            instrument = "ORCL"
            path = os.path.join(tmpPath, "alpha-vantage-daily-orcl.csv")
            alphavantage.download_daily_bars(instrument, path, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed()
            bf.addBarsFromCSV(instrument, path)
            # Need to setUseAdjustedValues(True) after loading the file because we
            # can't tell in advance if adjusted values are there or not.
            bf.setUseAdjustedValues(True)
            bf.loadAll()
            for b in bf[instrument]:
                if b.getDateTime() == datetime.datetime(2019, 12, 17):
                    self.assertEqual(b.getOpen(), 53.89)
                    self.assertEqual(b.getHigh(), 54.06)
                    self.assertEqual(b.getLow(), 52.83)
                    self.assertEqual(b.getClose(), 52.84)
                    self.assertEqual(b.getVolume(), 19778245.0)
                    self.assertEqual(b.getPrice(), 52.84)
                    self.assertEqual(b.getPrice(), b.getAdjClose())

            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEquals(bf[instrument][-1].getAdjClose(), None)

    def testDownloadAndParseDailyNoAdjClose(self):
        with common.TmpDir() as tmpPath:
            instrument = "ORCL"
            path = os.path.join(tmpPath, "alpha-vantage-daily-%s.csv" % (instrument,))
            alphavantage.download_daily_bars(instrument, path, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed()
            bf.setNoAdjClose()
            bf.addBarsFromCSV(instrument, path, skipMalformedBars=True)
            bf.loadAll()

            for b in bf[instrument]:
                if b.getDateTime() == datetime.datetime(2019, 12, 17):
                    self.assertEqual(b.getOpen(), 53.89)
                    self.assertEqual(b.getHigh(), 54.06)
                    self.assertEqual(b.getLow(), 52.83)
                    self.assertEqual(b.getClose(), 52.84)
                    self.assertEqual(b.getVolume(), 19778245.0)
                    self.assertEqual(b.getAdjClose(), None)
                    self.assertEqual(b.getPrice(), 52.84)

    def testDownloadAndParseWeekly(self):
        with common.TmpDir() as tmpPath:
            instrument = "AAPL"
            path = os.path.join(tmpPath, "alpha-vantage-aapl-weekly.csv")
            alphavantage.download_weekly_bars(instrument, path, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed(frequency=bar.Frequency.WEEK)
            bf.setBarFilter(csvfeed.DateRangeFilter(fromDate=datetime.datetime(2010, 1, 1),
                                                    toDate=datetime.datetime(2010, 12, 31)))
            bf.addBarsFromCSV(instrument, path)
            bf.loadAll()

            # Alpha Vantage used to report 2010-1-8 as the first week of 2010.
            self.assertTrue(
                bf[instrument][0].getDateTime() in [datetime.datetime(2010, 1, 8), datetime.datetime(2010, 1, 15)]
            )

            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 322.8519)
            self.assertEqual(bf[instrument][-1].getHigh(), 326.66)
            self.assertEqual(bf[instrument][-1].getLow(), 321.31)
            self.assertEqual(bf[instrument][-1].getClose(), 322.56)
            self.assertEqual(bf[instrument][-1].getVolume(), 33567200.0)
            self.assertEqual(bf[instrument][-1].getPrice(), 322.56)
            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEquals(bf[instrument][-1].getAdjClose(), None)

    def testDownloadAndParseHourly(self):
        with common.TmpDir() as tmpPath:
            instrument = "AAPL"
            path = os.path.join(tmpPath, "alpha-vantage-aapl-hourly.csv")
            alphavantage.download_intradays_bars(instrument, path, bar.Frequency.HOUR, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed(frequency=bar.Frequency.HOUR)
            bf.addBarsFromCSV(instrument, path)
            bf.loadAll()

            self.assertIsNotNone(bf[instrument][-1].getDateTime())
            self.assertIsNotNone(bf[instrument][-1].getOpen())
            self.assertIsNotNone(bf[instrument][-1].getHigh())
            self.assertIsNotNone(bf[instrument][-1].getLow())
            self.assertIsNotNone(bf[instrument][-1].getClose())
            self.assertIsNotNone(bf[instrument][-1].getVolume())
            self.assertIsNotNone(bf[instrument][-1].getPrice())
            # Not adjusted price in intraday bars
            self.assertIsNone(bf[instrument][-1].getAdjClose())

    def testDownloadAndParseMinutes(self):
        with common.TmpDir() as tmpPath:
            instrument = "AAPL"
            path = os.path.join(tmpPath, "alpha-vantage-aapl-minute.csv")
            alphavantage.download_intradays_bars(instrument, path, bar.Frequency.MINUTE, apiKey=ALPHA_VANTAGE_API_KEY)
            bf = alphavantagefeed.Feed(frequency=bar.Frequency.MINUTE)
            bf.addBarsFromCSV(instrument, path)
            bf.loadAll()

            self.assertIsNotNone(bf[instrument][-1].getDateTime())
            self.assertIsNotNone(bf[instrument][-1].getOpen())
            self.assertIsNotNone(bf[instrument][-1].getHigh())
            self.assertIsNotNone(bf[instrument][-1].getLow())
            self.assertIsNotNone(bf[instrument][-1].getClose())
            self.assertIsNotNone(bf[instrument][-1].getVolume())
            self.assertIsNotNone(bf[instrument][-1].getPrice())
            # Not adjusted price in intraday bars
            self.assertIsNone(bf[instrument][-1].getAdjClose())

    def testInvalidFrequency(self):
        with self.assertRaisesRegexp(Exception, "Invalid frequency.*"):
            alphavantagefeed.Feed(frequency=bar.Frequency.SECOND)

    def testBuildFeedDaily(self):
        with common.TmpDir() as tmpPath:
            instrument = "ORCL"
            bf = alphavantage.build_feed([instrument], tmpPath, datetime.datetime(2010, 1, 1),
                                         datetime.datetime(2010, 12, 31), apiKey=ALPHA_VANTAGE_API_KEY)
            bf.loadAll()

            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 31.22)
            self.assertEqual(bf[instrument][-1].getHigh(), 31.33)
            self.assertEqual(bf[instrument][-1].getLow(), 30.93)
            self.assertEqual(bf[instrument][-1].getClose(), 31.3)
            self.assertEqual(bf[instrument][-1].getVolume(), 11716300)
            self.assertEqual(bf[instrument][-1].getPrice(), 31.3)
            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEqual(bf[instrument][-1].getAdjClose(), None)

    def testBuildFeedWeekly(self):
        with common.TmpDir() as tmpPath:
            instrument = "AAPL"
            bf = alphavantage.build_feed(
                [instrument], tmpPath, datetime.datetime(2010, 1, 1),
                datetime.datetime(2010, 12, 31), bar.Frequency.WEEK,
                apiKey=ALPHA_VANTAGE_API_KEY
            )
            bf.loadAll()
            # Alpha Vantage used to report 2010-1-8 as the first week of 2010.
            self.assertTrue(
                bf[instrument][0].getDateTime() in [datetime.datetime(2010, 1, 8), datetime.datetime(2010, 1, 15)]
            )
            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 322.8519)
            self.assertEqual(bf[instrument][-1].getHigh(), 326.66)
            self.assertEqual(bf[instrument][-1].getLow(), 321.31)
            self.assertEqual(bf[instrument][-1].getClose(), 322.56)
            self.assertEqual(bf[instrument][-1].getVolume(), 33567200.0)
            self.assertEqual(bf[instrument][-1].getPrice(), 322.56)
            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEquals(bf[instrument][-1].getAdjClose(), None)

    def testInvalidInstrument(self):
        instrument = "inexistent"

        # Don't skip errors.
        with self.assertRaisesRegexp(Exception, "Invalid content-type: application/json"):
            with common.TmpDir() as tmpPath:
                alphavantage.build_feed([instrument], tmpPath, frequency=bar.Frequency.WEEK,
                    apiKey=ALPHA_VANTAGE_API_KEY
                )

        # Skip errors.
        with common.TmpDir() as tmpPath:
            bf = alphavantage.build_feed(
                [instrument], tmpPath, frequency=bar.Frequency.WEEK, skipErrors=True,
                apiKey=ALPHA_VANTAGE_API_KEY
            )
            bf.loadAll()
            self.assertNotIn(instrument, bf)

    def testBuildFeedDailyCreatingDir(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            instrument = "ORCL"
            bf = alphavantage.build_feed([instrument], tmpPath, datetime.datetime(2010,1,1),
                                         datetime.datetime(2010, 12, 31), apiKey=ALPHA_VANTAGE_API_KEY)
            bf.loadAll()

            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 31.22)
            self.assertEqual(bf[instrument][-1].getHigh(), 31.33)
            self.assertEqual(bf[instrument][-1].getLow(), 30.93)
            self.assertEqual(bf[instrument][-1].getClose(), 31.3)
            self.assertEqual(bf[instrument][-1].getVolume(), 11716300)
            self.assertEqual(bf[instrument][-1].getPrice(), 31.3)
            # Not checking against a specific value since this is going to change
            # as time passes by.
            self.assertNotEquals(bf[instrument][-1].getAdjClose(), None)
        finally:
            shutil.rmtree(tmpPath)

    def testCommandLineDailyCreatingDir(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            instrument = "ORCL"
            subprocess.call([
                "python", "-m", "pyalgotrade.tools.alphavantage",
                "--symbol=%s" % instrument,
                "--storage=%s" % tmpPath,
                "--api-key=%s" % ALPHA_VANTAGE_API_KEY
            ])
            bf = alphavantagefeed.Feed()
            bf.setBarFilter(csvfeed.DateRangeFilter(fromDate=datetime.datetime(2010, 1, 1),
                                                    toDate=datetime.datetime(2010, 12, 31)))
            bf.addBarsFromCSV(instrument, os.path.join(tmpPath, "ORCL-alpha-vantage.csv"))
            bf.loadAll()

            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 31.22)
            self.assertEqual(bf[instrument][-1].getHigh(), 31.33)
            self.assertEqual(bf[instrument][-1].getLow(), 30.93)
            self.assertEqual(bf[instrument][-1].getClose(), 31.3)
            self.assertEqual(bf[instrument][-1].getVolume(), 11716300)
            self.assertEqual(bf[instrument][-1].getPrice(), 31.3)
        finally:
            shutil.rmtree(tmpPath)

    def testCommandLineWeeklyCreatingDir(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            instrument = "AAPL"
            subprocess.call([
                "python", "-m", "pyalgotrade.tools.alphavantage",
                "--symbol=%s" % instrument,
                "--storage=%s" % tmpPath,
                "--frequency=weekly",
                "--api-key=%s" % ALPHA_VANTAGE_API_KEY
            ])
            bf = alphavantagefeed.Feed()
            bf.setBarFilter(csvfeed.DateRangeFilter(fromDate=datetime.datetime(2010, 1, 1),
                                                    toDate=datetime.datetime(2010, 12, 31)))
            bf.addBarsFromCSV(instrument, os.path.join(tmpPath, "AAPL-alpha-vantage.csv"))
            bf.loadAll()

            self.assertEqual(bf[instrument][-1].getDateTime(), datetime.datetime(2010, 12, 31))
            self.assertEqual(bf[instrument][-1].getOpen(), 322.8519)
            self.assertEqual(bf[instrument][-1].getHigh(), 326.66)
            self.assertEqual(bf[instrument][-1].getLow(), 321.31)
            self.assertEqual(bf[instrument][-1].getClose(), 322.56)
            self.assertEqual(bf[instrument][-1].getVolume(), 33567200.0)
            self.assertEqual(bf[instrument][-1].getPrice(), 322.56)
        finally:
            shutil.rmtree(tmpPath)

    def testIgnoreErrors(self):
        with common.TmpDir() as tmpPath:
            instrument = "inexistent"
            output = check_output(
                [
                    "python", "-m", "pyalgotrade.tools.alphavantage",
                    "--symbol=%s" % instrument,
                    "--storage=%s" % tmpPath,
                    "--frequency=daily",
                    "--ignore-errors"
                ],
                stderr=subprocess.STDOUT
            )
            self.assertIn("Invalid content-type: application/json", output)

    def testDontIgnoreErrors(self):
        with self.assertRaises(Exception) as e:
            with common.TmpDir() as tmpPath:
                instrument = "inexistent"
                check_output(
                    [
                        "python", "-m", "pyalgotrade.tools.alphavantage",
                        "--symbol=%s" % instrument,
                        "--storage=%s" % tmpPath,
                        "--frequency=daily"
                    ],
                    stderr=subprocess.STDOUT
                )
        self.assertIn("Invalid content-type: application/json", bytes_to_str(e.exception.output))

