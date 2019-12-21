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

import datetime
import os
import argparse

import six

from pyalgotrade import bar
from pyalgotrade.barfeed import alphavantagefeed
from pyalgotrade.utils import dt
from pyalgotrade.utils import csvutils
import pyalgotrade.logger


# https://www.alphavantage.co/documentation/

def download_csv(symbol, frequency, apiKey):
    url = "https://www.alphavantage.co/query"
    params = {
        "symbol": symbol,
        "datatype": "csv",
        "outputsize": "full"
    }

    if frequency == bar.Frequency.DAY:
        params["function"] = "TIME_SERIES_DAILY_ADJUSTED"
    elif frequency == bar.Frequency.WEEK:
        params["function"] = "TIME_SERIES_WEEKLY_ADJUSTED"

    if apiKey is not None:
        params["apikey"] = apiKey
    else:
        params["apikey"] = "demo"

    return csvutils.download_csv(url, params, content_type='application/x-download')


def download_daily_bars( symbol, csvFile, apiKey=None):
    """Download daily bars from Alpha Vantage

    :param symbol: The symbol.
    :type symbol: string.
    :param csvFile: The path to the CSV file to write.
    :type csvFile: string.
    :param apiKey: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type apiKey: string.
    """

    bars = download_csv(symbol, bar.Frequency.DAY, apiKey)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()


def download_weekly_bars(symbol, csvFile, apiKey=None):
    """Download weekly bars from Alpha Vantage

    :param symbol: The symbol.
    :type symbol: string.
    :param year: The year.
    :type year: int.
    :param csvFile: The quandlfeedpath to the CSV file to write.
    :type csvFile: string.
    :param apiKey: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type apiKey: string.
    """

    bars = download_csv(symbol, bar.Frequency.WEEK, apiKey)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()


def build_feed(symbols, fromYear, toYear, storage, frequency=bar.Frequency.DAY, timezone=None,
               skipErrors=False, apiKey=None, columnNames={}, forceDownload=False,
               skipMalformedBars=False
               ):
    """Build and load a :class:`pyalgotrade.barfeed.alphavantage.Feed` using CSV files downloaded from Alpha Vantage.
    CSV files are downloaded if they haven't been downloaded before.


    :param symbols: The symbols.
    :type symbols: list.
    :param fromYear: The first year.
    :type fromYear: int.
    :param toYear: The last year.
    :type toYear: int.
    :param storage: The path were the files will be loaded from, or downloaded to.
    :type storage: string.
    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param skipErrors: True to keep on loading/downloading files in case of errors.
    :type skipErrors: boolean.
    :param apiKey: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type apiKey: string.
    :param columnNames: Optional. A dictionary to map column names. Valid key values are:

        * datetime
        * open
        * high
        * low
        * close
        * volume
        * adj_close

    :type columnNames: dict.
    :param skipMalformedBars: True to skip errors while parsing bars.
    :type skipMalformedBars: boolean.

    :rtype: :class:`pyalgotrade.barfeed.quandlfeed.Feed`.
    """

    logger = pyalgotrade.logger.getLogger("alphavantage")
    ret = alphavantagefeed.Feed(frequency, timezone)

    # Additional column names.
    for col, name in six.iteritems(columnNames):
        ret.setColumnName(col, name)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for symbol in symbols:

        fileName = os.path.join(storage, "%s-alphavantage.csv" % (symbol,))
        if not os.path.exists(fileName) or forceDownload:
            logger.info("Downloading %s" % (symbol,))
            try:
                if frequency == bar.Frequency.DAY:
                    download_daily_bars(symbol, fileName, apiKey)
                else:
                    assert frequency == bar.Frequency.WEEK, "Invalid frequency"
                    download_weekly_bars(symbol, fileName, apiKey)
            except Exception as e:
                if skipErrors:
                    logger.error(str(e))
                    continue
                else:
                    raise e
        ret.addBarsFromCSV(symbols, fileName, skipMalformedBars=skipMalformedBars)
    return ret


def main():
    parser = argparse.ArgumentParser(description="Quandl utility")

    parser.add_argument("--auth-token", required=False, help="An authentication token needed if you're doing more than 50 calls per day")
    parser.add_argument("--source-code", required=True, help="The dataset source code")
    parser.add_argument("--table-code", required=True, help="The dataset table code")
    parser.add_argument("--from-year", required=True, type=int, help="The first year to download")
    parser.add_argument("--to-year", required=True, type=int, help="The last year to download")
    parser.add_argument("--storage", required=True, help="The path were the files will be downloaded to")
    parser.add_argument("--force-download", action='store_true', help="Force downloading even if the files exist")
    parser.add_argument("--ignore-errors", action='store_true', help="True to keep on downloading files in case of errors")
    parser.add_argument("--frequency", default="daily", choices=["daily", "weekly"], help="The frequency of the bars. Only daily or weekly are supported")

    args = parser.parse_args()

    logger = pyalgotrade.logger.getLogger("alphavantage")

    if not os.path.exists(args.storage):
        logger.info("Creating %s directory" % (args.storage))
        os.mkdir(args.storage)

    for year in range(args.from_year, args.to_year+1):
        fileName = os.path.join(args.storage, "%s-%s-%d-quandl.csv" % (args.source_code, args.table_code, year))
        if not os.path.exists(fileName) or args.force_download:
            logger.info("Downloading %s %d to %s" % (args.table_code, year, fileName))
            try:
                if args.frequency == "daily":
                    download_daily_bars(args.source_code, args.table_code, year, fileName, args.auth_token)
                else:
                    assert args.frequency == "weekly", "Invalid frequency"
                    download_weekly_bars(args.source_code, args.table_code, year, fileName, args.auth_token)
            except Exception as e:
                if args.ignore_errors:
                    logger.error(str(e))
                    continue
                else:
                    raise


if __name__ == "__main__":
    main()
