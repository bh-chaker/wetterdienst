# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import List, Optional, Union

import pandas as pd
import requests

from wetterdienst import Kind, Period, Provider
from wetterdienst.core.scalar.request import ScalarRequestCore
from wetterdienst.core.scalar.values import ScalarValuesCore
from wetterdienst.metadata.columns import Columns
from wetterdienst.metadata.datarange import DataRange
from wetterdienst.metadata.period import PeriodType
from wetterdienst.metadata.resolution import Resolution, ResolutionType
from wetterdienst.metadata.timezone import Timezone
from wetterdienst.provider.noaa.ghcn.parameter import NoaaGhcnParameter
from wetterdienst.provider.noaa.ghcn.unit import NoaaGhcnUnit
from wetterdienst.util.cache import CacheExpiry, metaindex_cache
from wetterdienst.util.network import download_file


class NoaaGhcnDatasetBase(Enum):
    DAILY = "daily"


class NoaaGhcnResolution(Enum):
    DAILY = Resolution.DAILY.value


class NoaaGhcnValues(ScalarValuesCore):
    _string_parameters = None

    _integer_parameters = None

    _irregular_parameters = None

    _data_tz = Timezone.USA

    _base_url = (
        "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{station_id}.csv.gz"
    )

    def _collect_station_parameter(
        self, station_id: str, parameter, dataset
    ) -> pd.DataFrame:
        """
        Collection method for NOAA GHCN data. Parameter and dataset can be ignored as data
        is provided as a whole.

        :param station_id: station id of the station being queried
        :param parameter: parameter being queried
        :param dataset: dataset being queried
        :return: dataframe with read data
        """
        # parameter and dataset can be ignored as data is provided as a whole

        url = self._base_url.format(station_id=station_id)

        file = download_file(url, CacheExpiry.FIVE_MINUTES)

        columns = (
            Columns.STATION_ID.value,
            Columns.DATE.value,
            Columns.PARAMETER.value,
            Columns.VALUE.value,
            Columns.QUALITY.value,
        )

        df = pd.read_csv(file, sep=",", compression="gzip", header=None)

        df = df.iloc[:, :-3]

        df.columns = columns

        df.loc[:, Columns.PARAMETER.value] = df.loc[
            :, Columns.PARAMETER.value
        ].str.lower()

        return df


class NoaaGhcnRequest(ScalarRequestCore):
    provider = Provider.NOAA
    kind = Kind.OBSERVATION

    _dataset_base = NoaaGhcnDatasetBase
    _parameter_base = NoaaGhcnParameter
    _dataset_tree = NoaaGhcnParameter

    _base_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

    # Listing colspecs derived from stations file provided by GHCN
    _listing_colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (41, 71)]

    _data_range = DataRange.FIXED

    _has_datasets = True
    _unique_dataset = True
    _has_tidy_data = True

    _period_type = PeriodType.FIXED
    _period_base = Period.HISTORICAL

    _resolution_type = ResolutionType.FIXED
    _resolution_base = NoaaGhcnResolution

    _unit_tree = NoaaGhcnUnit

    _values = NoaaGhcnValues

    _tz = Timezone.USA

    def __init__(
        self,
        parameter: List[str],
        start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
        end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
        humanize: bool = True,
        tidy: bool = True,
        si_units: bool = True,
    ) -> None:
        """

        :param parameter:
        :param start_date:
        :param end_date:
        :param humanize:
        :param tidy:
        :param si_units:
        """
        super(NoaaGhcnRequest, self).__init__(
            parameter=parameter,
            resolution=Resolution.DAILY,
            period=Period.HISTORICAL,
            start_date=start_date,
            end_date=end_date,
            humanize=humanize,
            tidy=tidy,
            si_units=si_units,
        )

    def _all(self) -> pd.DataFrame:
        """
        Method to acquire station listing,
        :return:
        """
        @metaindex_cache.cache_on_arguments()
        def _get_stations_listing():
            r = requests.get(self._base_url)

            r.raise_for_status()

            listing = BytesIO(r.content)

            return listing

        file = _get_stations_listing()

        df = pd.read_fwf(
            file,
            dtype=str,
            header=None,
            colspecs=self._listing_colspecs,
        )

        df.columns = [
            Columns.STATION_ID.value,
            Columns.LATITUDE.value,
            Columns.LONGITUDE.value,
            Columns.HEIGHT.value,
            Columns.NAME.value,
        ]

        return df


if __name__ == "__main__":
    request = NoaaGhcnRequest(parameter="precipitation_height").filter_by_station_id(
        station_id="GME00111464"
    )

    print(request.parameter)

    values = next(request.values.query())

    print(values.df)
