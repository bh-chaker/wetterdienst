from wetterdienst.provider.dwd.observation import (
    DwdObservationDataset,
    DwdObservationParameter,
    DwdObservationPeriod,
    DwdObservationRequest,
    DwdObservationResolution,
)
from wetterdienst.provider.dwd.observation.metadata.parameter import (
    DwdObservationDatasetTree,
)

parameters_reference = [
    (
        DwdObservationDatasetTree.DAILY.CLIMATE_SUMMARY.TEMPERATURE_AIR_MEAN_200,
        DwdObservationDataset.CLIMATE_SUMMARY,
    ),
    (
        DwdObservationDatasetTree.DAILY.CLIMATE_SUMMARY.TEMPERATURE_AIR_MAX_200,
        DwdObservationDataset.CLIMATE_SUMMARY,
    ),
    (
        DwdObservationDatasetTree.DAILY.CLIMATE_SUMMARY.TEMPERATURE_AIR_MIN_200,
        DwdObservationDataset.CLIMATE_SUMMARY,
    ),
    (
        DwdObservationDatasetTree.DAILY.CLIMATE_SUMMARY.PRECIPITATION_HEIGHT,
        DwdObservationDataset.CLIMATE_SUMMARY,
    ),
    (
        DwdObservationDatasetTree.DAILY.CLIMATE_SUMMARY.PRECIPITATION_FORM,
        DwdObservationDataset.CLIMATE_SUMMARY,
    ),
]


def test_dwd_observation_parameters_constants():
    request = DwdObservationRequest(
        parameter=[
            DwdObservationParameter.DAILY.TEMPERATURE_AIR_MEAN_200,  # tmk
            DwdObservationParameter.DAILY.TEMPERATURE_AIR_MAX_200,  # txk
            DwdObservationParameter.DAILY.TEMPERATURE_AIR_MIN_200,  # tnk
            DwdObservationParameter.DAILY.PRECIPITATION_HEIGHT,  # rsk
            DwdObservationParameter.DAILY.PRECIPITATION_FORM,  # rskf
        ],
        resolution=DwdObservationResolution.DAILY,
        period=DwdObservationPeriod.HISTORICAL,
    )

    assert request.parameter == parameters_reference


def test_dwd_observation_parameters_strings_lowercase():
    request = DwdObservationRequest(
        parameter=[
            "tmk",
            "txk",
            "tnk",
            "rsk",
            "rskf",
        ],
        resolution=DwdObservationResolution.DAILY,
        period=DwdObservationPeriod.HISTORICAL,
    )

    assert request.parameter == parameters_reference


def test_dwd_observation_parameters_strings_uppercase():
    request = DwdObservationRequest(
        parameter=[
            "TMK",
            "TXK",
            "TNK",
            "RSK",
            "RSKF",
        ],
        resolution=DwdObservationResolution.DAILY,
        period=DwdObservationPeriod.HISTORICAL,
    )

    assert request.parameter == parameters_reference
