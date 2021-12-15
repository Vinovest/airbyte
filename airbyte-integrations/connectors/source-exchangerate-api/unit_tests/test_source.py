#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_exchangerate_api.source import SourceExchangerateApi


def test_check_connection():
    source = SourceExchangerateApi()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams():
    source = SourceExchangerateApi()
    config_mock = {}
    config_mock["api_key"] = "aaaaaaaaaaaaaaaaaaaaaaaa"
    config_mock["base_currency"] = "USD"
    config_mock["start_date"] = "2021-01-01"
    streams = source.streams(config=config_mock)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
