#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import datetime

from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_exchangerate_api.source import ExchangeRates

@fixture
def stream():
    return ExchangeRates("USD", datetime.date(2021,1,1))

def test_cursor_field(stream):
    expected_cursor_field = "date"
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(stream):
    inputs = {"current_stream_state": {"date": "2021-01-01"}, "latest_record": {"year": 2021, "month": 12, "day": 1}}
    expected_state = {"date": "2021-12-01"}
    assert stream.get_updated_state(**inputs) == expected_state


def test_supports_incremental(stream):
    assert stream.supports_incremental


def test_source_defined_cursor(stream):
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(stream):
    expected_checkpoint_interval = 30
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
