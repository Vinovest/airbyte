import datetime

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


class ExchangerateApiStream(HttpStream, ABC):
    url_base = "https://v6.exchangerate-api.com/v6/"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        response = response.json()
        response["exchange_rate_date"] = datetime.date(
            response["year"], response["month"], response["day"]
        )
        return [response]


class ExchangeRates(ExchangerateApiStream):
    state_checkpoint_interval = 30

    def __init__(self, base: str, start_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.base = base
        self.start_date = start_date

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        date_path = stream_slice["date"].replace("-", "/")
        return f"history/{self.base}/{date_path}"

    @property
    def cursor_field(self) -> str:
        return "exchange_rate_date"

    @property
    def primary_key(self) -> str:
        return "exchange_rate_date"

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if current_stream_state is not None and "date" in current_stream_state:
            current_parsed_date = datetime.datetime.strptime(
                current_stream_state["date"], "%Y-%m-%d"
            ).date()
            latest_record_date = datetime.date(
                latest_record["year"], latest_record["month"], latest_record["day"]
            )
            return {
                "date": max(current_parsed_date, latest_record_date).strftime(
                    "%Y-%m-%d"
                )
            }
        else:
            return {"date": self.start_date.strftime("%Y-%m-%d")}

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date.date() <= datetime.datetime.now().date():
            dates.append({"date": start_date.strftime("%Y-%m-%d")})
            start_date += datetime.timedelta(days=1)
        return dates

    def stream_slices(
        self, stream_state: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Optional[Mapping[str, any]]]:
        start_date = (
            datetime.datetime.strptime(stream_state["date"], "%Y-%m-%d")
            if stream_state and "date" in stream_state
            else self.start_date
        )
        return self._chunk_date_range(start_date)


class SourceExchangerateApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            config["api_key"]
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"])
        start_date = datetime.datetime.strptime(config["start_date"], "%Y-%m-%d")
        return [
            ExchangeRates(
                authenticator=auth, base=config["base_currency"], start_date=start_date
            )
        ]
