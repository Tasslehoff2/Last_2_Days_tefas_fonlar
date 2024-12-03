import requests
import pandas as pd
import math
import ssl

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Union
from marshmallow import Schema, fields, EXCLUDE, pre_load, post_load
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# Special thanks to https://github.com/burakyilmaz321

class InfoSchema(Schema):
    code = fields.String(data_key="FONKODU", allow_none=True)
    code_title = fields.String(data_key="FONUNVAN", allow_none=True)
    date = fields.Date(data_key="TARIH", allow_none=True)
    price = fields.Float(data_key="FIYAT", allow_none=True)
    tedpay = fields.Float(data_key="TEDPAYSAYISI", allow_none=True)
    personcount = fields.Float(data_key="KISISAYISI", allow_none=True)
    portfoy_buyukluk = fields.Float(data_key="PORTFOYBUYUKLUK", allow_none=True)
    

    @pre_load
    def pre_load_hook(self, input_data, **kwargs):
        # Convert milliseconds Unix timestamp to date
        seconds_timestamp = int(input_data["TARIH"]) / 1000
        input_data["TARIH"] = date.fromtimestamp(seconds_timestamp).isoformat()
        return input_data

    @post_load
    def post_load_hool(self, output_data, **kwargs):
        # Fill missing fields with default None
        output_data = {f: output_data.setdefault(f) for f in self.fields}
        return output_data

    class Meta:
        unknown = EXCLUDE

class tefas_get:

    root_url = "https://www.tefas.gov.tr"
    detail_endpoint = "/api/DB/BindHistoryAllocation"
    info_endpoint = "/api/DB/BindHistoryInfo"
    headers = {
        "Connection": "keep-alive",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
        ),
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://www.tefas.gov.tr",
        "Referer": "https://www.tefas.gov.tr/TarihselVeriler.aspx",
    }

    def __init__(self):
        self.session = _get_session()
        _ = self.session.get(self.root_url)
        self.cookies = self.session.cookies.get_dict()

    def fetch(
        self,
        start: Union[str, datetime],
        end: Optional[Union[str, datetime]] = None,
        name: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:

        start_date_initial = datetime.strptime(start, "%Y-%m-%d")
        end_date_initial = datetime.strptime(end or start, "%Y-%m-%d")
        counter = 1
        start_date = start_date_initial
        end_date = end_date_initial

        range_date = end_date_initial - start_date_initial
        range_interval = 1

        info_schema = InfoSchema(many=True)
        merged = pd.DataFrame()

        if range_date.days > range_interval :
          counter = range_date.days / range_interval
          counter = math.ceil(counter)
          end_date = start_date + timedelta(days=range_interval)

        while counter > 0:
          counter -= 1
          #print(counter)
          #print(start_date)
          #print(end_date)

          data = {
              "fontip": "YAT",
              "bastarih": _parse_date(start_date),
              "bittarih": _parse_date(end_date),
              "fonkod": name.upper() if name else "",
          }

          # General info pane
          info = self._do_post(self.info_endpoint, data)
          info = info_schema.load(info)
          info = pd.DataFrame(info, columns=info_schema.fields.keys())

          merged = pd.concat([merged, info])

          # Return only desired columns
          merged = merged[columns] if columns else merged

          if counter > 0 :
            start_date = end_date + timedelta(days=1)
            end_date = end_date + timedelta(days=range_interval)
            if end_date > end_date_initial :
              end_date = end_date_initial

        return merged

    def _do_post(self, endpoint: str, data: Dict[str, str]) -> Dict[str, str]:
        # TODO: error handling. this is quiet fishy now.
        response = self.session.post(
            url=f"{self.root_url}/{endpoint}",
            data=data,
            cookies=self.cookies,
            headers=self.headers,
        )
        return response.json().get("data", {})

def _parse_date(date: Union[str, datetime]) -> str:
    if isinstance(date, datetime):
        formatted = datetime.strftime(date, "%d.%m.%Y")
    elif isinstance(date, str):
        try:
            parsed = datetime.strptime(date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(
                "Date string format is incorrect. " "It should be `YYYY-MM-DD`"
            ) from exc
        else:
            formatted = datetime.strftime(parsed, "%d.%m.%Y")
    else:
        raise ValueError(
            "`date` should be a string like 'YYYY-MM-DD' "
            "or a `datetime.datetime` object."
        )
    return formatted

def _get_session() -> requests.Session:

    class CustomHttpAdapter(HTTPAdapter):
        def __init__(self, ssl_context=None, **kwargs):
            self.ssl_context = ssl_context
            super().__init__(**kwargs)

        def init_poolmanager(
            self, connections, maxsize, block=False
        ):  # pylint: disable=arguments-differ
            self.poolmanager = PoolManager(
                num_pools=connections,
                maxsize=maxsize,
                block=block,
                ssl_context=self.ssl_context,
            )

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session

time_delta = 1
start_date_calc = date.today() - timedelta(days=time_delta)

tefas = tefas_get()

today_1_year_ago = date.today() - timedelta(days=time_delta)
date_start = today_1_year_ago.strftime("%Y-%m-%d")
date_end = date.today().strftime("%Y-%m-%d")

fetched_data = pd.DataFrame()
fetched_data = tefas.fetch(start=date_start, end=date_end, columns=["code", "code_title", "date", "price", "tedpay", "personcount", "portfoy_buyukluk"])
fetched_data['date'] = pd.to_datetime(fetched_data['date'], errors='coerce')
fetched_data['date'].dt.strftime('%Y-%m-%d')
fetched_data['price'].astype(float)

output_file_name = "tefas_data.xlsx"
fetched_data.to_excel(output_file_name, index=False)

print(f"Data successfully exported to {output_file_name}")
