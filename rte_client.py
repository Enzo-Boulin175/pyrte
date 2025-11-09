"""
Client Python minimal pour RTE Speed Data API (OAuth2 client_credentials)
Basé sur "RTE Speed Data API User Guide Version 2.0" (token endpoint & usage).
https://data.rte-france.org/documents/20182/22648/EN_GuideOauth2_v5.1.pdf/54d3d183-f20f-4290-9417-bcae122b9e46
"""

import base64
import json
import time
from typing import Any, Dict

import pandas as pd
import requests

import config
from config import APIService, PrevisionType

FREQ = "15min"


def _rte_data_cleaning(
    values: dict[str, Any],
    *,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    columns: list[str] = ["value"],
) -> pd.DataFrame:
    """
    Cleans the API response and
    Returns a Series with 15min frequency
    """
    df = pd.DataFrame(values, columns=["start_date", *columns])
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.tz_convert(
        "CET"
    )
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("start_date").drop_duplicates(subset="start_date")
    df = df.set_index("start_date", verify_integrity=True)

    # Ensure dates are rounded (they should already be)
    rounded_start = (start or df.index.min()).floor(FREQ)
    rounded_end = (end or df.index.max()).floor(FREQ)

    # Build uniform 15-minute index, missing values are filled with nan
    expected_index = pd.date_range(start=rounded_start, end=rounded_end, freq=FREQ)

    df_with_freq = df.reindex(expected_index)

    return df_with_freq


class TokenManager:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        cache_file: str | None = None,
        timeout: int = 10,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.timeout = timeout
        self.cache_file = cache_file

        self._access_token: str | None = None
        self._expires_at: float = 0.0

        if cache_file:
            self._load_from_file()

    def _basic_auth_header(self) -> str:
        pair = f"{self.client_id}:{self.client_secret}"
        b64 = base64.b64encode(pair.encode()).decode()
        return f"Basic {b64}"

    def _load_from_file(self):
        try:
            with open(self.cache_file, "r") as f:
                data = json.load(f)
            self._access_token = data.get("access_token")
            self._expires_at = float(data.get("expiry", 0))
        except Exception:
            self._access_token = None
            self._expires_at = 0.0

    def _save_to_file(self):
        if not self.cache_file:
            return
        data = {"access_token": self._access_token, "expiry": self._expires_at}
        with open(self.cache_file, "w") as f:
            json.dump(data, f)

    def _is_valid(self):
        return self._access_token and time.time() + 10 < self._expires_at

    def _fetch_token(self):
        headers = {
            "Authorization": self._basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        try:
            resp = requests.post(self.token_url, headers=headers, timeout=self.timeout)
        except requests.RequestException as e:
            raise RTEAuthError(f"Network error while fetching token: {e}")

        if resp.status_code != 200:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RTEAuthError(f"Token endpoint error {resp.status_code}: {err}")

        try:
            body = resp.json()
        except Exception as e:
            raise RTEAuthError(f"Invalid token JSON: {e}")

        access_token = body.get("access_token")
        expires_in = int(body.get("expires_in", 3600))
        if not access_token:
            raise RTEAuthError(f"Invalid token response: {body}")

        self._access_token = access_token
        self._expires_at = time.time() + expires_in
        self._save_to_file()

    def get_token(self, force_refresh: bool = False):
        if not force_refresh and self._is_valid():
            return self._access_token
        self._fetch_token()
        return self._access_token


class RTEAuthError(Exception):
    pass


class RTEClient:
    def __init__(
        self,
        api_services: list[APIService] = list(APIService),
        token_endpoint: str = config.TOKEN_ENDPOINT,
        api_base: str = config.RTE_BASE_URL,
        timeout: int = 10,
        use_cache_file: bool = True,
    ):
        self.api_base = api_base.rstrip("/") + "/"
        self.token_url = self.api_base + token_endpoint.rstrip("/") + "/"
        self.timeout = timeout

        self.services = {}
        for service in api_services:
            client_id, client_secret = config.API_TO_CREDENTIALS[service]
            self.services[service] = {
                "url": api_base + config.API_TO_ENDPOINT[service].rstrip("/"),
                "token_manager": TokenManager(
                    client_id,
                    client_secret,
                    self.token_url,
                    cache_file=f"token/{service}_token_cache.json"
                    if use_cache_file
                    else None,
                ),
            }

    # ---------- API request ----------
    def request(
        self,
        service: APIService,
        *,
        method: str = "POST",
        headers: Dict[str, str] | None = None,
        params: Dict[str, str] | None = None,
        data: Any = None,
        force_token_refresh_on_401: bool = True,
    ) -> requests.Response:
        """
        Call a endpoint (relative to api_base).
        """
        cfg = self.services[service]
        url = cfg["url"]

        token = cfg["token_manager"].get_token()

        req_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            req_headers.update(headers)

        method = method.upper()
        try:
            resp = requests.request(
                method,
                url,
                headers=req_headers,
                params=params,
                data=data,
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"Erreur lors de l'appel API: {e}")

        # handle common token issues
        if resp.status_code in (401, 403) and force_token_refresh_on_401:
            # maybe token expired or invalid -> try refresh once
            # avoid infinite loop by disabling refresh on next call
            try:
                token = cfg["token_manager"].get_token(force_refresh=True)
            except RTEAuthError:
                # token refresh failed; propagate original error
                pass
            # retry once
            token = cfg["token_manager"]._access_token
            if token:
                req_headers["Authorization"] = f"Bearer {token}"
                resp = requests.request(
                    method,
                    url,
                    headers=req_headers,
                    params=params,
                    data=data,
                    timeout=self.timeout,
                )

        return resp

    # ---------- API methods ----------
    def get_france_power_exchanges(self) -> pd.DataFrame:
        resp = self.request(APIService.wholesale_market, method="GET")
        resp.raise_for_status()
        data = resp.json().get("france_power_exchanges", [])
        total_values = []
        for entry in data:
            values = entry.get("values", [])
            if not values:
                continue
            total_values += values

        df = _rte_data_cleaning(total_values, columns=["value", "price"])
        return df

    def get_short_term_consumptions(
        self,
        types: PrevisionType | list[PrevisionType] | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> dict[PrevisionType, pd.Series]:
        """
        French realised load data (15Mmin)
        RTE only sends data for the whole day so we have to cut ourself.
        """
        params = {}
        if types:
            if isinstance(types, list):
                params["type"] = ",".join(types)
            else:
                params["type"] = types

        # I dont know why but RTE returns 15 min before the wanted end date so we can fix this by adding 15 min
        if start:
            params["start_date"] = start.floor("1D").isoformat()
        if end:
            params["end_date"] = end.ceil("1D").isoformat()

        resp = self.request(APIService.consumption, method="GET", params=params)
        resp.raise_for_status()
        data = resp.json().get("short_term", [])
        if not data:
            return {}

        previsions = {}
        for prevision in data:
            prevision_type = PrevisionType(prevision.get("type"))
            values = prevision.get("values", [])
            if not values:
                continue

            # We floor the end date because the last index is the end of the last period minus 15min
            previsions[prevision_type] = _rte_data_cleaning(
                values,
                start=start.floor(FREQ),
                end=end.ceil(FREQ) - pd.DateOffset(minutes=15),
            )

        return previsions

    def get_realised_consumption(
        self, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.Series:
        prevision = self.get_short_term_consumptions(
            types=PrevisionType.REALISED, start=start, end=end
        )
        df = prevision.get(PrevisionType.REALISED, pd.DataFrame())

        return df.squeeze()


# -------------------- Exemple d'utilisation --------------------

if __name__ == "__main__":
    client = RTEClient()

    # consumptions = client.get_short_term_consumptions()
    # print(consumptions)

    # consumptions = client.get_short_term_consumptions(
    #     types=["REALISED", "ID"],
    #     start=pd.Timestamp("2025-10-01T00:00:00+02:00"),
    #     end=pd.Timestamp("2025-10-02T00:00:00+02:00"),
    # )
    # print(consumptions)

    # df = client.get_france_power_exchanges()
    # print(df.head())
    start = pd.Timestamp("2020-01-01", tz="CET")
    end = start + pd.DateOffset(days=1)
    ts = client.get_realised_consumption(start, end)
    breakpoint()
    # ts.to_csv("consumption_backup.csv")
