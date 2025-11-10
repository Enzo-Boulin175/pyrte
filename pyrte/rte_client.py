import base64
from collections.abc import Generator
from enum import Enum
from typing import Any

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict

import pyrte.config

TZ = "CET"


class APIService(str, Enum):
    wholesale_market = "wholesale_market"
    short_term_consumption = "short_term_consumption"


class PrevisionType(str, Enum):
    REALISED = "REALISED"
    CORRECTED = "CORRECTED"
    ID = "ID"
    D_MINUS_1 = "D-1"
    D_MINUS_2 = "D-2"


class Token(BaseModel):
    token_url: str
    client_id: str
    client_secret: str

    token: str | None = None
    expires_at: pd.Timestamp | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RTEError(Exception):
    def __init__(
        self, code: int = 0, error_description: str = "Unknown", error: str = "Unknown"
    ):
        self.code = code
        self.error_description = error_description
        self.error = error

    def __str__(self) -> str:
        return f"Request failed with status code {self.code}: {self.error_description} (Error: {self.error})"


def _check_response_status_code(response: httpx.Response) -> None:
    response.read()
    if response.status_code != 200:
        try:
            error_details = response.json()
            error_description = error_details.get("error_description", "Unknown error")
            error = error_details.get("error", "Unknown error code")
        except ValueError:
            error_description = "Failed to parse error details"
            error = "Unknown"

        raise RTEError(
            code=response.status_code, error_description=error_description, error=error
        )


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    pair = f"{client_id}:{client_secret}"
    b64 = base64.b64encode(pair.encode()).decode()
    return f"Basic {b64}"


class RTEAuth(httpx.Auth):
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    token_url = pyrte.config.TOKEN_URL

    def __init__(self, api_creds: dict[APIService, dict[str, str]]):
        self.tokens = {}
        for service in APIService:
            creds = api_creds.get(service, None)
            self.tokens[service] = (
                Token(
                    token_url=self.token_url,
                    client_id=creds["client_id"],
                    client_secret=creds["client_secret"],
                )
                if creds is not None
                else None
            )

    def refresh_token(self, token: Token) -> Token:
        response = httpx.post(
            self.token_url,
            headers=self.headers
            | {
                "Authorization": _basic_auth_header(
                    token.client_id, token.client_secret
                )
            },
        )
        _check_response_status_code(response)
        body = response.json()
        token.token = body["access_token"]
        token.expires_at = pd.Timestamp.utcnow() + pd.Timedelta(
            seconds=int(body["expires_in"])
        )
        return token

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        # Send the request, with a custom "Authorization" header.
        service = request.extensions["service"]
        token = self.tokens[service]
        if token is None:
            raise ValueError(f"No credentials provided for RTE {service} service")
        now = pd.Timestamp.utcnow() - pd.Timedelta(minutes=5)
        if not token.expires_at or token.expires_at < now:
            token = self.refresh_token(token)
            self.tokens[service] = token

        request.headers["Authorization"] = f"Bearer {token.token}"
        yield request


class RTEClient(httpx.Client):
    default_timeout = httpx.Timeout(60, connect=20)

    def __init__(
        self,
        api_creds: dict[APIService, dict[str, str]],
        *,
        base_url: str = pyrte.config.RTE_BASE_URL,
        **kwargs: Any,
    ):
        auth = RTEAuth(api_creds)

        event_hooks = kwargs.get(
            "event_hooks", {"response": [_check_response_status_code]}
        )
        super().__init__(
            base_url=base_url,
            auth=auth,
            event_hooks=event_hooks,
            **kwargs,
        )

    def get_short_term_consumption(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        prevision_type: PrevisionType,
        freq: str = "15min",
    ) -> pd.Series:
        """
        French load data (15Mmin), can be forecast or realised based on the PrevisionType.
        NB:
        - RTE only sends data for the whole day so we have to cut ourself.

        - Although we could fetch multiple types at the same time, it feels useless so i just implemented for one.
        """
        # TODO: Cette erreur est générée si la période demandée est supérieure à 186 jours.
        # TODO: Cette erreur est générée si l’intervalle de temps entre start_date et end_date est inférieur 1 jour calendaire.
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end timestamps must be timezone-aware")

        params: dict[str, str] = {}

        params["type"] = prevision_type.value

        if prevision_type == PrevisionType.D_MINUS_2:
            freq = "30min"
        params["start_date"] = start.floor(freq).isoformat()
        params["end_date"] = end.ceil(freq).isoformat()

        url = f"{self.base_url}open_api/consumption/v1/short_term"
        response = self.get(
            url,
            params=params,
            extensions={"service": APIService.short_term_consumption},
        )

        data = response.json().get("short_term", [])
        if not data:
            return pd.Series()

        dfs = []
        for prevision in data:
            response_prevision_type = PrevisionType(prevision.get("type"))
            if response_prevision_type != prevision_type:
                raise ValueError(
                    f"Wrong prevision type returned : {response_prevision_type} instead of {prevision_type}"
                )
            values = prevision.get("values", [])
            if not values:
                continue

            df = pd.DataFrame(values, columns=["start_date", "value"])
            df["start_date"] = pd.to_datetime(df["start_date"])
            df = df.set_index("start_date", verify_integrity=True)

            target_index = pd.date_range(
                params["start_date"],
                params["end_date"],
                freq=freq,
                name="date",
                inclusive="left",
            ).tz_convert(TZ)

            try:
                ts = df.value.reindex(target_index)
            except Exception as e:
                raise ValueError(f"Reindexing failed: {e}")

            dfs.append(ts.rename(prevision_type.value))

        if dfs == []:
            return pd.Series()
        else:
            return dfs[0]


if __name__ == "__main__":
    api_creds = {
        APIService.short_term_consumption: {
            "client_id": "6a4825cb-10b9-4759-93f9-8f946879e212",
            "client_secret": "7203ec04-a36e-49e5-b858-1af16e1562aa",
        }
    }
    client = RTEClient(api_creds)

    start = pd.Timestamp("2025-01-01", tz=TZ)
    end = start + pd.DateOffset(days=3)
    print("Fetching data from", start.tz_convert("UTC"), "to", end.tz_convert("UTC"))
    ts = client.get_short_term_consumption(start, end, PrevisionType.D_MINUS_2)
    print(ts)
    breakpoint()
