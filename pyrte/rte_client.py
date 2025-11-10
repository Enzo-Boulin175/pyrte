import base64
from dataclasses import dataclass
from enum import Enum

import httpx
import pandas as pd

import pyrte.config as config


class APIService(str, Enum):
    wholesale_market = "wholesale_market"
    consumption = "consumption"


@dataclass
class TokenManager:
    token_url: str
    client_id: str
    client_secret: str

    token: str | None = None
    expires_at: pd.Timestamp | None = None


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
    token_url = config.TOKEN_URL

    def __init__(self, service_creds: dict[APIService, dict[str, str]]):
        self.tokens = {}
        for service in APIService:
            creds = service_creds.get(service, None)
            self.tokens[service] = creds or TokenManager(
                token_url=self.token_url,
                client_id=creds["client_id"],
                client_secret=creds["client_secret"],
            )

    def refresh_token(self, token: TokenManager) -> TokenManager:
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

    def auth_flow(self, request):
        # Send the request, with a custom `X-Authentication` header.
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
    pass
