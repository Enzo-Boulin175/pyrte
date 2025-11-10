from unittest.mock import Mock, patch

import httpx
import pandas as pd
import pytest
import vcr
from inline_snapshot import snapshot

from pyrte.rte_client import APIService, RTEAuth, RTEClient, _basic_auth_header, Token

VCR_DIR = "tests/cassettes/"


@pytest.fixture
def auth():
    service_creds = {
        APIService.consumption: {"client_id": "id", "client_secret": "secret"}
    }
    return RTEAuth(service_creds)


@patch("httpx.post")
def test_refresh_token(mock_post, auth):
    mock_response = Mock(spec=httpx.Response)
    token_json = {
        "access_token": "fake_token",
        "token_type": "Bearer",
        "expires_in": 10,
    }
    mock_response.json.return_value = token_json
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    token = auth.refresh_token(auth.tokens[APIService.consumption])

    mock_post.assert_called_once_with(
        auth.token_url,
        headers={**auth.headers, "Authorization": _basic_auth_header("id", "secret")},
    )
    assert token.model_dump(exclude="expires_at") == snapshot(
        {
            "token_url": "https://digital.iservices.rte-france.com/token/oauth/",
            "client_id": "id",
            "client_secret": "secret",
            "token": "fake_token",
        }
    )


def test_auth_flow(auth):
    request = Mock(spec=httpx.Request)
    request.extensions = {"service": APIService.consumption}
    request.headers = {}
    token = Token(
        token_url="url", client_id="id", client_secret="secret", token="token"
    )
    with patch.object(auth, "refresh_token", return_value=token):
        request = next(auth.auth_flow(request))

    assert request.headers["Authorization"] == "Bearer token"


@vcr.use_cassette(f"{VCR_DIR}test_realised_consumption_one_day.yaml")
def test_realised_consumption_one_day():
    client = RTEClient()

    start = pd.Timestamp("2020-01-01", tz="CET")
    end = start + pd.DateOffset(days=1)
    ts = client.get_realised_consumption(start, end)

    str(ts) == snapshot("""\
2020-01-01 00:00:00+01:00    65827
2020-01-01 00:15:00+01:00    65887
2020-01-01 00:30:00+01:00    64773
2020-01-01 00:45:00+01:00    63464
2020-01-01 01:00:00+01:00    63246
                             ...  \n\
2020-01-01 22:45:00+01:00    64157
2020-01-01 23:00:00+01:00    63639
2020-01-01 23:15:00+01:00    63319
2020-01-01 23:30:00+01:00    62808
2020-01-01 23:45:00+01:00    63322
Freq: 15min, Name: value, Length: 96, dtype: int64\
""")
