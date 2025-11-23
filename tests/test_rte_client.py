from unittest.mock import Mock, patch

import httpx
import pandas as pd
import pytest
from inline_snapshot import snapshot

from pyrte.rte_client import (
    APIService,
    PrevisionType,
    RTEAuth,
    RTEClient,
    Token,
    _basic_auth_header,
)

VCR_DIR = "tests/cassettes/"

pytestmark = [pytest.mark.vcr]


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "cassette_library_dir": VCR_DIR,
        "filter_headers": ["authorization"],
        "record_mode": "once",
    }


@pytest.fixture
def auth():
    api_creds = {
        APIService.short_term_consumption: {
            "client_id": "id",
            "client_secret": "secret",
        }
    }
    return RTEAuth(api_creds)


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

    token = auth.refresh_token(auth.tokens[APIService.short_term_consumption])

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
    request.extensions = {"service": APIService.short_term_consumption}
    request.headers = {}
    token = Token(
        token_url="url", client_id="id", client_secret="secret", token="token"
    )
    with patch.object(auth, "refresh_token", return_value=token):
        request = next(auth.auth_flow(request))

    assert request.headers["Authorization"] == "Bearer token"


@pytest.fixture
def client():
    api_creds = {
        APIService.short_term_consumption: {
            "client_id": "id",
            "client_secret": "secret",
        }
    }
    return RTEClient(api_creds)


def test_get_short_term_consumption(client):
    start = pd.Timestamp("2020-01-01", tz="CET")
    end = start + pd.DateOffset(days=1)
    ts = client.get_short_term_consumption(start, end, PrevisionType.REALISED)

    assert str(ts) == snapshot("""\
date
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
Freq: 15min, Name: REALISED, Length: 96, dtype: int64\
""")
