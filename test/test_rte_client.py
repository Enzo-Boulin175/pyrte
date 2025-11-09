import pandas as pd
import vcr
from inline_snapshot import snapshot

from rte_client import RTEClient

VCR_DIR = "tests/cassettes/"


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
