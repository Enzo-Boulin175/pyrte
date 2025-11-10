import pandas as pd


def format_ts(
    ts: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
    ts_tz: str | None = None,
    freq: str = "1h",
    include_start: bool = True,
    include_end: bool = False,
    include_equal_end: bool = False,
) -> pd.Series:
    """
    Format a timeseries to have:
    - hourly (or custom) frequency between start and end
    - UTC timezone
    - missing values filled with NaN
    - index floored/ceiled to full hour boundaries

    Parameters
    ----------
    ts : pd.Series
        Input timeseries with a datetime-like index.
    start : pd.Timestamp
        Start time (tz-aware).
    end : pd.Timestamp
        End time (tz-aware).
    ts_tz : str | None, optional
        Timezone of ts if its index is tz-naive. Required if index is tz-naive.
    freq : str, optional
        Frequency for reindexing (default "1h").
    include_start : bool, optional
        Whether to include the start timestamp (default True).
    include_end : bool, optional
        Whether to include the end timestamp (default False).
    include_equal_end : bool, optional
        If True, include end even if ts.index[-1] == end - pd.Timedelta(freq) (default False).

    Returns
    -------
    pd.Series
        Formatted timeseries with UTC timezone and given frequency.

    Raises
    ------
    ValueError
        If start or end are not tz-aware.
        If ts index is tz-naive and ts_tz is None.
        If reindexing fails.
    """

    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("Both `start` and `end` must be timezone-aware Timestamps.")

    if not isinstance(ts.index, pd.DatetimeIndex):
        raise ValueError("`ts` must have a DatetimeIndex.")

    idx = ts.index
    if idx.tz is None:
        if ts_tz is None:
            raise ValueError("`ts` index is tz-naive and `ts_tz` was not provided.")
        idx = idx.tz_localize(ts_tz)
    ts = ts.copy()
    ts.index = idx

    ts = ts.tz_convert("UTC")

    cut_start = (start.floor("h") if include_start else start.ceil("h")).tz_convert(
        "UTC"
    )
    cut_end = (end.ceil("h") if include_end else end.floor("h")).tz_convert("UTC")
    # If the end should not be included in case of equality, we adjust cut_end
    if not include_equal_end and ts.index[-1] + pd.Timedelta(freq) == end:
        cut_end = cut_end - pd.Timedelta(freq)

    try:
        target_index = pd.date_range(cut_start, cut_end, freq=freq, tz="UTC")
    except Exception as e:
        raise ValueError(f"Failed to create target index: {e}")

    try:
        ts = ts.reindex(target_index)
    except Exception as e:
        raise ValueError(f"Reindexing failed: {e}")

    return ts
