import logging
import re
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Iterable, List

import pandas as pd
import pydantic
import requests
from pandas.tseries.frequencies import to_offset
from pydantic import BaseModel

from config import config
from resources import AVAILABLE_SENSORS, Household, Koffer, Timeframe

__all__ = [
    "all_current",
    "all_household_records",
    "all_timeframe_records",
    "check_households",
    "downsample",
    "historical",
    "index_by_timestamp",
    "merge",
    "n_latest",
    "not_nan_any",
    "outer_join_by_timestamp",
    "past_timedelta",
    "timeframes_by_source",
]

_logger = logging.getLogger(__name__)

_BASE_URL = config["server"]["base_url"]

JSON = str


class ValueRecord(BaseModel):
    date: datetime
    value: List[Any]


class ValueTypeRecord(BaseModel):
    type: str
    name: str
    unit: str


class SensorRecord(BaseModel):
    id: str
    source: str
    values: List[ValueRecord]
    valueTypes: List[ValueTypeRecord]


def _parse_SensorRecord(json_str: JSON) -> SensorRecord:
    """Parse a JSON string to a `SensorRecord`.

    Parameters
    ----------
    json_str : str
        A JSON string, as returned by the open.INC API (see
        `historical_sensordata` for example.)

    Returns
    -------
    SensorRecord
        A sensor record extracting the necessary information in a simpler form.

    Raises
    ------
    ValueError
        If JSON parsing fails.
    """
    if json_str in ["", r"{}"]:
        raise ValueError(f"Received JSON string '{json_str}' is empty")
    try:
        sensor_record = SensorRecord.model_validate_json(json_str)
        return sensor_record
    except pydantic.ValidationError as e:
        _logger.error("The JSON string failed to parse: %s", json_str)
        raise ValueError(f"Failed to parse the JSON file as SensorRecord (error: {e}).")


def _parse_SensorRecords(json_str: JSON) -> List[SensorRecord]:
    """Parse a list of `SensorRecord` from a JSON string."""
    try:
        sensor_records = pydantic.TypeAdapter(List[SensorRecord]).validate_json(
            json_str
        )
        return sensor_records
    except pydantic.ValidationError as e:
        _logger.error("The JSON string failed to parse: %s", json_str)
        raise ValueError(
            f"Failed to parse the JSON file as SensorRecords (error: {e})."
        )


def all_current(source: Koffer | None = None) -> List[pd.DataFrame]:
    """Request one record for each sensor, from the given source, where the
    requesting user has access to.

    Parameters
    ----------
    source : Koffer | None, optional
        If passed, restrict the sensor values to this source. Otherwise, return
        from all available sources. Defaults to `None`.

    Returns
    -------
    List[pd.DataFrame]
        A DataFrame for each sensor (and source combination), with a timestamp
        column (not index) and a sensor value column.
    """
    url = _build_url([_BASE_URL, "items", source])
    json = _json_from_url(url)
    sensor_records = _parse_SensorRecords(json)
    dataframes = [_to_dataframe(sr) for sr in sensor_records]
    return dataframes


def _json_from_url(
    url: str,
    params: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> JSON:
    resp = _get_with_od_session_header(url, params, session)
    resp.raise_for_status()
    json = _json_from_response(resp)
    return json


def _build_url(parts: Iterable[Any], filter_none: bool = True) -> str:
    parts = [str(part) for part in parts if not (filter_none and part is None)]
    url = "/".join(parts)
    return url


def _get_with_od_session_header(
    url: str,
    params: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> requests.Response:
    headers = {"OD-SESSION": config["user"]["od_session"]}
    _logger.debug(
        "GET requesting URL %s with parameters %s and headers %s", url, params, headers
    )
    if session is None:
        get = requests.get
    else:
        get = session.get
    resp = get(url=url, headers=headers, params=params, timeout=(10, 10))
    return resp


def _json_from_response(response: requests.Response) -> JSON:
    if "application/json" in response.headers.get("Content-Type", ""):
        return response.content.decode("utf8")
    else:
        _logger.error("This response does not contain JSON: %s", response)
        raise ValueError("Response doesn't contain JSON!")


def _to_dataframe(sensor_record: SensorRecord) -> pd.DataFrame:
    data = [
        (value_record.date, value)
        for value_record in sensor_record.values
        for value in value_record.value
    ]
    columns = ["timestamp", sensor_record.id]
    df = pd.DataFrame(
        data=data,
        columns=columns,
    )
    df = _simplify_colnames(df)
    return df


def historical(
    sensor_id: str,
    source: Koffer,
    oldest_record: datetime,
    newest_record: datetime | None = None,
    tag: str | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Request all sensor data from a specific sensor and source whose timestamp
    is in a certain range.

    Parameters
    ----------
    sensor_id : str
        The last part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "Gas". This identifies uniquely, at least within
        a source, a specific sensor.
    source : Koffer
        The first part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "koffer1". This determines the source where to
        look for the `sensor_id`.
    oldest_record : datetime
        The lower bound (inclusive) of the record's timestamp. Should be a UTC
        datetime.
    newest_record : datetime | None, optional
        The upper bound (inclusive) of the record's timestamp. Should be a UTC
        datetime. By default `None`, which means
        `datetime.now(tzinfo=timezone.utc)`.
    tag : str | None, optional
        The `tag` somewhat defines "where" the source is located. It might be
        `"koffer1"`, `"koffer2"`, `"sshXX"` or `"haushaltXX"`, where X is a
        digit. By default, `tag` is assigned the value of `source`.

    Returns
    -------
    pd.DataFrame
        A DataFrame with a timestamp column (not index) and a sensor value
        column.

    Raises
    ------
    ValueError
        If `sensor_id` is unknown / invalid.
    """
    _check_sensor_id(sensor_id)
    if newest_record is None:
        newest_record = datetime.now(tz=timezone.utc)
    if tag is None:
        tag = source
    else:
        _check_tag(tag)
    parts = [
        _BASE_URL,
        "historical",
        tag,
        _expand_sensor_id(sensor_id, source),
        _posix_milliseconds_timestamp(oldest_record),
        _posix_milliseconds_timestamp(newest_record),
    ]
    url = _build_url(parts, filter_none=False)
    json = _json_from_url(url, session=session)
    sensor_record = _parse_SensorRecord(json)
    df = _to_dataframe(sensor_record)
    _logger.info(
        "Obtained %d records for sensor %s, source %s and tag %s from the time between "
        "%s and %s",
        len(df),
        sensor_id,
        source,
        tag,
        oldest_record,
        newest_record,
    )
    return df


def _check_sensor_id(sensor_id: str) -> None:
    if sensor_id not in AVAILABLE_SENSORS:
        raise ValueError(f"SensorID {sensor_id} is not valid.")


def _check_tag(tag: str) -> None:
    valid = False
    if tag in ["koffer1", "koffer2"]:
        valid = True
    pattern = re.compile(r"^ssh[0-9]+$")
    if pattern.match(tag):
        valid = True
    pattern = re.compile(r"^haushalt[0-9]+$")
    if pattern.match(tag):
        valid = True
    if not valid:
        raise ValueError(f"Tag {tag} is not valid.")


def _expand_sensor_id(sensor_id: str, source: Koffer) -> str:
    _check_sensor_id(sensor_id)
    return f"{source}.sensor.{sensor_id}"


def _posix_milliseconds_timestamp(dt: datetime) -> int:
    ts = int(dt.replace(tzinfo=timezone.utc).timestamp()) * 1000
    return ts


def past_timedelta(
    sensor_id: str, source: Koffer, td: timedelta, tag: str | None = None
) -> pd.DataFrame:
    """Request the most recent `td` worth of sensor data for a specific sensor
    and source.

    For example, if `td=timedelta(hours=48)` is passed, request the sensor
    data from the last 48 hours up until now.

    Parameters
    ----------
    sensor_id : str
        The last part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "Gas". This identifies uniquely, at least within
        a source, a specific sensor.
    source : Koffer
        The first part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "koffer1". This determines the source where to
        look for the `sensor_id`.
    td : timedelta
        The timedelta to look back from now into the past.
    tag : str | None, optional
        The `tag` somewhat defines "where" the source is located. It might be
        `"koffer1"`, `"koffer2"`, `"sshXX"` or `"haushaltXX"`, where X is a
        digit. By default, `tag` is assigned the value of `source`.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing sensor data from most recent `td`.

    Raises
    ------
    ValueError
        If `sensor_id` is unknown / invalid.
    """
    _check_sensor_id(sensor_id)
    if tag is None:
        tag = source
    else:
        _check_tag(tag)
    now = datetime.now(tz=timezone.utc)
    then = now - td
    return historical(sensor_id, source, then, now, tag)


def n_latest(
    sensor_id: str, source: Koffer, n: int, tag: str | None = None
) -> pd.DataFrame:
    """Request the `n` latest records for a specific sensor and source.

    Parameters
    ----------
    sensor_id : str
        The last part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "Gas". This identifies uniquely, at least within
        a source, a specific sensor.
    source : Koffer
        The first part of a "koffer{n}.sensor.{identifier}" string. For example:
        "koffer1.sensor.Gas" -> "koffer1". This determines the source where to
        look for the `sensor_id`.
    n : int
        How many records to fetch, at most.
    tag : str | None, optional
        The `tag` somewhat defines "where" the source is located. It might be
        `"koffer1"`, `"koffer2"`, `"sshXX"` or `"haushaltXX"`, where X is a
        digit. By default, `tag` is assigned the value of `source`.

    Returns
    -------
    pt.DataFrame
        A DataFrame with a timestamp column (not index) and a sensor value
        column, containing `n` lines.
    Raises
    ------
    ValueError
        If `sensor_id` is unknown / invalid.
    """
    _check_sensor_id(sensor_id)
    if tag is None:
        tag = source
    else:
        _check_tag(tag)
    ts = str(_posix_milliseconds_timestamp(datetime.now(tz=timezone.utc)))
    parts = [_BASE_URL, "live", tag, _expand_sensor_id(sensor_id, source)]
    url = _build_url(parts, filter_none=False)
    params = dict(at=ts, values=str(n))
    json = _json_from_url(url, params)
    sensor_record = _parse_SensorRecord(json)
    df = _to_dataframe(sensor_record)
    return df


def all_household_records(
    household: Household, session: requests.Session | None = None
) -> pd.DataFrame:
    """Obtain all sensor records belonging to a household.

    Parameters
    ----------
    household : Household
        The household to obtain captured sensor values from, covering all its
        timeframes.
    session : requests.Session | None, optional
        The session object to use, in case you want to pool the multiple
        connection while calling this function multiple times. By default
        `None`, which means that an intermediate session object is used to pool
        at least the multiple connection this function opens itself.

    Returns
    -------
    pd.DataFrame
        A DataFrame holding all the available sensor values of the given
        household, indexed by timestamps.

    Notes
    -----
    - Colliding timestamps are handled by the default behavior of `merge`.
    - The timestamps are not binned/downsampled yet – so expect a lot of NaN
      values. Use `downsample` for further processing.
    - It might be the case that for some households, both boxes have been there
      (at different times). If you would like to keep the data from the two
      boxes separate, don't use this function as it does not differentiate them.
    """
    timeframe_dfs = [
        all_timeframe_records(timeframe, session) for timeframe in household.timeframes
    ]
    return pd.concat(timeframe_dfs, axis="index")


def all_timeframe_records(
    timeframe: Timeframe, session: requests.Session | None = None
) -> pd.DataFrame:
    """Obtain all sensor records belonging to a single timeframe.

    Parameters
    ----------
    timeframe : Timeframe
        The timeframe to obtain sensor records for.
    session : requests.Session | None, optional
        The session object to use, in case you want to pool the multiple
        connection while calling this function multiple times. By default
        `None`, which means that an intermediate session object is used to pool
        at least the multiple connection this function opens itself.

    Returns
    -------
    pd.DataFrame
        A DataFrame holding all the available sensor values of the given
        timeframe, indexed by timestamps.

    Notes
    -----
    Colliding timestamps are handled by the default behavior of `merge`. Also,
    the timestamps are not binned/downsampled yet – so expect a lot of NaN
    values. Use `downsample` for further processing.
    """
    if session is None:
        session = requests.Session()
    dfs = []
    for available_sensor in AVAILABLE_SENSORS:
        try:
            df = historical(
                sensor_id=available_sensor,
                source=timeframe.source,
                oldest_record=timeframe.oldest_record,
                newest_record=timeframe.newest_record,
                tag=timeframe.tag,
                session=session,
            )
            dfs.append(df)
        except (requests.HTTPError, ValueError):
            _logger.warning(
                "Unable to obtain records for sensor '%s'", available_sensor
            )
    merged_df = merge(dfs)
    return merged_df


def index_by_timestamp(df: pd.DataFrame, aggregation_function) -> pd.DataFrame:
    """For a DataFrame with column "timestamp", remove potential duplicates in
    that column, and make it the new DataFrame's index.

    The `aggregation_function` determines how to handle duplicate sensor values
    for the same timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame with "timestamp" column.
    aggregation_function : _type_
        An aggregation function. It should be of a kind that is accepted by
        pandas' `agg` function (see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.DataFrameGroupBy.aggregate.html
        and https://pandas.pydata.org/pandas-docs/stable/reference/groupby.html#dataframegroupby-computations-descriptive-stats).

    Returns
    -------
    pd.DataFrame


    Raises
    ------
    ValueError
        If de-duplication failed and the timestamp index would be ambiguous.
    """
    no_dup_df: pd.DataFrame = df.groupby("timestamp", as_index=False).agg(
        aggregation_function
    )
    try:
        indexed_df = no_dup_df.set_index("timestamp", verify_integrity=True)
        if len(df) != len(indexed_df):
            _logger.info(
                "De-duplication was actually necessary. Found %d duplicates.",
                len(df) - len(indexed_df),
            )
        return indexed_df
    except ValueError as e:
        raise ValueError(
            "Failed to set timestamp index for input DataFrame. "
            "Probably, something went wrong during previous de-duplication"
        ) from e


def outer_join_by_timestamp(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Join a list of dataframes on their index (which usually is the
    timestamp).

    As the join is performed "outer", you have to expect a lot of `nan` values
    and post-process the return value (e.g. by resampling).

    Parameters
    ----------
    dfs : list[pd.DataFrame]
        A list of dataframes with join-compatible `index`.

    Returns
    -------
    pd.DataFrame

    """
    if dfs:
        joined_df = dfs[0].join(dfs[1:], how="outer")  # type: ignore
    else:
        joined_df = pd.DataFrame()
    return joined_df


def merge(
    dfs: Iterable[pd.DataFrame], aggregation_functions: dict[str, Any] | None = None
) -> pd.DataFrame:
    """Perform indexing and joining for multiple DataFrames in one go.

    Parameters
    ----------
    dfs : list[pd.DataFrame]
        A list of DataFrames, one per sensor. Assume each DataFrame to contain
        two columns, one with name "timestamp" (not an index) and one with the
        a name identifying the specific sensor.
    aggregation_functions : dict[str, Any] | None
        A mapping of sensor column names to an aggregation functions that
        individually are accepted by pandas' `agg` function (see
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.DataFrameGroupBy.aggregate.html).
        See `index_by_timestamp` on why aggregation functions might be
        necessary. If `None`, use aggregation function 'first' for every sensor.
        By default, `None`.
        Please note, that if a column name is encountered that is not
        referenced in `aggregation_functions`, the aggregation function 'first'
        is chosen as default (instead of failing).

    Returns
    -------
    pd.DataFrame
        A DataFrame indexed by timestamp, containing all the sensor columns of
        `dfs`, which are present in `aggregation_functions.keys()`.
    """
    indexed_dfs = []
    if aggregation_functions is None:
        aggregation_functions = {}
    for df in dfs:
        cols = df.columns
        assert len(cols) == 2
        assert cols[0] == "timestamp"
        indexed_df = index_by_timestamp(df, aggregation_functions.get(cols[1], "first"))
        indexed_dfs.append(indexed_df)
    joined_df = outer_join_by_timestamp(indexed_dfs)
    return joined_df


def _simplify_colnames(df: pd.DataFrame) -> pd.DataFrame:
    """Strip the annoying "koffer1.sensor." or "koffer2.sensor." prefix from
    column labels.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame whose columns should be simplified.

    Returns
    -------
    pd.DataFrame
        The DataFrame with simplified columns.
    """
    return df.rename(columns=_rename)


def _rename(colname: str) -> str:
    return colname.replace("koffer1.sensor.", "").replace("koffer2.sensor.", "")


def downsample(
    joined_df: pd.DataFrame, timedelta, aggregation_function
) -> pd.DataFrame:
    """Downsample, or "bin", and aggregate a (sparse) DataFrame of several
    sensor values (e.g. the output of the `outer_join_by_timestamp` function)

    Parameters
    ----------
    joined_df : pd.DataFrame
        The DataFrame to downsample. It is expected to have a datetime index.
    timedelta
        Something that may be used in pandas' `Timedelta` constructor, e.g.
        `"1d"`, `"3min"` or `"10s"`.
    aggregation_function
        An aggregation function. It should be of a kind that is accepted by
        pandas' `agg` function (see
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.DataFrameGroupBy.aggregate.html).

    Returns
    -------
    pd.DataFrame
        The downsampled DataFrame.
    """
    downsampled_df = joined_df.groupby(
        partial(_round_timestamps, timedelta=timedelta)
    ).agg(aggregation_function)
    return downsampled_df


def _round_timestamps(timestamps, timedelta):
    timedelta = to_offset(timedelta)
    td = pd.Timedelta(timedelta)  # type: ignore
    return pd.Timestamp((timestamps.value // td.value) * td.value)


def not_nan_any(series: pd.Series) -> bool:
    """Indicate whether the given series contains any actual `True` values.

    `NaN` values are ignored. If a series contains only `NaN` values, return
    `False`.

    Parameters
    ----------
    series : pd.Series
        The boolean series to assess.

    Returns
    -------
    bool
        `True` iff `series` contains any `True` value. `False` otherwise.
    """
    return series.dropna().any()


def timeframes_by_source(
    households: dict[str, Household],
) -> dict[Koffer, list[Timeframe]]:
    """Group timeframes by their source (i.e. the box their belong to).

    Parameters
    ----------
    households : dict[str, Household]
        The households dictionary, grouped by household.

    Returns
    -------
    dict[Koffer, list[Timeframe]]
        Timeframes by source.
    """
    tbs = dict(
        koffer1=[],
        koffer2=[],
    )

    # group by source
    for household in households.values():
        for timeframe in household.timeframes:
            tbs[timeframe.source].append(timeframe)
    return tbs  # type: ignore


def check_households(households: dict[str, Household]) -> None:
    """Check whether there are overlapping timeframes in the household data for
    a given source.

    If there would be overlapping timeframes for the same source, that would
    mean the very same box would be at two different places at the same time.

    Parameters
    ----------
    households : dict[str, Household]
        The household data.

    Raises
    ------
    ValueError
        If and only if there are timeframes that have a non-empty intersection
        (which should not happen).
    """
    tbs = timeframes_by_source(households)

    # sort each group by newest_record
    for timeframes in tbs.values():
        timeframes.sort(key=lambda tf: tf.newest_record)

    for timeframes in tbs.values():
        for i in range(1, len(timeframes)):
            t1, t2 = timeframes[i - 1], timeframes[i]
            if t1.newest_record > t2.oldest_record:
                raise ValueError(
                    f"Sanity check failed: Timeframes {t1} and {t2} overlap."
                )
