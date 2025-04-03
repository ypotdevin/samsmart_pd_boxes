from typing import Iterable

import numpy as np
import pandas as pd

from resources import AVAILABLE_SENSORS

__all__ = [
    "absolute_humidity",
    "column_sum",
    "nominals_cardinals",
    "normalize",
    "remove_outliers",
    "smoothed_average",
]


def nominals_cardinals(
    df: pd.DataFrame,
    nominal_cols: list[str] | None = None,
    cardinal_cols: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a dataframe of multiple sensor columns into two dataframes, one
    containing all nominal sensor records and the other containing all cardinal
    sensor records

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to split.
    nominal_cols : list[str] | None, optional
        Which columns to consider as nominal. By default `None`. If `None`, use
        the declaration given in the `resources.AVAILABLE_SENSORS` constant.
    cardinal_cols : list[str] | None, optional
        Which columns to consider as cardinal. By default `None`. If `None`, use
        the declaration given in the `resources.AVAILABLE_SENSORS` constant.

    Returns
    -------
    (nominals, cardinals) : tuple[pd.DataFrame, pd.DataFrame]
        The nominal DataFrame and the cardinal DataFrame.
    """
    available_cols = df.columns
    if nominal_cols is None:
        nominal_cols = [
            col for col in available_cols if AVAILABLE_SENSORS.get(col, "") == "nominal"
        ]
    if cardinal_cols is None:
        cardinal_cols = [
            col
            for col in available_cols
            if AVAILABLE_SENSORS.get(col, "") == "cardinal"
        ]
    nominals = df[nominal_cols]
    cardinals = df[cardinal_cols]
    return (nominals, cardinals)


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove all outliers from a DataFrame, column-wise.

    An entry is an outlier, if it is further than 3 standard deviations away
    from the mean.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to remove the outliers from. It is assumed, that it only
        contains cardinal data (not nominal).

    Returns
    -------
    pd.DataFrame
        A DataFrame without outliers.
    """
    return df.mask(((df - df.mean()) / df.std()).abs() > 3)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize (scale the entries to be in range [0.0, 1.0]) a DataFrame,
    column-wise.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to normalize. It is assumed, that it only contains
        cardinal data (not nominal).

    Returns
    -------
    pd.DataFrame
        The normalized DataFrame.
    """
    _df = df - df.min()
    _df = _df / _df.max()
    return _df


def _saturated_vapor_pressure(temperature: pd.Series) -> pd.Series:
    """Estimate the saturated vapor pressure by Teten's equation.

    Parameters
    ----------
    temperature : pd.Series
        Temperature measured in degrees Celsius.

    Returns
    -------
    pd.Series
        Saturated vapor pressure, measured in kilopascals (kPa).

    Notes
    -----
    See https://en.wikipedia.org/wiki/Tetens_equation.
    """
    result = pd.Series(
        index=temperature.index,
        data=0.61078 * np.exp(17.27 * temperature / (temperature + 237.3)),
        name="saturated_vapor_pressure",
    )  # type: ignore
    return result


def _absolute_temperature(celsius_temperature: pd.Series) -> pd.Series:
    """Convert degrees Celsius to degrees Kelvin."""
    return celsius_temperature + 273.15


def absolute_humidity(
    temperature: pd.Series, relative_humidity: pd.Series
) -> pd.Series:
    """Derive the absolute humidity from relative humidity and temperature.

    Parameters
    ----------
    temperature : pd.Series
        Temperature measured in degrees Celsius.
    relative_humidity : pd.Series
        Relative humidity measured in percentage points (so for 62.5%, expect
        62.5 and not 0.625).

    Returns
    -------
    pd.Series
        Absolute humidity in g/m³.

    Notes
    -----
    See https://www.calctool.org/atmospheric-thermodynamics/absolute-humidity.
    """
    specific_gas_constant_for_water_vapor = 461.5
    result = (
        1000
        * (
            1000
            * _saturated_vapor_pressure(temperature)
            * (1 / 100)
            * relative_humidity
        )
        / (specific_gas_constant_for_water_vapor * _absolute_temperature(temperature))
    )
    return result


def expected_relative_humidity(
    reference_temperature: pd.Series,
    reference_relative_humidity: pd.Series,
    current_temperature: pd.Series,
) -> pd.Series:
    """Deprecation warning: Use `absolute_humidity` instead!

    Given an (earlier) reference measurement of temperature and relative
    humidity, and a current temperature, estimate the current relative humidity
    that would be measured, if the no other parameters change (e.g. pressure or
    absolute humidity)"""
    return reference_relative_humidity * (
        _saturated_vapor_pressure(reference_temperature)
        / _saturated_vapor_pressure(current_temperature)
    )


def smoothed_average(halflife: float, df: pd.DataFrame) -> pd.DataFrame:
    """Smooth (cardinal) sequential data of a DataFrame, by using `pd.ewm`.

    Parameters
    ----------
    halflife : float
        The number of time steps after which an observation decayed to half of
        its value (see the docs of `pd.ewm`'s `halflife` parameter).
    df : pd.DataFrame
        A DataFrame containing sequential data to smooth.

    Returns
    -------
    pd.DataFrame
        A smoothed DataFrame.
    """
    return df.ewm(halflife=halflife).mean()


def column_sum(
    df: pd.DataFrame,
    columns_to_sum: Iterable[str],
    sum_title: str | None = None,
) -> pd.DataFrame:
    """A single-column DataFrame which represents the sum of selected columns of
    another DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the columns to sum.
    columns_to_sum : Iterable[str]
        The columns that should be part of the summation.
    sum_title : str | None, optional
        An optional custom column title for the sum column. If `None`, the new
        column's title is "sum". By default `None`.

    Returns
    -------
    pd.DataFrame
        A new DataFrame containing the sum column.
    """
    result = pd.DataFrame(index=df.index)
    result["sum" if sum_title is None else sum_title] = sum(
        df[col_to_sum] for col_to_sum in columns_to_sum
    )
    return result
