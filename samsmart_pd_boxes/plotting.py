import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

__all__ = ["plot_isna", "line_plot", "scatter_plot"]


def scatter_plot(df: pd.DataFrame) -> Figure:
    """Scatter plot sensor data using the timestamp as x axis."""
    plotable_df = df.reset_index()
    fig = px.scatter(
        plotable_df,
        x="timestamp",
        y=plotable_df.columns,
    )
    fig.update_traces(marker=dict(size=2))
    return fig


def line_plot(df: pd.DataFrame) -> Figure:
    """Line plot sensor data using the timestamp as x axis."""
    plotable_df = df.reset_index()
    fig = px.line(
        plotable_df,
        x="timestamp",
        y=plotable_df.columns,
    )
    return fig


def plot_isna(df: pd.DataFrame, **kwargs) -> Figure:
    """Plot `NaN` values red, non-nan values green, as an image."""
    fig = px.imshow(
        df.isna(),
        zmax=1,
        color_continuous_scale=[[0, "rgb(88,138,135)"], [1, "rgb(238,169,149)"]],
        **kwargs,
    )
    return fig
