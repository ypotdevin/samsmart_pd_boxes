import pandas as pd
import plotly.express as px


def scatter_plot(df: pd.DataFrame):
    plotable_df = df.reset_index()
    fig = px.scatter(
        plotable_df,
        x="timestamp",
        y=plotable_df.columns,
    )
    fig.update_traces(marker=dict(size=2))
    return fig


def line_plot(df: pd.DataFrame):
    plotable_df = df.reset_index()
    fig = px.line(
        plotable_df,
        x="timestamp",
        y=plotable_df.columns,
    )
    return fig


def data_availability(df: pd.DataFrame, **kwargs):
    fig = px.imshow(
        df.isna(),
        zmax=1,
        color_continuous_scale=[[0, "rgb(88,138,135)"], [1, "rgb(238,169,149)"]],
        **kwargs,
    )
    return fig
