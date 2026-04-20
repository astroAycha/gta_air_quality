"""
map_builder.py — Builds Plotly scatter-mapbox figures from a readings DataFrame.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

TORONTO_LAT = 43.70
TORONTO_LON = -79.385

COLOR_SCALE = "tealrose"
MAPBOX_STYLE = "carto-positron"
DEFAULT_ZOOM = 9


def _common_layout(fig: go.Figure,
                   pm25_min: float,
                   pm25_max: float) -> go.Figure:
    fig.update_layout(
        margin=dict(l=0, r=0, t=36, b=0),
        coloraxis_colorbar=dict(
            title="PM2.5<br>(µg/m³)",
            thickness=14,
            len=0.6,
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family="'DM Sans', sans-serif", color="#e8e0d4"),
    )
    fig.update_coloraxes(
        cmin=pm25_min,
        cmax=pm25_max,
        colorscale=COLOR_SCALE,
    )
    return fig


def build_latest_map(df: pd.DataFrame) -> go.Figure:
    """
    Single-frame map showing the most-recent reading per sensor.

    Parameters
    ----------
    df : DataFrame with columns [Date, PM2.5, name, latitude, longitude]
    """
    if df.empty:
        return _empty_figure("No data available yet — first fetch in progress.")

    pm25_min = df["PM2.5"].min()
    pm25_max = df["PM2.5"].max()

    # Ensure non-zero size range so tiny values still show a dot
    size_col = (df["PM2.5"] - pm25_min + 1).clip(lower=1)

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="PM2.5",
        size=size_col,
        color_continuous_scale=COLOR_SCALE,
        center={"lat": TORONTO_LAT, "lon": TORONTO_LON},
        zoom=DEFAULT_ZOOM,
        mapbox_style=MAPBOX_STYLE,
        hover_name="name",
        hover_data={"PM2.5": ":.1f", "Date": True,
                    "latitude": False, "longitude": False},
        title="Latest PM2.5 Readings",
    )

    return _common_layout(fig, pm25_min, pm25_max)


def build_historical_map(df: pd.DataFrame) -> go.Figure:
    """
    Animated map with one frame per date.

    Parameters
    ----------
    df : DataFrame with columns [Date, PM2.5, name, latitude, longitude]
    """
    if df.empty:
        return _empty_figure("No historical data available yet.")

    # Aggregate: one value per (date, sensor) — mean in case of duplicates
    agg = (
        df.groupby(["Date", "name", "sensor_id", "latitude", "longitude"], as_index=False)
        ["PM2.5"].mean()
    )
    agg = agg.sort_values("Date")

    pm25_min = agg["PM2.5"].min()
    pm25_max = agg["PM2.5"].max()
    size_col = (agg["PM2.5"] - pm25_min + 1).clip(lower=1)
    agg = agg.copy()
    agg["_size"] = size_col

    fig = px.scatter_mapbox(
        agg,
        lat="latitude",
        lon="longitude",
        color="PM2.5",
        size="_size",
        animation_frame="Date",
        color_continuous_scale=COLOR_SCALE,
        center={"lat": TORONTO_LAT, "lon": TORONTO_LON},
        zoom=DEFAULT_ZOOM,
        mapbox_style=MAPBOX_STYLE,
        hover_name="name",
        hover_data={"PM2.5": ":.1f", "_size": False,
                    "latitude": False, "longitude": False},
        title="PM2.5 History (30 days)",
    )

    # Tune animation speed
    if fig.layout.updatemenus:
        fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 800
        fig.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 200

    return _common_layout(fig, pm25_min, pm25_max)


def _empty_figure(message: str) -> go.Figure:
    """Placeholder map shown while data is loading."""
    fig = go.Figure(go.Scattermapbox())
    fig.update_layout(
        mapbox=dict(
            style=MAPBOX_STYLE,
            center={"lat": TORONTO_LAT, "lon": TORONTO_LON},
            zoom=DEFAULT_ZOOM,
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        annotations=[dict(
            text=message,
            x=0.5, y=0.5,
            xref="paper", yref="paper",
            showarrow=False,
            font=dict(size=16, color="#aaa"),
        )],
        paper_bgcolor='rgba(0,0,0,0)',
    )
    return fig
