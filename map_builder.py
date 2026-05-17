"""
map_builder.py — Builds Plotly scatter-mapbox figures from a readings DataFrame.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

TORONTO_LAT = 43.70
TORONTO_LON = -79.385

MAPBOX_STYLE = "carto-positron"
DEFAULT_ZOOM = 9

# Fixed AQI bounds matching EPA breakpoints
AQI_MIN = 0.0
AQI_MAX = 250.4

# Custom colorscale anchored exactly to AQI breakpoints
# Each entry: [normalised position (0-1), colour]
# Breakpoints: Good=0-12, Moderate=12-35.4, Sensitive=35.4-55.4,
#              Unhealthy=55.4-150.4, Very Unhealthy=150.4-250.4
def _norm(v):
    return round(v / AQI_MAX, 4)

AQI_COLORSCALE = [
    [0.0,          "#00e5a0"],   # Good (low)
    [_norm(12),    "#00e5a0"],   # Good (high)
    [_norm(12.01), "#f5c518"],   # Moderate (low)
    [_norm(35.4),  "#f5c518"],   # Moderate (high)
    [_norm(35.41), "#ff8c42"],   # Sensitive (low)
    [_norm(55.4),  "#ff8c42"],   # Sensitive (high)
    [_norm(55.41), "#e05252"],   # Unhealthy (low)
    [_norm(150.4), "#e05252"],   # Unhealthy (high)
    [_norm(150.41),"#9b59b6"],   # Very Unhealthy (low)
    [1.0,          "#9b59b6"],   # Very Unhealthy (high) / Hazardous
]


def _size_col(series: pd.Series) -> pd.Series:
    """Scale PM2.5 values to marker sizes, ensuring a visible minimum."""
    return (series - AQI_MIN + 1).clip(lower=1)


def _common_layout(fig: go.Figure) -> go.Figure:
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
    # Always pin to fixed AQI range so colours match the legend
    fig.update_coloraxes(
        cmin=AQI_MIN,
        cmax=AQI_MAX,
        colorscale=AQI_COLORSCALE,
    )
    return fig


def build_latest_map(df: pd.DataFrame) -> go.Figure:
    """
    Single-frame map showing the most-recent reading per sensor.
    """
    if df.empty:
        return _empty_figure("No data available yet — first fetch in progress.")

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="PM2.5",
        size=_size_col(df["PM2.5"]),
        color_continuous_scale=AQI_COLORSCALE,
        range_color=[AQI_MIN, AQI_MAX],
        center={"lat": TORONTO_LAT, "lon": TORONTO_LON},
        zoom=DEFAULT_ZOOM,
        mapbox_style=MAPBOX_STYLE,
        hover_name="name",
        hover_data={"PM2.5": ":.1f", "Date": True,
                    "latitude": False, "longitude": False},
        title="Latest PM2.5 Readings",
    )

    return _common_layout(fig)


def build_historical_map(df: pd.DataFrame) -> go.Figure:
    """
    Animated map with one frame per date.
    """
    if df.empty:
        return _empty_figure("No historical data available yet.")

    # Aggregate: one value per (date, sensor) — mean in case of duplicates
    agg = (
        df.groupby(["Date", "name", "sensor_id", "latitude", "longitude"], as_index=False)
        ["PM2.5"].mean()
    )
    agg = agg.sort_values("Date")
    agg["_size"] = _size_col(agg["PM2.5"])

    fig = px.scatter_mapbox(
        agg,
        lat="latitude",
        lon="longitude",
        color="PM2.5",
        size="_size",
        animation_frame="Date",
        color_continuous_scale=AQI_COLORSCALE,
        range_color=[AQI_MIN, AQI_MAX],
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

    return _common_layout(fig)


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
