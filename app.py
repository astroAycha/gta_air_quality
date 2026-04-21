"""
app.py — Dash application for the GTA Air Quality dashboard.

Run:
    python app.py
"""

import logging
from datetime import datetime

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

from storage import load_latest_readings, load_readings
from map_builder import build_latest_map, build_historical_map

# ── Logging ──────────────────────────────────────────────────────────────────
import os as _os
_os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/app.log', mode='a'),
    ]
)

# ── Bootstrap + custom theme ─────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.CYBORG,
        "https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500&family=Space+Mono:wght@400;700&display=swap",
    ],
    title="GTA Air Quality",
    update_title=None,
)
server = app.server   # expose Flask server for production WSGI

# ── Layout ───────────────────────────────────────────────────────────────────
SIDEBAR_STYLE = {
    "background": "#0d1117",
    "borderRight": "1px solid #1e2a38",
    "padding": "2rem 1.5rem",
    "minHeight": "100vh",
    "display": "flex",
    "flexDirection": "column",
    "gap": "1.5rem",
}

METRIC_CARD = {
    "background": "#111827",
    "border": "1px solid #1e2a38",
    "borderRadius": "12px",
    "padding": "1rem 1.25rem",
}


def make_aqi_badge(value: float) -> html.Span:
    """Return a coloured AQI category badge."""
    if value is None:
        return html.Span("–", style={"color": "#666"})
    if value <= 12:
        cat, color = "Good", "#00e5a0"
    elif value <= 35.4:
        cat, color = "Moderate", "#f5c518"
    elif value <= 55.4:
        cat, color = "Sensitive", "#ff8c42"
    elif value <= 150.4:
        cat, color = "Unhealthy", "#e05252"
    elif value <= 250.4:
        cat, color = "Very Unhealthy", "#9b59b6"
    else:
        cat, color = "Hazardous", "#7b241c"

    return html.Span(
        cat,
        style={
            "background": color + "22",
            "color": color,
            "border": f"1px solid {color}55",
            "borderRadius": "6px",
            "padding": "2px 10px",
            "fontSize": "0.75rem",
            "fontFamily": "'Space Mono', monospace",
            "letterSpacing": "0.08em",
        }
    )


app.layout = dbc.Container(
    fluid=True,
    style={"background": "#080d13", "minHeight": "100vh", "padding": 0},
    children=[

        # ── Auto-refresh interval (every 60 min) ─────────────────────────
        dcc.Interval(id="interval", interval=60 * 60 * 1000, n_intervals=0)  # refresh every hour,

        dbc.Row(
            style={"margin": 0, "minHeight": "100vh"},
            children=[

                # ── Sidebar ───────────────────────────────────────────────
                dbc.Col(
                    width=3,
                    style=SIDEBAR_STYLE,
                    children=[

                        # Title
                        html.Div([
                            html.P(
                                "AIR QUALITY",
                                style={
                                    "fontFamily": "'Space Mono', monospace",
                                    "fontSize": "0.65rem",
                                    "letterSpacing": "0.22em",
                                    "color": "#4a90a4",
                                    "margin": 0,
                                }
                            ),
                            html.H1(
                                "Greater Toronto",
                                style={
                                    "fontFamily": "'DM Sans', sans-serif",
                                    "fontWeight": 300,
                                    "fontSize": "1.6rem",
                                    "color": "#e8e0d4",
                                    "margin": "0.25rem 0 0 0",
                                    "lineHeight": 1.1,
                                }
                            ),
                        ]),

                        html.Hr(style={"borderColor": "#1e2a38", "margin": "0.5rem 0"}),

                        # View toggle
                        html.Div([
                            html.P(
                                "VIEW",
                                style={
                                    "fontFamily": "'Space Mono', monospace",
                                    "fontSize": "0.6rem",
                                    "letterSpacing": "0.2em",
                                    "color": "#4a6a7a",
                                    "marginBottom": "0.5rem",
                                }
                            ),
                            dbc.RadioItems(
                                id="view-toggle",
                                options=[
                                    {"label": "Latest reading", "value": "latest"},
                                    {"label": "30-day history",  "value": "history"},
                                ],
                                value="latest",
                                inline=False,
                                inputStyle={"marginRight": "8px"},
                                labelStyle={
                                    "fontFamily": "'DM Sans', sans-serif",
                                    "fontSize": "0.9rem",
                                    "color": "#c8bfb5",
                                    "cursor": "pointer",
                                },
                            ),
                        ]),

                        html.Hr(style={"borderColor": "#1e2a38", "margin": "0.5rem 0"}),

                        # Summary metrics
                        html.Div(id="summary-cards"),

                        html.Hr(style={"borderColor": "#1e2a38", "margin": "0.5rem 0"}),

                        # Last updated
                        html.Div(
                            id="last-updated",
                            style={
                                "fontFamily": "'Space Mono', monospace",
                                "fontSize": "0.65rem",
                                "color": "#3a5060",
                                "marginTop": "auto",
                            }
                        ),

                        # PM2.5 scale legend
                        html.Div([
                            html.P(
                                "PM2.5 AQI GUIDE (µg/m³)",
                                style={
                                    "fontFamily": "'Space Mono', monospace",
                                    "fontSize": "0.58rem",
                                    "letterSpacing": "0.15em",
                                    "color": "#4a6a7a",
                                    "marginBottom": "0.5rem",
                                }
                            ),
                            *[
                                html.Div(
                                    [
                                        html.Span("●", style={"color": c, "marginRight": "6px"}),
                                        html.Span(label, style={"color": "#8899aa", "fontSize": "0.75rem"}),
                                    ],
                                    style={"fontFamily": "'DM Sans', sans-serif", "marginBottom": "3px"}
                                )
                                for label, c in [
                                    ("Good  ≤12", "#00e5a0"),
                                    ("Moderate  ≤35.4", "#f5c518"),
                                    ("Sensitive  ≤55.4", "#ff8c42"),
                                    ("Unhealthy  ≤150.4", "#e05252"),
                                    ("Very Unhealthy  ≤250.4", "#9b59b6"),
                                    ("Hazardous  >250.4", "#7b241c"),
                                ]
                            ],
                        ]),
                    ]
                ),

                # ── Map panel ─────────────────────────────────────────────
                dbc.Col(
                    width=9,
                    style={"padding": 0, "position": "relative"},
                    children=[
                        dcc.Graph(
                            id="air-quality-map",
                            style={"height": "100vh"},
                            config={"displayModeBar": True, "scrollZoom": True},
                        ),
                    ]
                ),
            ]
        )
    ]
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

@callback(
    Output("air-quality-map",  "figure"),
    Output("summary-cards",    "children"),
    Output("last-updated",     "children"),
    Input("interval",          "n_intervals"),
    Input("view-toggle",       "value"),
)
def update_dashboard(n_intervals, view):
    # ── Map figure ────────────────────────────────────────────────────────
    if view == "latest":
        df = load_latest_readings()
        fig = build_latest_map(df)
    else:
        df = load_readings(days=30)
        fig = build_historical_map(df)

    # ── Summary cards ─────────────────────────────────────────────────────
    latest_df = load_latest_readings()

    if latest_df.empty:
        cards = html.P("Fetching data …",
                       style={"color": "#4a6a7a", "fontSize": "0.85rem"})
    else:
        avg   = latest_df["PM2.5"].mean()
        worst = latest_df.loc[latest_df["PM2.5"].idxmax()]
        best  = latest_df.loc[latest_df["PM2.5"].idxmin()]
        n     = len(latest_df)

        def stat_row(label, val, extra=None):
            return html.Div([
                html.P(label, style={
                    "fontFamily": "'Space Mono', monospace",
                    "fontSize": "0.58rem",
                    "letterSpacing": "0.18em",
                    "color": "#4a6a7a",
                    "marginBottom": "2px",
                }),
                html.Div([
                    html.Span(f"{val:.1f} µg/m³", style={
                        "fontFamily": "'DM Sans', sans-serif",
                        "fontSize": "1.1rem",
                        "color": "#e8e0d4",
                        "marginRight": "8px",
                    }),
                    make_aqi_badge(val),
                ]),
                html.P(extra, style={
                    "fontFamily": "'DM Sans', sans-serif",
                    "fontSize": "0.72rem",
                    "color": "#5a7a8a",
                    "marginTop": "2px",
                }) if extra else None,
            ], style={**METRIC_CARD, "marginBottom": "0.6rem"})

        cards = html.Div([
            stat_row("NETWORK AVERAGE", avg, f"{n} active sensors"),
            stat_row("HIGHEST",  worst["PM2.5"], worst["name"]),
            stat_row("LOWEST",   best["PM2.5"],  best["name"]),
        ])

    # ── Timestamp ─────────────────────────────────────────────────────────
    ts = f"UPDATED {datetime.now():%Y-%m-%d %H:%M}"

    return fig, cards, ts


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _os.makedirs("logs", exist_ok=True)
    app.run(debug=False, host="0.0.0.0", port=7860)
