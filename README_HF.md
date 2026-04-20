---
title: GTA Air Quality
emoji: 🌫️
colorFrom: blue
colorTo: green
sdk: docker
app_file: app.py
pinned: false
license: mit
---

# Spatiotemporal PM2.5 Analysis (GTA)

This project focuses on analyzing and visualizing PM2.5 air pollution data in the Greater Toronto Area (GTA). It automates data ingestion, processes geospatial data, and generates animated pollution maps to provide insights into air quality trends over time.

## Features

- **Automated Data Ingestion**: Fetches air quality data from OpenAQ, updated every hour via GitHub Actions.
- **Geospatial Processing**: Processes and analyzes spatial data for the GTA.
- **Visualization**: Interactive map showing PM2.5 sensor readings — marker size and colour indicate pollution level.
- **Historical View**: Animated 30-day timeline to explore trends over time.

![Example air quality in the GTA](https://github.com/astroAycha/gta_air_quality/raw/main/snapshot.png)

## Data Source

Air quality data is sourced from [OpenAQ](https://openaq.org), an open-source platform aggregating air quality data from government monitoring stations worldwide.

## PM2.5 AQI Guide (µg/m³)

| Category | Range |
|---|---|
| 🟢 Good | 0 – 12 |
| 🟡 Moderate | 12.1 – 35.4 |
| 🟠 Sensitive Groups | 35.5 – 55.4 |
| 🔴 Unhealthy | 55.5 – 150.4 |
| 🟣 Very Unhealthy | 150.5 – 250.4 |
| ⚫ Hazardous | > 250.4 |

## Source Code

[github.com/astroAycha/gta_air_quality](https://github.com/astroAycha/gta_air_quality)
