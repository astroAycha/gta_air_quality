import requests
from requests.exceptions import ConnectionError, HTTPError
import os
import logging
import pandas as pd
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_path = os.path.join(log_dir, 'downloads.log')

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class DataDownload():
    """Download air quality data from OpenAQ API."""

    def __init__(self):
        self.api_key = os.getenv('OPENAQ_API_KEY')
        self.headers = {'X-API-Key': self.api_key}

    def find_sensors(self,
                     lat: float,
                     lon: float,
                     radius: float) -> pd.DataFrame:
        """
        Find sensors within a radius of a given point.

        Parameters
        ----------
        lat : float
            Latitude of the centre point.
        lon : float
            Longitude of the centre point.
        radius : float
            Search radius in metres.

        Returns
        -------
        pd.DataFrame
        """
        url = "https://api.openaq.org/v3/locations"

        params = {
            "coordinates": f"{lat},{lon}",
            "radius": radius,          # fixed: was {radius} (a set literal)
            "limit": 1000
        }

        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()

        data = response.json()
        results_df = pd.json_normalize(data['results'])
        logging.info(f"Found {len(results_df)} sensor locations near ({lat}, {lon})")
        return results_df

    def download_daily(self,
                       id: int,
                       pollutant: str,
                       start_date: str) -> pd.DataFrame:
        """
        Download daily measurements for a single sensor.

        Parameters
        ----------
        id : int
            Sensor ID.
        pollutant : str
            Label used as the column name (e.g. 'PM2.5').
        start_date : str
            ISO date string, e.g. '2024-01-01'.

        Returns
        -------
        pd.DataFrame  with columns [Date, <pollutant>]
        """
        data_url = f"https://api.openaq.org/v3/sensors/{id}/measurements/hourly"
        all_results = []
        page = 1
        limit = 1000

        while True:
            params = {
                'datetime_from': start_date,
                "page": page,
                "limit": limit
            }

            try:
                response = requests.get(url=data_url,
                                        params=params,
                                        headers=self.headers)
                response.raise_for_status()

            except ConnectionError as conn_err:
                logging.error(f"Connection error for sensor {id}: {conn_err}")
                break

            except HTTPError as http_err:
                logging.error(f"HTTP error for sensor {id}: {http_err}")
                break

            else:
                data = response.json()
                results = data.get("results", [])

                if not results:
                    logging.info(f"No more results for sensor {id} (page {page})")
                    break

                all_results.extend(results)

                meta = data.get("meta", {})
                if meta.get("page", 1) * meta.get("limit", limit) >= meta.get("found", 0):
                    break

                page += 1

        logging.info(f"Sensor {id}: collected {len(all_results)} records")

        if not all_results:
            return pd.DataFrame(columns=['Date', pollutant])

        dates = [rec['period']['datetimeFrom']['local'] for rec in all_results]
        vals = [rec['value'] for rec in all_results]

        return pd.DataFrame({'Date': dates, pollutant: vals})

    def fetch_pm25_sensors(self,
                           lat: float,
                           lon: float,
                           radius: float,
                           start_date: str) -> pd.DataFrame:
        """
        Convenience method: find all PM2.5 sensors near a point and
        download their daily readings in one call.

        Returns
        -------
        pd.DataFrame with columns [Date, PM2.5, name, sensor_id, latitude, longitude]
        """
        sensors_df = self.find_sensors(lat, lon, radius)

        if sensors_df.empty:
            logging.warning("No sensors found.")
            return pd.DataFrame()

        df_list = []
        for i in range(len(sensors_df)):
            for s in sensors_df['sensors'][i]:
                if s['parameter']['name'] == 'pm25':
                    sensor_id = s['id']
                    name = sensors_df.iloc[i]['name']
                    s_lat = sensors_df.iloc[i]['coordinates.latitude']
                    s_lon = sensors_df.iloc[i]['coordinates.longitude']

                    data = self.download_daily(id=sensor_id,
                                               pollutant='PM2.5',
                                               start_date=start_date)

                    if data.empty:
                        continue

                    data['name'] = name
                    data['sensor_id'] = sensor_id
                    data['latitude'] = s_lat
                    data['longitude'] = s_lon
                    df_list.append(data)

        if not df_list:
            return pd.DataFrame()

        result = pd.concat(df_list, ignore_index=True)
        result['Date'] = pd.to_datetime(result['Date']).dt.strftime('%Y-%m-%d %H:%M')
        logging.info(f"fetch_pm25_sensors: {result.shape[0]} total rows")
        return result


if __name__ == "__main__":
    dd = DataDownload()
    df = dd.fetch_pm25_sensors(
        lat=43.6532,
        lon=-79.3832,
        radius=25000,
        start_date='2026-01-01'
    )
    print(df.shape)
    print(df.columns.tolist())
    print(df.head())
