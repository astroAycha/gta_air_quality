import requests
from requests.exceptions import ConnectionError, HTTPError
import os
import logging
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


log_file = 'downloads.log'
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    filemode='a')


class DataDownload():
    """ download data"""

    def __init__(self):

        self.api_key = os.getenv('OPENAQ_API_KEY')
        self.headers = {'X-API-Key':self.api_key}

        hourly_url = os.getenv('HOURLY_URL')
        daily_url = os.getenv('HOURLY_URL')

        
    
    def find_sensors(self,
                     lat: float,
                     long: float,
                     radius: float):
        """
         Find the sensors within a radius given a point
        """

        url = "https://api.openaq.org/v3/locations"

        params = {
            "coordinates": f"{lat}, {long}",
            "radius": {radius},
            "limit": 1000
        }

        headers = {
            "X-API-Key": os.environ["OPENAQ_API_KEY"]
        }

        response = requests.get(url, params=params, headers=headers)    
        response.raise_for_status()

        data = response.json()

        results_df = pd.json_normalize(data['results'])

        return results_df
    

    def download_daily(self,
                        id: int,
                        pollutant: str,
                        start_date: str) -> pd.DataFrame:
        """
        download the daily data for a given pollutant.
        
        Parameters
        ----------
        pollutant: str,  {O3, NO2, PM2.5}
            The pollutant to get data for
        start_date: str, format yyyy-mm-dd
            starting date, (e.g. '2024-03-23')

        Returns
        -------
        Pandas Dataframe
        """

        # logging.info(f'Get data for {pollutant}')
        # To-do add the date to the log

        data_url = f"https://api.openaq.org/v3/sensors/{id}/measurements/daily"
        all_results = []
        page = 1
        limit = 1000

        while True:
            params = {'datetime_from': start_date,
                "page": page,
                "limit": limit
            }
            
            try:
                response = requests.get(url=data_url,
                                    params=params,
                                    headers=self.headers)
                
                response.raise_for_status()

            except ConnectionError as conn_err:
                logging.info(f"Connection Error {conn_err}")

            else:
                data = response.json()

                results = data.get("results", [])
                if not results:
                    logging.info(f"{response.status_code}")
                    logging.info(f' {len(results)} returned')
                    break

                all_results.extend(results)

                # Stop when last page is reached
                meta = data.get("meta", {})
                if meta["page"] * meta["limit"] >= meta["found"]:
                    break

                page += 1

        logging.info(f"Collected {len(all_results)} records")

        dates = [rec['period']['datetimeFrom']['local'] for rec in all_results]
        vals = [rec['value'] for rec in all_results]

        data_dict = {'Date': dates,
                    pollutant: vals,
                    }
        
        data_df = pd.DataFrame(data_dict)
        
        return data_df
    
    if __name__ == "__main__":

        import data_download 
        dd = data_download.DataDownload()
        results_df = dd.find_sensors(43.6532,
                                    -79.3832,
                                    25000)

        df_list = []
        for i in range(len(results_df)):
            for s in results_df['sensors'][i]:
                if s['parameter']['name']=='pm25':
                    pm25_sensor_id = s['id']
                    pm25_sensor_loc = results_df.iloc[i]['name']
                    lat = results_df.iloc[i]['coordinates.latitude']
                    lon = results_df.iloc[i]['coordinates.longitude']
                    # print(pm25_sensor_id)
                    # print(pm25_sensor_loc)
                    data = dd.download_daily(id= pm25_sensor_id,
                                            pollutant= 'PM2.5',
                                            start_date='2026-01-01'
                                            )
                    
                    data['name'] = pm25_sensor_loc
                    data['sensor_id'] = pm25_sensor_id
                    data['latitude'] = lat
                    data['longitude'] = lon
                    
                    # print(data.shape)

                    df_list.append(data)

        gta_sensors_df = pd.concat(df_list, ignore_index=True)
        print(gta_sensors_df.shape)
        print(gta_sensors_df.columns)