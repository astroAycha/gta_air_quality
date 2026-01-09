import pandas as pd
import matplotlib.pylab as plt
import plotly.express as px

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

            data = dd.download_daily(id= pm25_sensor_id,
                                    pollutant= 'PM2.5',
                                    start_date='2026-01-01'
                                    )
            
            data['name'] = pm25_sensor_loc
            data['sensor_id'] = pm25_sensor_id
            data['latitude'] = lat
            data['longitude'] = lon

            df_list.append(data)

gta_sensors_df = pd.concat(df_list, ignore_index=True)


gta_sensors_df['Date'] = pd.to_datetime(gta_sensors_df['Date'].dt.date)

toronto_lat = 43.6532
toronto_lon = -79.3832
fig = px.scatter_mapbox(gta_sensors_df, 
                        lat="latitude", 
                        lon="longitude", 
                        color="PM2.5", 
                        size="PM2.5",
                        animation_frame="Date",
                        color_continuous_scale="haline_r",
                        center={"lat": toronto_lat, "lon": toronto_lon}, 
                        zoom=8.5,
                        mapbox_style="carto-positron", 
                        hover_name="name")

fig.update_layout(margin=dict(l=0, r=0, t=30, b=10),
                  coloraxis_colorbar=dict(title="PM2.5")
                  )

fig.update_coloraxes(
    cmin=gta_sensors_df["PM2.5"].min(),
    cmax=gta_sensors_df["PM2.5"].max()
)

fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 800
fig.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 300


# fig.show()

fig.write_html("pm25_gta_animation.html")