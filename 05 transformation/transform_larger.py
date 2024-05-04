import pandas as pd
from datetime import datetime, timedelta

def timestamp(row):
    datetimevalue = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    timedeltavalue = timedelta(seconds=row['ACT_TIME'])
    return datetimevalue + timedeltavalue

file_path = 'bc_veh4223_230215.csv'

kept_columns = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE']
data_usecol = pd.read_csv(file_path, usecols=kept_columns)

data_usecol['TIMESTAMP'] = data_usecol.apply(timestamp, axis=1)

data_usecol = data_usecol.drop(['OPD_DATE', 'ACT_TIME'], axis=1)

results = pd.DataFrame()

for trip_id in data_usecol['EVENT_NO_TRIP'].unique():
    trip_data = data_usecol[data_usecol['EVENT_NO_TRIP'] == trip_id]
    
    trip_data['dMETERS'] = trip_data['METERS'].diff()
    trip_data['dTIMESTAMP'] = trip_data['TIMESTAMP'].diff().dt.total_seconds()
    
    trip_data['SPEED'] = trip_data.apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'] if row['dTIMESTAMP'] != 0 else 0, axis=1)
    
    trip_data = trip_data.drop(['dMETERS', 'dTIMESTAMP'], axis=1)
    
    speed_data = trip_data['SPEED'].describe()
    print(speed_data)
    results = pd.concat([results, trip_data])

#speed_data = data_usecol['SPEED'].describe()

#print(data_usecol.head())
#print(results.head())
#print(speed_data)
for trip_id, group in results.groupby('EVENT_NO_TRIP'):
    print(f"First 5 records for EVENT_NO_TRIP: {trip_id}")
    print(group.head(5))
    print()