import pandas as pd
from datetime import datetime, timedelta

def timestamp(row):
    datetimevalue = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    timedeltavalue = timedelta(seconds=row['ACT_TIME'])
    return datetimevalue + timedeltavalue

file_path = 'bc_trip259172515_230215.csv'
#data = pd.read_csv(file_path)
#data_dropped = data.drop(['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'], axis=1)

kept_columns = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE']
data_usecol = pd.read_csv(file_path, usecols=kept_columns)

data_usecol['TIMESTAMP'] = data_usecol.apply(timestamp, axis=1)

data_usecol = data_usecol.drop(['OPD_DATE', 'ACT_TIME'], axis=1)

data_usecol['dMETERS'] = data_usecol['METERS'].diff()
data_usecol['dTIMESTAMP'] = data_usecol['TIMESTAMP'].diff().dt.total_seconds()
data_usecol['SPEED'] = data_usecol.apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'], axis = 1)
data_usecol.drop(['dMETERS', 'dTIMESTAMP'], axis=1)

speed_data = data_usecol['SPEED'].describe()
#print(data.head())
#print(data_dropped.head())
print(data_usecol.head())
#print(data_usecol['TIMESTAMP'])
print(speed_data)