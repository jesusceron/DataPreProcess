import os
import pandas
import math
from beac import sync_beacons

from pandas import DataFrame

path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)
file_name = 'NilsPod-A8CE_20200127_1657'

# Reading CSV raw files
nilspod_csv = pandas.read_csv('dataset/' + file_name + '.csv', skiprows=1)

# Sorting DataFrames based on timestamp
# accelerometer_CSV.sort_values(by='Timestamp', inplace=True)  # inplace to keep the changes

# Getting columns as lists from DataFrames
accX = nilspod_csv['acc_x'].tolist()
accY = nilspod_csv['acc_y'].tolist()
accZ = nilspod_csv['acc_z'].tolist()
gyrX = nilspod_csv['gyro_x'].tolist()
gyrY = nilspod_csv['gyro_y'].tolist()
gyrZ = nilspod_csv['gyro_z'].tolist()
baro = nilspod_csv['baro'].tolist()

initial_timestamp = 1580140627000
final_timestamp = 1580141123000
timestamps = [initial_timestamp]
num_samples = len(accX)

num_packs_of_five_seconds = math.floor(num_samples / 1024)

for i_pack in range(1, num_packs_of_five_seconds + 1):

    for i_sample_4_seconds in range(1, 820):  # timestamps for samples of the first 4 seconds
        timestamps.append(timestamps[(i_sample_4_seconds - 1) * i_pack] + 4.878)
    for i_sample_5_second in range(820, 1024):  # timestamps for samples of the fifth second
        timestamps.append(timestamps[(i_sample_5_second - 1) * i_pack] + 4.901)

    timestamps.append(timestamps[0] + 1000 * i_pack)  # first sample of the next packet

for i_remaining_samples in range(1024 * num_packs_of_five_seconds + 1, num_samples + 1):  # timestamps for the remaining samples
    timestamps.append(timestamps[i_remaining_samples - 1] + 4.882)

dict_nilspod = {'Timestamps': timestamps, 'AccX': accX, 'AccY': accY, 'AccZ': accZ, 'GyrX': gyrX, 'GyrY': gyrY, 'GyrZ': gyrZ}
df_nilspod = DataFrame(dict_nilspod)
df_nilspod.to_csv(dir_path + "\\dataset\\" + file_name + '_UPDATED.csv', index=None, header=True)
