import os
import pandas
import math
from beac import sync_beacons
from pandas import DataFrame

path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)

#  Before running this script, remember cut the files in order to get the same number of rows
#  DO NOT forget enter these data :
file_names = ['NilsPod-A8CE_20200308_1734',  # Right wrist (Master NilsPod)
              'NilsPod-C6AA_20200308_1734',  # Left wrist
              'NilsPodX-EB9E_20200308_1734',  # Right foot
              'NilsPodX-9FBB_20200308_1734']  # Left foot
# To use timestamps from master NilsPod as reference time
initial_timestamp = 1583685272000  # Add 3 zeros at the end of the utc_start and utc_stop number
final_timestamp = 1583685322000
# Participant ID
participant_id = '8080'

for i_file in range(0, 4):
    # Reading CSV raw files
    nilspod_csv = pandas.read_csv('dataset/' + file_names[i_file] + '.csv', header=1)

    # Getting columns as lists from DataFrames
    accX = nilspod_csv['acc_x'].tolist()
    accY = nilspod_csv['acc_y'].tolist()
    accZ = nilspod_csv['acc_z'].tolist()
    gyrX = nilspod_csv['gyro_x'].tolist()
    gyrY = nilspod_csv['gyro_y'].tolist()
    gyrZ = nilspod_csv['gyro_z'].tolist()
    baro = nilspod_csv['baro'].tolist()

    timestamps = [initial_timestamp]
    num_samples = len(accX)

    num_packs_of_five_seconds = math.floor(num_samples / 1024)

    for i_pack in range(0, num_packs_of_five_seconds):

        for i_sample_4_seconds in range(1 + (1024 * i_pack),
                                        820 + (1024 * i_pack)):  # timestamps for samples of the first 4 seconds
            timestamps.append(timestamps[i_sample_4_seconds - 1] + 4.878)  # 1sec/205(sam/sec) = 4.878ms
        for i_sample_5_second in range(820 + (1024 * i_pack),
                                       1024 + (1024 * i_pack)):  # timestamps for samples of the fifth second
            timestamps.append(timestamps[i_sample_5_second - 1] + 4.901)  # 1sec/204(sam/sec) = 4.901ms

        timestamps.append(timestamps[0] + 5000 * (i_pack + 1))  # first sample of the next packet

    for i_remaining_samples in range(1024 * num_packs_of_five_seconds + 1,
                                     num_samples):  # timestamps for the remaining samples
        timestamps.append(timestamps[i_remaining_samples - 1] + 4.882)  # 1sec/204.8(sam/sec) = 4.882ms

    if i_file == 0:
        dict_nilspods = {'Timestamp': timestamps, 'AccX_right_wrist': accX, 'AccY_right_wrist': accY,
                              'AccZ_right_wrist': accZ, 'GyrX_right_wrist': gyrX, 'GyrY_right_wrist': gyrY,
                              'GyrZ_right_wrist': gyrZ, 'Baro_right_wrist': baro}
    elif i_file == 1:
        dict_nilspods.update({'AccX_left_wrist': accX, 'AccY_left_wrist': accY, 'AccZ_left_wrist': accZ,
                              'GyrX_left_wrist': gyrX, 'GyrY_left_wrist': gyrY, 'GyrZ_left_wrist': gyrZ,
                              'Baro_left_wrist': baro})
    elif i_file == 2:
        dict_nilspods.update({'AccX_right_foot': accX, 'AccY_right_foot': accY, 'AccZ_right_foot': accZ,
                              'GyrX_right_foot': gyrX, 'GyrY_right_foot': gyrY, 'GyrZ_right_foot': gyrZ,
                              'Baro_right_foot': baro})
    elif i_file == 3:
        dict_nilspods.update({'AccX_left_foot': accX, 'AccY_left_foot': accY, 'AccZ_left_foot': accZ,
                              'GyrX_left_foot': gyrX, 'GyrY_left_foot': gyrY, 'GyrZ_left_foot': gyrZ,
                              'Baro_left_foot': baro})

    # df_nilspod = DataFrame(dict_nilspods)
    # df_nilspod.to_csv(dir_path + "\\dataset\\" + file_names[i_file] + '_with_timestamp.csv', index=None, header=True)

df_nilspod = DataFrame(dict_nilspods)
df_nilspod.to_csv(dir_path + "\\dataset\\" + 'NilsPods_synchronized.csv', index=None, header=True)

# Reading CSV beacons' raw files
beacons_CSV = pandas.read_csv('dataset/' + participant_id + '_beacons_raw.csv')
# Sorting DataFrames based on timestamp
# beacons_CSV.sort_values(by='Timestamp', inplace=True)  # inplace to keep the changes
# Getting columns as lists from DataFrames
beacons_timestamps = beacons_CSV['Timestamp'].tolist()
beacons_RSSI = beacons_CSV['RSSI'].tolist()
beacons_service_packet = beacons_CSV['Estimote TLM packet'].tolist()

dict_beac_sync = sync_beacons(timestamps, beacons_timestamps, beacons_RSSI, beacons_service_packet)
dict_nilspods.update(dict_beac_sync)
df_nilspod = DataFrame(dict_nilspods)
df_nilspod.to_csv(dir_path + "\\dataset\\" + 'NilsPods_beacons_synchronized.csv', index=None, header=True)