import os
import pandas
from beac import sync_beacons

from pandas import DataFrame
from shutil import copy

file_name = '1'

# Reading CSV raw files
accelerometer_CSV = pandas.read_csv('dataset/' + file_name + '_acc_raw.csv')
beacons_CSV = pandas.read_csv('dataset/' + file_name + '_beacons_raw.csv')
gyroscope_CSV = pandas.read_csv('dataset/' + file_name + '_gyr_raw.csv')

# Sorting DataFrames based on timestamp
accelerometer_CSV.sort_values(by='Timestamp', inplace=True)  # inplace to keep the changes
beacons_CSV.sort_values(by='Timestamp', inplace=True)
gyroscope_CSV.sort_values(by='Timestamp', inplace=True)

# Getting columns as lists from DataFrames
acc_timestamps = accelerometer_CSV['Timestamp'].tolist()
accX = accelerometer_CSV['accX'].tolist()
accY = accelerometer_CSV['accY'].tolist()
accZ = accelerometer_CSV['accZ'].tolist()
gyro_timestamps = gyroscope_CSV['Timestamp'].tolist()
gyrX = gyroscope_CSV['gyrX'].tolist()
gyrY = gyroscope_CSV['gyrY'].tolist()
gyrZ = gyroscope_CSV['gyrZ'].tolist()
beacons_timestamps = beacons_CSV['Timestamp'].tolist()
beacons_RSSI = beacons_CSV['RSSI'].tolist()
beacons_TLM_packet = beacons_CSV['Estimote TLM packet'].tolist()

# Declaring variables
reference_aligned_timestamp_acc = []
reference_aligned_timestamp_gyr = []
reference_aligned_accX = []
reference_aligned_accY = []
reference_aligned_accZ = []
reference_aligned_gyrX = []
reference_aligned_gyrY = []
reference_aligned_gyrZ = []
target_aligned_timestamp_acc = []
target_aligned_timestamp_gyr = []
target_aligned_accX = []
target_aligned_accY = []
target_aligned_accZ = []
target_aligned_gyrX = []
target_aligned_gyrY = []
target_aligned_gyrZ = []
target_timestamp_index = -1
last_difference = 0
path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)


def appenddata(reference_index, target_index):
    if reference_file == "gyr":
        reference_aligned_timestamp_gyr.append(timestamps_reference[reference_index])
        reference_aligned_gyrX.append(gyrX[reference_index])
        reference_aligned_gyrY.append(gyrY[reference_index])
        reference_aligned_gyrZ.append(gyrZ[reference_index])

        target_aligned_timestamp_acc.append(timestamps_target[target_index])
        target_aligned_accX.append(accX[target_index])
        target_aligned_accY.append(accY[target_index])
        target_aligned_accZ.append(accZ[target_index])

    else:
        reference_aligned_timestamp_acc.append(timestamps_reference[reference_index])
        reference_aligned_accX.append(accX[reference_index])
        reference_aligned_accY.append(accY[reference_index])
        reference_aligned_accZ.append(accZ[reference_index])

        target_aligned_timestamp_gyr.append(timestamps_target[target_index])
        target_aligned_gyrX.append(gyrX[target_index])
        target_aligned_gyrY.append(gyrY[target_index])
        target_aligned_gyrZ.append(gyrZ[target_index])


# Choosing the reference file based on the timestamp of the first sample of acc and gyro
if acc_timestamps[0] > gyro_timestamps[0]:
    reference_file = 'acc'
    timestamps_reference = acc_timestamps.copy()  # acc_file
    timestamps_target = gyro_timestamps.copy()
else:
    reference_file = 'gyr'
    timestamps_reference = gyro_timestamps.copy()  # gyro_file
    timestamps_target = acc_timestamps.copy()

# Finding the closer timestamp from reference to target
# Check if the reference file has more samples than target file
if len(timestamps_reference) > len(timestamps_target):
    range_reference_size = len(timestamps_target)
else:
    range_reference_size = len(timestamps_reference)

reference_timestamp = timestamps_reference[0]

for j in range(target_timestamp_index + 1, len(timestamps_target)):
    target_timestamp = timestamps_target[j]

    current_difference = reference_timestamp - target_timestamp
    if current_difference > 0:
        last_difference = current_difference
    else:
        if abs(last_difference) > abs(current_difference):
            target_timestamp_index = j
            appenddata(0, target_timestamp_index)
            break
        else:
            target_timestamp_index = j - 1
            appenddata(reference_timestamp, target_timestamp_index)
            break

for i in range(1, range_reference_size):
    appenddata(i, i + target_timestamp_index)

# Saving synchronized CSV files
# Code snippet to generate only one CSV file with data of acc and gyro
if reference_file == "gyr":
    CSV_synchronized = {'Timestamp_Acc': target_aligned_timestamp_acc,
                        'Timestamp_Gyr': reference_aligned_timestamp_gyr,
                        'accX': target_aligned_accX,
                        'accY': target_aligned_accY,
                        'accZ': target_aligned_accZ,
                        'gyrX': reference_aligned_gyrX,
                        'gyrY': reference_aligned_gyrY,
                        'gyrZ': reference_aligned_gyrZ
                        }

else:
    CSV_synchronized = {'Timestamp_Acc': reference_aligned_timestamp_acc,
                        'Timestamp_Gyr': target_aligned_timestamp_gyr,
                        'accX': reference_aligned_accX,
                        'accY': reference_aligned_accY,
                        'accZ': reference_aligned_accZ,
                        'gyrX': target_aligned_gyrX,
                        'gyrY': target_aligned_gyrY,
                        'gyrZ': target_aligned_gyrZ
                        }

df_acc_gyr_sync = DataFrame(CSV_synchronized, columns=['Timestamp_Acc', 'Timestamp_Gyr',
                                                       'accX', 'accY', 'accZ',
                                                       'gyrX', 'gyrY', 'gyrZ'])

df_acc_gyr_sync.to_csv(dir_path + "\\dataset\\" + file_name + '_sync.csv', index=None, header=True)

# go to sync beacons data
if df_acc_gyr_sync['Timestamp_Acc'][0] >= df_acc_gyr_sync['Timestamp_Gyr'][0]:
    df_acc_gyr_beac_sync = (df_acc_gyr_sync['Timestamp_gyr'].tolist(), beacons_timestamps, beacons_RSSI, beacons_TLM_packet)
else:
    df_acc_gyr_beac_sync = (df_acc_gyr_sync['Timestamp_acc'].tolist(), beacons_timestamps, beacons_RSSI, beacons_TLM_packet)

df_acc_gyr_beac_sync.to_csv(dir_path + "\\dataset\\" + file_name + '_beacons.csv', index=None, header=True)
