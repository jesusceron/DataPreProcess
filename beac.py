import os

import pandas

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
beacons_timestamps = beacons_CSV['RSSI'].tolist()
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


# Choosing the reference file based on the timestamp of the first sample
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

for i in range(range_reference_size):
    reference_timestamp = timestamps_reference[i]
    reference_timestamp_index = i
    for j in range(target_timestamp_index+1, len(timestamps_target)):
        target_timestamp = timestamps_target[j]

        current_difference = reference_timestamp - target_timestamp
        if current_difference > 0:
            last_difference = current_difference
        else:
            if i == 0:
                if abs(last_difference) > abs(current_difference):
                    target_sample_index = j
                    appenddata(reference_timestamp_index, target_timestamp_index)
                else:
                    target_sample_index = j - 1
                    appenddata(reference_timestamp_index, target_timestamp_index)
            else:
                target_sample_index = j
                appenddata(reference_timestamp_index, target_timestamp_index)
            break

# Saving synchronized CSV files
if reference_file == "gyr":
    gyr_CSV_synchronized = {'Timestamp': reference_aligned_timestamp_gyr,
                            'gyrX': reference_aligned_gyrX,
                            'gyrY': reference_aligned_gyrY,
                            'gyrZ': reference_aligned_gyrZ
                            }
    df_gyr = DataFrame(gyr_CSV_synchronized, columns=['Timestamp', 'gyrX', 'gyrY', 'gyrZ'])
    df_gyr.to_csv(dir_path + "\\dataset\\" + file_name + '_gyr.csv', index=None, header=True)

    acc_CSV_synchronized = {'Timestamp': target_aligned_timestamp_acc,
                            'accX': target_aligned_accX,
                            'accY': target_aligned_accY,
                            'accZ': target_aligned_accZ
                            }
    df_acc = DataFrame(acc_CSV_synchronized, columns=['Timestamp', 'accX', 'accY', 'accZ'])
    df_acc.to_csv(dir_path + "\\dataset\\" + file_name + '_acc.csv', index=None, header=True)

else:
    gyr_CSV_synchronized = {'Timestamp': target_aligned_timestamp_gyr,
                            'gyrX': target_aligned_gyrX,
                            'gyrY': target_aligned_gyrY,
                            'gyrZ': target_aligned_gyrZ
                            }
    df_gyr = DataFrame(gyr_CSV_synchronized, columns=['Timestamp', 'gyrX', 'gyrY', 'gyrZ'])
    df_gyr.to_csv(dir_path + "\\dataset\\" + file_name + '_gyr.csv', index=None, header=True)

    acc_CSV_synchronized = {'Timestamp': reference_aligned_timestamp_acc,
                            'accX': reference_aligned_accX,
                            'accY': reference_aligned_accY,
                            'accZ': reference_aligned_accZ
                            }
    df_acc = DataFrame(acc_CSV_synchronized, columns=['Timestamp', 'accX', 'accY', 'accZ'])
    df_acc.to_csv(dir_path + "\\dataset\\" + file_name + '_acc.csv', index=None, header=True)


# Finding the closer timestamp from reference to target

for i in range(0, len(beacons_timestamps)):
    beacons_timestamp = beacons_timestamps[i]

    for j in range(acc_sample_index+1, acc_CSV_synchronized):
        target_sample = timestamps_target[j]

        current_difference = reference_sample - target_sample
        if current_difference > 0:
            last_difference = current_difference
        else:
            if i == 0:
                if abs(last_difference) > abs(current_difference):
                    acc_sample_index = j
                    appenddata(beacons_sample_index, acc_sample_index)
                else:
                    acc_sample_index = j - 1
                    appenddata(beacons_sample_index, acc_sample_index)
            else:
                acc_sample_index = j
                appenddata(beacons_sample_index, acc_sample_index)
            break