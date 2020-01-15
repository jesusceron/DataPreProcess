import os

import pandas

from pandas import DataFrame
from shutil import copy

file_name = '1'

# Reading CSV raw files
accelerometer_CSV = pandas.read_csv('dataset/raw_data/' + file_name + '_acc_raw.csv')
beacons_CSV = pandas.read_csv('dataset/raw_data/' + file_name + '_beacons_raw.csv')
gyroscope_CSV = pandas.read_csv('dataset/raw_data/' + file_name + '_gyr_raw.csv')

# Getting data from the files
acc_timestamps = accelerometer_CSV['Timestamp']
beacons_timestamps = beacons_CSV['Timestamp']
gyro_timestamps = gyroscope_CSV['Timestamp']
accX = accelerometer_CSV['accX']
accY = accelerometer_CSV['accY']
accZ = accelerometer_CSV['accZ']
gyrX = gyroscope_CSV['gyrX']
gyrY = gyroscope_CSV['gyrY']
gyrZ = gyroscope_CSV['gyrZ']

# Declaring variables
target_aligned_timestamp = []
target_aligned_accX = []
target_aligned_accY = []
target_aligned_accZ = []
target_aligned_gyrX = []
target_aligned_gyrY = []
target_aligned_gyrZ = []
sample_match_index = 0
last_difference = 0
path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)


def appenddata(sample):
    if reference_file == "gyr":
        target_aligned_timestamp.append(target[sample])
        target_aligned_accX.append(accX[sample])
        target_aligned_accY.append(accY[sample])
        target_aligned_accZ.append(accZ[sample])
    else:
        target_aligned_timestamp.append(target[sample])
        target_aligned_gyrX.append(gyrX[sample])
        target_aligned_gyrY.append(gyrY[sample])
        target_aligned_gyrZ.append(gyrZ[sample])


# Choosing the reference file based on the timestamp of the first sample
if acc_timestamps[0] > gyro_timestamps[0]:
    reference_file = 'acc'
    reference = acc_timestamps  # acc_file
    target = gyro_timestamps
else:
    reference_file = 'gyr'
    reference = gyro_timestamps  # gyro_file
    target = acc_timestamps

# Finding the closer timestamp from
for i in range(reference.size):
    reference_sample = reference[i]
    for j in range(sample_match_index, target.size):
        target_sample = target[j]

        current_difference = reference_sample - target_sample
        if current_difference > 0:
            last_difference = current_difference
        else:
            if abs(current_difference) >= last_difference:
                sample_match_index = j - 1
                appenddata(sample_match_index)
                break
            else:
                sample_match_index = j
                appenddata(sample_match_index)
                break

# Saving synchronized CSV files
if reference_file == 'gyr':
    acc_CSV_synchronized = {'Timestamp': target_aligned_timestamp,
                            'accX': target_aligned_accX,
                            'accY': target_aligned_accY,
                            'accZ': target_aligned_accZ
                            }
    df_acc = DataFrame(acc_CSV_synchronized, columns=['Timestamp', 'accX', 'accY', 'accZ'])
    df_acc.to_csv(dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_acc.csv', index=None, header=True)
    df_acc.to_csv(dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_acc.csv', index=None, header=True)
    copy(dir_path + "\\dataset\\raw_data\\" + file_name + '_gyr_raw.csv',
         dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_gyr.csv')
else:
    gyr_CSV_synchronized = {'Timestamp': target_aligned_timestamp,
                            'gyrX': target_aligned_gyrX,
                            'gyrY': target_aligned_gyrY,
                            'gyrZ': target_aligned_gyrZ
                            }
    df_gyr = DataFrame(gyr_CSV_synchronized, columns=['Timestamp', 'accX', 'accY', 'accZ'])
    df_gyr.to_csv(dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_gyr.csv', index=None, header=True)
    df_gyr.to_csv(dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_gyr.csv', index=None, header=True)
    copy(dir_path + "\\dataset\\raw_data\\" + file_name + '_acc_raw.csv',
         dir_path + "\\dataset\\pre-processed_data\\" + file_name + '_acc.csv')
