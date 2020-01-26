import os
import pandas
from beac import sync_beacons

from pandas import DataFrame

file_name = '2'

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

# Finding the first match between the initial timestamp of reference and the target timestamps
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
            appenddata(0, target_timestamp_index)
            break

# copy the remaining timestamps (from sample 1 to the end)
for i in range(1, range_reference_size):
    appenddata(i, i + target_timestamp_index)

# Saving synchronized CSV files
# Code snippet to generate only one CSV file with data of acc and gyro
if reference_file == "gyr":
    dict_acc_gyr = {'Timestamp_Acc': target_aligned_timestamp_acc,
                    'Timestamp_Gyr': reference_aligned_timestamp_gyr,
                    'accX': target_aligned_accX,
                    'accY': target_aligned_accY,
                    'accZ': target_aligned_accZ,
                    'gyrX': reference_aligned_gyrX,
                    'gyrY': reference_aligned_gyrY,
                    'gyrZ': reference_aligned_gyrZ
                    }

else:
    dict_acc_gyr = {'Timestamp_Acc': reference_aligned_timestamp_acc,
                    'Timestamp_Gyr': target_aligned_timestamp_gyr,
                    'accX': reference_aligned_accX,
                    'accY': reference_aligned_accY,
                    'accZ': reference_aligned_accZ,
                    'gyrX': target_aligned_gyrX,
                    'gyrY': target_aligned_gyrY,
                    'gyrZ': target_aligned_gyrZ
                    }

df_acc_gyr_sync = DataFrame(dict_acc_gyr)

df_acc_gyr_sync.to_csv(dir_path + "\\dataset\\" + file_name + '_acc_gyr_sync.csv', index=None, header=True)

# go to sync beacons data
if df_acc_gyr_sync['Timestamp_Acc'][0] >= df_acc_gyr_sync['Timestamp_Gyr'][0]:
    df_beac_sync = sync_beacons(df_acc_gyr_sync['Timestamp_Gyr'].tolist(), beacons_timestamps, beacons_RSSI,
                                beacons_TLM_packet)
else:
    df_beac_sync = sync_beacons(df_acc_gyr_sync['Timestamp_Acc'].tolist(), beacons_timestamps, beacons_RSSI,
                                beacons_TLM_packet)

dict_acc_gyr_beac = {'Timestamps_ref': df_beac_sync['Timestamps_ref'].tolist(),

                     'Timestamps_beacon_1': df_beac_sync['Timestamps_beacon_1'].tolist(),
                     'Timestamps_beacon_2': df_beac_sync['Timestamps_beacon_2'].tolist(),
                     'Timestamps_beacon_3': df_beac_sync['Timestamps_beacon_3'].tolist(),
                     'Timestamps_beacon_4': df_beac_sync['Timestamps_beacon_4'].tolist(),
                     'Timestamps_beacon_5': df_beac_sync['Timestamps_beacon_5'].tolist(),
                     'Timestamps_beacon_6': df_beac_sync['Timestamps_beacon_6'].tolist(),
                     'Timestamps_beacon_7': df_beac_sync['Timestamps_beacon_7'].tolist(),
                     'Timestamps_beacon_8': df_beac_sync['Timestamps_beacon_8'].tolist(),
                     'Timestamps_beacon_9': df_beac_sync['Timestamps_beacon_9'].tolist(),
                     'Timestamps_beacon_10': df_beac_sync['Timestamps_beacon_10'].tolist(),

                     'RSSIs_beacon_1': df_beac_sync['RSSIs_beacon_1'].tolist(),
                     'RSSIs_beacon_2': df_beac_sync['RSSIs_beacon_2'].tolist(),
                     'RSSIs_beacon_3': df_beac_sync['RSSIs_beacon_3'].tolist(),
                     'RSSIs_beacon_4': df_beac_sync['RSSIs_beacon_4'].tolist(),
                     'RSSIs_beacon_5': df_beac_sync['RSSIs_beacon_5'].tolist(),
                     'RSSIs_beacon_6': df_beac_sync['RSSIs_beacon_6'].tolist(),
                     'RSSIs_beacon_7': df_beac_sync['RSSIs_beacon_7'].tolist(),
                     'RSSIs_beacon_8': df_beac_sync['RSSIs_beacon_8'].tolist(),
                     'RSSIs_beacon_9': df_beac_sync['RSSIs_beacon_9'].tolist(),
                     'RSSIs_beacon_10': df_beac_sync['RSSIs_beacon_10'].tolist(),

                     'TLM_packets_beacon_1': df_beac_sync['TLM_packets_beacon_1'].tolist(),
                     'TLM_packets_beacon_2': df_beac_sync['TLM_packets_beacon_2'].tolist(),
                     'TLM_packets_beacon_3': df_beac_sync['TLM_packets_beacon_3'].tolist(),
                     'TLM_packets_beacon_4': df_beac_sync['TLM_packets_beacon_4'].tolist(),
                     'TLM_packets_beacon_5': df_beac_sync['TLM_packets_beacon_5'].tolist(),
                     'TLM_packets_beacon_6': df_beac_sync['TLM_packets_beacon_6'].tolist(),
                     'TLM_packets_beacon_7': df_beac_sync['TLM_packets_beacon_7'].tolist(),
                     'TLM_packets_beacon_8': df_beac_sync['TLM_packets_beacon_8'].tolist(),
                     'TLM_packets_beacon_9': df_beac_sync['TLM_packets_beacon_9'].tolist(),
                     'TLM_packets_beacon_10': df_beac_sync['TLM_packets_beacon_10'].tolist(),

                     'accX_beacon_1': df_beac_sync['accX_beacon_1'].tolist(),
                     'accX_beacon_2': df_beac_sync['accX_beacon_2'].tolist(),
                     'accX_beacon_3': df_beac_sync['accX_beacon_3'].tolist(),
                     'accX_beacon_4': df_beac_sync['accX_beacon_4'].tolist(),
                     'accX_beacon_5': df_beac_sync['accX_beacon_5'].tolist(),
                     'accX_beacon_6': df_beac_sync['accX_beacon_6'].tolist(),
                     'accX_beacon_7': df_beac_sync['accX_beacon_7'].tolist(),
                     'accX_beacon_8': df_beac_sync['accX_beacon_8'].tolist(),
                     'accX_beacon_9': df_beac_sync['accX_beacon_9'].tolist(),
                     'accX_beacon_10': df_beac_sync['accX_beacon_10'].tolist(),

                     'accY_beacon_1': df_beac_sync['accY_beacon_1'].tolist(),
                     'accY_beacon_2': df_beac_sync['accY_beacon_2'].tolist(),
                     'accY_beacon_3': df_beac_sync['accY_beacon_3'].tolist(),
                     'accY_beacon_4': df_beac_sync['accY_beacon_4'].tolist(),
                     'accY_beacon_5': df_beac_sync['accY_beacon_5'].tolist(),
                     'accY_beacon_6': df_beac_sync['accY_beacon_6'].tolist(),
                     'accY_beacon_7': df_beac_sync['accY_beacon_7'].tolist(),
                     'accY_beacon_8': df_beac_sync['accY_beacon_8'].tolist(),
                     'accY_beacon_9': df_beac_sync['accY_beacon_9'].tolist(),
                     'accY_beacon_10': df_beac_sync['accY_beacon_10'].tolist(),

                     'accZ_beacon_1': df_beac_sync['accZ_beacon_1'].tolist(),
                     'accZ_beacon_2': df_beac_sync['accZ_beacon_2'].tolist(),
                     'accZ_beacon_3': df_beac_sync['accZ_beacon_3'].tolist(),
                     'accZ_beacon_4': df_beac_sync['accZ_beacon_4'].tolist(),
                     'accZ_beacon_5': df_beac_sync['accZ_beacon_5'].tolist(),
                     'accZ_beacon_6': df_beac_sync['accZ_beacon_6'].tolist(),
                     'accZ_beacon_7': df_beac_sync['accZ_beacon_7'].tolist(),
                     'accZ_beacon_8': df_beac_sync['accZ_beacon_8'].tolist(),
                     'accZ_beacon_9': df_beac_sync['accZ_beacon_9'].tolist(),
                     'accZ_beacon_10': df_beac_sync['accZ_beacon_10'].tolist(),

                     'prev_motion_duration_beacon_1': df_beac_sync['prev_motion_duration_beacon_1'].tolist(),
                     'prev_motion_duration_beacon_2': df_beac_sync['prev_motion_duration_beacon_2'].tolist(),
                     'prev_motion_duration_beacon_3': df_beac_sync['prev_motion_duration_beacon_3'].tolist(),
                     'prev_motion_duration_beacon_4': df_beac_sync['prev_motion_duration_beacon_4'].tolist(),
                     'prev_motion_duration_beacon_5': df_beac_sync['prev_motion_duration_beacon_5'].tolist(),
                     'prev_motion_duration_beacon_6': df_beac_sync['prev_motion_duration_beacon_6'].tolist(),
                     'prev_motion_duration_beacon_7': df_beac_sync['prev_motion_duration_beacon_7'].tolist(),
                     'prev_motion_duration_beacon_8': df_beac_sync['prev_motion_duration_beacon_8'].tolist(),
                     'prev_motion_duration_beacon_9': df_beac_sync['prev_motion_duration_beacon_9'].tolist(),
                     'prev_motion_duration_beacon_10': df_beac_sync['prev_motion_duration_beacon_10'].tolist(),

                     'current_motion_duration_beacon_1': df_beac_sync['current_motion_duration_beacon_1'].tolist(),
                     'current_motion_duration_beacon_2': df_beac_sync['current_motion_duration_beacon_2'].tolist(),
                     'current_motion_duration_beacon_3': df_beac_sync['current_motion_duration_beacon_3'].tolist(),
                     'current_motion_duration_beacon_4': df_beac_sync['current_motion_duration_beacon_4'].tolist(),
                     'current_motion_duration_beacon_5': df_beac_sync['current_motion_duration_beacon_5'].tolist(),
                     'current_motion_duration_beacon_6': df_beac_sync['current_motion_duration_beacon_6'].tolist(),
                     'current_motion_duration_beacon_7': df_beac_sync['current_motion_duration_beacon_7'].tolist(),
                     'current_motion_duration_beacon_8': df_beac_sync['current_motion_duration_beacon_8'].tolist(),
                     'current_motion_duration_beacon_9': df_beac_sync['current_motion_duration_beacon_9'].tolist(),
                     'current_motion_duration_beacon_10': df_beac_sync['current_motion_duration_beacon_10'].tolist(),

                     'motion_state_beacon_1': df_beac_sync['motion_state_beacon_1'].tolist(),
                     'motion_state_beacon_2': df_beac_sync['motion_state_beacon_2'].tolist(),
                     'motion_state_beacon_3': df_beac_sync['motion_state_beacon_3'].tolist(),
                     'motion_state_beacon_4': df_beac_sync['motion_state_beacon_4'].tolist(),
                     'motion_state_beacon_5': df_beac_sync['motion_state_beacon_5'].tolist(),
                     'motion_state_beacon_6': df_beac_sync['motion_state_beacon_6'].tolist(),
                     'motion_state_beacon_7': df_beac_sync['motion_state_beacon_7'].tolist(),
                     'motion_state_beacon_8': df_beac_sync['motion_state_beacon_8'].tolist(),
                     'motion_state_beacon_9': df_beac_sync['motion_state_beacon_9'].tolist(),
                     'motion_state_beacon_10': df_beac_sync['motion_state_beacon_10'].tolist(),

                     'clock_error_beacon_1': df_beac_sync['clock_error_beacon_1'].tolist(),
                     'clock_error_beacon_2': df_beac_sync['clock_error_beacon_2'].tolist(),
                     'clock_error_beacon_3': df_beac_sync['clock_error_beacon_3'].tolist(),
                     'clock_error_beacon_4': df_beac_sync['clock_error_beacon_4'].tolist(),
                     'clock_error_beacon_5': df_beac_sync['clock_error_beacon_5'].tolist(),
                     'clock_error_beacon_6': df_beac_sync['clock_error_beacon_6'].tolist(),
                     'clock_error_beacon_7': df_beac_sync['clock_error_beacon_7'].tolist(),
                     'clock_error_beacon_8': df_beac_sync['clock_error_beacon_8'].tolist(),
                     'clock_error_beacon_9': df_beac_sync['clock_error_beacon_9'].tolist(),
                     'clock_error_beacon_10': df_beac_sync['clock_error_beacon_10'].tolist(),

                     'firmware_error_beacon_1': df_beac_sync['firmware_error_beacon_1'].tolist(),
                     'firmware_error_beacon_2': df_beac_sync['firmware_error_beacon_2'].tolist(),
                     'firmware_error_beacon_3': df_beac_sync['firmware_error_beacon_3'].tolist(),
                     'firmware_error_beacon_4': df_beac_sync['firmware_error_beacon_4'].tolist(),
                     'firmware_error_beacon_5': df_beac_sync['firmware_error_beacon_5'].tolist(),
                     'firmware_error_beacon_6': df_beac_sync['firmware_error_beacon_6'].tolist(),
                     'firmware_error_beacon_7': df_beac_sync['firmware_error_beacon_7'].tolist(),
                     'firmware_error_beacon_8': df_beac_sync['firmware_error_beacon_8'].tolist(),
                     'firmware_error_beacon_9': df_beac_sync['firmware_error_beacon_9'].tolist(),
                     'firmware_error_beacon_10': df_beac_sync['firmware_error_beacon_10'].tolist(),

                     }
df_acc_gyr_beac_sync = DataFrame(dict_acc_gyr_beac)
df_acc_gyr_beac_sync.to_csv(dir_path + "\\dataset\\" + file_name + '_beacons_sync.csv', index=None, header=True)
