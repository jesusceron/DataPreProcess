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
    for j in range(target_timestamp_index + 1, len(timestamps_target)):
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


accelerometer_timestamps = df_acc['Timestamps'].tolist()
acc_timestamp_index = -1

beacon_1_timestamps = [0] * len(accelerometer_timestamps)
beacon_1_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_1_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_2_timestamps = [0] * len(accelerometer_timestamps)
beacon_2_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_2_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_3_timestamps = [0] * len(accelerometer_timestamps)
beacon_3_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_3_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_4_timestamps = [0] * len(accelerometer_timestamps)
beacon_4_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_4_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_5_timestamps = [0] * len(accelerometer_timestamps)
beacon_5_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_5_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_6_timestamps = [0] * len(accelerometer_timestamps)
beacon_6_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_6_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_7_timestamps = [0] * len(accelerometer_timestamps)
beacon_7_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_7_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_8_timestamps = [0] * len(accelerometer_timestamps)
beacon_8_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_8_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_9_timestamps = [0] * len(accelerometer_timestamps)
beacon_9_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_9_tlm_ps = [0] * len(accelerometer_timestamps)

beacon_10_timestamps = [0] * len(accelerometer_timestamps)
beacon_10_rssis = [0] * len(accelerometer_timestamps)
beacobeacon_10_tlm_ps = [0] * len(accelerometer_timestamps)

def beacon_1(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_2(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_3(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_4(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_5(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_6(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_7(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_8(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_9(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]

def beacon_10(beacon_sample_index, acc_sample_index):
    beacon_1_timestamps[acc_sample_index] = beacons_timestamps[beacon_sample_index]
    beacon_1_rssis[acc_sample_index] = beacons_RSSI[beacon_sample_index]
    beacobeacon_1_tlm_ps[acc_sample_index] = beacons_TLM_packet[beacon_sample_index]
    

def appenddatabeacon(beacon_ID,beacon_sample_index, acc_sample_index):
    switcher = {
        1: beacon_1,
        2: beacon_2,
        3: beacon_3,
        4: beacon_4,
        5: beacon_5,
        6: beacon_6
        7: beacon_7,
        8: beacon_8,
        9: beacon_9,
        10: beacon_10
    }
    # Get the function from switcher dictionary
    func = switcher.get(beacon_ID, lambda: "Invalid month")
    # Execute the function
    print
    func(beacon_sample_index, acc_sample_index)


# Finding the closer timestamp from reference to target
beacons_timestamps = beacons_CSV['Timestamp'].tolist()
beacons_RSSI = beacons_CSV['RSSI'].tolist()
beacons_TLM_packet = beacons_CSV['Estimote TLM packet'].tolist()

beacons_ID = {'46846e6187678448': '1',  # Coconut
              '7897b2192cd1330e': '2',  # Mint
              'f32a65edd388bbd4': '3',  # Ice
              'c7f00010b342cf9e': '4',  # Blueberry
              '992074a3a75b01dd': '5',  # P2
              'eeaf86657d2312d5': '6',  # P1
              '7bb8ba833ded2db9': '7',  # B2
              '3e03d2aaf4265aa5': '8',  # B1
              '3c53d934182ed091': '9',  # G2
              '318da9517131bfab': '10'  # G1
              }

for i in range(0, len(beacons_timestamps)):
    beacon_timestamp = beacons_timestamps[i]
    beacon_TLM_packet = beacons_TLM_packet[i]
    beacon_ID = beacons_ID[beacon_TLM_packet[2:18]]

    beacon_sample_index = i

    for j in range(acc_timestamp_index + 1, len(accelerometer_timestamps)):
        accelerometer_timestamp = accelerometer_timestamps[j]

        current_difference = beacon_timestamp - accelerometer_timestamp
        if current_difference > 0:
            last_difference = current_difference
        else:
            if i == 0:
                if abs(last_difference) > abs(current_difference):
                    acc_sample_index = j
                    appenddatabeacon(beacon_ID, beacon_sample_index, acc_sample_index)
                else:
                    acc_sample_index = j - 1
                    appenddatabeacon(beacon_ID, beacon_sample_index, acc_sample_index)
            else:
                acc_sample_index = j
                appenddatabeacon(beacon_ID, beacon_sample_index, acc_sample_index)
            break

beacons_CSV_aligned = {'Timestamp_acc': accelerometer_timestamps,
                        'Timestamp_beacon_1': beacon_1_timestamps,
                        'Timestamp_beacon_2': beacon_2_timestamps,
                        'Timestamp_beacon_3': beacon_3_timestamps,
                        'Timestamp_beacon_4': beacon_4_timestamps,
                        'Timestamp_beacon_5': beacon_5_timestamps,
                        'Timestamp_beacon_6': beacon_6_timestamps,
                        'Timestamp_beacon_7': beacon_7_timestamps,
                        'Timestamp_beacon_8': beacon_8_timestamps,
                        'Timestamp_beacon_9': beacon_9_timestamps,
                        'Timestamp_beacon_10': beacon_10_timestamps
                        }
df_beacons = DataFrame(beacons_CSV_aligned, columns=['Timestamp_acc', 'Timestamp_beacon_1', 'Timestamp_beacon_2',
                                                 'Timestamp_beacon_3', 'Timestamp_beacon_4', 'Timestamp_beacon_5',
                                                 'Timestamp_beacon_6', 'Timestamp_beacon_7', 'Timestamp_beacon_8',
                                                 'Timestamp_beacon_9', 'Timestamp_beacon_10'])
df_beacons.to_csv(dir_path + "\\dataset\\" + file_name + '_beacons.csv', index=None, header=True)