# def to synchronize beacons data with the already synchronized acc and gyro data
import os
from pandas import DataFrame
import math


def sync_beacons(reference_timestamps, beacons_timestamps, beacons_rssis, beacons_tlm_packets):
    beacons_id_dict = {'46846e6187678448': 1,  # Coconut
                       '7897b2192cd1330e': 2,  # Mint
                       'f32a65edd388bbd4': 3,  # Ice
                       'c7f00010b342cf9e': 4,  # Blueberry
                       '992074a3a75b01dd': 5,  # P2
                       'eeaf86657d2312d5': 6,  # P1
                       '7bb8ba833ded2db9': 7,  # B2
                       '3e03d2aaf4265aa5': 8,  # B1
                       '3c53d934182ed091': 9,  # G2
                       '318da9517131bfab': 10  # G1
                       }

    # def sync_beacons(reference_timestamps, beacons_timestamps, beacons_rssis, beacons_tlm_packets):
    # Timestamps of the accelerometer already matched with the gyroscope t

    beacon_1_timestamps = [0] * len(reference_timestamps)
    beacon_1_rssis = [0] * len(reference_timestamps)
    beacon_1_tlm_packets = [0] * len(reference_timestamps)
    beacon_1_acc_x = [0] * len(reference_timestamps)
    beacon_1_acc_y = [0] * len(reference_timestamps)
    beacon_1_acc_z = [0] * len(reference_timestamps)
    beacon_1_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_1_current_motion_duration = [0] * len(reference_timestamps)
    beacon_1_motion_state = [0] * len(reference_timestamps)
    beacon_1_clock_error = [0] * len(reference_timestamps)
    beacon_1_firmware_error = [0] * len(reference_timestamps)

    beacon_2_timestamps = [0] * len(reference_timestamps)
    beacon_2_rssis = [0] * len(reference_timestamps)
    beacon_2_tlm_packets = [0] * len(reference_timestamps)
    beacon_2_acc_x = [0] * len(reference_timestamps)
    beacon_2_acc_y = [0] * len(reference_timestamps)
    beacon_2_acc_z = [0] * len(reference_timestamps)
    beacon_2_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_2_current_motion_duration = [0] * len(reference_timestamps)
    beacon_2_motion_state = [0] * len(reference_timestamps)
    beacon_2_clock_error = [0] * len(reference_timestamps)
    beacon_2_firmware_error = [0] * len(reference_timestamps)

    beacon_3_timestamps = [0] * len(reference_timestamps)
    beacon_3_rssis = [0] * len(reference_timestamps)
    beacon_3_tlm_packets = [0] * len(reference_timestamps)
    beacon_3_acc_x = [0] * len(reference_timestamps)
    beacon_3_acc_y = [0] * len(reference_timestamps)
    beacon_3_acc_z = [0] * len(reference_timestamps)
    beacon_3_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_3_current_motion_duration = [0] * len(reference_timestamps)
    beacon_3_motion_state = [0] * len(reference_timestamps)
    beacon_3_clock_error = [0] * len(reference_timestamps)
    beacon_3_firmware_error = [0] * len(reference_timestamps)

    beacon_4_timestamps = [0] * len(reference_timestamps)
    beacon_4_rssis = [0] * len(reference_timestamps)
    beacon_4_tlm_packets = [0] * len(reference_timestamps)
    beacon_4_acc_x = [0] * len(reference_timestamps)
    beacon_4_acc_y = [0] * len(reference_timestamps)
    beacon_4_acc_z = [0] * len(reference_timestamps)
    beacon_4_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_4_current_motion_duration = [0] * len(reference_timestamps)
    beacon_4_motion_state = [0] * len(reference_timestamps)
    beacon_4_clock_error = [0] * len(reference_timestamps)
    beacon_4_firmware_error = [0] * len(reference_timestamps)

    beacon_5_timestamps = [0] * len(reference_timestamps)
    beacon_5_rssis = [0] * len(reference_timestamps)
    beacon_5_tlm_packets = [0] * len(reference_timestamps)
    beacon_5_acc_x = [0] * len(reference_timestamps)
    beacon_5_acc_y = [0] * len(reference_timestamps)
    beacon_5_acc_z = [0] * len(reference_timestamps)
    beacon_5_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_5_current_motion_duration = [0] * len(reference_timestamps)
    beacon_5_motion_state = [0] * len(reference_timestamps)
    beacon_5_clock_error = [0] * len(reference_timestamps)
    beacon_5_firmware_error = [0] * len(reference_timestamps)

    beacon_6_timestamps = [0] * len(reference_timestamps)
    beacon_6_rssis = [0] * len(reference_timestamps)
    beacon_6_tlm_packets = [0] * len(reference_timestamps)
    beacon_6_acc_x = [0] * len(reference_timestamps)
    beacon_6_acc_y = [0] * len(reference_timestamps)
    beacon_6_acc_z = [0] * len(reference_timestamps)
    beacon_6_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_6_current_motion_duration = [0] * len(reference_timestamps)
    beacon_6_motion_state = [0] * len(reference_timestamps)
    beacon_6_clock_error = [0] * len(reference_timestamps)
    beacon_6_firmware_error = [0] * len(reference_timestamps)

    beacon_7_timestamps = [0] * len(reference_timestamps)
    beacon_7_rssis = [0] * len(reference_timestamps)
    beacon_7_tlm_packets = [0] * len(reference_timestamps)
    beacon_7_acc_x = [0] * len(reference_timestamps)
    beacon_7_acc_y = [0] * len(reference_timestamps)
    beacon_7_acc_z = [0] * len(reference_timestamps)
    beacon_7_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_7_current_motion_duration = [0] * len(reference_timestamps)
    beacon_7_motion_state = [0] * len(reference_timestamps)
    beacon_7_clock_error = [0] * len(reference_timestamps)
    beacon_7_firmware_error = [0] * len(reference_timestamps)

    beacon_8_timestamps = [0] * len(reference_timestamps)
    beacon_8_rssis = [0] * len(reference_timestamps)
    beacon_8_tlm_packets = [0] * len(reference_timestamps)
    beacon_8_acc_x = [0] * len(reference_timestamps)
    beacon_8_acc_y = [0] * len(reference_timestamps)
    beacon_8_acc_z = [0] * len(reference_timestamps)
    beacon_8_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_8_current_motion_duration = [0] * len(reference_timestamps)
    beacon_8_motion_state = [0] * len(reference_timestamps)
    beacon_8_clock_error = [0] * len(reference_timestamps)
    beacon_8_firmware_error = [0] * len(reference_timestamps)

    beacon_9_timestamps = [0] * len(reference_timestamps)
    beacon_9_rssis = [0] * len(reference_timestamps)
    beacon_9_tlm_packets = [0] * len(reference_timestamps)
    beacon_9_acc_x = [0] * len(reference_timestamps)
    beacon_9_acc_y = [0] * len(reference_timestamps)
    beacon_9_acc_z = [0] * len(reference_timestamps)
    beacon_9_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_9_current_motion_duration = [0] * len(reference_timestamps)
    beacon_9_motion_state = [0] * len(reference_timestamps)
    beacon_9_clock_error = [0] * len(reference_timestamps)
    beacon_9_firmware_error = [0] * len(reference_timestamps)

    beacon_10_timestamps = [0] * len(reference_timestamps)
    beacon_10_rssis = [0] * len(reference_timestamps)
    beacon_10_tlm_packets = [0] * len(reference_timestamps)
    beacon_10_acc_x = [0] * len(reference_timestamps)
    beacon_10_acc_y = [0] * len(reference_timestamps)
    beacon_10_acc_z = [0] * len(reference_timestamps)
    beacon_10_prev_motion_duration = [0] * len(reference_timestamps)
    beacon_10_current_motion_duration = [0] * len(reference_timestamps)
    beacon_10_motion_state = [0] * len(reference_timestamps)
    beacon_10_clock_error = [0] * len(reference_timestamps)
    beacon_10_firmware_error = [0] * len(reference_timestamps)

    def get_tlm_packet_data(tlm_packet):

        # Bytes 10-12. Beacon acceleration
        beacon_acc_x = int(tlm_packet[20:22], 16) * 2 / 127.0
        beacon_acc_y = int(tlm_packet[22:24], 16) * 2 / 127.0
        beacon_acc_z = int(tlm_packet[24:26], 16) * 2 / 127.0

        # Byte 13-14. Beacon previous and current motion state duration
        # previous motion duration
        byte_13 = "{0:08b}".format(int(tlm_packet[26:28], 16))
        byte_13_state = int(byte_13[0:2], 8)
        byte_13_number = int(byte_13[2:8], 8)

        if byte_13_state == 0:
            # seconds
            prev_motion_duration = byte_13_number
        else:
            if byte_13_state == 1:
                # minutes
                prev_motion_duration = byte_13_number * 60
            else:
                if byte_13_state == 2:
                    # hours
                    prev_motion_duration = byte_13_number * 3600
                else:
                    if byte_13_state == 3 and byte_13_number <= 32:
                        # days
                        prev_motion_duration = byte_13_number * 86400
                    else:
                        # weeks
                        prev_motion_duration = byte_13_number * 604800

        # current motion duration
        byte_14 = "{0:08b}".format(int(tlm_packet[28:30], 16))
        byte_14_state = int(byte_14[0:2], 8)
        byte_14_number = int(byte_14[2:8], 8)

        if byte_14_state == 0:
            # seconds
            current_motion_duration = byte_14_number
        else:
            if byte_14_state == 1:
                # minutes
                current_motion_duration = byte_14_number * 60
            else:
                if byte_14_state == 2:
                    # hours
                    current_motion_duration = byte_14_number * 3600
                else:
                    if byte_14_state == 3 and byte_14_number <= 32:
                        # days
                        current_motion_duration = byte_14_number * 86400
                    else:
                        # weeks
                        current_motion_duration = byte_14_number * 604800

        # Byte 15
        byte_15 = "{0:08b}".format(int(tlm_packet[30:32], 16))
        clock_error = int(byte_15[4], 8)
        firmware_error = int(byte_15[5], 8)
        motion_state = int(byte_15[6:8], 8)

        dict_tlm_packet_data = {'acc_x': beacon_acc_x, 'acc_y': beacon_acc_y,
                                'acc_z': beacon_acc_z,
                                'prev_motion_duration': prev_motion_duration,
                                'current_motion_duration': current_motion_duration,
                                'motion_state': motion_state,
                                'clock_error': clock_error,
                                'firmware_error': firmware_error
                                }
        return dict_tlm_packet_data

    def beacon_1(b_index, r_index):
        beacon_1_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_1_rssis[r_index] = beacons_rssis[b_index]

        beacon_1_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_1_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_1_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_1_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_1_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_1_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_1_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_1_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_1_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_2(b_index, r_index):
        beacon_2_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_2_rssis[r_index] = beacons_rssis[b_index]

        beacon_2_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_2_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_2_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_2_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_2_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_2_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_2_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_2_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_2_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_3(b_index, r_index):
        beacon_3_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_3_rssis[r_index] = beacons_rssis[b_index]

        beacon_3_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_3_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_3_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_3_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_3_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_3_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_3_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_3_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_3_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_4(b_index, r_index):
        beacon_4_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_4_rssis[r_index] = beacons_rssis[b_index]

        beacon_4_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_4_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_4_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_4_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_4_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_4_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_4_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_4_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_4_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_5(b_index, r_index):
        beacon_5_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_5_rssis[r_index] = beacons_rssis[b_index]

        beacon_5_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_5_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_5_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_5_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_5_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_5_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_5_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_5_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_5_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_6(b_index, r_index):
        beacon_6_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_6_rssis[r_index] = beacons_rssis[b_index]

        beacon_6_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_6_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_6_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_6_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_6_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_6_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_6_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_6_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_6_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_7(b_index, r_index):
        beacon_7_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_7_rssis[r_index] = beacons_rssis[b_index]

        beacon_7_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_7_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_7_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_7_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_7_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_7_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_7_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_7_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_7_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_8(b_index, r_index):
        beacon_8_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_8_rssis[r_index] = beacons_rssis[b_index]

        beacon_8_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_8_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_8_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_8_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_8_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_8_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_8_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_8_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_8_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_9(b_index, r_index):
        beacon_9_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_9_rssis[r_index] = beacons_rssis[b_index]

        beacon_9_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_9_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_9_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_9_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_9_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_9_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_9_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_9_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_9_firmware_error[r_index] = tlm_packet_data['firmware_error']

    def beacon_10(b_index, r_index):
        beacon_10_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_10_rssis[r_index] = beacons_rssis[b_index]

        beacon_10_tlm_packets[r_index] = beacons_tlm_packets[b_index]
        tlm_packet_data = get_tlm_packet_data(beacons_tlm_packets[b_index])
        beacon_10_acc_x[r_index] = tlm_packet_data['acc_x']
        beacon_10_acc_y[r_index] = tlm_packet_data['acc_y']
        beacon_10_acc_z[r_index] = tlm_packet_data['acc_z']
        beacon_10_prev_motion_duration[r_index] = tlm_packet_data['prev_motion_duration']
        beacon_10_current_motion_duration[r_index] = tlm_packet_data['current_motion_duration']
        beacon_10_motion_state[r_index] = tlm_packet_data['motion_state']
        beacon_10_clock_error[r_index] = tlm_packet_data['clock_error']
        beacon_10_firmware_error[r_index] = tlm_packet_data['firmware_error']

    switcher = {
        1: beacon_1,
        2: beacon_2,
        3: beacon_3,
        4: beacon_4,
        5: beacon_5,
        6: beacon_6,
        7: beacon_7,
        8: beacon_8,
        9: beacon_9,
        10: beacon_10
    }

    def appenddatabeacon(beac_id, beac_index, ref_index):
        # Get the function from switcher dictionary
        func = switcher.get(beac_id, lambda: "Invalid beacon")
        # Execute the function
        func(beac_index, ref_index)

    # Finding the closer timestamp from reference to target
    reference_timestamp_index = 0
    last_difference = 201
    for i in range(0, len(beacons_timestamps)):
        beacon_timestamp = beacons_timestamps[i]

        # get beacon's id from TLM packet
        beacon_tlm_packet = beacons_tlm_packets[i]
        beacon_id = beacons_id_dict[beacon_tlm_packet[2:18]]
        print(i)
        beacon_index = i

        for j in range(reference_timestamp_index, len(reference_timestamps)):
            reference_timestamp = reference_timestamps[j]

            current_difference = beacon_timestamp - reference_timestamp
            if current_difference > 0:
                last_difference = current_difference
            else:
                if i == 0:
                    if abs(current_difference) < 200:
                        reference_timestamp_index = j
                        appenddatabeacon(beacon_id, beacon_index, reference_timestamp_index)
                    else:
                        break
                else:
                    if abs(last_difference) < abs(current_difference):
                        reference_timestamp_index = j - 1
                        appenddatabeacon(beacon_id, beacon_index, reference_timestamp_index)
                    else:
                        reference_timestamp_index = j
                        appenddatabeacon(beacon_id, beacon_index, reference_timestamp_index)
                break

    beacons_dict = {'Timestamps_ref': reference_timestamps,

                    'Timestamps_beacon_1': beacon_1_timestamps, 'RSSIs_beacon_1': beacon_1_rssis,
                    'TLM_packets_beacon_1': beacon_1_tlm_packets,
                    'accX_beacon_1': beacon_1_acc_x,
                    'accY_beacon_1': beacon_1_acc_y,
                    'accZ_beacon_1': beacon_1_acc_z,
                    'prev_motion_duration_beacon_1': beacon_1_prev_motion_duration,
                    'current_motion_duration_beacon_1': beacon_1_current_motion_duration,
                    'motion_state_beacon_1': beacon_1_motion_state,
                    'clock_error_beacon_1': beacon_1_clock_error,
                    'firmware_error_beacon_1': beacon_1_firmware_error,

                    'Timestamps_beacon_2': beacon_2_timestamps, 'RSSIs_beacon_2': beacon_2_rssis,
                    'TLM_packets_beacon_2': beacon_2_tlm_packets,
                    'accX_beacon_2': beacon_2_acc_x,
                    'accY_beacon_2': beacon_2_acc_y,
                    'accZ_beacon_2': beacon_2_acc_z,
                    'prev_motion_duration_beacon_2': beacon_2_prev_motion_duration,
                    'current_motion_duration_beacon_2': beacon_2_current_motion_duration,
                    'motion_state_beacon_2': beacon_2_motion_state,
                    'clock_error_beacon_2': beacon_2_clock_error,
                    'firmware_error_beacon_2': beacon_2_firmware_error,

                    'Timestamps_beacon_3': beacon_3_timestamps, 'RSSIs_beacon_3': beacon_3_rssis,
                    'TLM_packets_beacon_3': beacon_3_tlm_packets,
                    'accX_beacon_3': beacon_3_acc_x,
                    'accY_beacon_3': beacon_3_acc_y,
                    'accZ_beacon_3': beacon_3_acc_z,
                    'prev_motion_duration_beacon_3': beacon_3_prev_motion_duration,
                    'current_motion_duration_beacon_3': beacon_3_current_motion_duration,
                    'motion_state_beacon_3': beacon_3_motion_state,
                    'clock_error_beacon_3': beacon_3_clock_error,
                    'firmware_error_beacon_3': beacon_3_firmware_error,

                    'Timestamps_beacon_4': beacon_4_timestamps, 'RSSIs_beacon_4': beacon_4_rssis,
                    'TLM_packets_beacon_4': beacon_4_tlm_packets,
                    'accX_beacon_4': beacon_4_acc_x,
                    'accY_beacon_4': beacon_4_acc_y,
                    'accZ_beacon_4': beacon_4_acc_z,
                    'prev_motion_duration_beacon_4': beacon_4_prev_motion_duration,
                    'current_motion_duration_beacon_4': beacon_4_current_motion_duration,
                    'motion_state_beacon_4': beacon_4_motion_state,
                    'clock_error_beacon_4': beacon_4_clock_error,
                    'firmware_error_beacon_4': beacon_4_firmware_error,

                    'Timestamps_beacon_5': beacon_5_timestamps, 'RSSIs_beacon_5': beacon_5_rssis,
                    'TLM_packets_beacon_5': beacon_5_tlm_packets,
                    'accX_beacon_5': beacon_5_acc_x,
                    'accY_beacon_5': beacon_5_acc_y,
                    'accZ_beacon_5': beacon_5_acc_z,
                    'prev_motion_duration_beacon_5': beacon_5_prev_motion_duration,
                    'current_motion_duration_beacon_5': beacon_5_current_motion_duration,
                    'motion_state_beacon_5': beacon_5_motion_state,
                    'clock_error_beacon_5': beacon_5_clock_error,
                    'firmware_error_beacon_5': beacon_5_firmware_error,

                    'Timestamps_beacon_6': beacon_6_timestamps, 'RSSIs_beacon_6': beacon_6_rssis,
                    'TLM_packets_beacon_6': beacon_6_tlm_packets,
                    'accX_beacon_6': beacon_6_acc_x,
                    'accY_beacon_6': beacon_6_acc_y,
                    'accZ_beacon_6': beacon_6_acc_z,
                    'prev_motion_duration_beacon_6': beacon_6_prev_motion_duration,
                    'current_motion_duration_beacon_6': beacon_6_current_motion_duration,
                    'motion_state_beacon_6': beacon_6_motion_state,
                    'clock_error_beacon_6': beacon_6_clock_error,
                    'firmware_error_beacon_6': beacon_6_firmware_error,

                    'Timestamps_beacon_7': beacon_7_timestamps, 'RSSIs_beacon_7': beacon_7_rssis,
                    'TLM_packets_beacon_7': beacon_7_tlm_packets,
                    'accX_beacon_7': beacon_7_acc_x,
                    'accY_beacon_7': beacon_7_acc_y,
                    'accZ_beacon_7': beacon_7_acc_z,
                    'prev_motion_duration_beacon_7': beacon_7_prev_motion_duration,
                    'current_motion_duration_beacon_7': beacon_7_current_motion_duration,
                    'motion_state_beacon_7': beacon_7_motion_state,
                    'clock_error_beacon_7': beacon_7_clock_error,
                    'firmware_error_beacon_7': beacon_7_firmware_error,

                    'Timestamps_beacon_8': beacon_8_timestamps, 'RSSIs_beacon_8': beacon_8_rssis,
                    'TLM_packets_beacon_8': beacon_8_tlm_packets,
                    'accX_beacon_8': beacon_8_acc_x,
                    'accY_beacon_8': beacon_8_acc_y,
                    'accZ_beacon_8': beacon_8_acc_z,
                    'prev_motion_duration_beacon_8': beacon_8_prev_motion_duration,
                    'current_motion_duration_beacon_8': beacon_8_current_motion_duration,
                    'motion_state_beacon_8': beacon_8_motion_state,
                    'clock_error_beacon_8': beacon_8_clock_error,
                    'firmware_error_beacon_8': beacon_8_firmware_error,

                    'Timestamps_beacon_9': beacon_9_timestamps, 'RSSIs_beacon_9': beacon_9_rssis,
                    'TLM_packets_beacon_9': beacon_9_tlm_packets,
                    'accX_beacon_9': beacon_9_acc_x,
                    'accY_beacon_9': beacon_9_acc_y,
                    'accZ_beacon_9': beacon_9_acc_z,
                    'prev_motion_duration_beacon_9': beacon_9_prev_motion_duration,
                    'current_motion_duration_beacon_9': beacon_9_current_motion_duration,
                    'motion_state_beacon_9': beacon_9_motion_state,
                    'clock_error_beacon_9': beacon_9_clock_error,
                    'firmware_error_beacon_9': beacon_9_firmware_error,

                    'Timestamps_beacon_10': beacon_10_timestamps, 'RSSIs_beacon_10': beacon_10_rssis,
                    'TLM_packets_beacon_10': beacon_10_tlm_packets,
                    'accX_beacon_10': beacon_10_acc_x,
                    'accY_beacon_10': beacon_10_acc_y,
                    'accZ_beacon_10': beacon_10_acc_z,
                    'prev_motion_duration_beacon_10': beacon_10_prev_motion_duration,
                    'current_motion_duration_beacon_10': beacon_10_current_motion_duration,
                    'motion_state_beacon_10': beacon_10_motion_state,
                    'clock_error_beacon_10': beacon_10_clock_error,
                    'firmware_error_beacon_10': beacon_10_firmware_error
                    }
    df_beac_sync = DataFrame(beacons_dict)
    return df_beac_sync
