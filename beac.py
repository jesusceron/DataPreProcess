# def to synchronize beacons data with the already synchronized acc and gyro data
import os
from pandas import DataFrame


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

    beacon_2_timestamps = [0] * len(reference_timestamps)
    beacon_2_rssis = [0] * len(reference_timestamps)
    beacon_2_tlm_packets = [0] * len(reference_timestamps)

    beacon_3_timestamps = [0] * len(reference_timestamps)
    beacon_3_rssis = [0] * len(reference_timestamps)
    beacon_3_tlm_packets = [0] * len(reference_timestamps)

    beacon_4_timestamps = [0] * len(reference_timestamps)
    beacon_4_rssis = [0] * len(reference_timestamps)
    beacon_4_tlm_packets = [0] * len(reference_timestamps)

    beacon_5_timestamps = [0] * len(reference_timestamps)
    beacon_5_rssis = [0] * len(reference_timestamps)
    beacon_5_tlm_packets = [0] * len(reference_timestamps)

    beacon_6_timestamps = [0] * len(reference_timestamps)
    beacon_6_rssis = [0] * len(reference_timestamps)
    beacon_6_tlm_packets = [0] * len(reference_timestamps)

    beacon_7_timestamps = [0] * len(reference_timestamps)
    beacon_7_rssis = [0] * len(reference_timestamps)
    beacon_7_tlm_packets = [0] * len(reference_timestamps)

    beacon_8_timestamps = [0] * len(reference_timestamps)
    beacon_8_rssis = [0] * len(reference_timestamps)
    beacon_8_tlm_packets = [0] * len(reference_timestamps)

    beacon_9_timestamps = [0] * len(reference_timestamps)
    beacon_9_rssis = [0] * len(reference_timestamps)
    beacon_9_tlm_packets = [0] * len(reference_timestamps)

    beacon_10_timestamps = [0] * len(reference_timestamps)
    beacon_10_rssis = [0] * len(reference_timestamps)
    beacon_10_tlm_packets = [0] * len(reference_timestamps)

    def beacon_1(b_index, r_index):
        beacon_1_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_1_rssis[r_index] = beacons_rssis[b_index]
        beacon_1_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_2(b_index, r_index):
        beacon_2_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_2_rssis[r_index] = beacons_rssis[b_index]
        beacon_2_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_3(b_index, r_index):
        beacon_3_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_3_rssis[r_index] = beacons_rssis[b_index]
        beacon_3_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_4(b_index, r_index):
        beacon_4_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_4_rssis[r_index] = beacons_rssis[b_index]
        beacon_4_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_5(b_index, r_index):
        beacon_5_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_5_rssis[r_index] = beacons_rssis[b_index]
        beacon_5_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_6(b_index, r_index):
        beacon_6_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_6_rssis[r_index] = beacons_rssis[b_index]
        beacon_6_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_7(b_index, r_index):
        beacon_7_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_7_rssis[r_index] = beacons_rssis[b_index]
        beacon_7_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_8(b_index, r_index):
        beacon_8_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_8_rssis[r_index] = beacons_rssis[b_index]
        beacon_8_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_9(b_index, r_index):
        beacon_9_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_9_rssis[r_index] = beacons_rssis[b_index]
        beacon_9_tlm_packets[r_index] = beacons_tlm_packets[b_index]

    def beacon_10(b_index, r_index):
        beacon_10_timestamps[r_index] = beacons_timestamps[b_index]
        beacon_10_rssis[r_index] = beacons_rssis[b_index]
        beacon_10_tlm_packets[r_index] = beacons_tlm_packets[b_index]

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
                    'Timestamps_beacon_1': beacon_1_timestamps, 'RSSIs_beacon_1': beacon_1_rssis, 'TLM_packets_beacon_1': beacon_1_tlm_packets,
                    'Timestamps_beacon_2': beacon_2_timestamps, 'RSSIs_beacon_2': beacon_2_rssis, 'TLM_packets_beacon_2': beacon_2_tlm_packets,
                    'Timestamps_beacon_3': beacon_3_timestamps, 'RSSIs_beacon_3': beacon_3_rssis, 'TLM_packets_beacon_3': beacon_3_tlm_packets,
                    'Timestamps_beacon_4': beacon_4_timestamps, 'RSSIs_beacon_4': beacon_4_rssis, 'TLM_packets_beacon_4': beacon_4_tlm_packets,
                    'Timestamps_beacon_5': beacon_5_timestamps, 'RSSIs_beacon_5': beacon_5_rssis, 'TLM_packets_beacon_5': beacon_5_tlm_packets,
                    'Timestamps_beacon_6': beacon_6_timestamps, 'RSSIs_beacon_6': beacon_6_rssis, 'TLM_packets_beacon_6': beacon_6_tlm_packets,
                    'Timestamps_beacon_7': beacon_7_timestamps, 'RSSIs_beacon_7': beacon_7_rssis, 'TLM_packets_beacon_7': beacon_7_tlm_packets,
                    'Timestamps_beacon_8': beacon_8_timestamps, 'RSSIs_beacon_8': beacon_8_rssis, 'TLM_packets_beacon_8': beacon_8_tlm_packets,
                    'Timestamps_beacon_9': beacon_9_timestamps, 'RSSIs_beacon_9': beacon_9_rssis, 'TLM_packets_beacon_9': beacon_9_tlm_packets,
                    'Timestamps_beacon_10': beacon_10_timestamps, 'RSSIs_beacon_10': beacon_10_rssis, 'TLM_packets_beacon_10': beacon_10_tlm_packets
                    }
    df_beac_sync = DataFrame(beacons_dict)
    return df_beac_sync
