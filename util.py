import pandas
import matplotlib.pyplot as plt


def difference_acc_gyr_synchronized():
    file_name = '1'
    # File with Acc and Gyr timestamps synchronized
    acc_gyr_sync_CSV = pandas.read_csv('dataset/' + file_name + '_acc_gyr_sync.csv')
    acc_sync_timestamps = acc_gyr_sync_CSV['Timestamp_Acc'].tolist()
    gyr_sync_timestamps = acc_gyr_sync_CSV['Timestamp_Gyr'].tolist()

    difference_acc_gyr_timestamp_sync = []
    for i in range(0, len(acc_sync_timestamps)):
        difference_acc_gyr_timestamp_sync.append(abs(acc_sync_timestamps[i] - gyr_sync_timestamps[i]))
    plt.plot(difference_acc_gyr_timestamp_sync)
    plt.show()


def min_max_acc_gyr():
    file_name = '1'
    accelerometer_CSV = pandas.read_csv('dataset/' + file_name + '_acc_raw.csv')
    accelerometer_CSV.sort_values(by='Timestamp', inplace=True)
    acc_timestamps = accelerometer_CSV['Timestamp'].tolist()

    gyroscope_CSV = pandas.read_csv('dataset/' + file_name + '_gyr_raw.csv')
    gyroscope_CSV.sort_values(by='Timestamp', inplace=True)
    gyro_timestamps = gyroscope_CSV['Timestamp'].tolist()

    beacons_CSV = pandas.read_csv('dataset/' + file_name + '_beacons_raw.csv')
    beacons_CSV.sort_values(by='Timestamp', inplace=True)
    beacons_timestamps = beacons_CSV['Timestamp'].tolist()

    difference_btw_acc_samples = []
    biggest_difference_acc = 0
    smallest_difference_acc = 10
    index_biggest_difference_acc = 0
    index_smallest_difference_acc = 0

    for i in range(0, len(acc_timestamps) - 2):
        difference_acc = acc_timestamps[i + 1] - acc_timestamps[i]

        if difference_acc > biggest_difference_acc:
            biggest_difference_acc = difference_acc
            index_biggest_difference_acc = i

        if difference_acc < smallest_difference_acc:
            smallest_difference_acc = difference_acc
            index_smallest_difference_acc = i

        difference_btw_acc_samples.append(difference_acc)

    difference_btw_acc_samples.sort(reverse=True)
    print('Biggest difference between acc samples: ' + str(difference_btw_acc_samples[0]) + "ms" +
          ' \nsample index: ' + str(index_biggest_difference_acc))
    print('Smallest difference between acc samples: ' + str(
        difference_btw_acc_samples[len(difference_btw_acc_samples) - 1]) + "ms" +
          ' \nsample index: ' + str(index_smallest_difference_acc))
    # print(difference_btw_acc_samples)

    difference_btw_gyro_samples = []
    biggest_difference_gyro = 0
    smallest_difference_gyro = 10
    index_biggest_difference_gyro = 0
    index_smallest_difference_gyro = 0
    for i in range(0, len(gyro_timestamps) - 2):
        difference_gyro = gyro_timestamps[i + 1] - gyro_timestamps[i]

        if difference_gyro > biggest_difference_gyro:
            biggest_difference_gyro = difference_gyro
            index_biggest_difference_gyro = i

        if difference_gyro < smallest_difference_gyro:
            smallest_difference_gyro = difference_gyro
            index_smallest_difference_gyro = i

        difference_btw_gyro_samples.append(difference_gyro)

    difference_btw_gyro_samples.sort(reverse=True)
    print('biggest difference between gyro samples: ' + str(difference_btw_gyro_samples[0]) + "ms" +
          ' \nsample index: ' + str(index_biggest_difference_gyro))
    print('Smallest difference between gyro samples: ' + str(
        difference_btw_gyro_samples[len(difference_btw_gyro_samples) - 1]) + "ms" +
          ' \nsample index: ' + str(index_smallest_difference_gyro))
    # print(difference_btw_gyro_samples)

    difference_btw_beacons_samples = []
    biggest_difference_beacons = 0
    smallest_difference_beacons = 10
    index_biggest_difference_beacons = 0
    index_smallest_difference_beacons = 0
    for i in range(0, len(beacons_timestamps) - 2):
        difference_beacons = beacons_timestamps[i + 1] - beacons_timestamps[i]

        if difference_beacons > biggest_difference_beacons:
            biggest_difference_beacons = difference_beacons
            index_biggest_difference_beacons = i

        if difference_beacons < smallest_difference_beacons:
            smallest_difference_beacons = difference_beacons
            index_smallest_difference_beacons = i

        difference_btw_beacons_samples.append(difference_beacons)

    difference_btw_beacons_samples.sort(reverse=True)
    print('biggest difference between beacons samples: ' + str(difference_btw_beacons_samples[0]) + "ms" +
          ' \nsample index: ' + str(index_biggest_difference_beacons))
    print('Smallest difference between beacons samples: ' + str(
        difference_btw_beacons_samples[len(difference_btw_beacons_samples) - 1]) + "ms" +
          ' \nsample index: ' + str(index_smallest_difference_beacons))


def get_tlm_packet_data(tlm_packet):

    def twos_complement(num_binary):
        # Inverting the bits one by one
        num = ''
        for i in range(8):
            if num_binary[i] == '1':
                num += '0'
            else:
                num += '1'

        num = (int(num, 2) + 1) * -1

        return num

    # Bytes 10-12. Beacon acceleration
    beacon_acc = [int(tlm_packet[20:22], 16), int(tlm_packet[22:24], 16), int(tlm_packet[24:26], 16)]

    for i in range(0, 3):
        beacon_acc_binary = "{0:08b}".format(beacon_acc[i])

        if beacon_acc_binary[0] == '1':
            beacon_acc[i] = twos_complement(beacon_acc_binary)
            beacon_acc[i] = (beacon_acc[i] * 2 / 127.0) * 9.81
        else:
            beacon_acc[i] = (beacon_acc[i] * 2 / 127.0) * 9.81

    print(beacon_acc)

# Choose the function you want to run:
# difference_acc_gyr_synchronized()
# min_max_acc_gyr()
get_tlm_packet_data("22f32a65edd388bbd400fcfe3e4182f1ffffffff") # get accelerometer data of the beacon
