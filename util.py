import pandas
import matplotlib.pyplot as plt

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

# File with Acc and Gyr timestamps synchronized
acc_gyr_sync_CSV = pandas.read_csv('dataset/' + file_name + '_sync.csv')
acc_sync_timestamps = acc_gyr_sync_CSV['Timestamp_Acc'].tolist()
gyr_sync_timestamps = acc_gyr_sync_CSV['Timestamp_Gyr'].tolist()


def difference_acc_gyr_synchronized():
    difference_acc_gyr_timestamp_sync = []
    for i in range(0, len(acc_sync_timestamps)):
        difference_acc_gyr_timestamp_sync.append(abs(acc_sync_timestamps[i] - gyr_sync_timestamps[i]))
    plt.plot(difference_acc_gyr_timestamp_sync)
    plt.show()


def min_max_acc_gyr():

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
    print('Biggest difference between acc samples: ' + str(difference_btw_acc_samples[0]) +
          ' index: ' + str(index_biggest_difference_acc))
    print('Smallest difference between acc samples: ' + str(difference_btw_acc_samples[len(difference_btw_acc_samples)-1]) +
          ' index: ' + str(index_smallest_difference_acc))
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
    print('biggest difference between gyro samples: ' + str(difference_btw_gyro_samples[0]) +
          ' index: ' + str(index_biggest_difference_gyro))
    print('Smallest difference between gyro samples: ' + str(difference_btw_gyro_samples[len(difference_btw_gyro_samples)-1]) +
          ' index: ' + str(index_smallest_difference_gyro))
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
    print('biggest difference between beacons samples: ' + str(difference_btw_beacons_samples[0]) +
          ' index: ' + str(index_biggest_difference_beacons))
    print('Smallest difference between beacons samples: ' + str(difference_btw_beacons_samples[len(difference_btw_beacons_samples)-1]) +
          ' index: ' + str(index_smallest_difference_beacons))
    # print(difference_btw_beacons_samples)


difference_acc_gyr_synchronized()
min_max_acc_gyr()