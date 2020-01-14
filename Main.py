import pandas

# read CSV files
accelerometer_CSV = pandas.read_csv('dataset/1_acc.csv')
beacons_CSV = pandas.read_csv('dataset/1_beacons.csv')
gyroscope_CSV = pandas.read_csv('dataset/1_gyr.csv')

# getting acc and gyr timestamps for matching them
acc_timestamps = accelerometer_CSV['Timestamp']
beacons_timestamps = beacons_CSV['Timestamp']
gyro_timestamps = gyroscope_CSV['Timestamp']

if acc_timestamps[0] > gyro_timestamps[0]:
    start_reference = acc_timestamps[0]
else:
    start_reference = gyro_timestamps[0]

print(start_reference)