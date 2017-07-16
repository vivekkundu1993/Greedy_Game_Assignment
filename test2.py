import datetime
ts_epoch = 1462762799905
ts = datetime.datetime.fromtimestamp(ts_epoch).strftime('%Y-%m-%d %H:%M:%S')
print(ts)
