from influxdb import InfluxDBClient
from dotenv import load_dotenv
from datetime import datetime
import logging
import psutil
import time
import os
load_dotenv()

# ENV variables
CONFIG = {
  'NUM_PATHS': os.environ.get('NUM_PATHS'),
  'DISK_PATHS': os.environ.get('DISK_PATHS').split(','),
  'INTERVAL': int(os.environ.get('INTERVAL')),
  'ERR_INTERVAL': int(os.environ.get('ERR_INTERVAL')),
}

DB = {
  'HOST': os.environ.get('DB_HOST'),
  'PORT': int(os.environ.get('DB_PORT')),
  'USER': os.environ.get('DB_USER'),
  'PASSWORD': os.environ.get('DB_PASSWORD'),
  'DATABASE': os.environ.get('DB_DATABASE')
}

# Setup
influx = InfluxDBClient(DB['HOST'], DB['PORT'], DB['USER'], DB['PASSWORD'], DB['DATABASE'])

def get_data_of_disk(disk, dtime):
  # Get the space information for the disk
  disk_usage = psutil.disk_usage(disk)
  
  # Format the data as a JSON object
  data = {
    "measurement": "disk_space",
    "tags": {
      "disk": disk
    },
    "fields": {
      "total": disk_usage.total,
      "used": disk_usage.used,
      "free": disk_usage.free,
      "percent": disk_usage.percent
    },
    "time": dtime
  }

  return data

def store_data(data):
  # Write the data to the database
  if influx.write_points(data) != True:
    raise

if __name__ == '__main__':
  print('[Init application]: Disk monitoring.')
  logging.info('[Scanning]: Disks.')

  while(True):
    try:
      dt = datetime.now()
      dtime = int(datetime.timestamp(dt))
      data = []
      for disk in CONFIG['DISK_PATHS']:  
        record = get_data_of_disk(disk, dtime)
        data.append(record)

      store_data(data)
      print(f'[Data stored] at: {dtime}')
      time.sleep(CONFIG['INTERVAL'])
    except Exception as err:
      logging.error(f'[error] saving disk path. {err}')
      time.sleep(CONFIG['ERR_INTERVAL'])

