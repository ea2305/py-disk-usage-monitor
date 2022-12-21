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
influx = InfluxDBClient(DB['HOST'], DB['PORT'], DB['USER'], DB['PASSWORD'])
dbs = influx.get_list_database()
if DB['DATABASE'] not in dbs:
  influx.create_database(DB['DATABASE'])

influx.switch_database(DB['DATABASE'])

def get_data_of_disk(disk, dtime):
  # Get the space information for the disk
  disk_usage = psutil.disk_usage(disk)
  
  # Format the data as a JSON object
  json_body = [
      {
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
  ]

  return json_body

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
      for disk in CONFIG['DISK_PATHS']:  
        data = get_data_of_disk(disk, dtime)
        store_data(data)

      print('[Data stored]')
      time.sleep(CONFIG['INTERVAL'])
    except Exception as err:
      logging.error(f'[error] saving disk path. {err}')
      time.sleep(CONFIG['ERR_INTERVAL'])

