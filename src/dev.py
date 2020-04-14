#!/usr/bin/python3
from fronius2influx import Fronius2Influx
from influxdb_client import InfluxDBClient
from astral import Observer
import pytz

city = Observer(50.118890, 10.675173, 15)
client = InfluxDBClient.from_config_file('../conf/config.ini')
bucket = 'grafana'
tz = pytz.timezone("Europe/Berlin")
endpoints = [
    'http://10.0.0.210/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DataCollection=3PInverterData&DeviceId=1',
    'http://10.0.0.210/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DataCollection=CommonInverterData&DeviceId=1',
    'http://10.0.0.210/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DataCollection=MinMaxInverterData&DeviceId=1',
    'http://10.0.0.210/solar_api/v1/GetMeterRealtimeData.cgi?Scope=Device&DeviceId=0',
    'http://10.0.0.210/solar_api/v1/GetLoggerInfo.cgi'
]
z = Fronius2Influx(client, endpoints, bucket, city, tz)
z.IGNORE_SUN_DOWN = False
z.run()