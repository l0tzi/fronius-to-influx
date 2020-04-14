#!/bin/bash
# coding: utf-8
import datetime
import json
import time

import pytz
from astral import Observer, sun
from requests import get
from requests.exceptions import ConnectionError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
class WrongFroniusData(Exception):
    pass

class SunIsDown(Exception):
    pass

class DataCollectionError(Exception):
    pass

class Fronius2Influx(object):
    IGNORE_SUN_DOWN = False
    BACKOFF_INTERVAL = 3

    def __init__(self, client, endpoints, bucket, location, tz):
        super().__init__()
        self.client = client
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.endpoints = endpoints
        self.bucket = bucket
        self.location = location
        self.tz = tz
    
    def on_exit(self):
        self.write_api.__del__()
        self.client.__del__()

    def get_float_or_zero(self, value):
        internal_data = None
        try:
            internal_data = self.data['Body']['Data']
        except KeyError:
            raise WrongFroniusData('Response structure is not healthy.')
        return float(internal_data.get(value, {}).get('Value', 0))

    def get_timestamp(self):
        return self.data['Head']['Timestamp']
    def translate_response(self):
        collection = None
        if 'DataCollection' in self.data['Head']['RequestArguments']:
            collection = self.data['Head']['RequestArguments']['DataCollection']
        elif 'DeviceClass' in self.data['Head']['RequestArguments']:
            collection = collection = self.data['Head']['RequestArguments']['DeviceClass']
        elif 'LoggerInfo' in self.data['Body'].keys():
            collection = 'LoggerInfo'
        if collection == 'CommonInverterData':
            return{
                        'ErrorCode': self.data['Body']['Data']['DeviceStatus']['ErrorCode'],
                        'LEDColor': self.data['Body']['Data']['DeviceStatus']['LEDColor'],
                        'LEDState': self.data['Body']['Data']['DeviceStatus']['LEDState'],
                        'MgmtTimerRemainingTime': self.data['Body']['Data']['DeviceStatus']['MgmtTimerRemainingTime'],
                        'StateToReset': self.data['Body']['Data']['DeviceStatus']['StateToReset'],
                        'StatusCode': self.data['Body']['Data']['DeviceStatus']['StatusCode'],
                        'FAC': self.get_float_or_zero('FAC'),
                        'IAC': self.get_float_or_zero('IAC'),
                        'IDC': self.get_float_or_zero('IDC'),
                        'PAC': self.get_float_or_zero('PAC'),
                        'UAC': self.get_float_or_zero('UAC'),
                        'UDC': self.get_float_or_zero('UDC'),
                        'DAY_ENERGY': self.get_float_or_zero('DAY_ENERGY'),
                        'YEAR_ENERGY': self.get_float_or_zero('YEAR_ENERGY'),
                        'TOTAL_ENERGY': self.get_float_or_zero('TOTAL_ENERGY'),
                    }
        elif collection == '3PInverterData':
            return {
                        'IAC_L1': self.get_float_or_zero('IAC_L1'),
                        'IAC_L2': self.get_float_or_zero('IAC_L2'),
                        'IAC_L3': self.get_float_or_zero('IAC_L3'),
                        'UAC_L1': self.get_float_or_zero('UAC_L1'),
                        'UAC_L2': self.get_float_or_zero('UAC_L2'),
                        'UAC_L3': self.get_float_or_zero('UAC_L3'),
                }
        elif collection == 'MinMaxInverterData':
            return {
                        'DAY_PMAX': self.get_float_or_zero('DAY_PMAX'),
                        'DAY_UACMAX': self.get_float_or_zero('DAY_UACMAX'),
                        'DAY_UDCMAX': self.get_float_or_zero('DAY_UDCMAX'),
                        'YEAR_PMAX': self.get_float_or_zero('YEAR_PMAX'),
                        'YEAR_UACMAX': self.get_float_or_zero('YEAR_UACMAX'),
                        'YEAR_UDCMAX': self.get_float_or_zero('YEAR_UDCMAX'),
                        'TOTAL_PMAX': self.get_float_or_zero('TOTAL_PMAX'),
                        'TOTAL_UACMAX': self.get_float_or_zero('TOTAL_UACMAX'),
                        'TOTAL_UDCMAX': self.get_float_or_zero('TOTAL_UDCMAX'),
                    }
        elif collection == 'Meter':
            return {
                        'Current_AC_Phase_1': self.data["Body"]["Data"]["Current_AC_Phase_1"],
                        'Current_AC_Phase_2': self.data["Body"]["Data"]["Current_AC_Phase_2"],
                        'Current_AC_Phase_3': self.data["Body"]["Data"]["Current_AC_Phase_3"],
                        'MeterManufacturer': self.data["Body"]["Data"]["Details"]['Manufacturer'],
                        'MeterModel': self.data["Body"]["Data"]["Details"]['Model'],
                        'MeterSerial': self.data["Body"]["Data"]["Details"]['Serial'],
                        'MeterEnable': self.data["Body"]["Data"]["Enable"],
                        'EnergyReactive_VArAC_Sum_Consumed': self.data["Body"]["Data"]["EnergyReactive_VArAC_Sum_Consumed"],
                        'EnergyReactive_VArAC_Sum_Produced': self.data["Body"]["Data"]["EnergyReactive_VArAC_Sum_Produced"],
                        'EnergyReal_WAC_Minus_Absolute': self.data["Body"]["Data"]["EnergyReal_WAC_Minus_Absolute"],
                        'EnergyReal_WAC_Plus_Absolute': self.data["Body"]["Data"]["EnergyReal_WAC_Plus_Absolute"],
                        'EnergyReal_WAC_Sum_Consumed': self.data["Body"]["Data"]["EnergyReal_WAC_Sum_Consumed"],
                        'EnergyReal_WAC_Sum_Produced': self.data["Body"]["Data"]["EnergyReal_WAC_Sum_Produced"],
                        'Frequency_Phase_Average': self.data["Body"]["Data"]["Frequency_Phase_Average"],
                        'Meter_Location_Current': self.data["Body"]["Data"]["Meter_Location_Current"],
                        'PowerApparent_S_Phase_1': self.data["Body"]["Data"]["PowerApparent_S_Phase_1"],
                        'PowerApparent_S_Phase_2': self.data["Body"]["Data"]["PowerApparent_S_Phase_2"],
                        'PowerApparent_S_Phase_3': self.data["Body"]["Data"]["PowerApparent_S_Phase_3"],
                        'PowerApparent_S_Sum': self.data["Body"]["Data"]["PowerApparent_S_Sum"],
                        'PowerFactor_Phase_1': self.data["Body"]["Data"]["PowerFactor_Phase_1"],
                        'PowerFactor_Phase_2': self.data["Body"]["Data"]["PowerFactor_Phase_2"],
                        'PowerFactor_Phase_3': self.data["Body"]["Data"]["PowerFactor_Phase_3"],
                        'PowerFactor_Sum': self.data["Body"]["Data"]["PowerFactor_Sum"],
                        'PowerReactive_Q_Phase_1': self.data["Body"]["Data"]["PowerReactive_Q_Phase_1"],
                        'PowerReactive_Q_Phase_2': self.data["Body"]["Data"]["PowerReactive_Q_Phase_2"],
                        'PowerReactive_Q_Phase_3': self.data["Body"]["Data"]["PowerReactive_Q_Phase_3"],
                        'PowerReactive_Q_Sum': self.data["Body"]["Data"]["PowerReactive_Q_Sum"],
                        'PowerReal_P_Phase_1': self.data["Body"]["Data"]["PowerReal_P_Phase_1"],
                        'PowerReal_P_Phase_2': self.data["Body"]["Data"]["PowerReal_P_Phase_2"],
                        'PowerReal_P_Phase_3': self.data["Body"]["Data"]["PowerReal_P_Phase_3"],
                        'PowerReal_P_Phase_Sum': float(self.data["Body"]["Data"]["PowerReal_P_Phase_Sum"]),
                        'MeterTimeStamp': self.data["Body"]["Data"]["TimeStamp"],
                        'MeterVisible': self.data["Body"]["Data"]["Visible"],
                        'Voltage_AC_PhaseToPhase_12': self.data["Body"]["Data"]["Voltage_AC_PhaseToPhase_12"],
                        'Voltage_AC_PhaseToPhase_23': self.data["Body"]["Data"]["Voltage_AC_PhaseToPhase_23"],
                        'Voltage_AC_PhaseToPhase_31': self.data["Body"]["Data"]["Voltage_AC_PhaseToPhase_31"],
                        'Voltage_AC_Phase_1': self.data["Body"]["Data"]["Voltage_AC_Phase_1"],
                        'Voltage_AC_Phase_2': self.data["Body"]["Data"]["Voltage_AC_Phase_2"],
                        'Voltage_AC_Phase_3': self.data["Body"]["Data"]["Voltage_AC_Phase_3"],
                    }
        elif collection == 'LoggerInfo':
            keyname = 'LoggerInfo'
            return {
                        'CO2Factor': self.data['Body'][keyname]['CO2Factor'],
                        'CO2Unit': self.data['Body'][keyname]['CO2Unit'],
                        'CashCurrency': self.data['Body'][keyname]['CashCurrency'],
                        'CashFactor': self.data['Body'][keyname]['CashFactor'],
                        'DefaultLanguage': self.data['Body'][keyname]['DefaultLanguage'],
                        'DeliveryFactor': self.data['Body'][keyname]['DeliveryFactor'],
                        'HWVersion': self.data['Body'][keyname]['HWVersion'],
                        'PlatformID': self.data['Body'][keyname]['PlatformID'],
                        'ProductID': self.data['Body'][keyname]['ProductID'],
                        'SWVersion': self.data['Body'][keyname]['SWVersion'],
                        'TimezoneLocation': self.data['Body'][keyname]['TimezoneLocation'],
                        'TimezoneName': self.data['Body'][keyname]['TimezoneName'],
                        'UTCOffset': self.data['Body'][keyname]['UTCOffset'],
                        'UniqueID': self.data['Body'][keyname]['UniqueID'],
                    }
        else:
            raise DataCollectionError("Unknown data collection type.")

    def sun_is_shining(self):
        sunrise = sun.sunrise(self.location, datetime.datetime.now(), self.tz)
        sunset = sun.sunset(self.location, datetime.datetime.now(), self.tz)
        if not self.IGNORE_SUN_DOWN and not sunrise < datetime.datetime.now(tz = self.tz) < sunset:
            raise SunIsDown
        return None

    def run(self):
        try:
            while(True):
                try:
                    self.sun_is_shining()
                    collected_data = {}
                    for url in self.endpoints:
                        #print(url)
                        self.data = get(url).json()
                        timestamp = self.data['Head']['Timestamp']
                        collected_data.update(self.translate_response())
                    #create output -> Timestamp + Fields
                    export_struct = {
                        'measurement': 'fronius',
                        'time': timestamp,
                        'fields': collected_data,
                        'tags': {'location' : 'fronius'}
                    }
                    #write2Influx2
                    self.write_api.write(bucket= self.bucket, record= Point.from_dict(export_struct))
                    print("Write successful - sleeping 30 sec")
                    time.sleep(30)
                except SunIsDown:
                    time.sleep(60)
                    print('Waited 60 seconds for sunrise')
                except ConnectionError as e:
                    print("Exception: {}".format(e))
                    print("Waiting for connection...")
                    time.sleep(10)
                    print('Waited 10 seconds for connection')
                except KeyError:
                    raise WrongFroniusData('Response structure is not healthy')
                except Exception as e:
                    self.data = {}
                    time.sleep(10)
                    print("Exception: {}".format(e))
        except KeyboardInterrupt:
            print("Finishing. Goodbye!")
