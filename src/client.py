"""
Copyright 2018 Dario Russo <dario.russo@isti.cnr.it>

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""


import pymysql
from pandas.io import sql


import datetime as dt # to calculate the elapsed time

from device import Event
from device import DeviceType
from mapping import MappingException

import requests
import json

import pandas as pd
#from tables.tests.create_backcompat_indexes import row

class Client :
    def getSleepEvents(self, start_date, end_date):
        raise NotImplementedError
    
class C2KRestClient(Client) :
    
        # Database connection settings and related tables
    DB_HOST = 'xxx'
    DB_PORT = 3306
    DB_USERNAME = 'xxx'
    DB_PASSWORD = 'xxx'
    DB_NAME = 'xxx'
    DB_TABLE_DATA = 'xxx'
    DB_TABLE_CACHE = 'xxx'
    LEN_TO_CACHE = 3
    
    """
    Defines the client application to connect to C2K server.
    """
    
    BASE_REST_URL = "http://xxx/xxx/xxx"
    
    def __init__(self, params, mapping):
        """
        Creates an instance of the class.
        @type params: array
        @param params: the parameters passed with the invoke of the service. 
        @type mapping: EventMapping
        @param params: the parameters passed with the invoke of the service. 
        """
        self.user_id = params['sleep']['user_id']
        params = self.fixInputParams(params) 
        self.mapping = mapping
        self.createDeviceMap()
 
    def fixInputParams(self, params) :
        """
        Verifies input parameters and adjusts them if needed.
        @type params: array
        @param params: the parameters passed with the invoke of the service.
        @rtype array
        @return the adjusted params. 
        """
        if params['sleep']['param_1'] == None or \
         params['sleep']['param_1'] == "None" :
            params['sleep']['param_1'] = self.getAvailableDay('first')
        if params['sleep']['param_2'] == None \
         or params['sleep']['param_2'] == "None" :
            params['sleep']['param_2'] = self.getAvailableDay('last')
        if params['sleep']['action'] == 'analyze' : 
            if params['sleep']['param_3'] == None:
                params['sleep']['param_3'] = self.getAvailableDay('first')
            if params['sleep']['param_4'] == None:
                params['sleep']['param_4'] = self.getAvailableDay('last')
        return params
    
    def getAvailableDay(self, day) :
        """
        Gets the first or last day of available data.
        @type day: String
        @param day: can be 'fist' for the first day of data, 'last' for the last 
               day of data.
        @rtype date
        @return the requested date.     
        """
        data =   '{' \
                + '   "sensor": {' \
                + '     "id":""' \
                + '   },' \
                + '   "dateFrom":"30-01-2018 10:30",' \
                + '   "dateTo":"",' \
                + '   "patientId": "' + self.user_id + '"' \
                + '}'
        headers = {'Content-type': 'application/json'}
        response = \
         requests.post(self.BASE_REST_URL + '/measurements',
                       headers=headers, data=data)    
        loaded_json = json.loads(response.text)['RESULTS']
        loaded_json = sorted(loaded_json, key=lambda x: 
                             dt.datetime(x['timestamp']['year'],
                                        x['timestamp']['monthValue'],
                                        x['timestamp']['dayOfMonth'],
                                        x['timestamp']['hour'],
                                        x['timestamp']['minute'],
                                        x['timestamp']['second']))
        if day == 'first' :
            requested_date = dt.date(loaded_json[0]['timestamp']['year'],
                                     loaded_json[0]['timestamp']['monthValue'],
                                     loaded_json[0]['timestamp']['dayOfMonth'])
        else :
            requested_date = dt.date(loaded_json[len(loaded_json) - 1]
                                                ['timestamp']['year'],
                                     loaded_json[len(loaded_json) - 1]
                                                ['timestamp']['monthValue'],
                                     loaded_json[len(loaded_json) - 1]
                                                ['timestamp']['dayOfMonth'])
        return requested_date                           

    def getSleepEvents(self, start_datetime, end_datetime) :
        """
        Gets the events occurred in a certain period.
        @type start_date date
        @param start_date the starting date of the time interval.
        @type end_date date
        @param end_date the ending date of the time interval.
        @rtype list of Event
        @return the list of Event that occurred. 
        """
        # There is only one device for type so it is taken the first one.
        bed_device = self.mapping.searchDevice(DeviceType.BED)[0]
        if bed_device == None :
            sensor_id = ""
        else :
            sensor_id = bed_device.id
        events = []          
        data = \
              '{' \
            + '   "sensor": {' \
            + '     "id":"' + sensor_id + '"'\
            + '   },' \
            + '   "dateFrom":"' \
            + start_datetime.strftime("%d-%m-%Y %H:%M") + '", ' \
            + '   "dateTo":"' \
            + end_datetime.strftime("%d-%m-%Y %H:%M") + '", ' \
            + '   "patientId":"' + self.user_id + '"' \
            + '}'
        headers = {'Content-type': 'application/json'}
        response = requests.post(self.BASE_REST_URL + '/measurements',
                     data=data, headers=headers)
        
        loaded_json = json.loads(response.text)['RESULTS']
        for x in loaded_json :
            x['compact_timestamp'] = dt.datetime(x['timestamp']['year'],
                                                 x['timestamp']['monthValue'],
                                                 x['timestamp']['dayOfMonth'],
                                                 x['timestamp']['hour'],
                                                 x['timestamp']['minute'],
                                                 x['timestamp']['second']) 
        loaded_json = sorted(loaded_json, key=lambda x: x['compact_timestamp'])
        selected_data = []
        for row in loaded_json :
            if (row['compact_timestamp'] >= start_datetime) \
                & (row['compact_timestamp'] < end_datetime) :
                selected_data.append(row)
        for row in selected_data :
            try : 
                events.append(self.mapping.mapEvent(row))
            except MappingException as e:
                print(e.getMessage())
                pass                     
        closing_event = Event(dt.datetime(dt.MINYEAR, 1, 1, 1, 0, 0), 
                                  0, 0, 0)
        events.append(closing_event)
        return events  
    
    def createDeviceMap(self) :
        """
        Creates the virtual representation of devices.
        """
        headers = {'Content-type': 'application/json'}
        response = \
         requests.get(self.BASE_REST_URL + "/person/" 
                      + self.user_id + "/sensor.json",
                      headers=headers)
        loaded_json = json.loads(response.text)['RESULTS']
        for row in loaded_json :
            self.mapping.mapAndAddDeviceMap(row)
        self.mapping.createDevices()    

    def getCachedSleepData(self, start_date, end_date) :
        """
        Gets calculated sleep activities data that are stored in the server.
        @type start_date: date
        @param start_date: the beginning of the searched period of time.
        @type end_date: date
        @param end_date: the end of the searched period of time.
        @rtype dataframe
        @return the dataframe containing the calculated sleep activities.      
        """
        columns = ['date', 'start_datetime', 'end_datetime', 
                   'count_micro_awakenings', 'time_micro_awakenings', 
                   'count_awakenings', 'time_awakenings']
        cached_df = pd.DataFrame(columns=columns) 
        
        mysql_cn = \
         pymysql.connect(host=self.DB_HOST, port=self.DB_PORT, \
                         user=self.DB_USERNAME, passwd=self.DB_PASSWORD,
                         db=self.DB_NAME)
        sql_query = "SELECT date, start_datetime, end_datetime," \
                    + " count_micro_awakenings, time_micro_awakenings," \
                    + " count_awakenings, time_awakenings FROM " \
                    + self.DB_TABLE_CACHE + " WHERE date >= " \
                    + "'" + start_date.strftime('%Y-%m-%d') + "'"  \
                    + "and date <= " + "'" + end_date.strftime('%Y-%m-%d') \
                    + "'" + "order by date"  
        db_cache_dataframe = pd.read_sql(sql_query, mysql_cn)
        mysql_cn.close()
        
        cached_df = db_cache_dataframe       
        return cached_df
    
    def insertSleepDataToCache(self, df_sleep_data) :
        """
        Store the calculated sleep activities in the server.
        @type df_sleep_data: dataframe
        @param df_sleep_data: the dataframe containing data to be stored.
        """
                #start_time = dt.datetime.now()
        if not df_sleep_data.empty :
            mysql_cn = pymysql.connect(host=self.DB_HOST, port=self.DB_PORT, 
                                       user=self.DB_USERNAME, 
                                       passwd=self.DB_PASSWORD, db=self.DB_NAME)
            for _, row in df_sleep_data.iterrows() :                
                sql_query = "INSERT INTO " + self.DB_TABLE_CACHE \
                + " (date, start_datetime, end_datetime,"\
                + " count_micro_awakenings, time_micro_awakenings, "\
                + " count_awakenings, time_awakenings) VALUES" \
                + " ('" + row['date'].strftime('%Y-%m-%d') \
                + "', '" \
                + row['start_datetime'].strftime('%Y-%m-%d %H:%M:%S') + "', '"\
                + row['end_datetime'].strftime('%Y-%m-%d %H:%M:%S') + "', '" \
                + str(row['count_micro_awakenings']) + "', '"\
                + str(row['time_micro_awakenings']) + "', '"\
                + str(row['count_awakenings']) + "', '"\
                + str(row['time_awakenings']) + "');"
                sql.execute(sql_query, mysql_cn)
            mysql_cn.commit()
            mysql_cn.close()   
