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

"""
Implements all the intelligent algorithms that permits to extract user's 
information about his habits.
"""

import pandas as pd
import datetime as dt

from device import DeviceType
from device import DeviceStateName
from device import DeviceStateValue
from configuration import Configuration

class Intelligence:
    """
    Defines the structures and methods used to implement Intelligence classes.
    """
    ## Initializes the class with the list of sensors.
    # @param sensor_map the dict of sensors.
    def __init__(self, devices):
        """
        :rtype: Intelligence
        """
        self.devices = devices

    ## Tries to analyze data to deduct events.
    #  @param data the received data.
    def analyzeData(self, event) :
        event.pretty_print()
        self.sleep.sleep.compute_sleep_activities(event)
        #self.countUsers(data)  

class PartialSleepData :
    result = None
    to_cache = None

    def __init__(self, result, to_cache):
        self.result = result
        self.to_cache = to_cache
        
class Sleep(Intelligence) :
    """
    Analyzes and learn user's sleep habits.
    """  
    
    first_datetime = None

    # The datetime when the bed sensor is pressed for the first time 
    # during the current day or after at least 
    # MIN_TIME_INTERVAL_FOR_SLEEPING seconds
    start_bed = dt.datetime(dt.MINYEAR, 1, 1, 1, 0, 0)
    # The datetime when the bed sensor is pressed during a sleep activity, 
    # after that it was released
    tmp_start_bed = dt.datetime(dt.MINYEAR, 1, 1, 1, 0, 0)
    # The end of the sleep activity started with start_bed
    end_bed = dt.datetime(dt.MINYEAR, 1, 1, 1, 0, 0)    
    # Count of the micro awakening
    count_micro_awakening = 0
    # Count of the awakenings
    count_awakening = 0
    # Sum of time of micro awakening
    time_micro_awakening = 0
    # Sum of time of awakening
    time_awakening = 0
    # Stores the last sensor value read before current one
    last_bed_value = DeviceStateValue.NOT_PRESENT
    
    event_micro_awakening = False
    event_awakening = False
    event_sleep = False
    event_discard_sleep = False
    
    current_sleep_day = dt.date(dt.MINYEAR, 1, 1)
    beginning_of_data = dt.datetime(dt.MAXYEAR, 1, 1, 0, 0, 0)
                  
    def computeSleepActivities(self, event) :
        """
        Takes an event at time and analyze sleeping habits. 
        Daily habits are stored as record in a database.
        @type event dataframe row
        @param event the event to analyze.
        """
        columns = ['date', 'start_datetime', 'end_datetime',
                   'count_micro_awakenings', 'time_micro_awakenings', 
                  'count_awakenings', 'time_awakenings']
        sleep_df = pd.DataFrame(columns=columns)
        if event.datetime :
            if event.device_id in self.devices :
                device = self.devices[event.device_id]
                if device.type == DeviceType.BED :
                    if event.name_id == DeviceStateName.PRESENCE :
                        #print(event.pretty_print())
                        if event.value_id == \
                         DeviceStateValue.PRESENT and \
                         self.last_bed_value == DeviceStateValue.NOT_PRESENT :
                            # The bed sensor is pressed
                            #print("Bed sensor on: " \
                            #  + event.datetime.strftime('%Y-%m-%d %H:%M:%S'))
                            self.last_bed_value = DeviceStateValue.PRESENT
                            if self.start_bed.year == dt.MINYEAR:
                                # Fist time the sensor is pressed during the day. 
                                # It is necessary to initialize some values.
                                self.start_bed = event.datetime
                                self.tmp_start_bed = event.datetime
                            else :
                                self.tmp_start_bed = event.datetime
                            
                            if (self.tmp_start_bed - self.end_bed) \
                             .total_seconds() \
                                < Configuration.TIME_INTERVAL_FOR_MICRO_AWAKENING :
                                self.event_micro_awakening = True
                            elif (self.tmp_start_bed - self.end_bed) \
                             .total_seconds() \
                                  < Configuration.TIME_INTERVAL_FOR_AWAKENING :
                                self.event_awakening = True
                            elif self.end_bed.year !=  dt.MINYEAR :
                                if (self.end_bed - self.start_bed) \
                                 .total_seconds() >= \
                                  Configuration.MIN_TIME_INTERVAL_FOR_SLEEPING:
                                    self.event_sleep = True
                                else :
                                    self.event_discard_sleep = True
                        elif event.value_id == \
                         DeviceStateValue.NOT_PRESENT and \
                         self.last_bed_value == DeviceStateValue.PRESENT :
                            #print("Bed sensor off: " 
                            #  + event.datetime.strftime('%Y-%m-%d %H:%M:%S'))
                            self.last_bed_value = DeviceStateValue.NOT_PRESENT
                            self.end_bed = event.datetime
            
            # Caputures the event that permits to understand the raw data are
            # finished. In the event there is not a valid deviceId.
            # The event sleep is recognized even if start_bed time is a valid
            # datetime. If it is set as MINYEAR, it means that previously 
            # another finish event was successfully considered.    
            if event.datetime.year == dt.MINYEAR and \
               self.start_bed.year != dt.MINYEAR and \
                 (self.end_bed - self.start_bed).total_seconds() \
                  >= Configuration.MIN_TIME_INTERVAL_FOR_SLEEPING :
                self.event_sleep = True 
                            
        if self.event_micro_awakening :
            self.time_micro_awakening += \
             (self.tmp_start_bed - self.end_bed).total_seconds()
            self.count_micro_awakening += 1
            self.event_micro_awakening = False
        elif self.event_awakening :
            self.time_awakening += \
             (self.tmp_start_bed - self.end_bed).total_seconds()
            self.count_awakening += 1
            self.event_awakening = False
        elif self.event_sleep :
            if self.start_bed.time() >= \
                dt.time(Configuration.HH_DAY_SLEEP_SEPARATOR,
                        Configuration.MM_DAY_SLEEP_SEPARATOR,
                        Configuration.SS_DAY_SLEEP_SEPARATOR) \
                and self.start_bed.time() <= dt.time(23,59,59) :
                self.current_sleep_day = self.start_bed.date() + dt.timedelta(days=1)
            else :
                self.current_sleep_day = self.start_bed.date()
            #print("Sleeping day: " + self.current_sleep_day.strftime('%Y-%m-%d')
            #    + " sleeping from: "
            #    + self.start_bed.strftime('%Y-%m-%d %H:%M:%S') 
            #    + " to " 
            #    + self.end_bed.strftime('%Y-%m-%d %H:%M:%S') 
            #    + " for " 
            #    + str((self.end_bed - self.start_bed).total_seconds() 
            #           / 60) 
            #    + " min.; micro awakening: " 
            #    + str(self.micro_awakening) + "; awakening: " 
            #    + str(self.awakening)) 
            sleep_df.loc[sleep_df.size] = [self.current_sleep_day,
                             self.start_bed,
                             self.end_bed,
                             self.count_micro_awakening,
                             self.time_micro_awakening,
                             self.count_awakening,
                             self.time_awakening ]
            self.start_bed = event.datetime 
            self.count_micro_awakening = 0
            self.time_micro_awakening = 0
            self.count_awakening = 0
            self.time_awakening = 0
            self.event_sleep = False
 
        elif self.event_discard_sleep :
            self.start_bed = event.datetime 
            self.count_micro_awakening = 0
            self.time_micro_awakening = 0
            self.count_awakening = 0
            self.time_awakening = 0
            self.event_discard_sleep = False
        return sleep_df 

