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

import datetime as dt
import pandas as pd
import numpy as np
import math
#import matplotlib.pyplot as plt
import gc

from configuration import Configuration
from intelligence import Sleep
from device import DeviceStateValue

class Analitics :
    
    def __init__(self, client) :
        self.client = client

    def execute(self, params):
        """
        Main function.
        @param argv CLI parameters.
        """
        self.sleep = Sleep(self.client.mapping.devices)
        
        # ACTIVITY COMMAND
        if (params['sleep']['action'] == "activity"):
            sleep_activities = \
                self.sleepActivities(self.sleep, params['sleep']['param_1'], 
                                     params['sleep']['param_2'])  
            return sleep_activities
    
        # AVERAGE COMMAND
        elif(params['sleep']['action'] == "average"):
            params['sleep']['action'] = 'activity'
            sleep_activities = \
                self.sleepActivities(self.sleep, params['sleep']['param_1'], 
                                     params['sleep']['param_2'])  
            average = self.sleepAverage(sleep_activities)
            return average
    
        # ANALYZE COMMAND
        elif(params['sleep']['action'] == "analyze"):
            sleep_activities = self.sleepActivities(self.sleep, params['sleep']['param_1'], 
                                     params['sleep']['param_2'])
            average = self.sleepAverage(sleep_activities)
            analysis = self.sleep_analysis(sleep_activities, average)      
            return analysis
    
        # COSINE similarity
        elif(params['sleep']['action'] == "cosine"):
            cosine = self.cosine_for_days(params['sleep']['param_1'], 
                                           params['sleep']['param_2'], 
                                           params['sleep']['param_3'], 
                                           params['sleep']['param_4'])       
            return cosine   
        # PLOT
        elif(params['sleep']['action'] == "plot"):
            self.plot(params['sleep']['param_1'], 
                            params['sleep']['param_2'])  
        
    def sleepActivities(self, sleep, start_date, end_date):
        """
        Takes and calculates the sleep activities for an interval of days.
        @type start_date date
        @param start_date the starting date of the time interval.
        @type end_date date
        @param end_date the ending date of the time interval.
        @rtype dataframe
        @return the dataframe containing for each day the data related the 
         sleeping activities.
        """
        # The result dataframe to be filled
        columns = ['date', 'start_datetime', 'end_datetime',
                   'count_micro_awakenings', 'time_micro_awakenings', 
                  'count_awakenings', 'time_awakenings']
        sleep_df = pd.DataFrame(columns=columns)       
        if (start_date != None) and (end_date != None) : #and \
        #    (average_start_datetime != None) and \
        #    (average_end_datetime != None):
            #sleep_data = \
            # self.client.sleepDataForIntervalOfDays(start_date, end_date)
            start_datetime = \
                dt.datetime(start_date.year,
                            start_date.month,
                            start_date.day,
                            Configuration.HH_DAY_SLEEP_SEPARATOR,
                            Configuration.MM_DAY_SLEEP_SEPARATOR,
                            Configuration.SS_DAY_SLEEP_SEPARATOR) \
                            - dt.timedelta(days=1)                              
            end_datetime = \
                dt.datetime(end_date.year,
                            end_date.month,
                            end_date.day,
                            Configuration.HH_DAY_SLEEP_SEPARATOR,
                            Configuration.MM_DAY_SLEEP_SEPARATOR,
                            Configuration.SS_DAY_SLEEP_SEPARATOR) 
            # Initializes variables to loop in the period of interest taking as
            # step a day
            partial_start_datetime = start_datetime
            partial_end_datetime = \
             partial_start_datetime + dt.timedelta(days=1)
            data_for_day = start_date
            # Loops in the period of interest taking as step a day
            while end_datetime >= partial_end_datetime:
                # The requested day of interest in the period
                cached_data = \
                 self.client.getCachedSleepData(data_for_day, data_for_day)
                if cached_data.empty :
                    db_partial_dataframe = \
                        self.client.getSleepEvents(partial_start_datetime, 
                                                   partial_end_datetime)
                    for event in db_partial_dataframe :
                        if event.getDatetime().year != dt.MINYEAR :
                            self.client.mapping.changeDeviceState(event)
                        feedback = sleep.computeSleepActivities(event)
                        if not feedback.empty :
                            sleep_df = sleep_df.append(feedback)
                            self.client.insertSleepDataToCache(feedback) 
                else :
                    sleep_df = sleep_df.append(cached_data)
                partial_start_datetime += dt.timedelta(days=1)
                partial_end_datetime += dt.timedelta(days=1)
                data_for_day += dt.timedelta(days=1)    
        return sleep_df       

    def sleepAverage(self, sleep_activities, daily=False):
        """
        Calculates the average giving an interval of days
        @type sleep_activities: Dataframe
        @param sleep_activities: the data to be analyzed.
        @type daily: Boolean
        @param daily: True to have, for each day, the summarize of sleep 
        activities. False, to have the summarize for the period.
        @rtype: Dataframe
        @return the dataframe containing a row with the averages.
        """
        columns = ['start_date', 'end_date', 'duration',
                   'count_micro_awakenings', 'time_micro_awakenings',
                   'count_awakenings', 'time_awakenings']
        average = pd.DataFrame(columns=columns)
        if not sleep_activities.empty:
            start_date = None
            end_date = None
            current_date = 0
    
            amount_durations = 0
            amount_count_micro_awakenings = 0
            amount_time_micro_awakenings = 0
            amount_count_awakenings = 0
            amount_time_awakenings = 0
            list_durations = []
            list_count_micro_awakenings = []
            list_time_micro_awakenings = []
            list_count_awakenings = []            
            list_time_awakenings = []
    
            # iterrows returns a couple (index, row) where index is the index
            #  of the dataframe and row is the content of the current row.
            # Even if index is not used, it is necessary to map it.
            for _, row in sleep_activities.iterrows():
                if current_date == 0:
                    current_date = row['date']
                    start_date = current_date
                    end_date = current_date
                    amount_durations = math.floor((row['end_datetime']
                                                   - row['start_datetime'])
                                                  .total_seconds())
                    amount_count_micro_awakenings = \
                     row['count_micro_awakenings']
                    amount_time_micro_awakenings = \
                     row['time_micro_awakenings']
                    amount_count_awakenings = row['count_awakenings']
                    amount_time_awakenings = row['time_awakenings']
                else:
                    if current_date == row['date']:
                        amount_durations += \
                         math.floor((row['end_datetime']
                                     - row['start_datetime'])
                                    .total_seconds())
                        amount_count_micro_awakenings += \
                         row['count_micro_awakenings']
                        amount_time_micro_awakenings += \
                         row['time_micro_awakenings']
                        amount_count_awakenings += row['count_awakenings']
                        amount_time_awakenings += row['time_awakenings']
                    else:
                        end_date = row['date']
                        if amount_durations != 0:
                            if(daily) :
                                average.loc[len(average.index)] = \
                                 np.array([current_date, current_date, 
                                           amount_durations, 
                                           amount_count_micro_awakenings,
                                           amount_time_micro_awakenings,
                                           amount_count_awakenings,
                                           amount_time_awakenings])
                            else :
                                list_durations.append(
                                    amount_durations)
                                list_count_micro_awakenings.append(
                                    amount_count_micro_awakenings)
                                list_time_micro_awakenings.append(
                                    amount_time_micro_awakenings)
                                list_count_awakenings.append(
                                    amount_count_awakenings)
                                list_time_awakenings.append(
                                    amount_time_awakenings)  
                            current_date = row['date']
                            amount_durations = \
                             math.floor((row['end_datetime']
                                         - row['start_datetime'])
                                        .total_seconds())
                            amount_count_micro_awakenings = \
                             row['count_micro_awakenings']
                            amount_time_micro_awakenings = \
                             row['time_micro_awakenings']
                            amount_count_awakenings = row['count_awakenings']
                            amount_time_awakenings = row['time_awakenings']
            if amount_durations != 0:
                if(daily) :
                    average.loc[len(average.index)] = \
                    np.array([current_date, current_date, amount_durations,
                              amount_count_micro_awakenings,
                              amount_time_micro_awakenings,
                              amount_count_awakenings, 
                              amount_time_awakenings])
                else :
                    list_durations.append(amount_durations)
                    list_count_micro_awakenings.append(
                        amount_count_micro_awakenings)
                    list_time_micro_awakenings.append(
                        amount_time_micro_awakenings)
                    list_count_awakenings.append(amount_count_awakenings)                    
                    list_time_awakenings.append(amount_time_awakenings)
                    
                    average_durations = 0
                    average_count_micro_awakenings = 0
                    average_time_micro_awakenings = 0
                    average_count_awakenings = 0
                    average_time_awakenings = 0
    
                    if list_durations:
                        average_durations = np.average(list_durations)
                    if list_count_micro_awakenings:
                        average_count_micro_awakenings = \
                         np.average(list_count_micro_awakenings)
                    if list_time_micro_awakenings:
                        average_time_micro_awakenings = \
                         np.average(list_time_micro_awakenings)
                    if list_count_awakenings:
                        average_count_awakenings = \
                         np.average(list_count_awakenings)
                    if list_time_awakenings:
                        average_time_awakenings = \
                         np.average(list_time_awakenings)    
                    average.loc[len(average.index)] = \
                        np.array([start_date, end_date, average_durations,
                                  average_count_micro_awakenings, 
                                  average_time_micro_awakenings,
                                  average_count_awakenings, 
                                  average_time_awakenings])
        return average
    
    def sleep_analysis(self, sleep_df, average_df):
        """
        Analyzes the sleep activities for each day of a certain interval and
        compares them with the average of a certain interval.
        @type sleep_df: Dataframe
        @param sleep_df: the dataframe containing the sleep activities to be 
            analyzed
        @type average_df: Dataframe
        @param average_df: the dataframe containing the average values
        @rtype Dataframe
        @return the dataframe containing the result of the comparisons
        """
        columns = ['date', 'what', 'detected', 'average']
        analysis = pd.DataFrame(columns=columns)
        if not average_df.empty:
            average_duration = float(average_df.loc[0]['duration'])
            average_count_micro_awakenings = \
             float(average_df.loc[0]['count_micro_awakenings'])
            average_time_micro_awakenings = \
             float(average_df.loc[0]['time_micro_awakenings'])
            average_count_awakenings = \
             float(average_df.loc[0]['count_awakenings'])
            average_time_awakenings = \
             float(average_df.loc[0]['time_awakenings'])
    
            # iterrows returns a couple (index, row) where index is the index of the
            # dataframe and row is the content of the current row.
            # Even if index is not used, it is necessary to map it.
            for _, row in sleep_df.iterrows() :
                if (row['end_datetime'] \
                    - row['start_datetime']).total_seconds() \
                    - average_duration \
                 > average_duration * Configuration.DIFFERENCE_DURATION :                
                    #print("Detected more abnormal sleep activity on " \
                    #      + row['date'] \
                    #      + ". Average: " + str(average_duration) \
                    #      + " detected: " \
                    #      + str((row['end_datetime'] \
                    #             - row['start_datetime']).total_seconds()) \
                    #      + " ( +" 
                    #      + str((row['end_datetime'] \
                    #             - row['start_datetime']).total_seconds() * 100 \
                    #            / average_duration - 100) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 'duration', 
                                  (row['end_datetime'] 
                                   - row['start_datetime']).total_seconds(), 
                                  average_duration])
                elif average_duration \
                 - (row['end_datetime'] \
                    - row['start_datetime']).total_seconds() \
                 > average_duration * Configuration.DIFFERENCE_DURATION :
                    ##print("Detected less abnormal sleep activity on " \
                    #      + row['date'] \
                    #      + ". Average: " + str(average_duration) \
                    #      + " detected: " \
                    #      +  str((row['end_datetime'] \
                    #            - row['start_datetime']).total_seconds()) \
                    #      )
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 'duration', 
                                  (row['end_datetime'] 
                                   - row['start_datetime']).total_seconds(),
                                  average_duration])
                if row['count_micro_awakenings'] \
                 - average_count_micro_awakenings \
                 > average_count_micro_awakenings \
                 * Configuration.DIFFERENCE_COUNT_MICRO_AWAKENING :
                 
                    #print("Detected more count micro awakenings activity on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_count_micro_awakenings) +
                    #      " detected: " + str(row['count_micro_awakenings']) +
                    #      " ( +" + str(row['count_micro_awakenings'] * 100 /
                    #                   average_count_micro_awakenings - 100) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 
                                  'count_micro_awakenings',
                                  row['count_micro_awakenings'], 
                                  average_count_micro_awakenings])
                elif average_count_micro_awakenings \
                 - row['count_micro_awakenings'] \
                 > average_count_micro_awakenings \
                 * Configuration.DIFFERENCE_COUNT_MICRO_AWAKENING :
                    #print("Detected less abnormal count micro awakenings on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_count_micro_awakenings) + " detected: "
                    #      + str(row['count_micro_awakenings']) +
                    #      " ( -" + str(100 - row['count_micro_awakenings'] *
                    #                   100 / average_count_micro_awakenings) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 
                                  'count_micro_awakenings',
                                  row['count_micro_awakenings'], 
                                  average_count_micro_awakenings])
                if row['time_micro_awakenings'] \
                    - average_time_micro_awakenings \
                 > average_time_micro_awakenings \
                    * Configuration.DIFFERENCE_TIME_MICRO_AWAKENING :
                    #print("Detected more time micro awakenings activity on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_time_micro_awakenings) +
                    #      " detected: " + str(row['time_micro_awakenings']) +
                    #      " ( +" + str(row['time_micro_awakenings'] * 100 /
                    #                   average_time_micro_awakenings - 100) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 
                                  'time_micro_awakenings',
                                  row['time_micro_awakenings'], 
                                  average_time_micro_awakenings])
                elif average_time_micro_awakenings \
                 - row['time_micro_awakenings'] \
                 > average_time_micro_awakenings \
                 * Configuration.DIFFERENCE_TIME_MICRO_AWAKENING :
                    #print("Detected less abnormal time micro awakenings on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_time_micro_awakenings) + " detected: "
                    #      + str(row['time_micro_awakenings']) +
                    #      " ( -" + str(100 - row['time_micro_awakenings'] *
                    #                   100 / average_time_micro_awakenings) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 
                                  'time_micro_awakenings',
                                  row['time_micro_awakenings'], 
                                  average_time_micro_awakenings])
                if row['count_awakenings'] - average_count_awakenings \
                    > average_count_awakenings \
                    * Configuration.DIFFERENCE_COUNT_AWAKENING :
                    #print("Detected more count awakenings activity on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_count_awakenings) +
                    #      " detected: " + str(row['count_awakenings']) +
                    #      " ( +" + str(row['count_awakenings'] * 100 /
                    #                   average_count_awakenings - 100) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'], 'count_awakenings', 
                                  row['count_awakenings'], 
                                  average_count_awakenings])
                elif average_count_awakenings - row['count_awakenings'] \
                 > average_count_awakenings \
                 * Configuration.DIFFERENCE_COUNT_AWAKENING :
                    #print("Detected less abnormal count awakenings on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_count_awakenings) 
                    # + " detected: " +
                    #      str(row['count_awakenings']) +
                    #      " ( -" + str(100 - row['count_awakenings'] *
                    #                   100 / average_count_awakenings) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'],
                                  'count_awakenings', 
                                  row['count_awakenings'],
                                  average_count_awakenings])
                if row['time_awakenings'] - average_time_awakenings \
                 > average_time_awakenings \
                 * Configuration.DIFFERENCE_TIME_AWAKENING :
                    #print("Detected more time awakenings activity on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_time_awakenings) +
                    #      " detected: " + str(row['time_awakenings']) +
                    #      " ( +" + str(row['time_awakenings'] * 100 /
                    #                   average_time_awakenings - 100) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'],
                                  'time_awakenings', row['time_awakenings'],
                                  average_time_awakenings])
                elif average_time_awakenings - row['time_awakenings'] \
                 > average_time_awakenings \
                 * Configuration.DIFFERENCE_TIME_AWAKENING:                
                    #print("Detected less abnormal count awakenings on " +
                    #      row['date'] +
                    #      ". Average: " + str(average_time_awakenings) + 
                    #      " detected: " +
                    #      str(row['time_awakenings']) +
                    #      " ( -" + str(100 - row['time_awakenings'] *
                    #                   100 / average_time_awakenings) + "%).")
                    analysis.loc[len(analysis.index)] = \
                        np.array([row['date'],
                                   'time_awakenings', row['time_awakenings'],
                                  average_time_awakenings])                    
        return analysis
    
    def cosine_for_days(self, day_1, day_2, param_1, param_2):
        """
        Calculates the cosine distance of the sleep activities between two 
        periods of time.
        Periods of time can be two days, two day by day period, two average of
        two intervals of time.
        @type day_1 date
        @param day_1 with param_1 and param_2 set to None, it indicates the
        first day to compare; with param_1 not None and param_2 set to None,
        it indicates the starting date of the first period to compare day by
        day; with param_1 not None and param_2 not None, it indicates the 
        starting day of the first interval of days to compare.
        @type day_2 date
        @param day_2 day_1 with param_1 and param_2 set to None, it indicates 
        the second day to compare; with param_1 not None and param_2 set to 
        None, it indicates the starting date of the second period to compare 
        day by day; with param_1 not None and param_2 not None, it indicates 
        the ending day of the first interval of days to compare. 
        @type param_1 date, number, None
        @param param_1 with param_2 set to None, it indicates the
        number of days to be compared day by day, starting from day_1 and 
        day_2; with param_2 not None, it indicates the 
        it indicates the starting date of the second period to compare. 
        @type param_2 date, None
        @param it indicates the the ending date of the second period to 
        compare.          
        """
        columns = ['start_date_1', 'end_date_1', \
                   'start_date_2', 'end_date_2', 'similarity']
        similarity = pd.DataFrame(columns=columns) 
        if day_1 != None and day_2 != None :
            if param_1 == None :
                param_1 = day_2
                param_2 = day_2
                day_2 = day_1
                loop_for_days = 1
            else :
                if param_2 == None :
                    loop_for_days = param_1
                    param_1 = day_2
                    param_2 =day_2
                    day_2 = day_1
                else :
                    loop_for_days = 1
            for i in range (0, loop_for_days) :
                array_average_1 = []
                array_average_2 = []   
                sleep_activities_1 = \
                 self.sleepActivities(self.sleep,
                  day_1 + dt.timedelta(days=i),
                  day_2 + dt.timedelta(days=i))
                average_1 = self.sleepAverage(sleep_activities_1)
                if not average_1.empty :
                    array_average_1 = \
                        np.array(average_1.loc[0, ['duration', 
                                                   'count_micro_awakenings',
                                                   'time_micro_awakenings',
                                                   'count_awakenings', 
                                                   'time_awakenings']])
                sleep_activities_2 = \
                 self.sleepActivities(self.sleep,
                  param_1 + dt.timedelta(days=i),
                  param_2 + dt.timedelta(days=i))
                average_2 = self.sleepAverage(sleep_activities_2)
                if not average_2.empty :
                    array_average_2 = \
                        np.array(average_2.loc[0, ['duration', 
                                                   'count_micro_awakenings',
                                                   'time_micro_awakenings',
                                                   'count_awakenings', 
                                                   'time_awakenings']])
                if len(array_average_1) != 0 and len(array_average_2) != 0 : 
                    similarity_tmp = \
                     self.cosine_similarity(array_average_1, array_average_2)
                    similarity.loc[len(similarity.index)] = \
                     np.array(
                        [(day_1 + dt.timedelta(days=i)),
                         (day_2 + dt.timedelta(days=i)),
                         (param_1 + dt.timedelta(days=i)),
                         (param_2 + dt.timedelta(days=i)),                         
                              similarity_tmp.loc[0]['similarity']])      
        return similarity
    
    def cosine_similarity(self, v1, v2):
        columns = ['similarity']
        similarity = pd.DataFrame(columns=columns)
        "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
        sumxx, sumxy, sumyy = 0, 0, 0
        for i in range(len(v1)):
            x = float(v1[i]); y = float(v2[i])
            sumxx += x*x
            sumyy += y*y
            sumxy += x*y
        similarity.loc[len(similarity.index)] = \
                    np.array([sumxy/math.sqrt(sumxx*sumyy)])  
        return similarity
    
    def plot(self, start_date, end_date) :
        """
        Takes and calculates the sleep activities for an interval of days.
        @type start_date date
        @param start_date the starting date of the time interval.
        @type end_date date
        @param end_date the ending date of the time interval.
        @rtype dataframe
        @return the dataframe containing for each day the data related the 
        sleeping activities.
        """

        start_datetime = dt.datetime.combine(start_date, dt.datetime.min.time())                              
        end_datetime = dt.datetime.combine(end_date, dt.datetime.max.time())  

        # Gets the interesting Events from data source.
        db_dataframe = \
            self.client.getSleepEvents(start_datetime, 
                                       end_datetime)
            
        #for event in db_dataframe :
        #    if event.getNameId() == DeviceStateName.PRESENCE :
        #        event.prettyPrint()
            
        # To store the devices that have generated the data to be displayed
        devices = {}
        for event in db_dataframe :
            # The MINYEAR event is to signal the start end end of interesting
            # data
            if event.getDatetime().year != dt.MINYEAR :
                # Fills the structure of the available devices.
                # Each device contains a dict of detected changed states with
                # a dataframe that contains pairs of datetime and value.
                if not event.getDeviceId() in devices :
                    devices[event.getDeviceId()] = {}
                if not event.getNameId() in devices[event.getDeviceId()] :
                    columns = ['datetime', 'value']
                    devices[event.getDeviceId()][event.getNameId()] = \
                     pd.DataFrame(columns=columns)
                devices[event.getDeviceId()][event.getNameId()] \
                 .loc[len(devices[event.getDeviceId()][event.getNameId()])] = \
                 [event.getDatetime(), event.getValueId()]
               
        time_range = pd.date_range(start_datetime, end_datetime, freq='min')
        columns=[]
    
        for device_id, states in devices.items() :
            for state_id, df_state in states.items() :
                columns.append(device_id + "-" + str(state_id))

        dest = pd.DataFrame(index=time_range, columns=columns)
        dest.index.name = 'datetime'
        dest = dest.fillna(0)

        for device_id, states in devices.items() :
            for state_id, df_state in states.items() :
                self.fill_dataframe(df_state, dest, device_id + "-" + str(state_id))
        self.createPlots(start_datetime, end_datetime, dest)
        columns = ['result']
        result = pd.DataFrame(columns=columns)
        result.append("Plots created")
        return result

    def fill_dataframe(self, source, dest, column_name):
        """
        Fills the dataframe (dest) that have to contain the read value of
        sensors, with data coming from the dataframe that contains the 
        information (source).
        The dest dataframe is created using:
          * as index: a time series that generates the measuring of time with a 
            frequencies of a minute;
          * as name of columns: the names of the data of interest.
        The source dataframe is iterated and it is read each row.
        Each row represents the change of the state of a sensor. In each row is
        indicated the timestamp of the event and the new value related the change 
        of state.
        From a point of view to plot data, the timestamp read in the current row 
        indicates:
          * the time until the value read in the previous row is valid;
          * the time from which the value read in the current row is valid.
        This means that to obtain a range of time of validity of a value, it is 
        mandatory to read two rows and the correct value for that range is in 
        the first one. 
        The procedure must be invoked for each sensor of interest.
        @param source the source of the data.
        @param dest the destination of the data.
        @param column_name the name of the column where data have to be stored.
        """
        current_value = None
        start_datetime = None
        # Loop each row in the source dataframe
        #print(source)
        for _, row in source.iterrows():
            current_datetime = row['datetime']
            readed_value = row['value']
            if (readed_value == DeviceStateValue.INACTIVE) \
             or (readed_value == DeviceStateValue.CLOSED) \
             or (readed_value == DeviceStateValue.NOT_PRESENT) \
             or (readed_value == DeviceStateValue.NOT_IDENTIFIED) \
             or (readed_value == DeviceStateValue.DISCONNECTED) \
             or (readed_value == DeviceStateValue.LOW) :
                event_value = 0
            else :
                event_value = 1
 
            # First line of the source
            if current_value == None :
                current_datetime = current_datetime.replace(second=0, microsecond=0)
                # Supposes that values can be 0 or 1. 
                # If a value is changed, it is supposed that before the value was
                # not the current value read
                if event_value == 0 :
                    current_value = 1
                else :
                    current_value = 0
                dest.loc[:current_datetime, column_name] = current_value
                print("Modifying " + column_name + " to " + 
                    current_datetime.strftime('%Y-%m-%d %H:%M:%S') + 
                      " with value " + str(current_value))
                # Takes the value read in the row for the next iteration            
                current_value = readed_value
                # 
                start_datetime = current_datetime
            else :
                current_datetime = current_datetime.replace(second=0, microsecond=0)
                dest.loc[start_datetime:current_datetime,column_name] = current_value
                print("Modifying " + column_name + " from " + 
                          start_datetime.strftime('%Y-%m-%d %H:%M:%S') + 
                          " to " + current_datetime.strftime('%Y-%m-%d %H:%M:%S') + 
                          " with value " + str(current_value))
                current_value = event_value
                start_datetime = current_datetime
        # If no more rows are available, it can be supposed that last read value
        # is valid until the rest of the period of interest        
        if (current_value != None) & (start_datetime != None) :
            dest.loc[start_datetime:,column_name] = current_value
            print("Modifying " + column_name + " from " + 
                start_datetime.strftime('%Y-%m-%d %H:%M:%S') +  
                " with value " + str(current_value))
                
                        
    def createPlots(self, start_date, end_date, result_df)  :
        # Creates a different plot for each hour of activity
        time_ranges = pd.date_range(start_date, end_date, freq='H')
    
        i = 0
        while(i < time_ranges.size - 1) :
            if i == 0 :
                start = time_ranges[i]
                i = i + 1;
                end = time_ranges[i]
            else :
                start = end
                i = i + 1
                end = time_ranges[i]
            print("Creating dataframe from " + 
              start.strftime('%Y-%m-%d %H:%M:%S') + " to " + 
              end.strftime('%Y-%m-%d %H:%M:%S'))
            fig = result_df[start:end].plot.bar(subplots=True, figsize=(20, 20))
            #plt.show()
            plt.savefig("../data_plots/devices_" + 
                    start.strftime('%Y-%m-%d_%H:%M:%S') + ".png")
            # Free memory for the current plot
            plt.close()
            gc.collect()                         
                    
                    
                            