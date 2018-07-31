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

class Configuration :
    
    # To analyze sleep, it should be considered that for the day x, the sleep
    # activity starts the day x-1 after a certain time. The sleep activity can
    # be repeated many times during the day (e.g. the night, after lunch and 
    # so on). For this reason, during the same day, it should be choice a time
    # that separates the sleep activities for the day x and for the day x+1.
    HH_DAY_SLEEP_SEPARATOR = 19
    MM_DAY_SLEEP_SEPARATOR = 00
    SS_DAY_SLEEP_SEPARATOR = 00
    
    # Sets the time interval in seconds to recognize sleep phases
    # Mini awakening are when the user, for example, change his position
    TIME_INTERVAL_FOR_MICRO_AWAKENING = 60 * 1
    # Awakening is when the user stand-ups
    TIME_INTERVAL_FOR_AWAKENING = 60 * 45
    # The minimum required time to consider a rest time
    MIN_TIME_INTERVAL_FOR_SLEEPING = 60 * 60 * 1.5
    
    # Differences expressed as value [0..1] to signal an anomaly in sleep
    # activities
    DIFFERENCE_DURATION = 0.1
    DIFFERENCE_COUNT_MICRO_AWAKENING = 0.1
    DIFFERENCE_TIME_MICRO_AWAKENING = 0.1
    DIFFERENCE_COUNT_AWAKENING = 0.1
    DIFFERENCE_TIME_AWAKENING = 0.1
    