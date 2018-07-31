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

from enum import Enum 

"""
The representation of a smart device. In general a device can be a sensor, 
an actuator or both of them.
In particular, there are defined the device, how states are described and a
set of enumeration to standardize terms. 
"""

class Device:
    """
    Describes a device. 

    A state of a device is a particular condition that it can assume
    (e.g. open/close, present/ not present, and so on). 
    A device is installed inside a room and it is supposed that 
    its position can't change over time except for the wearable ones.
    """
    
    def __init__(self, device_id, device_type, states, room_id=None, 
                 pretty_name=None):
        """
        Initializes the class with some values.
        
        @type device_id: int
        @param device_id: it represents the instance of the device.
        @type device_type: Enum Types 
            (e.g. 'DRAWER', 'BED', 'FRIDGE', 'WC', 'ARMCHAIR', "MUSA")
        @param device_type: the type of the sensor. 
        @type states: dict
        @param states contains the current states
        @type room_id: int
        @param room_id: the id of the room where the sensor is located.
        @type pretty_name: string
        @param pretty_name a mnemonic name for the sensor.        
        @rtype: Device
        @return: the instance of the object.
        """
        
        self.id = device_id
        self.type = device_type
        self.room_id = room_id
        self.states = {}
        self.pretty_name = pretty_name
        for i in states :
            # To take trace of the last states before the new event, 
            # for each state is created a list.
            self.states[i] = [DeviceState(None, i, None)] 
            
    def setState(self, event):
        """
        Sets a new state for the device, according to an occurred event.
        It stores the data for the new event and keep trace of the previous 
        ones. 
        @type event: Event
        @param event: the event that makes the device to change its state.
        """
        if event.name_id in self.states :
            self.states[event.name_id].append(
                DeviceState(event.datetime, event.name_id, event.value_id))
            #print("[%s]: %s changed the state %s with %s" 
            #      % (self.states[event.name_id].datetime,
            #         self.type,
            #         self.states[event.name_id].name,
            #         self.states[event.name_id].value))
            if len(self.states[event.name_id]) > 2 :
                # Let's take only the last and the previous values
                self.states[event.name_id].pop(0)
        else :
            print("%s not supported by %s." % (event.name_id, self.type))
    
    def getCurrentState(self, name_id) :
        """
        Gets the last received state for the name.
        @type name_id: String
        @param name_id: the name of the state.
        @rtype DeviceState
        @return the state for the named state at size - offset position. 
        """
        return self.getState(name_id, 0)

    def getPreviousState(self, name_id) :
        """
        Gets the second to last received state for the named name.
        @type name_id: String
        @param name_id: the name of the state.
        @rtype DeviceState
        @return the state for the named state at size - offset position. 
        """
        return self.getState(name_id, 1)
    
    def getState(self, name_id, offset) :
        """
        Gets the last offset named state that of the device.
        @type name_id: String
        @param name_id: the name of the state.
        @type offset: int
        @param offset: the position starting from the last element.
        @rtype DeviceState
        @return the state for the named state at size - offset position, 
                None if the index is not found.   
        """
        if name_id in self.states :
            try :
                return self.states[name_id][len(self.states[name_id]) - offset - 1]
            except IndexError :
                return None
        else :
            print("%s not supported by %s." % (name_id, self.type))

class DeviceState():
    """
    Represents a state that a device can assume. It is used as item for the
    states dictionary in the Device class.
    """
    def __init__(self, datetime, name, value):
        """
        Build the DeviceState instance.
        @type datetime: datetime
        @param datetime: the datetime referred to the change of the current 
        state.
        @type name: string
        @param name: the name of the state
        @type value: string
        @param value: the current value for the named state.
        @rtype: DeviceState
        @return the instance of the representation of a current state owned 
        by a device.
        """
        self.datetime = datetime
        self.name = name
        self.value = value
        
    def getDatetime(self) :
        """
        Gets the datetime of the state.
        @rtype datetime
        @return the datetime of the state.
        """
        return self.datetime
    
    def getName(self) :
        """
        Gets the name of the state.
        @rtype String
        @return the name of the state.
        """        
        return self.name
    
    def getValue(self) :
        """
        Gets the value of the state.
        @rtype String
        @return the value of the state.
        """        
        return self.value

class Room:
    """
    Describes a room that can contain devices.
    The room may have other adjacent rooms.
    """
    def __init__(self, name, room_list=None):
        """
        Initializes the class with the adjacent rooms.
        @type name: String
        @param name the pretty name and mnemonic name of the room.
        @type room_list: array
        @param room_list a list containing the references of the adjacent rooms.
        Don't insert this value for an empty list. Rooms can be added later.
        @rtype Room
        @return the instance of the Room
        """
        
        self.name = name
        
        if room_list is None:
            self.adjacent_rooms = []
        else:
            self.adjacent_rooms = room_list

    def addAdjacent(self, room):
        """
        Adds a room as adjacent to the current instance.
        @type room: int
        @param room: the id of the room to add.
         """
        self.adjacent_rooms.append(room)

class Event:
    """
    Abstract the event correlated to the change of the state of a device 
    inside the environment.
    """
    datetime = None
    device_id = None
    name_id = None
    value_id = None

    def __init__(self, datetime, device_id, name_id, value_id) :
        """
        Creates an instance of an event.
        @type datetime: Datetime
        @param datetime the datetime of the event.
        @type device_id: String 
        @param device_id: the id of the involved device.
        @type name_id: DeviceStateName.
        @param name_id: the name of the involved state.
        @type value_id String / DeviceStateValue
        @param value_id: it can be a DeviceStateValue if applicable 
        (e.g. OPEN, CLOSE) otherwise it can be an arbitrary string 
        (e.g. the value of a temperature)
        @rtype Event
        @return the instance of the event.
        """    
        self.datetime = datetime
        self.device_id = device_id
        self.name_id = name_id
        self.value_id = value_id

    def getDatetime(self) :
        """
        Gets the datetime of the event.
        @rtype datetime
        @return the datetime of the state.
        """
        return self.datetime
    
    def getDeviceId(self) :
        """
        Gets the device identification of the device that generated the event.
        @rtype String
        @return the device identification of the state.
        """        
        return self.device_id
    
    def getNameId(self) :
        """
        Gets the name of the event.
        @rtype String
        @return the name of the event.
        """        
        return self.name_id
    
    def getValueId(self) :
        """
        Gets the value of the event.
        @rtype String
        @return the value of the event.
        """        
        return self.value_id
        
    def prettyPrint(self):
        """
        Outputs the event for debugging.
        """
        print("[%s] %s %s %s" % (self.datetime.strftime('%Y-%m-%d %H:%M:%S'), 
                              self.device_id, self.name_id, self.value_id))
        
class DeviceStateName(Enum) :
    """
    An enumeration of the supported states that a device can assume.
    """ 
    ACTIVE = 1
    OPEN = 2
    PRESENCE = 3
    TEMPERATURE = 4
    HUMIDITY = 5
    USER_ID = 6
    ACTIVATION = 7
    FALL_ALARM = 8
    EMERGENCY_CALL = 9
    CONNECTION = 10
    BATTERY_LEVEL = 11
    BATTERY_ALARM = 12
    HEARTBIT_RATE = 13
    MODE = 14

class DeviceStateValue(Enum) :
    """
    An enumeration of the supported value that a state can assume.
    """
    ACTIVE = 1
    INACTIVE = 2 
    CLOSED = 3
    OPEN = 4
    PRESENT = 5 
    NOT_PRESENT = 6
    NOT_IDENTIFIED = 7
    CONNECTED = 8
    DISCONNECTED = 9
    OK = 10
    LOW = 11
    NORMAL = 12
    INSTALLATION = 13

class DeviceType(Enum) :
    """
    An enumeration of type of device.
    """
    UNKNOWN = 0        
    DRAWER = 1
    BED = 2
    FRIDGE = 3
    WC = 4
    ARMCHAIR = 5
    MUSA = 6
    DOOR = 7
    PIR = 8
    
    
    
    