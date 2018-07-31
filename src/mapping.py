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
from device import Device
from device import DeviceType
from device import DeviceStateName
from device import DeviceStateValue
from device import Room
from device import Event
#from configuration import Configuration

"""
Maps the real world technologies in the abstraction layer provided in the 
device.
"""

class DeviceMapping :
    """
    Represents the infrastructure to map a device with its possible
    states.
    """
    def __init__(self, device_id, device_type, states, room, pretty_name=None):
        """
        Initializes the instance with parameters.
        @type device_id: String or a number
        @param device_id: the identifier of the device.
        @type device_type:  device.DeviceType enum
        @param device_type: the type of the device.
        @type states: list of list
        @param states: the states implemented by the device.
        @type room: device.Room
        @param room: the place where the device is installed.
        @type pretty_name: String
        @param pretty_name: a pretty name for the device.
        """
        self.id = device_id;
        self.type = device_type
        self.room = room
        self.states = states 
        self.pretty_name = pretty_name
       
class StateMapping :
    """
    Represents the infrastructure to map a device state and its possible 
    values.
    """
    def __init__(self, name, map_values) :
        """
        Creates an instance.
        @type name: DeviceStateName
        @param name: the name of the state of the device.
        @type map_values: dict
        @param map_values: a map containing the possibile values and 
        translations from the technology values and the abstracted ones.
        @rtype StateMapping
        @return the instance. 
        """
        self.name = name
        self.values = map_values

class EventMapping :
    """
    Finds the correlation between devices, states and values referred by the 
    technology with the devices, states and values used in the abstraction
    """
    def mapEvents(self, data_df) :
        """
        Maps a set of device events.
        @type data_df dataframe
        @param data_df contains the events to be mapped.
        @rtype dataframe
        @return the mapped events.
        """
        events = []
        for _, row in data_df.iterrows() :
            events.append(self.mapEvent(row))
        return events

    def changeDeviceState(self, event):
        """
        Changes the state of a device according to an event.
        @type event: Event
        @param event: the event that causes the change of state
        """
        #config = Configuration()
        if event.device_id in self.devices.keys() :
            device = self.devices.get(event.device_id)
            device.setState(event)
        else:
            print("ERROR: %s" % event.device_id)
            
    def createDevices(self) :
        """
        From the mappedDevice, it creates the devices.
        """  
        self.devices = {}
        for mappedDevice in self.device_map.values() :
            state_names = []
            for state in mappedDevice.states.values() :
                state_names.append(state.name)
            self.devices[mappedDevice.id] = \
                Device(mappedDevice.id, mappedDevice.type, 
                state_names, mappedDevice.room, mappedDevice.pretty_name)          
          
    #def pretty_print_to_translate(data, device_map):
    #    """
    #    Outputs the data coming from the technology, in a human readable way.
    #    """
    #    raise NotImplementedError
    
    def searchDeviceInRoom(self, device_type, device_room) :
        """
        Gets a device by type and room.
        @type device_type:  device.DeviceType enum
        @param device_type: the type of the device.
        @type room: device.Room
        @param room: the place where the device is installed.
        @rtype List of device.Device
        @return the list of devices, empty list otherwise.
        """
        found = []
        for _,device in self.devices.items() :
            if device.type == device_type and device.room_id.name == device_room :
                found.append(device)
        return found

    def searchDevice(self, device_type) :
        """
        Gets the devices by type.
        @type device_type:  device.DeviceType enum
        @param device_type: the type of the device.
        @rtype List of device.Device
        @return the list of devices, empty list otherwise.
        """
        found = []
        for _,device in self.devices.items() :
            if device.type == device_type :
                found.append(device)
        return found

class MappingException(Exception) :
    """
    Defines an exception that can be raised during the mapping actions.
    """
    def __init__(self, message) :
        self.message = message
    
    def getMessage(self) :
        return self.message

class C2KEventMapping(EventMapping) :
    """ 
    Implements the EventMapping class for C2K server.
    """

    device_map = {}
        
    def mapAndAddDeviceMap(self, row) :
        device_functions = {}
        device_room = Room("None")

        if row['sensorTypeId'] == 1 :
            device_type = DeviceType.PIR
            device_functions = { "DATA": StateMapping(DeviceStateName.PRESENCE, 
                                    { '0': DeviceStateValue.NOT_PRESENT, 
                                      '1' : DeviceStateValue.PRESENT 
                                    }),
                                 "BATTERY" : StateMapping(DeviceStateName.BATTERY_LEVEL, 
                                    {  
                                    })  
                                }            
        elif row['sensorTypeId'] == 2 :
            device_type = DeviceType.WC
            device_functions = { "DATA": StateMapping(DeviceStateName.PRESENCE, 
                                    { '0': DeviceStateValue.NOT_PRESENT, 
                                      '1' : DeviceStateValue.PRESENT
                                    }),
                                 "BATTERY" : StateMapping(DeviceStateName.BATTERY_LEVEL, 
                                    {  
                                    })  
                                }
        elif row['sensorTypeId'] == 3 :
            device_type = DeviceType.ARMCHAIR
            device_functions = { "DATA": StateMapping(DeviceStateName.OPEN, 
                                    { '0': DeviceStateValue.CLOSED, 
                                      '1' : DeviceStateValue.OPEN 
                                    }),
                                 "BATTERY" : StateMapping(DeviceStateName.BATTERY_LEVEL, 
                                    {  
                                    })  
                                }                         
        elif row['sensorTypeId'] == 4 :   
            device_type = DeviceType.BED
            device_functions = { "DATA": StateMapping(DeviceStateName.PRESENCE, 
                                    { '0': DeviceStateValue.NOT_PRESENT, 
                                      '1' : DeviceStateValue.PRESENT 
                                    }),
                                 "BATTERY" : StateMapping(DeviceStateName.BATTERY_LEVEL, 
                                    {  
                                    })  
                                }
        elif row['sensorTypeId'] == 5 :
            device_type = DeviceType.DOOR
            device_functions = { "DATA": StateMapping(DeviceStateName.OPEN, 
                                    { '0': DeviceStateValue.CLOSED, 
                                      '1' : DeviceStateValue.OPEN 
                                    }),
                                 "BATTERY" : StateMapping(DeviceStateName.BATTERY_LEVEL, 
                                    {  
                                    }) 
                                }
        else :
            device_type = DeviceType.UNKNOWN
            device_functions = {}

           
        #if row['roomId'] == "1" :
        #    device_room = Room("Bedroom")
        #elif row['roomId'] == "3" :
        #    device_room = Room("Living room")
        #else :
        device_room= Room("Unknown")
        
        self.device_map[row['id']] = DeviceMapping(row['id'], device_type,
                        device_functions, device_room, row['name'])
           
    def mapEvent(self, data) :
        """
        Translates in event the incoming technology data.
        @type data row of a data-frame
        @param data the data coming from the technology.
        @rtype: Event
        @return the translated event corresponding to the data.
        """
        mapped_service = None
        mapped_value = None
        mapped_state = None
        # Maps data
        datetime = data['compact_timestamp']
        device_id = data['sensor']['id']
        service_id = data['measurementTypeDesc']
        value_id = data['value']
        if device_id in self.device_map :
            mapped_device = self.device_map[device_id]
            if service_id in mapped_device.states :
                mapped_state = mapped_device.states[service_id]
                mapped_service = mapped_state.name
                if value_id in mapped_state.values :
                    mapped_value = mapped_state.values[value_id]
                else :
                    mapped_value = value_id
            else :
                raise MappingException('{"ERROR": "' + service_id 
                                       + ' is not supported by ' 
                                       + mapped_device.id + ".")
        else :
            raise MappingException('{"ERROR": "' + device_id 
                                       + ' is not supported."')
        return Event(datetime, mapped_device.id, mapped_service, mapped_value)
            
