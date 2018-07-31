#!/usr/bin/python3

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
Defines the introduction points to use the application.
"""

import os
from flask import Flask, jsonify, request

app = Flask(__name__, static_folder=os.path.join('/home/vcap/app/static'), static_url_path='')

import datetime as dt

from client import C2KRestClient
from mapping import C2KEventMapping
from analitics import Analitics

class InputParamsException(Exception) :
    def __init__(self, message) :
        self.message = message
    
    def getMessage(self) :
        return self.message
    
def fixInputParams(argv) :
    """
    Verifies input parameters and adjusts them if needed.
    @type argv: list
    @param argv: the parameters passed with the invoke of the service.
    @rtype array
    @return the adjusted parameters. 
    @raise InputParamsException when params are not correctly set.
    """
    # Initializes result structure
    params = {
        'sleep': {
            'user_id': None,
            'action': None,
            'param_1': None,
            'param_2': None,
            'param_3': None,
            'param_4': None
            }
    }
    
    try :
        params['sleep']['user_id'] = argv[0]
        params['sleep']['action'] = argv[1]
        params['sleep']['param_1'] = argv[2]
        params['sleep']['param_2'] = argv[3]
        params['sleep']['param_4'] = argv[4]
        params['sleep']['param_5'] = argv[5]
    except IndexError :
        pass
                                    
    if params['sleep']['param_1'] != None and \
     params['sleep']['param_1'] != "None" :
        try :
            params['sleep']['param_1'] = \
             dt.datetime.strptime(params['sleep']['param_1'], 
                                  '%Y-%m-%d')
        except ValueError :
            raise InputParamsException('{"ERROR": ' 
                    + params['sleep']['param_1'] + ' is not a valid date. ' 
                    + 'The date must be in the following format: '
                    + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
    if params['sleep']['param_2'] != None \
     and params['sleep']['param_2'] != "None" :
        try :
            params['sleep']['param_2'] =  \
             dt.datetime.strptime(params['sleep']['param_2'], 
                                  '%Y-%m-%d')
        except ValueError :
            raise InputParamsException('{"ERROR": ' 
                    + params['sleep']['param_2'] + ' is not a valid date. ' 
                    + 'The date must be in the following format: '
                    + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
    if params['sleep']['action'] == 'analyze' : 
        if params['sleep']['param_3'] != None:
            try :
                params['sleep']['param_3'] =  \
                 dt.datetime.strptime(params['sleep']['param_3'], 
                                      '%Y-%m-%d')
            except ValueError :
                raise InputParamsException('{"ERROR": ' 
                    + params['sleep']['param_3'] + ' is not a valid date. ' 
                    + 'The date must be in the following format: '
                    + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
        if params['sleep']['param_4'] != None:
            try :
                params['sleep']['param_4'] =  \
                 dt.datetime.strptime(params['sleep']['param_4'], 
                                      '%Y-%m-%d')
            except ValueError :
                raise InputParamsException('{"ERROR": ' 
                    + params['sleep']['param_1'] + ' is not a valid date. ' 
                    + 'The date must be in the following format: '
                    + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
    if params['sleep']['action'] == 'cosine' :
        if params['sleep']['param_3'] == None \
         or params['sleep']['param_3'] == "None" :
            params['sleep']['param_3'] = None
            params['sleep']['param_4'] = None
        else :
            if params['sleep']['param_4'] == None \
             or params['sleep']['param_4'] == "None" :
                params['sleep']['param_4'] = None 
                try :
                    params['sleep']['param_3'] = \
                     int(params['sleep']['param_3'])
                except ValueError :
                    raise InputParamsException('{"ERROR": ' 
                        + params['sleep']['param_3'] + ' is not an integer. ' 
                        + 'The date must be in the following format: '
                        + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
            else :
                try :
                    params['sleep']['param_3'] =  \
                     dt.datetime.strptime(params['sleep']['param_3'], 
                                          '%Y-%m-%d')
                except ValueError :
                    raise InputParamsException('{"ERROR": ' 
                        + params['sleep']['param_3'] + ' is not a valid date. ' 
                        + 'The date must be in the following format: '
                        + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
                try :
                    params['sleep']['param_4'] =  \
                     dt.datetime.strptime(params['sleep']['param_4'], 
                                          '%Y-%m-%d')
                except ValueError :
                    raise InputParamsException('{"ERROR": ' 
                        + params['sleep']['param_4'] + ' is not a valid date. ' 
                        + 'The date must be in the following format: '
                        + 'YYYY-mm-dd (e.g. 2017-07-29)."}')
    return params

def main(argv):
    """
    Main function.
    It takes argv as input and launches the analysis.
    @type argv: array
    @param argv CLI parameters.
    @rtype string
    @return the result of the requested analytic or an error message.
    """
    try :
        params = fixInputParams(argv)
    except InputParamsException as e:
        print(e.getMessage())
    else :
        mapping = C2KEventMapping()
        client = C2KRestClient(params, mapping)
        analitics = Analitics(client)
        return(analitics.execute(params) \
            .to_json(None, 'records', 'iso', 12, True, 's'))     

@app.route('/')
def Welcome():
    """ 
    Returns a page with the instructions.
    """
    #return app.send_static_file('../static/index.html')
    #root_dir = os.path.dirname(os.getcwd()) 
    #print("%s" % os.path.join(root_dir, 'app', 'static'))
    #print("MAINDIR: %s" % os.listdir(os.path.join(root_dir, 'app', 'static')))
    return app.send_static_file('index.html')
    
@app.route('/help')
def Help():
    """
    Help page.
    """
    return Welcome()

@app.route('/static/<path:path>')
def catch_all(path):
    return app.send_static_file(path)

@app.route('/<user_id>/<action>')
def action_no_params(user_id, action):
    argv = [user_id, action, None, None, None, None]
    return main(argv)

@app.route('/<user_id>/<action>/<param_1>')
def action_one_param(user_id, action, param_1):
    argv = [user_id, action, param_1, None, None, None]
    return main(argv)

@app.route('/<user_id>/<action>/<param_1>/<param_2>')
def action_two_params(user_id, action, param_1, param_2):
    argv = [user_id, action, param_1, param_2, None, None]
    return main(argv)


@app.route('/<user_id>/<action>/<param_1>/<param_2>/<param_3>')
def action_three_params(user_id, action, param_1, param_2, param_3):
    argv = [user_id, action, param_1, param_2, param_3, None]
    return main(argv)

@app.route('/<user_id>/<action>/<param_1>/<param_2>/<param_3>')
def action_four_params(user_id, action, param_1, param_2, param_3, param_4):
    argv = [user_id, action, param_1, param_2, param_3, param_4]
    return main(argv)

@app.route('/post', methods=['POST'])
def post():
    users = request.values.get('users')
    user_ids = users.split(',')
    yesterday = dt.datetime.now().date()
    yesterday -= dt.timedelta(days=1)
    for user_id in user_ids :
        argv = [user_id, 'activity', yesterday.strftime("%Y-%m-%d"),
                yesterday.strftime("%Y-%m-%d"), None, None]
        main(argv)
    return '{"RESULT": "OK"}'
        
    
port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port), debug=True)
