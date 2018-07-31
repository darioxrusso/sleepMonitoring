#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
import sys
import requests
import datetime as dt

def main(dict):
    print(dt.datetime.now())
    total_now = dt.datetime.now()
    BASE_REST_URL = "https://sleep.mybluemix.net"
    yesterday = dt.datetime.now().date() - dt.timedelta(days=1)
    users = ['60EE22C27013613E980']
    for user in users :
        try :
            partial_now = dt.datetime.now()
            headers = {'Content-type': 'application/json'}
            response = \
            requests.get(BASE_REST_URL + "/" + user + "/" + "activity" + "/" 
                          + yesterday.strftime("%Y-%m-%d") + "/" 
                          + yesterday.strftime("%Y-%m-%d"), headers=headers)
            print(response.text)
            print("* Partial elapsed time for user " + user + " date: " 
                  + yesterday.strftime("%Y-%m-%d") + ": " 
                + str((dt.datetime.now() - partial_now).total_seconds()) 
                + " seconds.")
            # loaded_json = json.loads(response.text)['RESULTS']
            # r = requests.post("https://sleep.mybluemix.net/post", 
            #                    data={'users': dict['patients'] })
        except Exception as e:
            print("Detected exception for user " + user + " for date " 
                  + yesterday.strftime("%Y-%m-%d") + ":")
            print(e)
    print("*** Total elapsed time for users in date: " 
              + yesterday.strftime("%Y-%m-%d") + ": " 
              + str((dt.datetime.now() - total_now).total_seconds()) 
              + " seconds.")
    print("-----------------------------------------------------------------")    
    return {"RESULT": "OK" }

#main({'patients': '60EE22C27013613E980'})
